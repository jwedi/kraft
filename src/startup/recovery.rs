//! Persistence recovery for votes and replication log.

use std::collections::HashMap;
use std::sync::Arc;

use crate::persistence::worker::VoteRow;
use crate::service_utils::app_time::now_millis;
use crate::service_utils::storage_utils::{self, SerializationData};

/// Recovered data from persistence storage.
pub struct RecoveredData {
    /// Deserialized replication log entries.
    pub replication_log: Vec<Arc<SerializationData>>,
    /// Map of term to the index where that term starts in the log.
    pub term_start_index: HashMap<u64, u64>,
    /// The index of the last log entry.
    pub last_log_index: u64,
    /// The term of the last log entry.
    pub last_log_term: u64,
    /// Map of term to the candidate voted for in that term.
    pub term_votes: HashMap<u64, u32>,
    /// The next term to use (max of vote terms and log terms, plus one).
    pub next_term: u64,
}

/// Deserializes the replication log from raw bytes.
///
/// Returns a tuple of (log entries, term start indices, last index, last term).
pub fn deserialize_replication_log(
    log_bytes: &[u8],
) -> (Vec<Arc<SerializationData>>, HashMap<u64, u64>, u64, u64) {
    let start_time = now_millis();

    if log_bytes.is_empty() {
        log::warn!("Replication log is empty, starting with an empty log");
        return (vec![], HashMap::new(), 0, 0);
    }

    log::info!("Recovered replication log with {} bytes", log_bytes.len());

    let replication_log = storage_utils::deserialize_all_data_as_arc(log_bytes).unwrap_or_else(|e| {
        log::warn!("Failed to deserialize replication log: {:?}, starting with empty log", e);
        vec![]
    });

    // Build term start index map
    let mut term_start_index: HashMap<u64, u64> = HashMap::new();
    for (i, entry) in replication_log.iter().enumerate() {
        if entry.index == 0 {
            term_start_index.insert(entry.term, i as u64);
        }
    }

    let last_entry = replication_log.last();
    let last_log_index = last_entry.map_or(0, |e| e.index);
    let last_log_term = last_entry.map_or(0, |e| e.term);

    let elapsed = now_millis() - start_time;
    log::info!(
        "Recovered replication log with {} entries, last index: {}, last term: {}. Deserialization took {} ms",
        replication_log.len(),
        last_log_index,
        last_log_term,
        elapsed
    );

    (replication_log, term_start_index, last_log_index, last_log_term)
}

/// Processes recovered votes and computes the next term.
///
/// Returns a tuple of (term_votes map, next_term).
pub fn process_votes(votes: Vec<VoteRow>, last_log_term: u64) -> (HashMap<u64, u32>, u64) {
    log::info!(
        "Recovered {} votes from disk, last one for {:?}",
        votes.len(),
        votes.last()
    );

    let default_vote = VoteRow { term: 0, candidate_id: 0 };
    let max_vote = votes.iter().max_by(|x, y| x.term.cmp(&y.term));
    let highest_term_vote = max_vote.unwrap_or(&default_vote);

    let next_term = std::cmp::max(highest_term_vote.term, last_log_term) + 1;

    let term_votes: HashMap<u64, u32> = votes
        .into_iter()
        .map(|v| (v.term, v.candidate_id))
        .collect();

    (term_votes, next_term)
}

/// Recovers all persisted data (log and votes) and returns a RecoveredData struct.
pub fn recover_persisted_data(
    log_bytes: Vec<u8>,
    votes: Vec<VoteRow>,
) -> RecoveredData {
    let (replication_log, term_start_index, last_log_index, last_log_term) =
        deserialize_replication_log(&log_bytes);

    let (term_votes, next_term) = process_votes(votes, last_log_term);

    RecoveredData {
        replication_log,
        term_start_index,
        last_log_index,
        last_log_term,
        term_votes,
        next_term,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_empty_log() {
        let (log, term_starts, last_idx, last_term) = deserialize_replication_log(&[]);

        assert!(log.is_empty());
        assert!(term_starts.is_empty());
        assert_eq!(last_idx, 0);
        assert_eq!(last_term, 0);
    }

    #[test]
    fn test_process_votes_empty() {
        let (term_votes, next_term) = process_votes(vec![], 0);

        assert!(term_votes.is_empty());
        assert_eq!(next_term, 1);
    }

    #[test]
    fn test_process_votes_with_data() {
        let votes = vec![
            VoteRow { term: 1, candidate_id: 2 },
            VoteRow { term: 3, candidate_id: 1 },
            VoteRow { term: 2, candidate_id: 3 },
        ];

        let (term_votes, next_term) = process_votes(votes, 2);

        assert_eq!(term_votes.len(), 3);
        assert_eq!(term_votes.get(&1), Some(&2));
        assert_eq!(term_votes.get(&3), Some(&1));
        // next_term = max(3, 2) + 1 = 4
        assert_eq!(next_term, 4);
    }

    #[test]
    fn test_process_votes_log_term_higher() {
        let votes = vec![VoteRow { term: 1, candidate_id: 2 }];

        let (_, next_term) = process_votes(votes, 5);

        // next_term = max(1, 5) + 1 = 6
        assert_eq!(next_term, 6);
    }

    #[test]
    fn test_recover_persisted_data_empty() {
        let data = recover_persisted_data(vec![], vec![]);

        assert!(data.replication_log.is_empty());
        assert!(data.term_start_index.is_empty());
        assert_eq!(data.last_log_index, 0);
        assert_eq!(data.last_log_term, 0);
        assert!(data.term_votes.is_empty());
        assert_eq!(data.next_term, 1);
    }
}
