//! Persistence recovery for votes and replication log.

use std::collections::HashMap;
use std::sync::Arc;

use crate::persistence::worker::VoteRow;
use crate::service_utils::app_time::now_millis;
use crate::transport::capnp::OwnedLogEntry;

/// Recovered data from persistence storage.
pub struct RecoveredData {
    /// Deserialized replication log entries.
    pub replication_log: Vec<Arc<OwnedLogEntry>>,
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
    /// Byte end offsets for each log entry in the persisted file.
    /// `log_entry_end_offsets[i]` is the byte position right after entry `i`.
    pub log_entry_end_offsets: Vec<u64>,
}

/// Deserializes the replication log from raw bytes (Cap'n Proto format).
///
/// Returns a tuple of (log entries, term start indices, last index, last term, entry end offsets).
pub fn deserialize_replication_log(
    log_bytes: &[u8],
) -> (Vec<Arc<OwnedLogEntry>>, HashMap<u64, u64>, u64, u64, Vec<u64>) {
    let start_time = now_millis();

    if log_bytes.is_empty() {
        log::warn!("Replication log is empty, starting with an empty log");
        return (vec![], HashMap::new(), 0, 0, vec![]);
    }

    log::info!("Recovered replication log with {} bytes", log_bytes.len());

    // Parse concatenated Cap'n Proto messages
    let mut replication_log = Vec::new();
    let mut entry_end_offsets = Vec::new();
    let mut offset = 0;

    while offset < log_bytes.len() {
        let mut slice = &log_bytes[offset..];
        match capnp::serialize::read_message_from_flat_slice(&mut slice, Default::default()) {
            Ok(_) => {
                let bytes_consumed = log_bytes.len() - offset - slice.len();
                let entry_bytes = log_bytes[offset..offset + bytes_consumed].to_vec();
                match OwnedLogEntry::from_bytes(entry_bytes) {
                    Ok(entry) => {
                        replication_log.push(Arc::new(entry));
                        offset += bytes_consumed;
                        entry_end_offsets.push(offset as u64);
                    }
                    Err(e) => {
                        log::warn!("Failed to parse log entry at offset {}: {:?}", offset, e);
                        offset += bytes_consumed;
                    }
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to read message at offset {}: {:?}, stopping recovery",
                    offset,
                    e
                );
                break;
            }
        }
    }

    // Build term start index map
    let mut term_start_index: HashMap<u64, u64> = HashMap::new();
    let mut prev_term: u64 = 0;
    for (i, entry) in replication_log.iter().enumerate() {
        let entry_term = entry.term();
        if entry_term != prev_term {
            term_start_index.insert(entry_term, i as u64);
            prev_term = entry_term;
        }
    }

    let last_entry = replication_log.last();
    let last_log_index = last_entry.map_or(0, |e| e.index());
    let last_log_term = last_entry.map_or(0, |e| e.term());

    let elapsed = now_millis() - start_time;
    log::info!(
        "Recovered replication log with {} entries, last index: {}, last term: {}. Deserialization took {} ms",
        replication_log.len(),
        last_log_index,
        last_log_term,
        elapsed
    );

    (
        replication_log,
        term_start_index,
        last_log_index,
        last_log_term,
        entry_end_offsets,
    )
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

    let default_vote = VoteRow {
        term: 0,
        candidate_id: 0,
    };
    let max_vote = votes.iter().max_by(|x, y| x.term.cmp(&y.term));
    let highest_term_vote = max_vote.unwrap_or(&default_vote);

    let next_term = std::cmp::max(highest_term_vote.term, last_log_term) + 1;

    let term_votes: HashMap<u64, u32> = votes.into_iter().map(|v| (v.term, v.candidate_id)).collect();

    (term_votes, next_term)
}

/// Recovers all persisted data (log and votes) and returns a RecoveredData struct.
pub fn recover_persisted_data(log_bytes: Vec<u8>, votes: Vec<VoteRow>) -> RecoveredData {
    let (replication_log, term_start_index, last_log_index, last_log_term, log_entry_end_offsets) =
        deserialize_replication_log(&log_bytes);

    let (term_votes, next_term) = process_votes(votes, last_log_term);

    RecoveredData {
        replication_log,
        term_start_index,
        last_log_index,
        last_log_term,
        term_votes,
        next_term,
        log_entry_end_offsets,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_empty_log() {
        let (log, term_starts, last_idx, last_term, end_offsets) = deserialize_replication_log(&[]);

        assert!(log.is_empty());
        assert!(term_starts.is_empty());
        assert_eq!(last_idx, 0);
        assert_eq!(last_term, 0);
        assert!(end_offsets.is_empty());
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
            VoteRow {
                term: 1,
                candidate_id: 2,
            },
            VoteRow {
                term: 3,
                candidate_id: 1,
            },
            VoteRow {
                term: 2,
                candidate_id: 3,
            },
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
        let votes = vec![VoteRow {
            term: 1,
            candidate_id: 2,
        }];

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
        assert!(data.log_entry_end_offsets.is_empty());
    }
}
