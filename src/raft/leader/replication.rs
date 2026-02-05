use log::info;
use std::sync::Arc;
use tracing::Span;

use crate::quorum::types::{LocalQuorumWorkerTask, LocalQuorumWorkerTaskType};
use crate::raft::raft_sm::SharedState;

/// Clones a subset of Arc elements from a slice.
pub fn clone_subset<T>(data: &[Arc<T>], start: usize, end: usize) -> Vec<Arc<T>> {
    data[start..end].iter().map(Arc::clone).collect()
}

/// Calculates the quorum worker index for a given node ID.
pub fn get_quorum_worker_index(quorum_node_id: u64, identity: u32) -> usize {
    if quorum_node_id == 0 {
        0
    } else if quorum_node_id > identity as u64 {
        (quorum_node_id.saturating_sub(2)) as usize
    } else {
        (quorum_node_id.saturating_sub(1)) as usize
    }
}

/// Sends a truncate signal to a quorum worker.
pub fn send_truncate_signal(
    shared_state: &mut SharedState,
    quorum_node_id: u64,
    prev_term: u64,
    last_index_for_term: u64,
) {
    log::info!(
        "Sending truncate signal to member {} for term {} index {}",
        quorum_node_id,
        prev_term,
        last_index_for_term
    );

    let quorum_node_index = get_quorum_worker_index(quorum_node_id, shared_state.identity);

    if let Some(queue) = shared_state.quorum_worker_tasks.get(quorum_node_index) {
        let message_id = shared_state.next_message_id;
        shared_state.next_message_id += 1;

        let task = LocalQuorumWorkerTaskType::TruncateLog {
            id: message_id,
            prev_term,
            prev_index: last_index_for_term,
            parent_span: Span::current(),
        };
        queue.push(LocalQuorumWorkerTask { task_type: task });
    }
}

/// Handles a backfill log request from a follower.
/// Returns true if handled (either sent backfill data or truncate signal).
pub fn handle_backfill_request(
    shared_state: &mut SharedState,
    quorum_node_id: u64,
    last_log_term: u64,
    last_log_index: u64,
) {
    log::info!(
        "Received backfill log request for member {}, last_log_term: {}, last_log_index: {} (absolute), head of log: term: {}, index: {}",
        quorum_node_id,
        last_log_term,
        last_log_index,
        shared_state.server_state.current_term,
        shared_state.volatile_server_state.last_log_index
    );

    let log_len = shared_state.volatile_server_state.replication_log.len() as u64;

    // Check if the follower's prev_index is beyond our log
    if last_log_index >= log_len {
        handle_follower_ahead(shared_state, quorum_node_id, log_len);
        return;
    }

    // Verify the term matches at the requested index
    if let Some(entry_at_index) = shared_state
        .volatile_server_state
        .replication_log
        .get(last_log_index as usize)
    {
        if entry_at_index.term() != last_log_term && last_log_term != 0 {
            // Terms start from 1, log term of 0 means empty
            handle_term_mismatch(shared_state, quorum_node_id, last_log_term, last_log_index);
            return;
        }
    }

    // Send backfill data
    send_backfill_data(shared_state, quorum_node_id, last_log_term, last_log_index);
}

/// Handles the case where follower has entries beyond our log.
fn handle_follower_ahead(shared_state: &mut SharedState, quorum_node_id: u64, log_len: u64) {
    let our_last_index = if log_len > 0 { log_len - 1 } else { 0 };
    let our_last_term = shared_state
        .volatile_server_state
        .replication_log
        .last()
        .map(|entry| entry.term())
        .unwrap_or(0);

    log::info!(
        "Leader doesn't have entry at index {}, sending truncate to index {}",
        log_len,
        our_last_index
    );
    send_truncate_signal(shared_state, quorum_node_id, our_last_term, our_last_index);
}

/// Handles a term mismatch between leader and follower logs.
///
/// Walks backwards through the log to find the last entry with a matching term,
/// then sends a truncate signal to the follower. This implements the Raft log
/// consistency check - if terms don't match, we need to find where they diverged.
fn handle_term_mismatch(shared_state: &mut SharedState, quorum_node_id: u64, last_log_term: u64, last_log_index: u64) {
    let mut truncate_to = last_log_index;
    let mut found_match = false;

    while truncate_to > 0 {
        truncate_to -= 1;
        if let Some(entry) = shared_state
            .volatile_server_state
            .replication_log
            .get(truncate_to as usize)
        {
            if entry.term() == last_log_term {
                // Found entry with matching term
                log::info!(
                    "Term mismatch at index {}, truncating follower to index {} term {}",
                    last_log_index,
                    truncate_to,
                    entry.term()
                );
                send_truncate_signal(shared_state, quorum_node_id, entry.term(), truncate_to);
                found_match = true;
                break;
            }
        }
    }

    if !found_match {
        // No matching term found, truncate everything
        info!("No matching term found between leader and follower, sending truncate everything signal");
        send_truncate_signal(shared_state, quorum_node_id, 0, 0);
    }
}

/// Sends backfill data to a follower.
fn send_backfill_data(shared_state: &mut SharedState, quorum_node_id: u64, last_log_term: u64, last_log_index: u64) {
    // log term 0 means empty replication log, so start from 0.
    let start_index = if last_log_term == 0 { 0 } else { last_log_index + 1 };

    let slice = shared_state.volatile_server_state.replication_log.as_slice();
    log::info!("Backfilling from index: {} to {}", start_index, slice.len());
    let data_slice = clone_subset(slice, start_index as usize, slice.len());

    let quorum_node_index = get_quorum_worker_index(quorum_node_id, shared_state.identity);

    if let Some(queue) = shared_state.quorum_worker_tasks.get(quorum_node_index) {
        queue.push(LocalQuorumWorkerTask {
            task_type: LocalQuorumWorkerTaskType::BackfillLog { data: data_slice },
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::raft_sm::{CommitState, RaftServerState, RaftVolatileState, SharedState, StateMachineConfig};
    use crate::transport::capnp::{build_owned_log_entry, OwnedLogEntry};
    use bus::Bus;
    use crossbeam_channel::unbounded;
    use crossbeam_queue::SegQueue;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;

    fn create_test_shared_state_with_log(log_entries: Vec<Arc<OwnedLogEntry>>) -> SharedState {
        let quorum_tasks: Vec<Arc<SegQueue<LocalQuorumWorkerTask>>> =
            vec![Arc::new(SegQueue::new()), Arc::new(SegQueue::new())];

        let last_log_index = if log_entries.is_empty() {
            0
        } else {
            log_entries.len() as u64 - 1
        };
        let last_log_term = log_entries.last().map(|e| e.term()).unwrap_or(0);
        let (persistence_tx, _persistence_rx) = unbounded();

        SharedState {
            server_state: RaftServerState {
                current_term: 2,
                leader_id: 1,
            },
            volatile_server_state: RaftVolatileState {
                next_term: 3,
                last_log_term,
                last_log_index,
                commit_index: 0,
                replication_log: log_entries,
                replication_log_term_starts: HashMap::new(),
                last_applied: 0,
                next_log_index: last_log_index + 1,
                commit_state: Arc::new(CommitState {
                    commit_index: AtomicU64::new(0),
                    version: AtomicU64::new(0),
                }),
                last_broadcast_index: None,
                has_deferred_broadcast: false,
            },
            next_message_id: 1,
            outstanding_messages: HashMap::new(),
            quorum_size: 2,
            quorum_worker_tasks: quorum_tasks,
            persistence_work: persistence_tx,
            persistence_response: Arc::new(SegQueue::new()),
            quorum_work: Arc::new(SegQueue::new()),
            quorum_response: Arc::new(SegQueue::new()),
            identity: 1,
            term_votes: HashMap::new(),
            state_machine_config: StateMachineConfig {
                max_message_size_bytes: 2048,
            },
            log_entry_bus: Bus::new(5000),
        }
    }

    fn create_log_entry(index: u64, term: u64) -> Arc<OwnedLogEntry> {
        let prev_index = if index > 0 { index - 1 } else { 0 };
        Arc::new(build_owned_log_entry(|mut builder| {
            builder.set_index(index);
            builder.set_term(term);
            builder.set_prev_log_index(prev_index);
            builder.set_prev_log_term(term);
            builder.set_message_id(index);
            builder.set_timestamp(0);
        }))
    }

    #[test]
    fn test_clone_subset_full_range() {
        let data: Vec<Arc<u32>> = vec![Arc::new(1), Arc::new(2), Arc::new(3), Arc::new(4)];
        let result = clone_subset(&data, 0, 4);

        assert_eq!(result.len(), 4);
        assert_eq!(*result[0], 1);
        assert_eq!(*result[3], 4);
    }

    #[test]
    fn test_clone_subset_partial_range() {
        let data: Vec<Arc<u32>> = vec![Arc::new(1), Arc::new(2), Arc::new(3), Arc::new(4)];
        let result = clone_subset(&data, 1, 3);

        assert_eq!(result.len(), 2);
        assert_eq!(*result[0], 2);
        assert_eq!(*result[1], 3);
    }

    #[test]
    fn test_clone_subset_empty_range() {
        let data: Vec<Arc<u32>> = vec![Arc::new(1), Arc::new(2), Arc::new(3)];
        let result = clone_subset(&data, 2, 2);

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_get_quorum_worker_index_zero_node() {
        assert_eq!(get_quorum_worker_index(0, 1), 0);
    }

    #[test]
    fn test_get_quorum_worker_index_node_greater_than_identity() {
        // Node 3, Identity 1 -> index should be 3 - 2 = 1
        assert_eq!(get_quorum_worker_index(3, 1), 1);

        // Node 2, Identity 1 -> index should be 2 - 2 = 0
        assert_eq!(get_quorum_worker_index(2, 1), 0);
    }

    #[test]
    fn test_get_quorum_worker_index_node_less_than_identity() {
        // Node 1, Identity 2 -> index should be 1 - 1 = 0
        assert_eq!(get_quorum_worker_index(1, 2), 0);

        // Node 1, Identity 3 -> index should be 1 - 1 = 0
        assert_eq!(get_quorum_worker_index(1, 3), 0);
    }

    #[test]
    fn test_send_truncate_signal_queues_task() {
        let log_entries = vec![create_log_entry(0, 1), create_log_entry(1, 1), create_log_entry(2, 2)];
        let mut shared_state = create_test_shared_state_with_log(log_entries);

        // Node 2 with identity 1 -> quorum worker index 0
        send_truncate_signal(&mut shared_state, 2, 1, 1);

        // Check that a truncate task was queued
        let task = shared_state.quorum_worker_tasks[0].pop();
        assert!(task.is_some());

        match task.unwrap().task_type {
            LocalQuorumWorkerTaskType::TruncateLog {
                prev_term, prev_index, ..
            } => {
                assert_eq!(prev_term, 1);
                assert_eq!(prev_index, 1);
            }
            _ => panic!("Expected TruncateLog task"),
        }
    }

    #[test]
    fn test_handle_backfill_request_sends_backfill_data() {
        let log_entries = vec![
            create_log_entry(0, 1),
            create_log_entry(1, 1),
            create_log_entry(2, 2),
            create_log_entry(3, 2),
        ];
        let mut shared_state = create_test_shared_state_with_log(log_entries);

        // Request backfill from index 1 (should send entries 2, 3)
        handle_backfill_request(&mut shared_state, 2, 1, 1);

        // Check that a backfill task was queued
        let task = shared_state.quorum_worker_tasks[0].pop();
        assert!(task.is_some());

        match task.unwrap().task_type {
            LocalQuorumWorkerTaskType::BackfillLog { data } => {
                assert_eq!(data.len(), 2);
                assert_eq!(data[0].index(), 2);
                assert_eq!(data[1].index(), 3);
            }
            _ => panic!("Expected BackfillLog task"),
        }
    }

    #[test]
    fn test_handle_backfill_request_empty_log_sends_truncate() {
        let log_entries = vec![create_log_entry(0, 1), create_log_entry(1, 1)];
        let mut shared_state = create_test_shared_state_with_log(log_entries);

        // Request with index beyond log - should trigger truncate
        handle_backfill_request(&mut shared_state, 2, 1, 10);

        let task = shared_state.quorum_worker_tasks[0].pop();
        assert!(task.is_some());

        match task.unwrap().task_type {
            LocalQuorumWorkerTaskType::TruncateLog { prev_index, .. } => {
                assert_eq!(prev_index, 1); // Our last index
            }
            _ => panic!("Expected TruncateLog task"),
        }
    }

    #[test]
    fn test_handle_backfill_request_term_mismatch_sends_truncate() {
        let log_entries = vec![create_log_entry(0, 1), create_log_entry(1, 1), create_log_entry(2, 2)];
        let mut shared_state = create_test_shared_state_with_log(log_entries);

        // Request with wrong term at index 2 (entry has term 2, request says term 3)
        handle_backfill_request(&mut shared_state, 2, 3, 2);

        let task = shared_state.quorum_worker_tasks[0].pop();
        assert!(task.is_some());

        match task.unwrap().task_type {
            LocalQuorumWorkerTaskType::TruncateLog { .. } => {
                // Expected truncate due to term mismatch
            }
            _ => panic!("Expected TruncateLog task due to term mismatch"),
        }
    }

    #[test]
    fn test_handle_backfill_request_from_empty_follower() {
        let log_entries = vec![create_log_entry(0, 1), create_log_entry(1, 1), create_log_entry(2, 2)];
        let mut shared_state = create_test_shared_state_with_log(log_entries);

        // Follower has empty log (term 0)
        handle_backfill_request(&mut shared_state, 2, 0, 0);

        let task = shared_state.quorum_worker_tasks[0].pop();
        assert!(task.is_some());

        match task.unwrap().task_type {
            LocalQuorumWorkerTaskType::BackfillLog { data } => {
                // Should send all entries
                assert_eq!(data.len(), 3);
            }
            _ => panic!("Expected BackfillLog task"),
        }
    }
}
