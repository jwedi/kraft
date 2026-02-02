use std::sync::Arc;
use std::sync::atomic::Ordering;
use log::error;
use tokio::sync::oneshot;
use tracing::{Level, Span};

use crate::persistence::worker::PersistenceTaskType;
use crate::raft::raft_sm::{
    LocalAppendEntries, LocalAppendEntriesCallbackResponse, LocalRaftResponseMessage,
    LocalRaftResponsePayload, OutstandingMessage, OutstandingMessageType,
    RaftMessageStateChange, SharedState,
};
use crate::service_utils::storage_utils::deserialize_data;

use super::election::ElectionTimer;

/// Validates that append entries request is consecutive with current log state.
pub fn validate_append_entries(
    request: &LocalAppendEntries,
    last_log_index: u64,
    last_log_term: u64,
) -> bool {
    request.prev_index == last_log_index && request.prev_term == last_log_term
}

/// Handles recognition of a new leader with a higher term.
pub fn handle_new_leader(
    request: &LocalAppendEntries,
    callback: oneshot::Sender<LocalRaftResponseMessage>,
    shared_state: &mut SharedState,
) {
    log::info!(
        "recognising leader for new term: {}, leader id: {}",
        request.term,
        request.leader_id
    );
    shared_state.server_state.current_term = request.term;
    shared_state.server_state.leader_id = request.leader_id;

    // Update Raft state metrics for new leader
    crate::transport::metrics::update_raft_state_metrics(
        shared_state.volatile_server_state.commit_index,
        shared_state.server_state.leader_id,
        shared_state.server_state.current_term,
        shared_state.volatile_server_state.replication_log.len(),
    );
    shared_state.volatile_server_state.next_term = request.term + 1;

    if let Some(entry) = &request.entry {
        error!(
            "Received append_entries request with entry when recognizing new leader, \
             but follower should not have entries. Leader: {}, term: {}, index: {}",
            request.leader_id, request.term, entry.index
        );
    }

    callback
        .send(LocalRaftResponseMessage {
            payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok),
        })
        .expect("sending append_entries response callback failed");
}

/// Handles a heartbeat request (request_id == 0).
/// Caller should verify request_id == 0 before calling this function.
pub fn handle_heartbeat(
    request: &LocalAppendEntries,
    callback: oneshot::Sender<LocalRaftResponseMessage>,
    shared_state: &mut SharedState,
    election_timer: &mut ElectionTimer,
) {
    // Reset election timeout on heartbeat
    election_timer.reset();

    let last_log_term = shared_state.volatile_server_state.last_log_term;
    let last_log_index = shared_state.volatile_server_state.last_log_index;

    if request.prev_term != last_log_term || request.prev_index != last_log_index {
        log::info!(
            "non-consecutive heartbeat request: prev term {}, req prev index: {}, \
             state: last log term {}, state: last log index {}, requestId: {}, will request backfill",
            request.prev_term,
            request.prev_index,
            last_log_term,
            last_log_index,
            request.request_id
        );
        callback
            .send(LocalRaftResponseMessage {
                payload: LocalRaftResponsePayload::AppendEntries(
                    LocalAppendEntriesCallbackResponse::WantedPreviousEntry {
                        last_term: last_log_term,
                        last_index: last_log_index,
                    },
                ),
            })
            .expect("sending append_entries response callback failed");
    } else {
        // Update commit index if needed
        if request.commit_index > shared_state.volatile_server_state.commit_index
            || shared_state.server_state.current_term != request.term
        {
            update_commit_index(shared_state, request.commit_index);
            log::info!("Updated commit index to {}", request.commit_index);

            // Update metrics for commit index change
            crate::transport::metrics::update_raft_state_metrics(
                shared_state.volatile_server_state.commit_index,
                shared_state.server_state.leader_id,
                shared_state.server_state.current_term,
                shared_state.volatile_server_state.replication_log.len(),
            );
        }
        log::debug!(
            "consecutive heartbeat req: term {}, index: {}, state: term {}, index {}",
            request.prev_term,
            request.prev_index,
            last_log_term,
            last_log_index
        );
        callback
            .send(LocalRaftResponseMessage {
                payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok {}),
            })
            .expect("sending append_entries response callback failed");
    }
}

/// Handles a non-consecutive append entries request by requesting backfill.
pub fn handle_non_consecutive_request(
    request: &LocalAppendEntries,
    callback: oneshot::Sender<LocalRaftResponseMessage>,
    shared_state: &SharedState,
    election_timer: &mut ElectionTimer,
) {
    let last_log_term = shared_state.volatile_server_state.last_log_term;
    let last_log_index = shared_state.volatile_server_state.last_log_index;

    log::info!(
        "non-consecutive append log req: req prev term {}, req prev index: {}, \
         state: last log term {}, state: last log index {}, requestId: {}, dataLen: {}",
        request.prev_term,
        request.prev_index,
        last_log_term,
        last_log_index,
        request.request_id,
        request.entry.as_ref().map(|e| e.data.len()).unwrap_or(0)
    );

    tracing::debug!("reset election timeout for term: {}", request.term);
    election_timer.reset();

    callback
        .send(LocalRaftResponseMessage {
            payload: LocalRaftResponsePayload::AppendEntries(
                LocalAppendEntriesCallbackResponse::WantedPreviousEntry {
                    last_term: last_log_term,
                    last_index: last_log_index,
                },
            ),
        })
        .expect("sending append_entries response callback failed");
}

/// Applies a log entry from the append entries request.
pub fn apply_log_entry(
    request: LocalAppendEntries,
    callback: oneshot::Sender<LocalRaftResponseMessage>,
    shared_state: &mut SharedState,
) {
    let message_id = shared_state.next_message_id;
    shared_state.next_message_id += 1;

    let data = if let Some(entry) = request.entry {
        shared_state.volatile_server_state.last_log_index = entry.index;
        shared_state.volatile_server_state.last_log_term = entry.term;
        if entry.prev_log_term != entry.term {
            // First entry for new term
            shared_state
                .volatile_server_state
                .replication_log_term_starts
                .insert(entry.term, shared_state.volatile_server_state.replication_log.len() as u64);
        }
        log::debug!("updating last log term {} and index {}", entry.term, entry.index);
        Arc::new(entry.data)
    } else {
        Arc::new(vec![])
    };

    if data.is_empty() {
        log::error!(
            "Received empty append_entries data request from leader: {}, term: {}, index: {}",
            request.leader_id,
            request.term,
            request.prev_index
        );
    }

    let persistence_task = PersistenceTaskType::AppendLog {
        id: message_id,
        data: Arc::clone(&data),
        parent_span: Span::current(),
        request_id: request.request_id,
    };
    shared_state.persistence_work.push(persistence_task);

    match deserialize_data(&data, 0) {
        Ok((_, deserialized)) => {
            let serialization_data = Arc::new(deserialized);
            shared_state
                .volatile_server_state
                .replication_log
                .push(Arc::clone(&serialization_data));
            // Note: Bus broadcast is deferred until commit_index advances (committed entries only)
        }
        Err(e) => {
            log::error!(
                "Failed to deserialize append_entries data from leader: {}, term: {}, index: {}, error: {}",
                request.leader_id,
                request.term,
                request.prev_index,
                e
            );
        }
    }

    if request.commit_index > shared_state.volatile_server_state.commit_index {
        update_commit_index(shared_state, request.commit_index);
    }

    // Update metrics after processing new entry and commit index
    crate::transport::metrics::update_raft_state_metrics(
        shared_state.volatile_server_state.commit_index,
        shared_state.server_state.leader_id,
        shared_state.server_state.current_term,
        shared_state.volatile_server_state.replication_log.len(),
    );

    let message_type = OutstandingMessageType::AppendLog { callback };
    let outstanding_message = OutstandingMessage {
        id: message_id,
        outstanding_responses: 1,
        message_type,
        span: Span::current(),
    };
    shared_state.outstanding_messages.insert(message_id, outstanding_message);
}

/// Sends an unrecognized leader response.
pub fn send_unrecognized_leader_response(
    request: &LocalAppendEntries,
    callback: oneshot::Sender<LocalRaftResponseMessage>,
    shared_state: &SharedState,
) {
    log::info!(
        "received append_entries request from non leader with lower term than current. \
         caller: {}, current leader: {}, term: {}, current term: {}",
        request.leader_id,
        shared_state.server_state.leader_id,
        request.term,
        shared_state.server_state.current_term
    );
    callback
        .send(LocalRaftResponseMessage {
            payload: LocalRaftResponsePayload::AppendEntries(
                LocalAppendEntriesCallbackResponse::UnrecognizedLeader,
            ),
        })
        .expect("sending append_entries response callback failed");
}

/// Updates the commit index and broadcasts committed entries.
fn update_commit_index(shared_state: &mut SharedState, new_commit_index: u64) {
    shared_state.volatile_server_state.commit_index = new_commit_index;
    shared_state
        .volatile_server_state
        .commit_state
        .commit_index
        .store(new_commit_index, Ordering::Release);
    shared_state
        .volatile_server_state
        .commit_state
        .version
        .fetch_add(1, Ordering::Release);

    // Broadcast newly committed entries to query workers
    shared_state.broadcast_committed_entries(new_commit_index);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::raft_sm::{
        CommitState, RaftServerState, RaftVolatileState, SharedState, StateMachineConfig,
    };
    use crate::transport::raft::raftproto::RemoteLogEntry;
    use bus::Bus;
    use crossbeam_queue::SegQueue;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use tokio::sync::oneshot;

    fn create_test_shared_state() -> SharedState {
        SharedState {
            server_state: RaftServerState {
                current_term: 1,
                leader_id: 1,
            },
            volatile_server_state: RaftVolatileState {
                next_term: 2,
                last_log_term: 1,
                last_log_index: 5,
                commit_index: 3,
                replication_log: vec![],
                replication_log_term_starts: HashMap::new(),
                last_applied: 0,
                next_log_index: 6,
                commit_state: Arc::new(CommitState {
                    commit_index: AtomicU64::new(3),
                    version: AtomicU64::new(0),
                }),
                last_broadcast_index: None,
            },
            next_message_id: 1,
            outstanding_messages: HashMap::new(),
            quorum_size: 3,
            quorum_worker_tasks: vec![],
            persistence_work: Arc::new(SegQueue::new()),
            persistence_response: Arc::new(SegQueue::new()),
            quorum_work: Arc::new(SegQueue::new()),
            quorum_response: Arc::new(SegQueue::new()),
            identity: 0,
            term_votes: HashMap::new(),
            state_machine_config: StateMachineConfig {
                max_message_size_bytes: 2048,
            },
            log_entry_bus: Bus::new(5000),
        }
    }

    #[test]
    fn test_validate_append_entries_returns_true_when_consecutive() {
        let request = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 1,
            prev_index: 5,
            commit_index: 3,
            request_id: 1,
            entry: None,
        };

        assert!(validate_append_entries(&request, 5, 1));
    }

    #[test]
    fn test_validate_append_entries_returns_false_when_index_mismatch() {
        let request = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 1,
            prev_index: 10, // Mismatch: expected 5
            commit_index: 3,
            request_id: 1,
            entry: None,
        };

        assert!(!validate_append_entries(&request, 5, 1));
    }

    #[test]
    fn test_validate_append_entries_returns_false_when_term_mismatch() {
        let request = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 2, // Mismatch: expected 1
            prev_index: 5,
            commit_index: 3,
            request_id: 1,
            entry: None,
        };

        assert!(!validate_append_entries(&request, 5, 1));
    }

    #[test]
    fn test_validate_append_entries_returns_false_when_both_mismatch() {
        let request = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 2,
            prev_index: 10,
            commit_index: 3,
            request_id: 1,
            entry: None,
        };

        assert!(!validate_append_entries(&request, 5, 1));
    }

    #[tokio::test]
    async fn test_handle_new_leader_updates_state() {
        let mut shared_state = create_test_shared_state();
        let (tx, rx) = oneshot::channel();

        let request = LocalAppendEntries {
            term: 5,
            leader_id: 3,
            prev_term: 1,
            prev_index: 5,
            commit_index: 3,
            request_id: 1,
            entry: None,
        };

        handle_new_leader(&request, tx, &mut shared_state);

        // Verify state was updated
        assert_eq!(shared_state.server_state.current_term, 5);
        assert_eq!(shared_state.server_state.leader_id, 3);
        assert_eq!(shared_state.volatile_server_state.next_term, 6);

        // Verify callback was sent
        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok) => {}
            _ => panic!("Expected Ok response"),
        }
    }

    #[tokio::test]
    async fn test_handle_heartbeat_consecutive_updates_commit_index() {
        let mut shared_state = create_test_shared_state();
        let mut election_timer = super::super::election::ElectionTimer::new();
        let (tx, rx) = oneshot::channel();

        let request = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 1,
            prev_index: 5, // Matches state
            commit_index: 10,
            request_id: 0, // Heartbeat
            entry: None,
        };

        handle_heartbeat(&request, tx, &mut shared_state, &mut election_timer);

        // Verify commit index was updated
        assert_eq!(shared_state.volatile_server_state.commit_index, 10);

        // Verify callback was sent with Ok
        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok {}) => {}
            _ => panic!("Expected Ok response"),
        }
    }

    #[tokio::test]
    async fn test_handle_heartbeat_non_consecutive_requests_backfill() {
        let mut shared_state = create_test_shared_state();
        let mut election_timer = super::super::election::ElectionTimer::new();
        let (tx, rx) = oneshot::channel();

        let request = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 1,
            prev_index: 100, // Does not match state (5)
            commit_index: 10,
            request_id: 0,
            entry: None,
        };

        handle_heartbeat(&request, tx, &mut shared_state, &mut election_timer);

        // Verify callback was sent with WantedPreviousEntry
        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(
                LocalAppendEntriesCallbackResponse::WantedPreviousEntry {
                    last_term,
                    last_index,
                },
            ) => {
                assert_eq!(last_term, 1);
                assert_eq!(last_index, 5);
            }
            _ => panic!("Expected WantedPreviousEntry response"),
        }
    }

    #[tokio::test]
    async fn test_handle_non_consecutive_request_sends_wanted_previous_entry() {
        let shared_state = create_test_shared_state();
        let mut election_timer = super::super::election::ElectionTimer::new();
        let (tx, rx) = oneshot::channel();

        let request = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 2,
            prev_index: 10,
            commit_index: 3,
            request_id: 5,
            entry: Some(RemoteLogEntry {
                index: 11,
                term: 1,
                data: vec![1, 2, 3],
                batch_index: 0,
                message_id: 1,
                prev_log_index: 10,
                prev_log_term: 2,
            }),
        };

        handle_non_consecutive_request(&request, tx, &shared_state, &mut election_timer);

        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(
                LocalAppendEntriesCallbackResponse::WantedPreviousEntry {
                    last_term,
                    last_index,
                },
            ) => {
                assert_eq!(last_term, 1);
                assert_eq!(last_index, 5);
            }
            _ => panic!("Expected WantedPreviousEntry response"),
        }
    }

    #[tokio::test]
    async fn test_send_unrecognized_leader_response() {
        let shared_state = create_test_shared_state();
        let (tx, rx) = oneshot::channel();

        let request = LocalAppendEntries {
            term: 1,
            leader_id: 99, // Not the current leader
            prev_term: 1,
            prev_index: 5,
            commit_index: 3,
            request_id: 1,
            entry: None,
        };

        send_unrecognized_leader_response(&request, tx, &shared_state);

        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(
                LocalAppendEntriesCallbackResponse::UnrecognizedLeader,
            ) => {}
            _ => panic!("Expected UnrecognizedLeader response"),
        }
    }
}
