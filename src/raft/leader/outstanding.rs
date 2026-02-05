use std::sync::atomic::Ordering;
use tracing::Level;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::persistence::worker::PersistenceResponseType;
use crate::quorum::types::{LocalQuorumTaskResponseType, LocalQuorumWorkerTask, LocalQuorumWorkerTaskType};
use crate::raft::raft_sm::{
    LocalRaftResponseMessage, LocalRaftResponsePayload, LocalRaftWriteBatchResponse, OutstandingMessageType,
    SharedState,
};
use crate::transport::capnp::build_owned_write_batch_response;

/// Processes persistence responses from the persistence worker.
/// Returns the number of responses processed.
pub fn process_persistence_responses(shared_state: &mut SharedState) -> u64 {
    let mut count: u64 = 0;
    while let Some(task) = shared_state.persistence_response.pop() {
        count += 1;
        match task {
            PersistenceResponseType::LogPersisted { id } => {
                handle_log_persisted(id, shared_state);
            }
            PersistenceResponseType::VotePersisted {
                id,
                term: _,
                candidate_id: _,
            } => {
                handle_vote_persisted(id, shared_state);
            }
        }
    }
    count
}

/// Handles a LogPersisted event from the persistence worker.
fn handle_log_persisted(id: u64, shared_state: &mut SharedState) {
    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
        let span = tracing::span!(Level::INFO, "outstanding_message_log_persisted_processing");
        span.set_parent(msg.span.context());
        let _entered = span.enter();
        tracing::event!(Level::INFO, "handling log persisted");

        let new_outstanding_resp = msg.outstanding_responses - 1;

        match msg.message_type {
            OutstandingMessageType::WriteBatch {
                persistence_done: _,
                quorum_acks,
                callback,
                index,
                start_time,
                batch_size,
            } => {
                if quorum_acks >= shared_state.quorum_size {
                    // Persistence done and replicated to quorum of nodes
                    complete_write_batch(id, index, batch_size, start_time, callback, shared_state);
                } else {
                    // Still waiting for quorum acks
                    msg.outstanding_responses = new_outstanding_resp;
                    let new_state = OutstandingMessageType::WriteBatch {
                        persistence_done: true,
                        quorum_acks,
                        callback,
                        index,
                        start_time,
                        batch_size,
                    };
                    msg.message_type = new_state;
                    shared_state.outstanding_messages.insert(id, msg);
                }
            }
            OutstandingMessageType::RequestVote { callback } => {
                let payload = LocalRaftResponsePayload::RequestVote(true);
                let response = LocalRaftResponseMessage { payload };
                let _ = callback.send(response);
            }
            OutstandingMessageType::NoOp {
                persistence_done: _,
                quorum_acks,
                index,
            } => {
                if quorum_acks >= shared_state.quorum_size {
                    // Persistence done and replicated to quorum of nodes
                    complete_noop(id, index, shared_state);
                } else {
                    // Still waiting for quorum acks
                    msg.outstanding_responses = new_outstanding_resp;
                    let new_state = OutstandingMessageType::NoOp {
                        persistence_done: true,
                        quorum_acks,
                        index,
                    };
                    msg.message_type = new_state;
                    shared_state.outstanding_messages.insert(id, msg);
                }
            }
            _ => {
                log::error!(
                    "Received unexpected outstanding_messages type for PersistenceResponseType.LogPersisted {}",
                    id
                );
            }
        }
    } else {
        log::error!("LogPersisted event with no outstanding message registered {}", id);
    }
}

/// Handles a VotePersisted event from the persistence worker.
fn handle_vote_persisted(id: u64, shared_state: &mut SharedState) {
    if let Some(msg) = shared_state.outstanding_messages.remove(&id) {
        let _entered = msg.span.enter();
        match msg.message_type {
            OutstandingMessageType::RequestVote { callback } => {
                let payload = LocalRaftResponsePayload::RequestVote(true);
                let response = LocalRaftResponseMessage { payload };
                let _ = callback.send(response);
            }
            _ => {
                log::error!(
                    "Received unexpected outstanding_messages type for PersistenceResponseType.VotePersisted {}",
                    id
                );
            }
        }
    } else {
        tracing::error!(
            "Received persistence event with no outstanding message registered {}",
            id
        );
    }
}

/// Processes quorum responses from quorum workers.
/// Returns (backfill_requests, work_done_count).
pub fn process_quorum_responses(shared_state: &mut SharedState) -> (Vec<(u64, u64, u64)>, u64) {
    let mut backfill_requests = Vec::new();
    let mut count: u64 = 0;

    while let Some(task) = shared_state.quorum_response.pop() {
        count += 1;
        match task.response_type {
            LocalQuorumTaskResponseType::AppendEntries { id, ok: _ } => {
                handle_append_entries_response(id, shared_state);
            }
            LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id,
                last_log_term,
                last_log_index,
            } => {
                backfill_requests.push((quorum_node_id, last_log_term, last_log_index));
            }
            LocalQuorumTaskResponseType::RequestVoteResponse {
                id: _,
                term,
                received_vote,
            } => {
                log::info!(
                    "Received vote response for term: {} while already leader for term: {} received_vote: {}",
                    term,
                    shared_state.server_state.current_term,
                    received_vote
                );
            }
            LocalQuorumTaskResponseType::TruncateLogResponse { id, ok } => {
                log::info!("Received truncate log response for message id: {}, ok: {}", id, ok);
            }
            _ => {
                log::error!("Received invalid quorum response message in current state");
            }
        }
    }

    (backfill_requests, count)
}

/// Handles an AppendEntries ack from a quorum worker.
fn handle_append_entries_response(id: u64, shared_state: &mut SharedState) {
    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
        let span = tracing::span!(Level::INFO, "outstanding_message_quorum_append_entries_processing");
        span.set_parent(msg.span.context());
        let _entered = span.enter();
        tracing::event!(Level::INFO, "handling quorum append entries");

        let new_outstanding_resp = msg.outstanding_responses - 1;

        match msg.message_type {
            OutstandingMessageType::WriteBatch {
                persistence_done,
                quorum_acks,
                callback,
                index,
                start_time,
                batch_size,
            } => {
                let new_acks = quorum_acks + 1;
                if new_acks >= shared_state.quorum_size && persistence_done {
                    // Log replication done
                    complete_write_batch(id, index, batch_size, start_time, callback, shared_state);
                } else if new_outstanding_resp > 0 {
                    msg.outstanding_responses = new_outstanding_resp;
                    let new_state = OutstandingMessageType::WriteBatch {
                        persistence_done,
                        quorum_acks: new_acks,
                        callback,
                        index,
                        start_time,
                        batch_size,
                    };
                    msg.message_type = new_state;
                    shared_state.outstanding_messages.insert(id, msg);
                } else {
                    log::info!("All nodes have acked write batch {}", id);
                }
            }
            OutstandingMessageType::NoOp {
                persistence_done,
                quorum_acks,
                index,
            } => {
                let new_acks = quorum_acks + 1;
                if new_acks >= shared_state.quorum_size && persistence_done {
                    // No-op replication done
                    complete_noop(id, index, shared_state);
                } else if new_outstanding_resp > 0 {
                    msg.outstanding_responses = new_outstanding_resp;
                    let new_state = OutstandingMessageType::NoOp {
                        persistence_done,
                        quorum_acks: new_acks,
                        index,
                    };
                    msg.message_type = new_state;
                    shared_state.outstanding_messages.insert(id, msg);
                } else {
                    log::info!("All nodes have acked no-op entry {}", id);
                }
            }
            _ => {
                log::error!("Received invalid outstanding quorum message in current state {}", id);
            }
        }
    } else {
        log::warn!(
            "Received quorum event AppendEntries with no outstanding message registered {}, maybe backfill",
            id
        );
    }
}

/// Completes a write batch by updating commit state and sending the callback response.
fn complete_write_batch(
    id: u64,
    index: u64,
    batch_size: usize,
    start_time: std::time::Instant,
    callback: tokio::sync::oneshot::Sender<LocalRaftResponseMessage>,
    shared_state: &mut SharedState,
) {
    log::debug!(
        "Write batch with message id {} and size {} has been persisted on quorum of nodes after {}ms",
        id,
        batch_size,
        start_time.elapsed().as_millis()
    );

    if index > shared_state.volatile_server_state.commit_index {
        shared_state.volatile_server_state.commit_index = index;
        shared_state
            .volatile_server_state
            .commit_state
            .commit_index
            .store(index, Ordering::Release);
        shared_state
            .volatile_server_state
            .commit_state
            .version
            .fetch_add(1, Ordering::Release);

        // Broadcast newly committed entries to query workers
        shared_state.broadcast_committed_entries(index);

        // Notify quorum workers to send immediate heartbeat with updated commit_index
        // This ensures followers learn about the new commit_index promptly
        shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
            let task = LocalQuorumWorkerTaskType::UpdateCommitIndex { commit_index: index };
            task_queue.push(LocalQuorumWorkerTask { task_type: task });
        });
    }

    // Update Raft state metrics
    crate::transport::metrics::update_raft_state_metrics(
        shared_state.volatile_server_state.commit_index,
        shared_state.server_state.leader_id,
        shared_state.server_state.current_term,
        shared_state.volatile_server_state.replication_log.len(),
    );

    // Record metrics for completed write batch (in microseconds)
    let duration = start_time.elapsed();
    crate::transport::metrics::record_write_batch_completion(duration.as_micros());
    crate::transport::metrics::WRITE_BATCH_SIZE.observe(batch_size as f64);
    crate::transport::metrics::WRITE_BATCH_TOTAL.inc();

    let r = LocalRaftWriteBatchResponse {
        message: build_owned_write_batch_response(|_| {}),
        err: None,
    };
    let payload = LocalRaftResponsePayload::WriteBatch(r);
    let response = LocalRaftResponseMessage { payload };
    let _ = callback.send(response);
}

/// Completes a no-op entry by updating commit state.
/// No callback is needed since no-op entries are internal to the leader.
fn complete_noop(id: u64, index: u64, shared_state: &mut SharedState) {
    log::debug!(
        "No-op entry with message id {} has been persisted on quorum of nodes",
        id
    );

    if index > shared_state.volatile_server_state.commit_index {
        shared_state.volatile_server_state.commit_index = index;
        shared_state
            .volatile_server_state
            .commit_state
            .commit_index
            .store(index, Ordering::Release);
        shared_state
            .volatile_server_state
            .commit_state
            .version
            .fetch_add(1, Ordering::Release);

        // Broadcast newly committed entries to query workers
        shared_state.broadcast_committed_entries(index);

        // Notify quorum workers to send immediate heartbeat with updated commit_index
        shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
            let task = LocalQuorumWorkerTaskType::UpdateCommitIndex { commit_index: index };
            task_queue.push(LocalQuorumWorkerTask { task_type: task });
        });
    }

    // Update Raft state metrics
    crate::transport::metrics::update_raft_state_metrics(
        shared_state.volatile_server_state.commit_index,
        shared_state.server_state.leader_id,
        shared_state.server_state.current_term,
        shared_state.volatile_server_state.replication_log.len(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::worker::PersistenceResponseType;
    use crate::quorum::types::{LocalQuorumResponse, LocalQuorumTaskResponseType};
    use crate::raft::raft_sm::{
        CommitState, OutstandingMessage, RaftServerState, RaftVolatileState, SharedState, StateMachineConfig,
    };
    use bus::Bus;
    use crossbeam_channel::unbounded;
    use crossbeam_queue::SegQueue;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    fn create_test_shared_state() -> SharedState {
        let (persistence_tx, _persistence_rx) = unbounded();
        SharedState {
            server_state: RaftServerState {
                current_term: 2,
                leader_id: 1,
            },
            volatile_server_state: RaftVolatileState {
                next_term: 3,
                last_log_term: 2,
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
                has_deferred_broadcast: false,
            },
            next_message_id: 100,
            outstanding_messages: HashMap::new(),
            quorum_size: 2,
            quorum_worker_tasks: vec![Arc::new(SegQueue::new()), Arc::new(SegQueue::new())],
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

    fn add_write_batch_outstanding_message(
        shared_state: &mut SharedState,
        id: u64,
        index: u64,
        persistence_done: bool,
        quorum_acks: u32,
    ) -> oneshot::Receiver<LocalRaftResponseMessage> {
        let (tx, rx) = oneshot::channel();
        let msg = OutstandingMessage {
            id,
            outstanding_responses: 3,
            message_type: OutstandingMessageType::WriteBatch {
                persistence_done,
                quorum_acks,
                callback: tx,
                index,
                start_time: std::time::Instant::now(),
                batch_size: 10,
            },
            span: tracing::Span::current(),
        };
        shared_state.outstanding_messages.insert(id, msg);
        rx
    }

    fn add_request_vote_outstanding_message(
        shared_state: &mut SharedState,
        id: u64,
    ) -> oneshot::Receiver<LocalRaftResponseMessage> {
        let (tx, rx) = oneshot::channel();
        let msg = OutstandingMessage {
            id,
            outstanding_responses: 1,
            message_type: OutstandingMessageType::RequestVote { callback: tx },
            span: tracing::Span::current(),
        };
        shared_state.outstanding_messages.insert(id, msg);
        rx
    }

    #[test]
    fn test_process_persistence_responses_log_persisted() {
        let mut shared_state = create_test_shared_state();
        let mut rx = add_write_batch_outstanding_message(&mut shared_state, 42, 10, false, 2);

        // Add persistence response
        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: 42 });

        // Process responses - should complete since quorum_acks >= quorum_size
        process_persistence_responses(&mut shared_state);

        // Outstanding message should be removed (completed)
        assert!(!shared_state.outstanding_messages.contains_key(&42));

        // Callback should have been triggered
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_process_persistence_responses_waiting_for_quorum() {
        let mut shared_state = create_test_shared_state();
        let _rx = add_write_batch_outstanding_message(&mut shared_state, 42, 10, false, 0);

        // Add persistence response
        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: 42 });

        // Process responses - should NOT complete since quorum_acks < quorum_size
        process_persistence_responses(&mut shared_state);

        // Outstanding message should still exist with persistence_done = true
        assert!(shared_state.outstanding_messages.contains_key(&42));

        let msg = shared_state.outstanding_messages.get(&42).unwrap();
        match &msg.message_type {
            OutstandingMessageType::WriteBatch { persistence_done, .. } => {
                assert!(*persistence_done);
            }
            _ => panic!("Expected WriteBatch"),
        }
    }

    #[test]
    fn test_process_persistence_responses_vote_persisted() {
        let mut shared_state = create_test_shared_state();
        let mut rx = add_request_vote_outstanding_message(&mut shared_state, 42);

        // Add vote persistence response
        shared_state
            .persistence_response
            .push(PersistenceResponseType::VotePersisted {
                id: 42,
                term: 2,
                candidate_id: 1,
            });

        process_persistence_responses(&mut shared_state);

        // Outstanding message should be removed
        assert!(!shared_state.outstanding_messages.contains_key(&42));

        // Callback should have received vote granted
        let response = rx.try_recv().unwrap();
        match response.payload {
            LocalRaftResponsePayload::RequestVote(granted) => {
                assert!(granted);
            }
            _ => panic!("Expected RequestVote response"),
        }
    }

    #[test]
    fn test_process_quorum_responses_append_entries() {
        let mut shared_state = create_test_shared_state();
        let mut rx = add_write_batch_outstanding_message(&mut shared_state, 42, 10, true, 1);

        // Add quorum response
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::AppendEntries { id: 42, ok: true },
        });

        // Process responses - should complete (persistence_done=true, quorum_acks will be 2)
        let (backfill_requests, _) = process_quorum_responses(&mut shared_state);

        // No backfill requests
        assert!(backfill_requests.is_empty());

        // Outstanding message should be removed (completed)
        assert!(!shared_state.outstanding_messages.contains_key(&42));

        // Callback should have been triggered
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_process_quorum_responses_waiting_for_persistence() {
        let mut shared_state = create_test_shared_state();
        let _rx = add_write_batch_outstanding_message(&mut shared_state, 42, 10, false, 1);

        // Add quorum response
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::AppendEntries { id: 42, ok: true },
        });

        // Process responses - should NOT complete since persistence_done = false
        let (backfill_requests, _) = process_quorum_responses(&mut shared_state);

        assert!(backfill_requests.is_empty());

        // Outstanding message should still exist with updated quorum_acks
        assert!(shared_state.outstanding_messages.contains_key(&42));

        let msg = shared_state.outstanding_messages.get(&42).unwrap();
        match &msg.message_type {
            OutstandingMessageType::WriteBatch { quorum_acks, .. } => {
                assert_eq!(*quorum_acks, 2);
            }
            _ => panic!("Expected WriteBatch"),
        }
    }

    #[test]
    fn test_process_quorum_responses_backfill_log() {
        let mut shared_state = create_test_shared_state();

        // Add backfill log response
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 2,
                last_log_term: 1,
                last_log_index: 3,
            },
        });

        let (backfill_requests, _) = process_quorum_responses(&mut shared_state);

        // Should have one backfill request
        assert_eq!(backfill_requests.len(), 1);
        assert_eq!(backfill_requests[0], (2, 1, 3));
    }

    #[test]
    fn test_process_quorum_responses_multiple_backfill_requests() {
        let mut shared_state = create_test_shared_state();

        // Add multiple backfill log responses
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 2,
                last_log_term: 1,
                last_log_index: 3,
            },
        });
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 3,
                last_log_term: 1,
                last_log_index: 5,
            },
        });

        let (backfill_requests, _) = process_quorum_responses(&mut shared_state);

        assert_eq!(backfill_requests.len(), 2);
    }

    #[test]
    fn test_process_persistence_responses_no_outstanding_message() {
        let mut shared_state = create_test_shared_state();

        // Add persistence response without corresponding outstanding message
        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: 999 });

        // Should not panic, just log error
        process_persistence_responses(&mut shared_state);
    }

    #[test]
    fn test_commit_index_updated_on_completion() {
        let mut shared_state = create_test_shared_state();
        let initial_commit = shared_state.volatile_server_state.commit_index;

        // Add write batch with index higher than commit
        let _rx = add_write_batch_outstanding_message(&mut shared_state, 42, 10, true, 2);

        // Add quorum response (this won't trigger completion since we need one more ack)
        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: 42 });

        process_persistence_responses(&mut shared_state);

        // Commit index should be updated to the write batch index
        assert_eq!(shared_state.volatile_server_state.commit_index, 10);
        assert!(shared_state.volatile_server_state.commit_index > initial_commit);
    }

    #[test]
    fn test_update_commit_index_dispatched_to_quorum_workers() {
        let mut shared_state = create_test_shared_state();

        // Add write batch with index higher than current commit (3)
        let _rx = add_write_batch_outstanding_message(&mut shared_state, 42, 10, true, 2);

        // Verify quorum worker task queues exist
        assert_eq!(shared_state.quorum_worker_tasks.len(), 2);

        // Add persistence response to complete the write batch
        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: 42 });

        process_persistence_responses(&mut shared_state);

        // Verify UpdateCommitIndex was dispatched to both quorum workers
        for queue in &shared_state.quorum_worker_tasks {
            let task = queue.pop().expect("Expected UpdateCommitIndex task");
            match task.task_type {
                LocalQuorumWorkerTaskType::UpdateCommitIndex { commit_index } => {
                    assert_eq!(commit_index, 10);
                }
                _ => panic!("Expected UpdateCommitIndex task, got {:?}", "other"),
            }
        }
    }

    #[test]
    fn test_no_update_commit_index_when_index_not_advanced() {
        let mut shared_state = create_test_shared_state();
        // Set commit_index to 10 already
        shared_state.volatile_server_state.commit_index = 10;

        // Add write batch with index equal to current commit (should not dispatch update)
        let _rx = add_write_batch_outstanding_message(&mut shared_state, 42, 10, true, 2);

        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: 42 });

        process_persistence_responses(&mut shared_state);

        // Verify no UpdateCommitIndex was dispatched (since commit_index didn't advance)
        for queue in &shared_state.quorum_worker_tasks {
            assert!(
                queue.pop().is_none(),
                "Should not dispatch UpdateCommitIndex when commit_index doesn't advance"
            );
        }
    }

    fn add_noop_outstanding_message(
        shared_state: &mut SharedState,
        id: u64,
        index: u64,
        persistence_done: bool,
        quorum_acks: u32,
    ) {
        let msg = OutstandingMessage {
            id,
            outstanding_responses: 3,
            message_type: OutstandingMessageType::NoOp {
                persistence_done,
                quorum_acks,
                index,
            },
            span: tracing::Span::current(),
        };
        shared_state.outstanding_messages.insert(id, msg);
    }

    #[test]
    fn test_noop_completion_advances_commit_index() {
        let mut shared_state = create_test_shared_state();
        let initial_commit = shared_state.volatile_server_state.commit_index;

        // Add no-op with index higher than commit, persistence done, enough quorum acks
        add_noop_outstanding_message(&mut shared_state, 42, 10, true, 2);

        // Add persistence response to trigger completion
        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: 42 });

        process_persistence_responses(&mut shared_state);

        // Commit index should be updated to the no-op index
        assert_eq!(shared_state.volatile_server_state.commit_index, 10);
        assert!(shared_state.volatile_server_state.commit_index > initial_commit);

        // Outstanding message should be removed
        assert!(!shared_state.outstanding_messages.contains_key(&42));
    }

    #[test]
    fn test_noop_waits_for_quorum_after_persistence() {
        let mut shared_state = create_test_shared_state();

        // Add no-op with insufficient quorum acks
        add_noop_outstanding_message(&mut shared_state, 42, 10, false, 0);

        // Add persistence response
        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: 42 });

        process_persistence_responses(&mut shared_state);

        // Should still be waiting (commit_index unchanged)
        assert_eq!(shared_state.volatile_server_state.commit_index, 3);

        // Outstanding message should still exist with persistence_done = true
        assert!(shared_state.outstanding_messages.contains_key(&42));

        let msg = shared_state.outstanding_messages.get(&42).unwrap();
        match &msg.message_type {
            OutstandingMessageType::NoOp { persistence_done, .. } => {
                assert!(*persistence_done);
            }
            _ => panic!("Expected NoOp"),
        }
    }

    #[test]
    fn test_noop_quorum_ack_advances_commit_when_persisted() {
        let mut shared_state = create_test_shared_state();
        let initial_commit = shared_state.volatile_server_state.commit_index;

        // Add no-op with persistence done, but one quorum ack away
        add_noop_outstanding_message(&mut shared_state, 42, 10, true, 1);

        // Add quorum response (quorum_size is 2, so this should complete)
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::AppendEntries { id: 42, ok: true },
        });

        let (backfill_requests, _) = process_quorum_responses(&mut shared_state);

        assert!(backfill_requests.is_empty());

        // Commit index should be updated to the no-op index
        assert_eq!(shared_state.volatile_server_state.commit_index, 10);
        assert!(shared_state.volatile_server_state.commit_index > initial_commit);

        // Outstanding message should be removed
        assert!(!shared_state.outstanding_messages.contains_key(&42));
    }

    #[test]
    fn test_noop_update_commit_index_dispatched_to_quorum_workers() {
        let mut shared_state = create_test_shared_state();

        // Add no-op with index higher than current commit (3)
        add_noop_outstanding_message(&mut shared_state, 42, 10, true, 2);

        // Verify quorum worker task queues exist
        assert_eq!(shared_state.quorum_worker_tasks.len(), 2);

        // Add persistence response to complete the no-op
        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: 42 });

        process_persistence_responses(&mut shared_state);

        // Verify UpdateCommitIndex was dispatched to both quorum workers
        for queue in &shared_state.quorum_worker_tasks {
            let task = queue.pop().expect("Expected UpdateCommitIndex task");
            match task.task_type {
                LocalQuorumWorkerTaskType::UpdateCommitIndex { commit_index } => {
                    assert_eq!(commit_index, 10);
                }
                _ => panic!("Expected UpdateCommitIndex task"),
            }
        }
    }
}
