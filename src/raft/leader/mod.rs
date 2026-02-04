//! Leader state delegate for the Raft consensus protocol.
//!
//! This module contains the leader state implementation split into focused submodules:
//! - `write_batch`: Write batch processing and serialization
//! - `outstanding`: Outstanding message handling (persistence and quorum responses)
//! - `replication`: Log replication and backfill handling

mod write_batch;
mod outstanding;
mod replication;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use log::info;
use rand::rngs::ThreadRng;
use rand::Rng;
use tokio::sync::oneshot;
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::persistence::worker::PersistenceTaskType;
use crate::quorum::types::{LocalQuorumWorkerTask, LocalQuorumWorkerTaskType};
use crate::raft::follower::RaftFollowerStateDelegate;
use crate::raft::raft_sm::{
    LocalAppendEntries, LocalAppendEntriesCallbackResponse, OutstandingMessage,
    OutstandingMessageType, RaftMessageStateChange, RaftNodeType, RaftProtocol,
    LocalRaftResponseMessage, LocalRaftResponsePayload, LocalRaftWriteBatchRequest,
    LocalRequestVoteRequest, SharedState,
};
use crate::service_utils::app_time::now_millis;

pub struct RaftLeaderStateDelegate {
    election_timeout: u128,
    rng: ThreadRng,
    pub(crate) initialized: bool,
}

impl RaftLeaderStateDelegate {
    pub fn new() -> Self {
        let now = now_millis();
        let mut rng = rand::thread_rng();
        let election_timout = rng.gen_range(32..128);
        Self {
            election_timeout: now + election_timout,
            rng,
            initialized: false,
        }
    }

    /// Initializes the leader state on first time_step.
    fn initialize_leadership(&mut self, shared_state: &mut SharedState) {
        let message_id = shared_state.next_message_id;
        shared_state.next_message_id += 1;

        let log_len = shared_state.volatile_server_state.replication_log.len() as u64;
        shared_state.volatile_server_state.next_log_index = log_len;
        info!(
            "Initializing leader for term {}. commit_index remains at {} (will advance after replicating current-term entry)",
            shared_state.server_state.current_term,
            shared_state.volatile_server_state.commit_index
        );

        // Update metrics for leader initialization (commit_index unchanged)
        crate::transport::metrics::update_raft_state_metrics(
            shared_state.volatile_server_state.commit_index,
            shared_state.server_state.leader_id,
            shared_state.server_state.current_term,
            shared_state.volatile_server_state.replication_log.len(),
        );

        // Tell quorum workers to start sending heartbeats.
        // The commit_index sent in heartbeats is the current (unchanged) commit_index.
        shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
            let task = LocalQuorumWorkerTaskType::StartHeartbeats {
                id: message_id,
                term: shared_state.server_state.current_term,
                prev_term: shared_state.volatile_server_state.last_log_term,
                prev_log_index: shared_state.volatile_server_state.last_log_index,
                commit_index: shared_state.volatile_server_state.commit_index,
            };
            task_queue.push(LocalQuorumWorkerTask { task_type: task });
        });

        // Only broadcast entries up to the current commit_index (not last_log_index).
        // New entries will be broadcast when they are committed via normal replication.
        if shared_state.volatile_server_state.commit_index > 0 {
            shared_state.broadcast_committed_entries(shared_state.volatile_server_state.commit_index);
        }

        // Append no-op entry per Raft Section 5.4.2
        // This allows commit_index to advance without waiting for client traffic.
        let noop_message_id = shared_state.next_message_id;
        let noop_idx = shared_state.volatile_server_state.next_log_index;
        shared_state.next_message_id += 1;

        let noop_entry = write_batch::create_and_append_noop_entry(noop_idx, noop_message_id, shared_state);

        if !write_batch::queue_persistence_task(noop_message_id, &noop_entry, shared_state) {
            log::error!("Failed to queue persistence task for no-op entry");
        }

        write_batch::dispatch_to_quorum(noop_message_id, noop_entry, shared_state);
        write_batch::register_noop_outstanding_message(noop_message_id, noop_idx, shared_state);
        write_batch::update_volatile_state(noop_idx, shared_state);

        self.initialized = true;
    }

    /// Handles a new leader announcement by stepping down to follower.
    fn handle_new_leader(
        &mut self,
        append_entries_request: LocalAppendEntries,
        callback: oneshot::Sender<LocalRaftResponseMessage>,
        shared_state: &mut SharedState,
    ) -> RaftMessageStateChange {
        log::info!(
            "recognising leader for new term: {}, leader id: {}",
            append_entries_request.term,
            append_entries_request.leader_id
        );

        // Step 1: Recognize new leader
        shared_state.server_state.current_term = append_entries_request.term;
        shared_state.server_state.leader_id = append_entries_request.leader_id;
        shared_state.volatile_server_state.next_term = append_entries_request.term + 1;

        let message_id = shared_state.next_message_id;
        shared_state.next_message_id += 1;

        // Stop heartbeats to all quorum workers
        shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
            let task = LocalQuorumWorkerTaskType::StopHeartbeats { id: message_id };
            task_queue.push(LocalQuorumWorkerTask { task_type: task });
        });

        // Step 2: Check if request is consecutive with our log state
        let last_log_index = shared_state.volatile_server_state.last_log_index;
        let last_log_term = shared_state.volatile_server_state.last_log_term;

        if append_entries_request.prev_index == last_log_index
            && append_entries_request.prev_term == last_log_term
        {
            // Request is consecutive - acknowledge and transition to follower
            callback
                .send(LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok),
                })
                .unwrap_or_else(|_| log::warn!("Failed to send append_entries callback: receiver dropped"));
        } else {
            // Request is not consecutive - request backfill (whether it has entry or not)
            log::info!(
                "New leader request not consecutive: prev_term={}, prev_index={}, local_term={}, local_index={}, has_entry={}",
                append_entries_request.prev_term, append_entries_request.prev_index, last_log_term, last_log_index,
                append_entries_request.entry.is_some()
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
                .unwrap_or_else(|_| log::warn!("Failed to send append_entries callback: receiver dropped"));
        }

        RaftMessageStateChange::Follower(Box::new(RaftFollowerStateDelegate::new()))
    }
}

impl RaftProtocol for RaftLeaderStateDelegate {
    fn get_node_type(&self) -> RaftNodeType {
        RaftNodeType::Leader
    }

    fn write_batch(
        &mut self,
        write_batch_request: LocalRaftWriteBatchRequest,
        callback: oneshot::Sender<LocalRaftResponseMessage>,
        shared_state: &mut SharedState,
    ) {
        let message_id = shared_state.next_message_id;
        let idx = shared_state.volatile_server_state.next_log_index;
        let batch_size = write_batch_request.message.len();

        tracing::info!("writing batches {} as message id: {} with index: {}", batch_size, message_id, idx);
        shared_state.next_message_id += 1;

        let span = tracing::span!(Level::INFO, "delegate_write_batch");
        let _enter = span.enter();

        // Validate the request
        if let Some(error_msg) = write_batch::validate_write_batch(&write_batch_request) {
            write_batch::send_error_response(error_msg, callback);
            return;
        }

        // Create OwnedLogEntry directly and append to log
        let owned_entry = write_batch::create_and_append_log_entry(
            &write_batch_request,
            idx,
            message_id,
            shared_state,
        );

        // Queue persistence task (writes Cap'n Proto bytes directly)
        if !write_batch::queue_persistence_task(message_id, &owned_entry, shared_state) {
            write_batch::send_error_response("Failed to queue persistence task", callback);
            return;
        }

        // Dispatch to quorum workers
        write_batch::dispatch_to_quorum(message_id, owned_entry, shared_state);

        // Register outstanding message
        write_batch::register_outstanding_message(message_id, idx, batch_size, callback, shared_state);

        // Update volatile state
        write_batch::update_volatile_state(idx, shared_state);
    }

    fn append_entries(
        &mut self,
        append_entries_request: LocalAppendEntries,
        callback: oneshot::Sender<LocalRaftResponseMessage>,
        shared_state: &mut SharedState,
    ) -> RaftMessageStateChange {
        let span = tracing::span!(Level::INFO, "leader_append_entries", leader = true);
        let _enter = span.enter();
        tracing::debug!(
            "handling append entries for term: {}, current term: {}",
            append_entries_request.term,
            shared_state.server_state.current_term
        );

        if append_entries_request.term > shared_state.server_state.current_term {
            // New leader with higher term - step down
            return self.handle_new_leader(append_entries_request, callback, shared_state);
        }

        // Reject append entries from lower term
        tracing::info!(
            "Received append_entries request from lower term candidate: {}, {}",
            append_entries_request.term,
            append_entries_request.leader_id
        );
        callback
            .send(LocalRaftResponseMessage {
                payload: LocalRaftResponsePayload::AppendEntries(
                    LocalAppendEntriesCallbackResponse::UnrecognizedLeader,
                ),
            })
            .unwrap_or_else(|_| log::warn!("Failed to send append_entries callback: receiver dropped"));
        RaftMessageStateChange::None
    }

    fn time_step(&mut self, shared_state: &mut SharedState) -> (RaftMessageStateChange, u64) {
        let mut work_done: u64 = 0;

        if !self.initialized {
            self.initialize_leadership(shared_state);
            work_done += 1;
        }

        // Process persistence responses
        work_done += outstanding::process_persistence_responses(shared_state);

        // Process quorum responses and collect backfill requests
        let (backfill_requests, quorum_work) = outstanding::process_quorum_responses(shared_state);
        work_done += quorum_work;

        // Handle backfill requests
        for (quorum_node_id, last_log_term, last_log_index) in backfill_requests {
            replication::handle_backfill_request(
                shared_state,
                quorum_node_id,
                last_log_term,
                last_log_index,
            );
        }

        // Retry deferred broadcasts
        work_done += shared_state.retry_deferred_broadcast();

        (RaftMessageStateChange::None, work_done)
    }

    fn request_vote(
        &mut self,
        request_vote_request: LocalRequestVoteRequest,
        callback: oneshot::Sender<LocalRaftResponseMessage>,
        shared_state: &mut SharedState,
    ) -> RaftMessageStateChange {
        if shared_state.volatile_server_state.next_term <= request_vote_request.term {
            shared_state.volatile_server_state.next_term = request_vote_request.term + 1;
        }

        if !self.should_accept_vote(
            request_vote_request.term,
            request_vote_request.last_log_term,
            request_vote_request.last_log_index,
            shared_state,
        ) {
            let _ = callback.send(LocalRaftResponseMessage {
                payload: LocalRaftResponsePayload::RequestVote(false),
            });
            return RaftMessageStateChange::None;
        }

        let message_id = shared_state.next_message_id;
        let span = tracing::span!(Level::INFO, "delegate_request_vote");
        let _enter = span.enter();

        let persistence_task = PersistenceTaskType::AppendVote {
            id: message_id,
            term: request_vote_request.term,
            candidate_id: request_vote_request.candidate_id,
            parent_span: Span::current(),
        };
        if let Err(e) = shared_state.persistence_work.send(persistence_task) {
            log::error!("Failed to send vote persistence task: {:?}", e);
            let _ = callback.send(LocalRaftResponseMessage {
                payload: LocalRaftResponsePayload::RequestVote(false),
            });
            return RaftMessageStateChange::None;
        }
        shared_state.term_votes.insert(request_vote_request.term, request_vote_request.candidate_id);
        shared_state.next_message_id = message_id + 1;

        let message_type = OutstandingMessageType::RequestVote { callback };
        let response_span = tracing::span!(Level::INFO, "request_vote_outstanding_message_processing");
        response_span.set_parent(span.context());
        let outstanding_message = OutstandingMessage {
            id: message_id,
            outstanding_responses: 1,
            message_type,
            span: response_span,
        };
        shared_state.outstanding_messages.insert(message_id, outstanding_message);

        RaftMessageStateChange::None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::*;
    use crate::raft::raft_sm::*;
    use tokio::sync::oneshot;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Instant;
    use bus::Bus;
    use crossbeam_channel::unbounded;
    use crossbeam_queue::SegQueue;
    use crate::quorum::types::LocalQuorumResponse;
    use crate::quorum::types::LocalQuorumTaskResponseType;
    use crate::persistence::worker::PersistenceResponseType;
    use crate::transport::capnp::{build_owned_write_batch, build_owned_log_entry};

    fn create_shared_state() -> (SharedState, crossbeam_channel::Receiver<crate::persistence::worker::PersistenceTaskType>) {
        let (persistence_tx, persistence_rx) = unbounded();
        (SharedState {
            server_state: RaftServerState {
                current_term: 1,
                leader_id: 1,
            },
            volatile_server_state: RaftVolatileState {
                next_term: 2,
                last_log_term: 1,
                last_log_index: 0,
                commit_index: 0,
                replication_log: vec![],
                replication_log_term_starts: HashMap::new(),
                last_applied: 0,
                next_log_index: 1,
                commit_state: Arc::new(CommitState {
                    commit_index: AtomicU64::new(0),
                    version: AtomicU64::new(0),
                }),
                last_broadcast_index: None,
                has_deferred_broadcast: false,
            },
            next_message_id: 1,
            outstanding_messages: std::collections::HashMap::new(),
            quorum_size: 3,
            quorum_worker_tasks: vec![],
            persistence_work: persistence_tx,
            persistence_response: Arc::new(SegQueue::new()),
            quorum_work: Arc::new(SegQueue::new()),
            quorum_response: Arc::new(SegQueue::new()),
            identity: 0,
            term_votes: HashMap::new(),
            state_machine_config: StateMachineConfig { max_message_size_bytes: 2048 },
            log_entry_bus: Bus::new(5000),
        }, persistence_rx)
    }

    #[tokio::test]
    async fn test_append_entries_recognize_new_leader() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();
        let (tx, rx) = oneshot::channel();

        let req = LocalAppendEntries {
            term: 2,
            leader_id: 2,
            prev_term: 1,
            prev_index: 0,
            commit_index: 0,
            request_id: 1,
            entry: None,
        };

        let result = delegate.append_entries(req, tx, &mut shared_state);
        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok) => {}
            _ => panic!("Unexpected response"),
        }
        assert_eq!(shared_state.server_state.current_term, 2);
        assert_eq!(shared_state.server_state.leader_id, 2);
        assert!(matches!(result, RaftMessageStateChange::Follower(..)));
    }

    #[tokio::test]
    async fn test_request_vote_rejects_invalid_vote() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();
        let (tx, rx) = oneshot::channel();

        let req = LocalRequestVoteRequest {
            term: 0,
            candidate_id: 2,
            last_log_term: 0,
            last_log_index: 0,
        };

        let result = delegate.request_vote(req, tx, &mut shared_state);
        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::RequestVote(false) => {}
            _ => panic!("Unexpected response"),
        }
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[tokio::test]
    async fn test_backfill_request_sends_truncate_signal_on_term_mismatch() {
        let mut delegate = RaftLeaderStateDelegate::new();
        delegate.initialized = true;
        let (mut shared_state, _persistence_rx) = create_shared_state();

        // Initialize as leader for term 3
        shared_state.server_state.current_term = 3;
        shared_state.server_state.leader_id = 0;
        shared_state.volatile_server_state.next_term = 4;

        // Leader has 5 entries: 3 in term 2, then 2 in term 3
        shared_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        shared_state.volatile_server_state.replication_log_term_starts.insert(3, 3);

        for i in 0..5u64 {
            let term = if i < 3 { 2 } else { 3 };
            let prev_index = if i > 0 { i - 1 } else { 0 };
            let prev_term = if i < 3 { 2 } else if i == 3 { 2 } else { 3 };
            shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(i);
                builder.set_term(term);
                builder.set_prev_log_index(prev_index);
                builder.set_prev_log_term(prev_term);
                builder.set_message_id(i);
                builder.set_timestamp(0);
            })));
        }

        // Set up quorum worker task queues
        shared_state.quorum_worker_tasks = vec![Arc::new(SegQueue::new()); 3];

        // Follower requests backfill at absolute index 3 but claims it's term 2
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                last_log_term: 2,
                last_log_index: 3,
            },
        });

        let (result, _work_done) = delegate.time_step(&mut shared_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        let queue = &shared_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        if let Some(task) = queue.pop() {
            match task.task_type {
                LocalQuorumWorkerTaskType::TruncateLog { prev_term, prev_index, .. } => {
                    assert_eq!(prev_term, 2);
                    assert_eq!(prev_index, 2);
                }
                _ => panic!("Expected TruncateLog task"),
            }
        }
    }

    #[tokio::test]
    async fn test_backfill_request_sends_truncate_when_follower_ahead() {
        let mut delegate = RaftLeaderStateDelegate::new();
        delegate.initialized = true;
        let (mut shared_state, _persistence_rx) = create_shared_state();

        shared_state.server_state.current_term = 3;
        shared_state.server_state.leader_id = 0;

        // Leader has 2 entries
        shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
            builder.set_index(0);
            builder.set_term(1);
            builder.set_prev_log_index(0);
            builder.set_prev_log_term(0);
            builder.set_message_id(0);
            builder.set_timestamp(0);
        })));
        shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
            builder.set_index(1);
            builder.set_term(3);
            builder.set_prev_log_index(0);
            builder.set_prev_log_term(1);
            builder.set_message_id(0);
            builder.set_timestamp(0);
        })));

        shared_state.quorum_worker_tasks = vec![Arc::new(SegQueue::new()); 3];

        // Follower requests backfill beyond leader's log
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                last_log_term: 2,
                last_log_index: 3,
            },
        });

        let (result, _work_done) = delegate.time_step(&mut shared_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        let queue = &shared_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        if let Some(task) = queue.pop() {
            match task.task_type {
                LocalQuorumWorkerTaskType::TruncateLog { prev_term, prev_index, .. } => {
                    assert_eq!(prev_term, 3);
                    assert_eq!(prev_index, 1);
                }
                _ => panic!("Expected TruncateLog task"),
            }
        }
    }

    #[tokio::test]
    async fn test_backfill_request_succeeds_when_leader_has_entries() {
        let mut delegate = RaftLeaderStateDelegate::new();
        delegate.initialized = true;
        let (mut shared_state, _persistence_rx) = create_shared_state();

        shared_state.server_state.current_term = 3;
        shared_state.server_state.leader_id = 0;

        shared_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        shared_state.volatile_server_state.replication_log_term_starts.insert(3, 6);

        for i in 0..10u64 {
            let term = if i < 6 { 2 } else { 3 };
            shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(i);
                builder.set_term(term);
                builder.set_prev_log_index(0);
                builder.set_prev_log_term(0);
                builder.set_message_id(i);
                builder.set_timestamp(0);
            })));
        }

        shared_state.quorum_worker_tasks = vec![Arc::new(SegQueue::new()); 3];

        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                last_log_term: 2,
                last_log_index: 3,
            },
        });

        let (result, _work_done) = delegate.time_step(&mut shared_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        let queue = &shared_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        if let Some(task) = queue.pop() {
            match task.task_type {
                LocalQuorumWorkerTaskType::BackfillLog { data } => {
                    assert!(data.len() > 0);
                }
                _ => panic!("Expected BackfillLog task"),
            }
        }
    }

    #[tokio::test]
    async fn test_truncate_log_response_handling() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();

        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::TruncateLogResponse { id: 123, ok: true },
        });

        let (result, _work_done) = delegate.time_step(&mut shared_state);
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[tokio::test]
    async fn test_write_batch_happy_path() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let (mut shared_state, persistence_rx) = create_shared_state();
        let (tx, _rx) = oneshot::channel();

        let write_batch_request = LocalRaftWriteBatchRequest {
            message: build_owned_write_batch(|mut builder| {
                builder.set_batch_id("test_batch");
                let mut requests = builder.init_requests(1);
                let mut req = requests.get(0);
                req.set_id("put_1");
                req.set_payload(b"payload");
                req.set_node_id(1);
            }),
        };

        shared_state.quorum_worker_tasks.push(Arc::new(SegQueue::new()));

        delegate.write_batch(write_batch_request, tx, &mut shared_state);

        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 1);
        let entry = &shared_state.volatile_server_state.replication_log[0];
        assert_eq!(entry.index(), 1);
        assert_eq!(entry.term(), shared_state.server_state.current_term);
        assert_eq!(entry.command_count(), 1);
        // Verify command via for_each_command
        let mut ids = Vec::new();
        entry.for_each_command(|id, _, _| ids.push(id.to_string())).unwrap();
        assert_eq!(ids[0], "put_1");

        assert!(shared_state.outstanding_messages.contains_key(&(shared_state.next_message_id - 1)));
        assert!(persistence_rx.try_recv().is_ok());

        let queue = &shared_state.quorum_worker_tasks[0];
        assert!(queue.pop().is_some());
    }

    #[tokio::test]
    async fn test_write_batch_empty_requests_failure() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();
        let (tx, _rx) = oneshot::channel();

        let write_batch_request = LocalRaftWriteBatchRequest {
            message: build_owned_write_batch(|mut builder| {
                builder.set_batch_id("empty_batch");
                builder.init_requests(0);
            }),
        };

        shared_state.quorum_worker_tasks.push(Arc::new(SegQueue::new()));

        delegate.write_batch(write_batch_request, tx, &mut shared_state);

        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 0);
    }

    fn create_shared_state_with_quorum() -> SharedState {
        let (persistence_tx, _persistence_rx) = unbounded();
        SharedState {
            server_state: RaftServerState {
                current_term: 1,
                leader_id: 0,
            },
            volatile_server_state: RaftVolatileState {
                next_term: 2,
                last_log_term: 1,
                last_log_index: 0,
                commit_index: 0,
                replication_log: vec![],
                replication_log_term_starts: std::collections::HashMap::new(),
                last_applied: 0,
                next_log_index: 1,
                commit_state: Arc::new(CommitState {
                    commit_index: AtomicU64::new(0),
                    version: AtomicU64::new(0),
                }),
                last_broadcast_index: None,
                has_deferred_broadcast: false,
            },
            next_message_id: 1,
            outstanding_messages: std::collections::HashMap::new(),
            quorum_size: 2,
            quorum_worker_tasks: vec![Arc::new(SegQueue::new()), Arc::new(SegQueue::new())],
            persistence_work: persistence_tx,
            persistence_response: Arc::new(SegQueue::new()),
            quorum_work: Arc::new(SegQueue::new()),
            quorum_response: Arc::new(SegQueue::new()),
            identity: 0,
            term_votes: std::collections::HashMap::new(),
            state_machine_config: StateMachineConfig { max_message_size_bytes: 2048 },
            log_entry_bus: Bus::new(5000),
        }
    }

    #[test]
    fn test_time_step_initializes_leader_and_sends_heartbeats() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state_with_quorum();

        assert!(!delegate.initialized);

        let (result, _work_done) = delegate.time_step(&mut shared_state);

        assert!(delegate.initialized);

        for queue in &shared_state.quorum_worker_tasks {
            let task = queue.pop().unwrap();
            match task.task_type {
                LocalQuorumWorkerTaskType::StartHeartbeats { id, term, prev_term, prev_log_index, .. } => {
                    assert_eq!(id, 1);
                    assert_eq!(term, 1);
                    assert_eq!(prev_term, 1);
                    assert_eq!(prev_log_index, 0);
                }
                _ => panic!("Expected StartHeartbeats task"),
            }
        }

        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[test]
    fn test_time_step_handles_log_persisted_and_quorum_ack_happy_path() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state_with_quorum();
        shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
            builder.set_index(0);
            builder.set_term(1);
            builder.set_prev_log_index(0);
            builder.set_prev_log_term(0);
            builder.set_message_id(0);
            builder.set_timestamp(0);
        })));
        shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
            builder.set_index(1);
            builder.set_term(1);
            builder.set_prev_log_index(0);
            builder.set_prev_log_term(0);
            builder.set_message_id(0);
            builder.set_timestamp(0);
        })));

        let (callback_tx, mut callback_rx) = oneshot::channel();
        let msg_id = shared_state.next_message_id;
        let outstanding = OutstandingMessage {
            id: msg_id,
            outstanding_responses: 2,
            message_type: OutstandingMessageType::WriteBatch {
                persistence_done: false,
                quorum_acks: 1,
                callback: callback_tx,
                index: 1,
                start_time: Instant::now(),
                batch_size: 0,
            },
            span: tracing::span!(Level::INFO, "test"),
        };
        shared_state.outstanding_messages.insert(msg_id, outstanding);

        shared_state.persistence_response.push(PersistenceResponseType::LogPersisted { id: msg_id });
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::AppendEntries { id: msg_id, ok: true },
        });

        let (result, _work_done) = delegate.time_step(&mut shared_state);

        assert_eq!(shared_state.volatile_server_state.commit_index, 1);

        let response = callback_rx.try_recv().unwrap();
        match response.payload {
            LocalRaftResponsePayload::WriteBatch(r) => {
                assert!(r.err.is_none());
            }
            _ => panic!("Expected WriteBatch response"),
        }

        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[test]
    fn test_time_step_handles_backfill_log_request() {
        let mut delegate = RaftLeaderStateDelegate::new();
        delegate.initialized = true;
        let mut shared_state = create_shared_state_with_quorum();

        shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
            builder.set_index(0);
            builder.set_term(1);
            builder.set_prev_log_index(0);
            builder.set_prev_log_term(0);
            builder.set_message_id(0);
            builder.set_timestamp(0);
        })));
        shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
            builder.set_index(1);
            builder.set_term(1);
            builder.set_prev_log_index(0);
            builder.set_prev_log_term(1);
            builder.set_message_id(1);
            builder.set_timestamp(0);
        })));

        shared_state.volatile_server_state.replication_log_term_starts.insert(1, 0);

        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                last_log_term: 1,
                last_log_index: 0,
            },
        });

        let (result, _work_done) = delegate.time_step(&mut shared_state);

        let queue = &shared_state.quorum_worker_tasks[0];
        let task = queue.pop().unwrap();
        match task.task_type {
            LocalQuorumWorkerTaskType::BackfillLog { data } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0].index(), 1);
                assert_eq!(data[0].term(), 1);
            }
            _ => panic!("Expected BackfillLog task"),
        }

        assert!(matches!(result, RaftMessageStateChange::None));
    }

    // =========================================================================
    // Regression test: Leader does NOT advance commit_index on initialization
    // Fix: Per Raft Section 5.4.2, a leader cannot commit entries from previous
    //      terms until it replicates an entry from its current term.
    // =========================================================================
    #[test]
    fn test_leader_initialization_does_not_advance_commit_index() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state_with_quorum();

        // Simulate scenario: previous leader left uncommitted entries
        // Leader has log entries from previous terms
        shared_state.volatile_server_state.last_log_index = 5;
        shared_state.volatile_server_state.last_log_term = 2; // Previous term

        // commit_index is behind last_log_index (uncommitted entries exist)
        let initial_commit_index = 3;
        shared_state.volatile_server_state.commit_index = initial_commit_index;
        shared_state.volatile_server_state.commit_state.commit_index.store(
            initial_commit_index,
            std::sync::atomic::Ordering::Release,
        );

        // Current term is 3 (new leader election)
        shared_state.server_state.current_term = 3;

        // Add some log entries to simulate the uncommitted state
        for i in 0..=5 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(i);
                builder.set_term(2); // All entries from previous term
                builder.set_prev_log_index(if i > 0 { i - 1 } else { 0 });
                builder.set_prev_log_term(2);
                builder.set_message_id(i);
                builder.set_timestamp(0);
            })));
        }

        // Initialize the leader (first time_step)
        let (_result, _work_done) = delegate.time_step(&mut shared_state);

        // CRITICAL: commit_index should NOT be advanced to last_log_index
        // The old buggy behavior was: commit_index = last_log_index = 5
        // The correct behavior is: commit_index remains at 3
        assert_eq!(
            shared_state.volatile_server_state.commit_index,
            initial_commit_index,
            "Leader should NOT advance commit_index on initialization. \
             Entries from previous terms cannot be committed until a current-term entry is replicated."
        );

        // The atomic commit_index should also remain unchanged
        assert_eq!(
            shared_state.volatile_server_state.commit_state.commit_index.load(std::sync::atomic::Ordering::Acquire),
            initial_commit_index,
            "Atomic commit_index should also remain unchanged"
        );
    }

    // =========================================================================
    // Regression test: Heartbeats use correct (unchanged) commit_index
    // Fix: Ensure heartbeats don't use inflated commit_index
    // =========================================================================
    #[test]
    fn test_leader_heartbeats_use_correct_commit_index() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state_with_quorum();

        // Set up state with uncommitted entries
        shared_state.volatile_server_state.last_log_index = 10;
        shared_state.volatile_server_state.commit_index = 5; // Only 5 are committed
        shared_state.server_state.current_term = 3;

        // Add log entries
        for i in 0..=10 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(i);
                builder.set_term(2);
                builder.set_prev_log_index(if i > 0 { i - 1 } else { 0 });
                builder.set_prev_log_term(2);
                builder.set_message_id(i);
                builder.set_timestamp(0);
            })));
        }

        // Initialize leader
        let (_result, _work_done) = delegate.time_step(&mut shared_state);

        // Check heartbeat commit_index
        for queue in &shared_state.quorum_worker_tasks {
            let task = queue.pop().unwrap();
            match task.task_type {
                LocalQuorumWorkerTaskType::StartHeartbeats { commit_index, .. } => {
                    assert_eq!(
                        commit_index, 5,
                        "Heartbeat commit_index should be the actual committed index (5), not last_log_index (10)"
                    );
                }
                _ => panic!("Expected StartHeartbeats task"),
            }
        }
    }

    // =========================================================================
    // Test: Leader initialization creates a no-op entry per Raft Section 5.4.2
    // =========================================================================
    #[test]
    fn test_leader_init_creates_noop_entry() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state_with_quorum();

        // Set up initial state
        shared_state.server_state.current_term = 3;
        shared_state.volatile_server_state.last_log_term = 2;
        shared_state.volatile_server_state.last_log_index = 5;
        shared_state.volatile_server_state.next_log_index = 6;
        shared_state.volatile_server_state.commit_index = 3;

        // Add some existing log entries
        for i in 0..=5 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(i);
                builder.set_term(2);
                builder.set_prev_log_index(if i > 0 { i - 1 } else { 0 });
                builder.set_prev_log_term(2);
                builder.set_message_id(i);
                builder.set_timestamp(0);
            })));
        }

        let initial_log_len = shared_state.volatile_server_state.replication_log.len();
        let initial_message_id = shared_state.next_message_id;

        // Initialize leader
        let (_result, _work_done) = delegate.time_step(&mut shared_state);

        // Verify no-op entry was created and appended
        assert_eq!(
            shared_state.volatile_server_state.replication_log.len(),
            initial_log_len + 1,
            "No-op entry should be appended to replication log"
        );

        // Get the no-op entry
        let noop_entry = shared_state.volatile_server_state.replication_log.last().unwrap();

        // Verify no-op entry properties
        assert_eq!(noop_entry.term(), 3, "No-op entry should be in current term");
        assert_eq!(noop_entry.index(), 6, "No-op entry should have correct index");
        assert_eq!(noop_entry.command_count(), 0, "No-op entry should have zero commands");
        assert_eq!(noop_entry.prev_log_index(), 5, "No-op entry should reference previous entry");
        assert_eq!(noop_entry.prev_log_term(), 2, "No-op entry should reference previous term");

        // Verify volatile state was updated
        assert_eq!(shared_state.volatile_server_state.last_log_index, 6);
        assert_eq!(shared_state.volatile_server_state.last_log_term, 3);
        assert_eq!(shared_state.volatile_server_state.next_log_index, 7);

        // Verify outstanding message was registered for the no-op
        // The no-op message ID should be initial_message_id + 1 (after heartbeat message)
        let noop_message_id = initial_message_id + 1;
        assert!(
            shared_state.outstanding_messages.contains_key(&noop_message_id),
            "No-op outstanding message should be registered"
        );

        let outstanding = shared_state.outstanding_messages.get(&noop_message_id).unwrap();
        match &outstanding.message_type {
            OutstandingMessageType::NoOp { persistence_done, quorum_acks, index } => {
                assert!(!persistence_done);
                assert_eq!(*quorum_acks, 0);
                assert_eq!(*index, 6);
            }
            _ => panic!("Expected NoOp outstanding message type"),
        }

        // Verify AppendEntries tasks were dispatched to quorum workers for the no-op
        // After StartHeartbeats, there should be an AppendEntries for the no-op
        for queue in &shared_state.quorum_worker_tasks {
            // First task should be StartHeartbeats
            let task1 = queue.pop().unwrap();
            match task1.task_type {
                LocalQuorumWorkerTaskType::StartHeartbeats { .. } => {}
                _ => panic!("Expected StartHeartbeats task first"),
            }

            // Second task should be AppendEntries for the no-op
            let task2 = queue.pop().unwrap();
            match task2.task_type {
                LocalQuorumWorkerTaskType::AppendEntries { id, entry, .. } => {
                    assert_eq!(id, noop_message_id);
                    assert_eq!(entry.command_count(), 0, "AppendEntries should contain no-op entry");
                }
                _ => panic!("Expected AppendEntries task for no-op"),
            }
        }
    }
}
