pub mod append_entries;
pub mod election;

use std::sync::atomic::Ordering;
use tokio::sync::oneshot;
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::persistence::worker::PersistenceResponseType;
use crate::quorum::types::LocalQuorumTaskResponseType;
use crate::raft::raft_sm::{
    LocalAppendEntries, LocalAppendEntriesCallbackResponse, LocalRaftResponseMessage,
    LocalRaftResponsePayload, LocalRequestVoteRequest, OutstandingMessage,
    OutstandingMessageType, RaftMessageStateChange, RaftNodeType, RaftProtocol, SharedState,
};
use crate::service_utils::app_time::now_millis;

use election::ElectionTimer;

pub struct RaftFollowerStateDelegate {
    election_timer: ElectionTimer,
}

impl RaftFollowerStateDelegate {
    pub fn new() -> Self {
        Self {
            election_timer: ElectionTimer::new(),
        }
    }

    fn truncate_log(&self, shared_state: &mut SharedState, prev_term: u64, prev_index: u64) {
        log::info!("Truncating log to term {} index {} (absolute)", prev_term, prev_index);

        let target_length = (prev_index + 1) as usize;

        // Truncate the replication log to the specified point
        if target_length < shared_state.volatile_server_state.replication_log.len() {
            shared_state.volatile_server_state.replication_log.truncate(target_length);
            log::info!("Truncated replication log to length {}", target_length);

            // Update the volatile state to reflect the new log end
            shared_state.volatile_server_state.last_log_term = prev_term;
            shared_state.volatile_server_state.last_log_index = prev_index;
            shared_state.volatile_server_state.next_log_index = prev_index + 1;

            // Remove any term starts that are beyond the truncation point
            shared_state
                .volatile_server_state
                .replication_log_term_starts
                .retain(|_term, start| *start <= prev_index);

            log::info!(
                "Updated log state: last_log_term={}, last_log_index={}, next_log_index={}",
                shared_state.volatile_server_state.last_log_term,
                shared_state.volatile_server_state.last_log_index,
                shared_state.volatile_server_state.next_log_index
            );
        }
    }
}

impl RaftProtocol for RaftFollowerStateDelegate {
    fn init(&self) {
        todo!()
    }

    fn get_node_type(&self) -> RaftNodeType {
        RaftNodeType::Follower
    }

    fn append_entries(
        &mut self,
        append_entries_request: LocalAppendEntries,
        callback: oneshot::Sender<LocalRaftResponseMessage>,
        shared_state: &mut SharedState,
    ) -> RaftMessageStateChange {
        let span = tracing::span!(Level::INFO, "follower_append_entries", leader = false);
        let _enter = span.enter();
        tracing::debug!(
            "handling append entries for term: {}, current term: {}",
            append_entries_request.term,
            shared_state.server_state.current_term
        );

        let request_term = append_entries_request.term;

        if append_entries_request.term > shared_state.server_state.current_term {
            append_entries::handle_new_leader(&append_entries_request, callback, shared_state);
            tracing::debug!("reset election timeout for term: {}", request_term);
            self.election_timer.reset();
            return RaftMessageStateChange::None;
        }

        if append_entries_request.leader_id == shared_state.server_state.leader_id {
            tracing::debug!(
                "received append_entries request from current leader: {}, term: {}",
                append_entries_request.leader_id,
                append_entries_request.term
            );

            // Check if heartbeat (request_id == 0)
            if append_entries_request.request_id == 0 {
                append_entries::handle_heartbeat(
                    &append_entries_request,
                    callback,
                    shared_state,
                    &mut self.election_timer,
                );
                return RaftMessageStateChange::None;
            }

            // Validate consecutive append entries
            if !append_entries::validate_append_entries(
                &append_entries_request,
                shared_state.volatile_server_state.last_log_index,
                shared_state.volatile_server_state.last_log_term,
            ) {
                append_entries::handle_non_consecutive_request(
                    &append_entries_request,
                    callback,
                    shared_state,
                    &mut self.election_timer,
                );
                return RaftMessageStateChange::None;
            }

            log::debug!(
                "consecutive append log req: term {}, index: {}, state: term {}, index {}",
                append_entries_request.prev_term,
                append_entries_request.prev_index,
                shared_state.volatile_server_state.last_log_term,
                shared_state.volatile_server_state.last_log_index
            );

            append_entries::apply_log_entry(append_entries_request, callback, shared_state);
        } else {
            append_entries::send_unrecognized_leader_response(
                &append_entries_request,
                callback,
                shared_state,
            );
        }

        tracing::debug!("reset election timeout for term: {}", request_term);
        self.election_timer.reset();
        RaftMessageStateChange::None
    }

    fn time_step(&mut self, shared_state: &mut SharedState) -> (RaftMessageStateChange, u64) {
        let mut work_done: u64 = 0;

        // Process persistence responses
        while let Some(task) = shared_state.persistence_response.pop() {
            work_done += 1;
            match task {
                PersistenceResponseType::LogPersisted { id } => {
                    self.handle_log_persisted(id, shared_state);
                }
                PersistenceResponseType::VotePersisted { id, term, candidate_id } => {
                    self.handle_vote_persisted(id, shared_state);
                }
                _ => {
                    tracing::error!("Persistence event not valid in current state");
                }
            }
        }

        // Process quorum responses
        while let Some(task) = shared_state.quorum_response.pop() {
            work_done += 1;
            match task.response_type {
                LocalQuorumTaskResponseType::TruncateLog {
                    quorum_node_id,
                    prev_term,
                    prev_index,
                } => {
                    log::info!(
                        "Received truncate log request from leader, truncating to term {} index {}",
                        prev_term,
                        prev_index
                    );
                    self.truncate_log(shared_state, prev_term, prev_index);

                    // Update commit state (truncation only removes uncommitted entries)
                    shared_state
                        .volatile_server_state
                        .commit_state
                        .version
                        .fetch_add(1, Ordering::Release);

                    log::info!(
                        "After truncation, will request backfill from term {} index {}",
                        prev_term,
                        prev_index
                    );
                }
                LocalQuorumTaskResponseType::RequestVoteResponse { .. } => {
                    log::warn!("Received invalid quorum response message in current state: RequestVoteResponse");
                }
                LocalQuorumTaskResponseType::BackfillLog { .. } => {
                    log::warn!("Received invalid quorum response message in current state: BackfillLog");
                }
                LocalQuorumTaskResponseType::AppendEntries { .. } => {
                    log::warn!("Received invalid quorum response message in current state: AppendEntries");
                }
                LocalQuorumTaskResponseType::TruncateLogResponse { .. } => {
                    log::warn!("Received invalid quorum response message in current state: TruncateLogResponse");
                }
                _ => {
                    log::error!("Received invalid quorum response message in current state");
                }
            }
        }

        // Check election timeout
        if let Some(state_change) = self.election_timer.check_timeout() {
            return (state_change, work_done);
        }

        (RaftMessageStateChange::None, work_done)
    }

    fn request_vote(
        &mut self,
        request_vote_request: LocalRequestVoteRequest,
        callback: oneshot::Sender<LocalRaftResponseMessage>,
        shared_state: &mut SharedState,
    ) -> RaftMessageStateChange {
        let span = tracing::span!(Level::INFO, "follower_request_vote");
        let _enter = span.enter();

        if shared_state.volatile_server_state.next_term <= request_vote_request.term {
            shared_state.volatile_server_state.next_term = request_vote_request.term + 1;
        }

        if !self.should_accept_vote(
            request_vote_request.term,
            request_vote_request.last_log_term,
            request_vote_request.last_log_index,
            shared_state,
        ) {
            callback
                .send(LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::RequestVote(false),
                })
                .expect("response callback for request_vote failed");
            return RaftMessageStateChange::None;
        }

        let message_id = shared_state.next_message_id;
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

impl RaftFollowerStateDelegate {
    fn handle_log_persisted(&mut self, id: u64, shared_state: &mut SharedState) {
        if let Some(msg) = shared_state.outstanding_messages.remove(&id) {
            let span = tracing::span!(Level::INFO, "outstanding_message_log_persisted_processing");
            span.set_parent(msg.span.context());
            let _entered = span.enter();
            tracing::event!(Level::INFO, "handling log persisted");

            match msg.message_type {
                OutstandingMessageType::AppendLog { callback } => {
                    let payload =
                        LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok);
                    let response = LocalRaftResponseMessage { payload };
                    if callback.send(response).is_err() {
                        tracing::error!(
                            "Writing log persisted callback failed because reader closed channel"
                        );
                    }
                }
                _ => {
                    tracing::error!(
                        message = "Received unexpected outstanding_messages type for PersistenceResponseType.LogPersisted",
                        id
                    );
                    log::error!(
                        "Received unexpected outstanding_messages type for PersistenceResponseType.LogPersisted {}",
                        id
                    );
                }
            }
        } else {
            tracing::error!(message = "LogPersisted event with no outstanding message registered", id);
        }
    }

    fn handle_vote_persisted(&mut self, id: u64, shared_state: &mut SharedState) {
        if let Some(msg) = shared_state.outstanding_messages.remove(&id) {
            let span_clone = msg.span.clone();
            let _entered = span_clone.enter();
            match msg.message_type {
                OutstandingMessageType::RequestVote { callback } => {
                    let payload = LocalRaftResponsePayload::RequestVote(true);
                    let response = LocalRaftResponseMessage { payload };
                    callback.send(response).unwrap();
                }
                _ => {
                    tracing::error!(
                        message = "Received unexpected outstanding_messages type for PersistenceResponseType.VotePersisted",
                        id
                    );
                }
            }
        } else {
            tracing::error!(
                message = "Received persistence event with no outstanding message registered",
                id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::*;
    use crate::raft::raft_sm::*;
    use crate::quorum::types::{LocalQuorumResponse, LocalQuorumTaskResponseType};
    use tokio::sync::oneshot;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use bus::Bus;
    use crossbeam_channel::unbounded;
    use crossbeam_queue::SegQueue;
    use crate::transport::raft::raftproto::{RemoteLogEntry, RemotePutRequest};
    use crate::service_utils::storage_utils::{SerializationData, serialize_data};
    use crate::persistence::worker::PersistenceResponseType;

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
                replication_log_term_starts: std::collections::HashMap::new(),
                last_applied: 0,
                next_log_index: 1,
                commit_state: Arc::new(CommitState {
                    commit_index: AtomicU64::new(0),
                    version: AtomicU64::new(0),
                }),
                last_broadcast_index: None,
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

    #[test]
    fn test_new_delegate_initializes_election_timeout() {
        let delegate = RaftFollowerStateDelegate::new();
        assert!(delegate.election_timer.election_timeout > 0);
        assert!(delegate.election_timer.election_timeout_min < delegate.election_timer.election_timeout_max);
    }

    #[tokio::test]
    async fn test_append_entries_recognize_new_leader() {
        let mut delegate = RaftFollowerStateDelegate::new();
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
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[tokio::test]
    async fn test_append_entries_heartbeat_updates_commit_index() {
        let mut delegate = RaftFollowerStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();
        let (tx, rx) = oneshot::channel();

        let req = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 1,
            prev_index: 0,
            commit_index: 5,
            request_id: 0,
            entry: None,
        };

        let result = delegate.append_entries(req, tx, &mut shared_state);
        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok { .. }) => {}
            _ => panic!("Unexpected response"),
        }
        assert_eq!(shared_state.volatile_server_state.commit_index, 5);
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[tokio::test]
    async fn test_append_entries_appends_log_entry() {
        let mut delegate = RaftFollowerStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();
        let (tx, rx) = oneshot::channel();

        let put_request = RemotePutRequest {
            id: "put_1".to_string(),
            payload: "test_payload".to_string(),
            node_id: 42,
        };
        let serialisation_data = SerializationData {
            index: 1,
            term: 1,
            timestamp: 123456789,
            prev_index: 0,
            prev_term: 0,
            message_id: 100,
            requests: vec![put_request],
        };
        let buffer = vec![0u8; 2048];
        let (size, mut serialised_data) = serialize_data(&serialisation_data, buffer, 0);
        serialised_data.truncate(size);
        let req = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 1,
            prev_index: 0,
            commit_index: 1,
            request_id: 1,
            entry: Some(RemoteLogEntry {
                term: 1,
                index: 1,
                data: serialised_data,
                batch_index: 0,
                message_id: 1,
                prev_log_index: 0,
                prev_log_term: 1,
            }),
        };

        let message_index = shared_state.next_message_id;
        let result = delegate.append_entries(req, tx, &mut shared_state);
        // Send persistence response to mock that entries have been written to disk
        shared_state
            .persistence_response
            .push(PersistenceResponseType::LogPersisted { id: message_index });
        // Should see the persisted log message and trigger the callback.
        delegate.time_step(&mut shared_state);
        let response = tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("Timed out waiting for response")
            .unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok { .. }) => {}
            _ => panic!("Unexpected response"),
        }
        assert_eq!(
            shared_state.volatile_server_state.last_log_index, 1,
            "last_log_index should be updated to 1"
        );
        assert_eq!(
            shared_state.volatile_server_state.last_log_term, 1,
            "last_log_term should be updated to 1"
        );
        assert_eq!(
            shared_state.volatile_server_state.commit_index, 1,
            "commit_index should be updated to 1"
        );
        assert_eq!(
            shared_state.volatile_server_state.replication_log.len(),
            1,
            "replication_log should contain one entry"
        );
        assert!(
            matches!(result, RaftMessageStateChange::None),
            "result should be RaftMessageStateChange::None"
        );
        let log_entry = &shared_state.volatile_server_state.replication_log[0];
        assert_eq!(log_entry.index, 1, "Log entry index should be 1");
        assert_eq!(log_entry.term, 1, "Log entry term should be 1");
        assert_eq!(log_entry.requests.len(), 1, "Log entry should contain one request");
        assert_eq!(log_entry.requests[0].id, "put_1", "Request ID should match");
        assert_eq!(
            log_entry.requests[0].payload, "test_payload",
            "Request payload should match"
        );
    }

    #[tokio::test]
    async fn test_request_vote_rejects_invalid_vote() {
        let mut delegate = RaftFollowerStateDelegate::new();
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
    async fn test_time_step_triggers_candidate_transition() {
        let mut delegate = RaftFollowerStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();
        delegate.election_timer.election_timeout = 0; // Simulate timeout

        let (state_change, _work_done) = delegate.time_step(&mut shared_state);
        match state_change {
            RaftMessageStateChange::Candidate(_) => {}
            _ => panic!("Should transition to candidate"),
        }
    }

    #[tokio::test]
    async fn test_truncate_log_basic_functionality() {
        let delegate = RaftFollowerStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();

        // Set up a log with multiple terms using absolute indexing
        shared_state
            .volatile_server_state
            .replication_log_term_starts
            .insert(1, 0);
        shared_state
            .volatile_server_state
            .replication_log_term_starts
            .insert(2, 3);
        shared_state
            .volatile_server_state
            .replication_log_term_starts
            .insert(3, 8);

        // Add log entries for terms 1, 2, and 3 with absolute indexes
        for i in 0..12u64 {
            shared_state
                .volatile_server_state
                .replication_log
                .push(Arc::new(SerializationData {
                    requests: vec![],
                    term: if i < 3 { 1 } else if i < 8 { 2 } else { 3 },
                    timestamp: 0,
                    prev_index: if i > 0 { i - 1 } else { 0 },
                    prev_term: if i < 3 { 1 } else if i < 8 { 2 } else { 3 },
                    index: i, // Absolute index
                    message_id: i,
                }));
        }

        shared_state.volatile_server_state.last_log_term = 3;
        shared_state.volatile_server_state.last_log_index = 11;
        shared_state.volatile_server_state.next_log_index = 12;

        // Truncate to term 2 at absolute index 4 (should keep entries 0,1,2,3,4)
        delegate.truncate_log(&mut shared_state, 2, 4);

        // Verify log was truncated correctly
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 5);
        assert_eq!(shared_state.volatile_server_state.last_log_term, 2);
        assert_eq!(shared_state.volatile_server_state.last_log_index, 4); // Absolute index
        assert_eq!(shared_state.volatile_server_state.next_log_index, 5);

        // Verify term starts were cleaned up - should only have terms 1 and 2
        assert!(shared_state
            .volatile_server_state
            .replication_log_term_starts
            .contains_key(&1));
        assert!(shared_state
            .volatile_server_state
            .replication_log_term_starts
            .contains_key(&2));
        assert!(!shared_state
            .volatile_server_state
            .replication_log_term_starts
            .contains_key(&3));
    }

    #[tokio::test]
    async fn test_truncate_log_to_term_boundary() {
        let delegate = RaftFollowerStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();

        // Set up a log with multiple terms
        shared_state
            .volatile_server_state
            .replication_log_term_starts
            .insert(1, 0);
        shared_state
            .volatile_server_state
            .replication_log_term_starts
            .insert(2, 5);

        for i in 0..10 {
            shared_state
                .volatile_server_state
                .replication_log
                .push(Arc::new(SerializationData {
                    requests: vec![],
                    term: if i < 5 { 1 } else { 2 },
                    timestamp: 0,
                    prev_index: 0,
                    prev_term: 0,
                    index: i % 5,
                    message_id: i,
                }));
        }

        shared_state.volatile_server_state.last_log_term = 2;
        shared_state.volatile_server_state.last_log_index = 9;

        // Truncate to end of term 1 (index 4, term 1)
        delegate.truncate_log(&mut shared_state, 1, 4);

        // Should truncate to exactly the end of term 1
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 5);
        assert_eq!(shared_state.volatile_server_state.last_log_term, 1);
        assert_eq!(shared_state.volatile_server_state.last_log_index, 4);

        // Should only have term 1 now
        assert!(shared_state
            .volatile_server_state
            .replication_log_term_starts
            .contains_key(&1));
        assert!(!shared_state
            .volatile_server_state
            .replication_log_term_starts
            .contains_key(&2));
    }

    #[tokio::test]
    async fn test_truncate_log_with_absolute_index() {
        let delegate = RaftFollowerStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();

        // Set up a log with only term 3
        shared_state
            .volatile_server_state
            .replication_log_term_starts
            .insert(3, 0);
        for i in 0..5u64 {
            shared_state
                .volatile_server_state
                .replication_log
                .push(Arc::new(SerializationData {
                    requests: vec![],
                    term: 3,
                    timestamp: 0,
                    prev_index: if i > 0 { i - 1 } else { 0 },
                    prev_term: 3,
                    index: i,
                    message_id: i,
                }));
        }

        shared_state.volatile_server_state.last_log_term = 3;
        shared_state.volatile_server_state.last_log_index = 4;

        // Truncate to absolute index 1 (keeps entries 0, 1)
        delegate.truncate_log(&mut shared_state, 3, 1);

        // With absolute indexing, truncation happens at the specified absolute index
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 2);
        assert_eq!(shared_state.volatile_server_state.last_log_term, 3);
        assert_eq!(shared_state.volatile_server_state.last_log_index, 1);
    }

    #[tokio::test]
    async fn test_truncate_log_no_truncation_needed() {
        let delegate = RaftFollowerStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();

        // Set up a log
        shared_state
            .volatile_server_state
            .replication_log_term_starts
            .insert(2, 0);
        for i in 0..5 {
            shared_state
                .volatile_server_state
                .replication_log
                .push(Arc::new(SerializationData {
                    requests: vec![],
                    term: 2,
                    timestamp: 0,
                    prev_index: 0,
                    prev_term: 0,
                    index: i,
                    message_id: i,
                }));
        }

        shared_state.volatile_server_state.last_log_term = 2;
        shared_state.volatile_server_state.last_log_index = 4;

        // Try to truncate to an index beyond current log
        delegate.truncate_log(&mut shared_state, 2, 10);

        // Should not modify the log since truncation point is beyond current log
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 5);
        assert_eq!(shared_state.volatile_server_state.last_log_term, 2);
        assert_eq!(shared_state.volatile_server_state.last_log_index, 4);
    }

    #[tokio::test]
    async fn test_follower_handles_truncate_log_message() {
        let mut delegate = RaftFollowerStateDelegate::new();
        let (mut shared_state, _persistence_rx) = create_shared_state();

        // Set up a log that will be truncated
        shared_state
            .volatile_server_state
            .replication_log_term_starts
            .insert(2, 0);
        shared_state
            .volatile_server_state
            .replication_log_term_starts
            .insert(3, 5);

        for i in 0..10 {
            shared_state
                .volatile_server_state
                .replication_log
                .push(Arc::new(SerializationData {
                    requests: vec![],
                    term: if i < 5 { 2 } else { 3 },
                    timestamp: 0,
                    prev_index: 0,
                    prev_term: 0,
                    index: i,
                    message_id: i,
                }));
        }

        shared_state.volatile_server_state.last_log_term = 3;
        shared_state.volatile_server_state.last_log_index = 9;

        // Simulate receiving a truncate log message from leader
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::TruncateLog {
                quorum_node_id: 0, // Leader ID
                prev_term: 2,
                prev_index: 2,
            },
        });

        let (state_change, _work_done) = delegate.time_step(&mut shared_state);

        // Should remain follower
        assert!(matches!(state_change, RaftMessageStateChange::None));

        // Log should be truncated
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 3);
        assert_eq!(shared_state.volatile_server_state.last_log_term, 2);
        assert_eq!(shared_state.volatile_server_state.last_log_index, 2);
        assert_eq!(shared_state.volatile_server_state.next_log_index, 3);

        // Term 3 should be removed from term starts
        assert!(shared_state
            .volatile_server_state
            .replication_log_term_starts
            .contains_key(&2));
        assert!(!shared_state
            .volatile_server_state
            .replication_log_term_starts
            .contains_key(&3));
    }
}
