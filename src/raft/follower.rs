use std::sync::Arc;
use std::time::Duration;
use crossbeam_queue::SegQueue;
use csv::WriterBuilder;
use log::{error, log};
use prost::bytes::Bytes;
use rand::Rng;
use rand::rngs::ThreadRng;
use tokio::sync::oneshot;
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::persistence::worker::{PersistenceResponseType, PersistenceTaskType};
use crate::quorum::worker::{LocalQuorumWorkerTask, LocalQuorumTaskResponseType, LocalQuorumWorkerTaskType};
use crate::raft::candidate::RaftCandidateStateDelegate;
use crate::raft::raft_sm::{LocalAppendEntries, LocalAppendEntriesCallbackResponse, OutstandingMessage, OutstandingMessageType, RaftMessageStateChange, RaftNodeType, RaftProtocol, LocalRaftResponseMessage, LocalRaftResponsePayload, RaftServerState, RaftVolatileState, LocalRaftWriteBatchResponse, LocalRequestVoteRequest, SharedState, TermVote};
use crate::service_utils::app_time::{now_millis, now_plus_duration_millis};
use crate::service_utils::storage_utils::deserialize_data;

pub struct RaftFollowerStateDelegate {
    election_timeout: u128,
    rng: ThreadRng,
    election_timeout_min: u128,
    election_timeout_max: u128
}

impl RaftFollowerStateDelegate {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        let min: u128 = 600;
        let max: u128 = 1300;
        let election_timout = rng.gen_range(min..max);
        Self {
            election_timeout: now_plus_duration_millis(Duration::from_millis(election_timout as u64)),
            rng,
            election_timeout_min: min,
            election_timeout_max: max
        }
    }

    fn truncate_log(&self, shared_state: &mut SharedState, prev_term: u64, prev_index: u64) {
        log::info!("Truncating log to term {} index {}", prev_term, prev_index);

        // Find the term start for the given term
        if let Some(&term_start) = shared_state.volatile_server_state.replication_log_term_starts.get(&prev_term) {
            let target_length = (term_start + prev_index + 1) as usize;

            // Truncate the replication log to the specified point
            if target_length < shared_state.volatile_server_state.replication_log.len() {
                shared_state.volatile_server_state.replication_log.truncate(target_length);
                log::info!("Truncated replication log to length {}", target_length);

                // Update the volatile state to reflect the new log end
                shared_state.volatile_server_state.last_log_term = prev_term;
                shared_state.volatile_server_state.last_log_index = prev_index;
                shared_state.volatile_server_state.next_log_index = prev_index + 1;

                // Remove any term starts that are beyond the truncation point
                shared_state.volatile_server_state.replication_log_term_starts.retain(|_term, start| {
                    *start <= term_start + prev_index
                });

                log::info!("Updated log state: last_log_term={}, last_log_index={}, next_log_index={}",
                          shared_state.volatile_server_state.last_log_term,
                          shared_state.volatile_server_state.last_log_index,
                          shared_state.volatile_server_state.next_log_index);
            }
        } else {
            log::warn!("Cannot truncate to unknown term {}", prev_term);
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
    fn append_entries(&mut self, append_entries_request: LocalAppendEntries, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {
        let span = tracing::span!(Level::INFO, "follower_append_entries", leader=false);
        let _enter = span.enter();
        tracing::debug!("handling append entries for term: {}, current term: {}", append_entries_request.term, shared_state.server_state.current_term);

        if append_entries_request.term > shared_state.server_state.current_term {
            log::info!("recognising leader for new term: {}, leader id: {}", append_entries_request.term, append_entries_request.leader_id);
            // TODO check if prev term or prev index is same as our own, else inform new leader that we need backfill.
            shared_state.server_state.current_term = append_entries_request.term;
            shared_state.server_state.leader_id = append_entries_request.leader_id;

            // Update Raft state metrics for new leader
            crate::metrics::update_raft_state_metrics(
                shared_state.volatile_server_state.commit_index,
                shared_state.server_state.leader_id,
                shared_state.server_state.current_term,
                shared_state.volatile_server_state.replication_log.len()
            );
            shared_state.volatile_server_state.next_term = append_entries_request.term + 1;
            shared_state.volatile_server_state.replication_log_term_starts.insert(shared_state.server_state.current_term, shared_state.volatile_server_state.replication_log.len() as u64);

            // TODO this isn't correct, fix when log backfill and storage works
            //shared_state.volatile_server_state.last_log_term = append_entries_request.term;
            //shared_state.volatile_server_state.last_log_index = 0;
            if let Some(entry) = append_entries_request.entry {
                error!("Received append_entries request with entry when recognizing new leader, but follower should not have entries. Leader: {}, term: {}, index: {}", append_entries_request.leader_id, append_entries_request.term, entry.index);
            }

            // TODO write data
            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok)
                }
            ).expect("sending append_entries response callback failed");
        } else if append_entries_request.leader_id == shared_state.server_state.leader_id { // TODO maybe check leader id.
            tracing::debug!("received append_entries request from current leader: {}, term: {}", append_entries_request.leader_id, append_entries_request.term);

            // Check if heartbeat
            if append_entries_request.request_id == 0 {
                // Heartbeat request
                self.election_timeout = now_plus_duration_millis(Duration::from_millis(self.rng.gen_range(self.election_timeout_min..self.election_timeout_max) as u64));

                if append_entries_request.prev_term != shared_state.volatile_server_state.last_log_term || append_entries_request.prev_index != shared_state.volatile_server_state.last_log_index {
                    log::info!("non-consecutive heartbeat request: prev term {}, req prev index: {}, state: last log term {}, state: last log index {}, requestId: {}, will request backfill", append_entries_request.prev_term, append_entries_request.prev_index, shared_state.volatile_server_state.last_log_term, shared_state.volatile_server_state.last_log_index, append_entries_request.request_id);
                    callback.send(
                        LocalRaftResponseMessage {
                            payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::WantedPreviousEntry { last_term: shared_state.volatile_server_state.last_log_term, last_index: shared_state.volatile_server_state.last_log_index})
                        }
                    ).expect("sending append_entries response callback failed");
                } else {
                    if append_entries_request.commit_index > shared_state.volatile_server_state.commit_index {
                        shared_state.volatile_server_state.commit_index = append_entries_request.commit_index;
                        log::info!("Updated commit index to {}", append_entries_request.commit_index);

                        // Update metrics for commit index change
                        crate::metrics::update_raft_state_metrics(
                            shared_state.volatile_server_state.commit_index,
                            shared_state.server_state.leader_id,
                            shared_state.server_state.current_term,
                            shared_state.volatile_server_state.replication_log.len()
                        );
                    }
                    log::debug!("consecutive heartbeat req: term {}, index: {}, state: term {}, index {}", append_entries_request.prev_term, append_entries_request.prev_index, shared_state.volatile_server_state.last_log_term, shared_state.volatile_server_state.last_log_index);
                    callback.send(
                        LocalRaftResponseMessage {
                            payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok { })
                        }
                    ).expect("sending append_entries response callback failed");
                }

                return RaftMessageStateChange::None
            }

            // Check request if valid in current state:

            if !self.validate_append_entries(&append_entries_request, shared_state.volatile_server_state.last_log_index, shared_state.volatile_server_state.last_log_term) {
                log::info!("non-consecutive append log req: req prev term {}, req prev index: {}, state: last log term {}, state: last log index {}, requestId: {}, dataLen: {}", append_entries_request.prev_term, append_entries_request.prev_index, shared_state.volatile_server_state.last_log_term, shared_state.volatile_server_state.last_log_index, append_entries_request.request_id, append_entries_request.entry.map(|e| e.data.len()).unwrap_or(0));

                tracing::debug!("reset election timeout for term: {}", append_entries_request.term);
                self.election_timeout = now_plus_duration_millis(Duration::from_millis(self.rng.gen_range(self.election_timeout_min..self.election_timeout_max) as u64));

                callback.send(
                    LocalRaftResponseMessage {
                        payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::WantedPreviousEntry { last_term: shared_state.volatile_server_state.last_log_term, last_index: shared_state.volatile_server_state.last_log_index})
                    }
                ).expect("sending append_entries response callback failed");
                return RaftMessageStateChange::None
            } else {
                log::debug!("consecutive append log req: term {}, index: {}, state: term {}, index {}", append_entries_request.prev_term, append_entries_request.prev_index, shared_state.volatile_server_state.last_log_term, shared_state.volatile_server_state.last_log_index);
            }

            let message_id = shared_state.next_message_id;
            shared_state.next_message_id += 1;

            let data = if let Some(entry) = append_entries_request.entry {
                shared_state.volatile_server_state.last_log_index = entry.index;
                shared_state.volatile_server_state.last_log_term = entry.term;
                log::debug!("updating last log term {} and index {}", entry.term, entry.index);
                entry.data
            } else {
                vec![]
            };

            if data.is_empty() {
                log::error!("Received empty append_entries data request from leader: {}, term: {}, index: {}", append_entries_request.leader_id, append_entries_request.term, append_entries_request.prev_index);
            }

            let persistence_task = PersistenceTaskType::AppendLog{id: message_id, data: data.to_vec(), parent_span: Span::current(), request_id: append_entries_request.request_id};
            shared_state.persistence_work.push(persistence_task);

            match deserialize_data(&data, 0) {
                Ok((_, deserialized)) => {
                    shared_state.volatile_server_state.replication_log.push(Arc::new(deserialized));
                }
                Err(e) => {
                    log::error!("Failed to deserialize append_entries data from leader: {}, term: {}, index: {}, error: {}", append_entries_request.leader_id, append_entries_request.term, append_entries_request.prev_index, e);
                }
            }
            if append_entries_request.commit_index > shared_state.volatile_server_state.commit_index {
                shared_state.volatile_server_state.commit_index = append_entries_request.commit_index;
            }

            // Update metrics after processing new entry and commit index
            crate::metrics::update_raft_state_metrics(
                shared_state.volatile_server_state.commit_index,
                shared_state.server_state.leader_id,
                shared_state.server_state.current_term,
                shared_state.volatile_server_state.replication_log.len()
            );

            let message_type = OutstandingMessageType::AppendLog{ callback };
            let outstanding_message = OutstandingMessage{
                id: message_id,
                outstanding_responses: 1,
                message_type,
                span: Span::current()
            };
            shared_state.outstanding_messages.insert(message_id, outstanding_message);
        } else {
            log::info!("received append_entries request from non leader with lower term than current. caller: {}, current leader: {}, term: {}, current term: {}", append_entries_request.leader_id, shared_state.server_state.leader_id, append_entries_request.term, shared_state.server_state.current_term);
            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::UnrecognizedLeader)
                }
            ).expect("sending append_entries response callback failed");
        }
        tracing::debug!("reset election timeout for term: {}", append_entries_request.term);
        self.election_timeout = now_plus_duration_millis(Duration::from_millis(self.rng.gen_range(self.election_timeout_min..self.election_timeout_max) as u64));
        RaftMessageStateChange::None
    }

    fn time_step(&mut self, shared_state: &mut SharedState) -> RaftMessageStateChange {
        while let Some(task) = shared_state.persistence_response.pop() {
            match task {
                PersistenceResponseType::LogPersisted { id } => {
                    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
                        let span = tracing::span!(Level::INFO, "outstanding_message_log_persisted_processing");
                        span.set_parent(msg.span.context());
                        let _entered = span.enter();
                        tracing::event!(Level::INFO, "handling log persisted");

                        match msg.message_type {
                            OutstandingMessageType::AppendLog{callback} => {
                                let payload = LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok);
                                let response = LocalRaftResponseMessage {payload};
                                if let Err(_) = callback.send(response) {
                                    tracing::error!("Writing log persisted callback failed because reader closed channel")
                                }
                            }
                            _ => {
                                tracing::error!(message = "Received unexpected outstanding_messages type for PersistenceResponseType.LogPersisted", id);
                                log::error!("Received unexpected outstanding_messages type for PersistenceResponseType.LogPersisted {}", id)
                            }
                        }
                    } else {
                        tracing::error!(message = "LogPersisted event with no outstanding message registered", id);
                    }
                }
                PersistenceResponseType::VotePersisted { id, term, candidate_id } => {
                    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
                        let span_clone = msg.span.clone();
                        let _entered = span_clone.enter();
                        match msg.message_type {
                            OutstandingMessageType::RequestVote{callback} => {
                                let payload = LocalRaftResponsePayload::RequestVote(true);
                                let response = LocalRaftResponseMessage {payload};
                                callback.send(response).unwrap();
                            }
                            _ => {
                                tracing::error!(message = "Received unexpected outstanding_messages type for PersistenceResponseType.VotePersisted", id);
                            }
                        }

                    } else {
                        tracing::error!(message = "Received persistence event with no outstanding message registered", id);
                    }
                }

                _ => {
                    tracing::error!("Persistence event not valid in current state");
                }
            }
        }

        while let Some(task) = shared_state.quorum_response.pop() {
            match task.response_type {
                LocalQuorumTaskResponseType::TruncateLog { quorum_node_id, prev_term, prev_index } => {
                    log::info!("Received truncate log request from leader, truncating to term {} index {}", prev_term, prev_index);
                    self.truncate_log(shared_state, prev_term, prev_index);
                    // After truncation, request backfill again
                    log::info!("After truncation, will request backfill from term {} index {}", prev_term, prev_index);
                }
                _ => {
                    tracing::error!(message = "Received invalid quorum response message in current state");
                }
            }
        }
        let now = now_millis();
        if self.election_timeout < now {
            return RaftMessageStateChange::Candidate(
                Box::new(RaftCandidateStateDelegate::new())
            );
        }
        return RaftMessageStateChange::None;
    }

    fn request_vote(&mut self, request_vote_request: LocalRequestVoteRequest, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {
        let span = tracing::span!(Level::INFO, "follower_request_vote");
        let _enter = span.enter();
        if !self.should_accept_vote(request_vote_request.term, request_vote_request.last_log_term, request_vote_request.last_log_index, shared_state) {
            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::RequestVote(false)
                }
            ).expect("response callback for request_vote failed");
            return RaftMessageStateChange::None
        }
        if shared_state.volatile_server_state.next_term <= request_vote_request.term {
            shared_state.volatile_server_state.next_term = request_vote_request.term+1;
        }

        let message_id = shared_state.next_message_id;
        shared_state.next_message_id = message_id+1;
        let message_type = OutstandingMessageType::RequestVote{callback};
        let response_span = tracing::span!(Level::INFO, "request_vote_outstanding_message_processing");
        response_span.set_parent(span.context());
        let outstanding_message = OutstandingMessage{
            id: message_id,
            outstanding_responses: 1,
            message_type,
            span: response_span
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
    use crate::quorum::worker::{LocalQuorumResponse, LocalQuorumTaskResponseType};
    use tokio::sync::oneshot;
    use std::sync::Arc;
    use crate::server::raftproto::{RemoteLogEntry, RemotePutRequest};
    use crate::service_utils::storage_utils::{SerializationData, serialize_data};

    fn create_shared_state() -> SharedState {
        SharedState {
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
            },
            next_message_id: 1,
            outstanding_messages: std::collections::HashMap::new(),
            quorum_size: 3,
            quorum_worker_tasks: vec![],
            persistence_work: Arc::new(SegQueue::new()),
            persistence_response: Arc::new(SegQueue::new()),
            quorum_work: Arc::new(SegQueue::new()),
            quorum_response: Arc::new(SegQueue::new()),
            identity: 0,
            term_votes: HashMap::new(),
            state_machine_config: StateMachineConfig { max_message_size_bytes: 2048},
        }
    }

    #[test]
    fn test_new_delegate_initializes_election_timeout() {
        let delegate = RaftFollowerStateDelegate::new();
        assert!(delegate.election_timeout > 0);
        assert!(delegate.election_timeout_min < delegate.election_timeout_max);
    }

    #[tokio::test]
    async fn test_append_entries_recognize_new_leader() {
        let mut delegate = RaftFollowerStateDelegate::new();
        let mut shared_state = create_shared_state();
        let (tx, rx) = oneshot::channel();

        let req = LocalAppendEntries {
            term: 2,
            leader_id: 2,
            prev_term: 1,
            prev_index: 0,
            commit_index: 0,
            request_id: 1,
            entry: None,
            entries: vec![]
        };

        let result = delegate.append_entries(req, tx, &mut shared_state);
        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok) => {},
            _ => panic!("Unexpected response"),
        }
        assert_eq!(shared_state.server_state.current_term, 2);
        assert_eq!(shared_state.server_state.leader_id, 2);
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[tokio::test]
    async fn test_append_entries_heartbeat_updates_commit_index() {
        let mut delegate = RaftFollowerStateDelegate::new();
        let mut shared_state = create_shared_state();
        let (tx, rx) = oneshot::channel();

        let req = LocalAppendEntries {
            term: 1,
            leader_id: 1,
            prev_term: 1,
            prev_index: 0,
            commit_index: 5,
            request_id: 0,
            entry: None,
            entries: vec![]
        };

        let result = delegate.append_entries(req, tx, &mut shared_state);
        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok { .. }) => {},
            _ => panic!("Unexpected response"),
        }
        assert_eq!(shared_state.volatile_server_state.commit_index, 5);
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[tokio::test]
    async fn test_append_entries_appends_log_entry() {
        let mut delegate = RaftFollowerStateDelegate::new();
        let mut shared_state = create_shared_state();
        let (tx, rx) = oneshot::channel();

        let put_request = RemotePutRequest{
            id: "put_1".to_string(),
            payload: "test_payload".to_string(),
            node_id: 42,
        };
        let serialisation_data = SerializationData{
            index: 1,
            term: 1,
            timestamp: 123456789,
            prev_index: 0,
            prev_term: 0,
            message_id: 100,
            requests: vec![put_request],
        };
        let mut buffer = vec![0u8; 2048];
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
            entries: vec![]
        };

        let message_index = shared_state.next_message_id;
        let result = delegate.append_entries(req, tx, &mut shared_state);
        // Send persistence response to mock that entries have been written to disk
        shared_state.persistence_response.push(PersistenceResponseType::LogPersisted { id: message_index });
        // Should see the persisted log message and trigger the callback.
        delegate.time_step(&mut shared_state);
        let response = tokio::time::timeout(std::time::Duration::from_secs(2), rx).await.expect("Timed out waiting for response").unwrap();
        match response.payload {
            LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok { .. }) => {},
            _ => panic!("Unexpected response"),
        }
        assert_eq!(shared_state.volatile_server_state.last_log_index, 1, "last_log_index should be updated to 1");
        assert_eq!(shared_state.volatile_server_state.last_log_term, 1, "last_log_term should be updated to 1");
        assert_eq!(shared_state.volatile_server_state.commit_index, 1, "commit_index should be updated to 1");
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 1, "replication_log should contain one entry");
        assert!(matches!(result, RaftMessageStateChange::None), "result should be RaftMessageStateChange::None");
        let log_entry = &shared_state.volatile_server_state.replication_log[0];
        assert_eq!(log_entry.index, 1, "Log entry index should be 1");
        assert_eq!(log_entry.term, 1, "Log entry term should be 1");
        assert_eq!(log_entry.requests.len(), 1, "Log entry should contain one request");
        assert_eq!(log_entry.requests[0].id, "put_1", "Request ID should match");
        assert_eq!(log_entry.requests[0].payload, "test_payload", "Request payload should match");
    }

    #[tokio::test]
    async fn test_request_vote_rejects_invalid_vote() {
        let mut delegate = RaftFollowerStateDelegate::new();
        let mut shared_state = create_shared_state();
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
            LocalRaftResponsePayload::RequestVote(false) => {},
            _ => panic!("Unexpected response"),
        }
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[tokio::test]
    async fn test_time_step_triggers_candidate_transition() {
        let mut delegate = RaftFollowerStateDelegate::new();
        let mut shared_state = create_shared_state();
        delegate.election_timeout = 0; // Simulate timeout

        let result = delegate.time_step(&mut shared_state);
        match result {
            RaftMessageStateChange::Candidate(_) => {},
            _ => panic!("Should transition to candidate"),
        }
    }

    #[tokio::test]
    async fn test_truncate_log_basic_functionality() {
        let delegate = RaftFollowerStateDelegate::new();
        let mut shared_state = create_shared_state();

        // Set up a log with multiple terms
        shared_state.volatile_server_state.replication_log_term_starts.insert(1, 0);
        shared_state.volatile_server_state.replication_log_term_starts.insert(2, 3);
        shared_state.volatile_server_state.replication_log_term_starts.insert(3, 8);

        // Add log entries for terms 1, 2, and 3
        for i in 0..12 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: if i < 3 { 1 } else if i < 8 { 2 } else { 3 },
                index: i,
            }));
        }

        shared_state.volatile_server_state.last_log_term = 3;
        shared_state.volatile_server_state.last_log_index = 11;
        shared_state.volatile_server_state.next_log_index = 12;

        // Truncate to term 2, index 1 (should keep entries 0,1,2,3,4)
        delegate.truncate_log(&mut shared_state, 2, 1);

        // Verify log was truncated correctly
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 5);
        assert_eq!(shared_state.volatile_server_state.last_log_term, 2);
        assert_eq!(shared_state.volatile_server_state.last_log_index, 1);
        assert_eq!(shared_state.volatile_server_state.next_log_index, 2);

        // Verify term starts were cleaned up - should only have terms 1 and 2
        assert!(shared_state.volatile_server_state.replication_log_term_starts.contains_key(&1));
        assert!(shared_state.volatile_server_state.replication_log_term_starts.contains_key(&2));
        assert!(!shared_state.volatile_server_state.replication_log_term_starts.contains_key(&3));
    }

    #[tokio::test]
    async fn test_truncate_log_to_term_boundary() {
        let delegate = RaftFollowerStateDelegate::new();
        let mut shared_state = create_shared_state();

        // Set up a log with multiple terms
        shared_state.volatile_server_state.replication_log_term_starts.insert(1, 0);
        shared_state.volatile_server_state.replication_log_term_starts.insert(2, 5);

        for i in 0..10 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: if i < 5 { 1 } else { 2 },
                index: i,
            }));
        }

        shared_state.volatile_server_state.last_log_term = 2;
        shared_state.volatile_server_state.last_log_index = 9;

        // Truncate to end of term 1 (index 4, term 1)
        delegate.truncate_log(&mut shared_state, 1, 4);

        // Should truncate to exactly the end of term 1
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 6); // 0-4 + 1 for next
        assert_eq!(shared_state.volatile_server_state.last_log_term, 1);
        assert_eq!(shared_state.volatile_server_state.last_log_index, 4);

        // Should only have term 1 now
        assert!(shared_state.volatile_server_state.replication_log_term_starts.contains_key(&1));
        assert!(!shared_state.volatile_server_state.replication_log_term_starts.contains_key(&2));
    }

    #[tokio::test]
    async fn test_truncate_log_unknown_term_warning() {
        let delegate = RaftFollowerStateDelegate::new();
        let mut shared_state = create_shared_state();

        // Set up a log with only term 3
        shared_state.volatile_server_state.replication_log_term_starts.insert(3, 0);
        for i in 0..5 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: 3,
                index: i,
            }));
        }

        shared_state.volatile_server_state.last_log_term = 3;
        shared_state.volatile_server_state.last_log_index = 4;

        let original_len = shared_state.volatile_server_state.replication_log.len();

        // Try to truncate to unknown term 2
        delegate.truncate_log(&mut shared_state, 2, 1);

        // Should not modify the log since term 2 is unknown
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), original_len);
        assert_eq!(shared_state.volatile_server_state.last_log_term, 3);
        assert_eq!(shared_state.volatile_server_state.last_log_index, 4);
    }

    #[tokio::test]
    async fn test_truncate_log_no_truncation_needed() {
        let delegate = RaftFollowerStateDelegate::new();
        let mut shared_state = create_shared_state();

        // Set up a log
        shared_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        for i in 0..5 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: 2,
                index: i,
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
        let mut shared_state = create_shared_state();

        // Set up a log that will be truncated
        shared_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        shared_state.volatile_server_state.replication_log_term_starts.insert(3, 5);

        for i in 0..10 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: if i < 5 { 2 } else { 3 },
                index: i,
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
            }
        });

        let result = delegate.time_step(&mut shared_state);

        // Should remain follower
        assert!(matches!(result, RaftMessageStateChange::None));

        // Log should be truncated
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 4); // 0,1,2 + 1 for next
        assert_eq!(shared_state.volatile_server_state.last_log_term, 2);
        assert_eq!(shared_state.volatile_server_state.last_log_index, 2);
        assert_eq!(shared_state.volatile_server_state.next_log_index, 3);

        // Term 3 should be removed from term starts
        assert!(shared_state.volatile_server_state.replication_log_term_starts.contains_key(&2));
        assert!(!shared_state.volatile_server_state.replication_log_term_starts.contains_key(&3));
    }
}