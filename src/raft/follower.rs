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
use crate::quorum::worker::{QuorumTask, QuorumTaskResponseType, QuorumTaskType};
use crate::raft::candidate::RaftCandidateStateDelegate;
use crate::raft::raft_sm::{LocalAppendEntries, LocalAppendEntriesCallbackResponse, OutstandingMessage, OutstandingMessageType, RaftMessageStateChange, RaftNodeType, RaftProtocol, LocalRaftResponseMessage, LocalRaftResponsePayload, RaftServerState, RaftVolatileState, LocalRaftWriteBatchResponse, LocalRequestVoteRequest, SharedState, TermVote};
use crate::service_utils::app_time::{now_millis, now_plus_duration_millis};

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
            shared_state.volatile_server_state.next_term = append_entries_request.term + 1;

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
        } else if append_entries_request.term == shared_state.server_state.current_term { // TODO maybe check leader id.
            tracing::debug!("received append_entries request from current leader: {}, term: {}", append_entries_request.leader_id, append_entries_request.term);

            // Check if heartbeat
            if append_entries_request.request_id == 0 {
                // Heartbeat request
                self.election_timeout = now_plus_duration_millis(Duration::from_millis(self.rng.gen_range(self.election_timeout_min..self.election_timeout_max) as u64));

                callback.send(
                    LocalRaftResponseMessage {
                        payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::WantedPreviousEntry { last_term: shared_state.volatile_server_state.last_log_term, last_index: shared_state.volatile_server_state.last_log_index})
                    }
                ).expect("sending append_entries response callback failed");
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

            let message_type = OutstandingMessageType::AppendLog{ callback };
            let outstanding_message = OutstandingMessage{
                id: message_id,
                outstanding_responses: 1,
                message_type,
                span: Span::current()
            };
            shared_state.outstanding_messages.insert(message_id, outstanding_message);
        } else {
            tracing::info!("received append_entries request from non leader with lower term than current. caller: {}, term: {}, current term: {}", append_entries_request.leader_id, append_entries_request.term, shared_state.server_state.current_term);
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