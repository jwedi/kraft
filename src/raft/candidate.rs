use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use crossbeam_queue::SegQueue;
use log::{error, info};
use rand::Rng;
use rand::rngs::ThreadRng;
use tokio::sync::oneshot;
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::persistence::worker::{PersistenceResponseType, PersistenceTaskType};
use crate::quorum::types::{LocalQuorumResponse, LocalQuorumWorkerTask, LocalQuorumTaskResponseType, LocalQuorumWorkerTaskType};
use crate::raft::follower::RaftFollowerStateDelegate;
use crate::raft::leader::RaftLeaderStateDelegate;
use crate::raft::raft_sm::{LocalAppendEntries, LocalAppendEntriesCallbackResponse, OutstandingMessage, OutstandingMessageType, RaftMessageStateChange, RaftNodeType, RaftProtocol, LocalRaftResponseMessage, LocalRaftResponsePayload, RaftServerState, RaftVolatileState, LocalRequestVoteRequest, SharedState, TermVote};
use crate::service_utils::app_time::{now_millis, now_plus_duration_millis};

struct OngoingElection {
    term: u64,
    started_millis: u128
}
pub struct RaftCandidateStateDelegate {
    initiate_election_timeout: u128,
    ongoing_election: Option<OngoingElection>,
    rng: ThreadRng,
}

impl RaftCandidateStateDelegate {
    pub fn new() -> Self {
        Self {
            initiate_election_timeout: 0,
            ongoing_election: None,
            rng: rand::thread_rng(),
        }
    }
}

impl RaftProtocol for RaftCandidateStateDelegate {
    fn get_node_type(&self) -> RaftNodeType {
        RaftNodeType::Candidate
    }
    fn append_entries(&mut self, append_entries_request: LocalAppendEntries, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {
        tracing::debug!("handling append entries for term: {}, current term: {}", append_entries_request.term, shared_state.server_state.current_term);
        if append_entries_request.term >= shared_state.server_state.current_term {
            log::info!("recognising leader for new term: {}, leader id: {}", append_entries_request.term, append_entries_request.leader_id);

            // Step 1: Recognize the new leader
            shared_state.server_state.current_term = append_entries_request.term;
            shared_state.server_state.leader_id = append_entries_request.leader_id;
            shared_state.volatile_server_state.next_term = append_entries_request.term + 1;

            // Update metrics when candidate recognizes new leader
            crate::transport::metrics::update_raft_state_metrics(
                shared_state.volatile_server_state.commit_index,
                shared_state.server_state.leader_id,
                shared_state.server_state.current_term,
                shared_state.volatile_server_state.replication_log.len()
            );

            // Step 2: Check if request is consecutive with our log state
            let last_log_index = shared_state.volatile_server_state.last_log_index;
            let last_log_term = shared_state.volatile_server_state.last_log_term;

            if append_entries_request.prev_index == last_log_index
                && append_entries_request.prev_term == last_log_term
            {
                // Request is consecutive - acknowledge and transition to follower
                callback.send(
                    LocalRaftResponseMessage {
                        payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok)
                    }
                ).unwrap_or_else(|_| log::warn!("Failed to send channel message: receiver dropped"));
            } else {
                // Request is not consecutive - request backfill (whether it has entry or not)
                log::info!(
                    "New leader request not consecutive: prev_term={}, prev_index={}, local_term={}, local_index={}, has_entry={}",
                    append_entries_request.prev_term, append_entries_request.prev_index, last_log_term, last_log_index,
                    append_entries_request.entry.is_some()
                );
                callback.send(
                    LocalRaftResponseMessage {
                        payload: LocalRaftResponsePayload::AppendEntries(
                            LocalAppendEntriesCallbackResponse::WantedPreviousEntry {
                                last_term: last_log_term,
                                last_index: last_log_index,
                            }
                        )
                    }
                ).unwrap_or_else(|_| log::warn!("Failed to send channel message: receiver dropped"));
            }

            return RaftMessageStateChange::Follower(Box::new(RaftFollowerStateDelegate::new()));

        } else {
            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::UnrecognizedLeader)
                }
            ).unwrap_or_else(|_| log::warn!("Failed to send channel message: receiver dropped"));
        }
        RaftMessageStateChange::None
    }

    fn time_step(&mut self, shared_state: &mut SharedState) -> (RaftMessageStateChange, u64) {
        let span_root = Span::none();
        let mut work_done: u64 = 0;

        // Process persistence work
        while let Some(task) = shared_state.persistence_response.pop() {
            work_done += 1;
            match task {
                PersistenceResponseType::VotePersisted { id, term, candidate_id } => {
                    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
                        let span_clone = msg.span.clone();
                        let _entered = span_clone.enter();
                        let new_outstanding_resp = msg.outstanding_responses-1; // Maybe remove
                        match msg.message_type {
                            OutstandingMessageType::RequestVote{callback} => {
                                let payload = LocalRaftResponsePayload::RequestVote(true);
                                let response = LocalRaftResponseMessage {payload};
                                if let Err(_) = callback.send(response) {
                                    error!("Receiver dropped when sending RequestVote callback from candidate")
                                }
                            }

                            OutstandingMessageType::CandidateElection{persistence_done, quorum_votes} => {

                                if quorum_votes >= shared_state.quorum_size {
                                    // Candidate election done, Transition to leader.
                                    log::info!("Node elected leader for term {}", term);
                                    shared_state.server_state.leader_id = shared_state.identity;
                                    shared_state.server_state.current_term = term;

                                    // Update Raft state metrics for new leadership
                                    crate::transport::metrics::update_raft_state_metrics(
                                        shared_state.volatile_server_state.commit_index,
                                        shared_state.server_state.leader_id,
                                        shared_state.server_state.current_term,
                                        shared_state.volatile_server_state.replication_log.len()
                                    );

                                    return (RaftMessageStateChange::Leader(Box::new(RaftLeaderStateDelegate::new())), work_done);
                                } else {
                                    msg.outstanding_responses = new_outstanding_resp;
                                    // TODO maybe not recreate this all of the time.
                                    let new_state = OutstandingMessageType::CandidateElection { persistence_done: true, quorum_votes};
                                    msg.message_type = new_state;
                                    shared_state.outstanding_messages.insert(id, msg);
                                }
                            }
                            _ => {
                                log::error!("Received unexpected outstanding_messages type for PersistenceResponseType.VotePersisted {}", id);
                            }
                        }

                    } else {
                        log::error!("Received persistence event with no outstanding message registered {}", id);
                    }
                }
                PersistenceResponseType::LogPersisted { id } => {
                    if let Some(msg) = shared_state.outstanding_messages.remove(&id) {
                        let _entered = msg.span.enter();
                        match msg.message_type {
                            OutstandingMessageType::AppendLog{callback} => {
                                let payload = LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok);
                                let response = LocalRaftResponseMessage {payload};
                                callback.send(response).unwrap();
                            }
                            _ => {
                                log::error!("Received unexpected outstanding_messages type for PersistenceResponseType.LogPersisted {}", id);
                            }

                        }
                    } else {
                        log::error!("LogPersisted event with no outstanding message registered {}", id);
                    }
                }
            }
        }

        while let Some(task) = shared_state.quorum_response.pop() {
            work_done += 1;
            match task.response_type {
                LocalQuorumTaskResponseType::TruncateLog { quorum_node_id, prev_term, prev_index } => {
                    log::info!("Received truncate log request while candidate, ignoring");
                }
                LocalQuorumTaskResponseType::RequestVoteResponse { id, term, received_vote } => {
                    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
                        let span_clone = msg.span.clone();
                        let _entered = span_clone.enter();
                        let new_outstanding_resp = msg.outstanding_responses-1;

                        match msg.message_type {
                            OutstandingMessageType::RequestVote{callback} => {
                                let payload = LocalRaftResponsePayload::RequestVote(received_vote);
                                let response = LocalRaftResponseMessage {payload};
                                callback.send(response).unwrap();
                            }

                            OutstandingMessageType::CandidateElection{persistence_done, quorum_votes} => {
                                let new_votes = if received_vote {
                                    quorum_votes+1
                                } else {
                                    quorum_votes
                                };
                                if new_votes >= shared_state.quorum_size && persistence_done {
                                    // Candidate election done, Transition to leader.
                                    tracing::info!(message = "Node elected leader for term", term);
                                    log::info!("Node was elected leader for term: {}, node id: {}", term, shared_state.identity);
                                    shared_state.server_state.leader_id = shared_state.identity;
                                    shared_state.server_state.current_term = term;

                                    // Update Raft state metrics for new leadership
                                    crate::transport::metrics::update_raft_state_metrics(
                                        shared_state.volatile_server_state.commit_index,
                                        shared_state.server_state.leader_id,
                                        shared_state.server_state.current_term,
                                        shared_state.volatile_server_state.replication_log.len()
                                    );

                                    return (RaftMessageStateChange::Leader(Box::new(RaftLeaderStateDelegate::new())), work_done);
                                } else if new_outstanding_resp > 0 {
                                    msg.outstanding_responses = new_outstanding_resp;
                                    // TODO maybe not recreate this all of the time.
                                    let new_state = OutstandingMessageType::CandidateElection { persistence_done, quorum_votes: new_votes};
                                    msg.message_type = new_state;
                                    shared_state.outstanding_messages.insert(id, msg);
                                } else {
                                    info!("Node lost election with no more pending votes {}", term);
                                }
                            }
                            OutstandingMessageType::AppendLog{callback} => {
                                let payload = LocalRaftResponsePayload::RequestVote(received_vote);
                                let response = LocalRaftResponseMessage {payload};
                                callback.send(response).unwrap();
                            }

                            _ => {
                                log::error!("Received invalid quorum event in current state");
                            }
                        }
                    } else {
                        log::error!("Received quorum event RequestVoteResponse with no outstanding message registered {}", id);
                    }
                }

                _ => {
                    log::error!("Received invalid quorum event in current state");
                }
            }
        }

        let now = now_millis();
        // Check if election timeout has expired.
        // We use a signed comparison to handle potential clock anomalies (NTP adjustments, etc.)
        // If the timeout is more than 1 hour in the future (indicating a potential clock rollback),
        // we reset the timeout to trigger an election immediately.
        let timeout_expired = if self.initiate_election_timeout > now {
            // Timeout is in the future - check if it's suspiciously far in the future
            let time_until_timeout = self.initiate_election_timeout - now;
            if time_until_timeout > 3_600_000 {
                // More than 1 hour in the future - likely a clock rollback, reset timeout
                log::warn!(
                    "Election timeout {} is suspiciously far in the future ({}ms), resetting due to potential clock rollback",
                    self.initiate_election_timeout,
                    time_until_timeout
                );
                true
            } else {
                false
            }
        } else {
            // Timeout is in the past or now
            true
        };

        if timeout_expired {
            work_done += 1;
            // Initiate ProposeVote procedure.
            let message_id = shared_state.next_message_id;
            log::warn!("Initiating election due to timeout {}, now {}, id {}", self.initiate_election_timeout, now, message_id);
            shared_state.next_message_id += 1;
            let next_term = shared_state.volatile_server_state.next_term; // TODO should be larger than any term previously proposed.
            if let Some(already_voted) = shared_state.term_votes.get(&next_term) {
                log::error!("trying to start an election for a term it has already voted {}", next_term)
            }
            shared_state.volatile_server_state.next_term = next_term+1;
            let new_election = OngoingElection{ term: next_term, started_millis: now };
            let last_term = shared_state.volatile_server_state.last_log_term;
            let last_index = shared_state.volatile_server_state.last_log_index;

            self.ongoing_election = Some(new_election);
            let span = tracing::span!(Level::INFO, "time_step_initiate_election");
            span.set_parent(span_root.context());
            let _enter = span.enter();
            shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
                let quorum_span = Span::current();
                let task = LocalQuorumWorkerTaskType::RequestVote{ id: message_id, term: next_term, last_term, last_index, parent_span: quorum_span};
                task_queue.push(LocalQuorumWorkerTask {task_type: task});
            });
            let persistence_span = Span::current();
            let persistence_task = PersistenceTaskType::AppendVote{id: message_id, term: next_term, candidate_id: shared_state.identity, parent_span: persistence_span};
            if let Err(e) = shared_state.persistence_work.send(persistence_task) {
                log::error!("Failed to send persistence task during election: {:?}", e);
                // Continue anyway - election can proceed, persistence failure will be detected later
            }
            shared_state.term_votes.insert(next_term, shared_state.identity); // TODO can this be inserted before persisted? Probably yes
            let message_type = OutstandingMessageType::CandidateElection{ persistence_done: false, quorum_votes: 1}; // Vote for self
            let response_span = tracing::span!(Level::INFO, "append_entries_outstanding_message_processing");
            response_span.set_parent(span.context());

            let outstanding_message = OutstandingMessage{
                id: message_id,
                outstanding_responses: shared_state.quorum_worker_tasks.len() as i32 + 1, // Persistence worker and quorum workers
                message_type,
                span: response_span
            };
            shared_state.outstanding_messages.insert(message_id, outstanding_message);

            self.initiate_election_timeout = now_plus_duration_millis(Duration::from_millis(self.rng.gen_range(500..3000) as u64));
        }

        // Retry deferred broadcasts
        work_done += shared_state.retry_deferred_broadcast();

        (RaftMessageStateChange::None, work_done)
    }

    fn request_vote(&mut self, request_vote_request: LocalRequestVoteRequest, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {
        let span = tracing::span!(Level::INFO, "candidate_request_vote");
        let _enter = span.enter();

        if shared_state.volatile_server_state.next_term <= request_vote_request.term {
            shared_state.volatile_server_state.next_term = request_vote_request.term+1;
        }

        if !self.should_accept_vote(request_vote_request.term, request_vote_request.last_log_term, request_vote_request.last_log_index, shared_state) {
            let payload = LocalRaftResponseMessage {
                payload: LocalRaftResponsePayload::RequestVote(false)
            };
            if let Err(_) = callback.send(payload) {
                error!("receiver dropped when trying to send request_vote callback")
            };
            return RaftMessageStateChange::None
        }

        let message_id = shared_state.next_message_id;
        let persistence_task = PersistenceTaskType::AppendVote{id: message_id, term: request_vote_request.term, candidate_id: request_vote_request.candidate_id, parent_span: Span::current()};
        if let Err(e) = shared_state.persistence_work.send(persistence_task) {
            log::error!("Failed to send vote persistence task: {:?}", e);
            // Can't properly handle this vote without persistence
            let _ = callback.send(LocalRaftResponseMessage {
                payload: LocalRaftResponsePayload::RequestVote(false)
            });
            return RaftMessageStateChange::None;
        }
        shared_state.term_votes.insert(request_vote_request.term, request_vote_request.candidate_id);
        shared_state.next_message_id = message_id+1;
        let response_span = tracing::span!(Level::INFO, "request_vote_outstanding_message_processing");
        response_span.set_parent(span.context());
        let message_type = OutstandingMessageType::RequestVote{callback};
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