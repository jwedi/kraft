use std::sync::Arc;
use std::time::Duration;
use crossbeam_queue::SegQueue;
use rand::Rng;
use rand::rngs::ThreadRng;
use tokio::sync::oneshot;
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::persistence::worker::{PersistenceResponseType, PersistenceTaskType};
use crate::quorum::worker::{LocalQuorumResponse, LocalQuorumWorkerTask, LocalQuorumTaskResponseType, LocalQuorumWorkerTaskType};
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
    fn init(&self) {
        todo!()
    }
    fn get_node_type(&self) -> RaftNodeType {
        RaftNodeType::Candidate
    }
    fn append_entries(&mut self, append_entries_request: LocalAppendEntries, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {
        tracing::debug!("handling append entries for term: {}, current term: {}", append_entries_request.term, shared_state.server_state.current_term);
        if append_entries_request.term >= shared_state.server_state.current_term {
            log::info!("recognising leader for new term: {}, leader id: {}", append_entries_request.term, append_entries_request.leader_id);
            // TODO check if prev term or prev index is same as our own, else inform new leader that we need backfill.

            // TODO this isn't correct, fix when log backfill and storage works
            //shared_state.volatile_server_state.last_log_term = append_entries_request.term;
            //shared_state.volatile_server_state.last_log_index = 1;

            // New leader
            shared_state.server_state.current_term = append_entries_request.term;
            shared_state.server_state.leader_id = append_entries_request.leader_id;
            shared_state.volatile_server_state.next_term = append_entries_request.term +1;
            shared_state.volatile_server_state.replication_log_term_starts.insert(shared_state.server_state.current_term, shared_state.volatile_server_state.replication_log.len() as u64);

            if let Some(entry) = append_entries_request.entry {
                shared_state.volatile_server_state.last_log_index = entry.index;
                shared_state.volatile_server_state.last_log_term = entry.term;
                log::debug!("updating last log term {} and index {}", entry.term, entry.index);
            }

            // Update metrics when candidate recognizes new leader
            crate::metrics::update_raft_state_metrics(
                shared_state.volatile_server_state.commit_index,
                shared_state.server_state.leader_id,
                shared_state.server_state.current_term,
                shared_state.volatile_server_state.replication_log.len()
            );

            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok)
                }
            ).expect("Sending channel message failed");
            return RaftMessageStateChange::Follower(Box::new(RaftFollowerStateDelegate::new()));

        } else {
            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::UnrecognizedLeader)
                }
            ).expect("Sending channel message failed");
        }
        RaftMessageStateChange::None
    }

    fn time_step(&mut self, shared_state: &mut SharedState) -> RaftMessageStateChange {
        let span_root = Span::none();
        // Process persistence work
        while let Some(task) = shared_state.persistence_response.pop() {
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
                                callback.send(response).unwrap();
                            }

                            OutstandingMessageType::CandidateElection{persistence_done, quorum_votes} => {

                                if quorum_votes >= shared_state.quorum_size {
                                    // Candidate election done, Transition to leader.
                                    log::info!("Node elected leader for term {}", term);
                                    shared_state.server_state.leader_id = shared_state.identity;
                                    shared_state.server_state.current_term = term;

                                    // Update Raft state metrics for new leadership
                                    crate::metrics::update_raft_state_metrics(
                                        shared_state.volatile_server_state.commit_index,
                                        shared_state.server_state.leader_id,
                                        shared_state.server_state.current_term,
                                        shared_state.volatile_server_state.replication_log.len()
                                    );

                                    return RaftMessageStateChange::Leader(Box::new(RaftLeaderStateDelegate::new()));
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
                                    crate::metrics::update_raft_state_metrics(
                                        shared_state.volatile_server_state.commit_index,
                                        shared_state.server_state.leader_id,
                                        shared_state.server_state.current_term,
                                        shared_state.volatile_server_state.replication_log.len()
                                    );

                                    return RaftMessageStateChange::Leader(Box::new(RaftLeaderStateDelegate::new()));
                                } else if new_outstanding_resp > 0 {
                                    msg.outstanding_responses = new_outstanding_resp;
                                    // TODO maybe not recreate this all of the time.
                                    let new_state = OutstandingMessageType::CandidateElection { persistence_done, quorum_votes: new_votes};
                                    msg.message_type = new_state;
                                    shared_state.outstanding_messages.insert(id, msg);
                                } else {
                                    tracing::info!(message = "Node lost election with no more pending votes", term);
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
                        log::error!("Received quorum event with no outstanding message registered {}", id);
                    }
                }

                _ => {
                    log::error!("Received invalid quorum event in current state");
                }
            }
        }

        let now = now_millis();
        if self.initiate_election_timeout < now {
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
            shared_state.persistence_work.push(persistence_task);
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

        RaftMessageStateChange::None
    }

    fn request_vote(&mut self, request_vote_request: LocalRequestVoteRequest, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {
        let span = tracing::span!(Level::INFO, "candidate_request_vote");
        let _enter = span.enter();

        if !self.should_accept_vote(request_vote_request.term, request_vote_request.last_log_term, request_vote_request.last_log_index, shared_state) {
            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::RequestVote(false)
                }
            ).expect("sending request_vote callback failed");
            return RaftMessageStateChange::None
        }

        if shared_state.volatile_server_state.next_term <= request_vote_request.term {
            shared_state.volatile_server_state.next_term = request_vote_request.term+1;
        }

        let message_id = shared_state.next_message_id;
        let persistence_task = PersistenceTaskType::AppendVote{id: message_id, term: request_vote_request.term, candidate_id: request_vote_request.candidate_id, parent_span: Span::current()};
        shared_state.persistence_work.push(persistence_task);
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