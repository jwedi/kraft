use std::sync::Arc;
use crossbeam_queue::SegQueue;
use prost::bytes::BufMut;
use rand::Rng;
use rand::rngs::ThreadRng;
use sbe_kraft_replication_schema::command_codec::CommandEncoder;
use sbe_kraft_replication_schema::log_entry_codec::encoder::CommandsEncoder;
use sbe_kraft_replication_schema::log_entry_codec::LogEntryEncoder;
use sbe_kraft_replication_schema::{message_header_codec, WriteBuf};
use sbe_kraft_replication_schema::command_type::CommandType;
use tokio::sync::oneshot;
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::persistence::worker::{PersistenceResponseType, PersistenceTaskType};
use crate::quorum::worker::{QuorumTask, QuorumTaskResponseType, QuorumTaskType};
use crate::raft::candidate::RaftCandidateStateDelegate;
use crate::raft::follower::RaftFollowerStateDelegate;
use crate::raft::raft_sm::{AppendEntries, AppendEntriesCallbackResponse, OutstandingMessage, OutstandingMessageType, RaftMessageStateChange, RaftNodeType, RaftProtocol, RaftResponseMessage, RaftResponsePayload, RaftServerState, RaftVolatileState, RaftWriteBatchRequest, RaftWriteBatchResponse, RequestVoteRequest, SharedState, TermVote};
use crate::server::raftproto::{AppendEntriesRequest, LogEntry, PutRequest};
use crate::service_utils::app_time::now_millis;
use crate::service_utils::storage_utils::{SerializationData, serialize_data};

pub struct RaftLeaderStateDelegate {
    election_timeout: u128,
    rng: ThreadRng,
    initialized: bool
}

impl RaftLeaderStateDelegate {
    pub fn new() -> Self {
        let now = now_millis();
        let mut rng = rand::thread_rng();
        let election_timout = rng.gen_range(32..128);
        Self {
            election_timeout: now + election_timout,
            rng,
            initialized: false
        }
    }
}

impl RaftProtocol for RaftLeaderStateDelegate {
    fn init(&self) {
        todo!()
    }
    fn get_node_type(&self) -> RaftNodeType {
        RaftNodeType::Leader
    }

    fn write_batch(&mut self, write_batch_request: RaftWriteBatchRequest, callback: oneshot::Sender<RaftResponseMessage>, shared_state: &mut SharedState) {
        let message_id = shared_state.next_message_id;
        let mut idx = shared_state.volatile_server_state.next_log_index;
        log::info!("leader handling writing batches {} as message id: {} with index: {}", write_batch_request.requests.len(), message_id, idx);
        shared_state.next_message_id += 1;

        let span = tracing::span!(Level::INFO, "delegate_write_batch");
        let _enter = span.enter();
        // TODO validate request
        // TODO serialize valid request bits
        let mut buffer = vec![0u8; 2048];
        let offset = 0usize;
        let (limit, mut data) = serialize_data(SerializationData{index: idx, term: shared_state.server_state.current_term, timestamp: now_millis() as u64, requests: write_batch_request.requests}, buffer, offset);
        data.truncate(limit);
        let persistence_task = PersistenceTaskType::AppendLog{id: message_id, data: data.clone(), parent_span: Span::current(), request_id: message_id};
        shared_state.persistence_work.push(persistence_task);

        // TODO log entry command should be just bytes
        shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
            let le = LogEntry { // TODO
                index: idx,
                data: data.clone(), // TODO maybe fundamental problem with the approach, doesn't support IO without copies...
                batch_index: idx,
                term: shared_state.server_state.current_term
            };
            let req = AppendEntriesRequest {
                term: shared_state.server_state.current_term,
                leader_id: shared_state.server_state.leader_id,
                entries: vec![],
                prev_log_index: shared_state.volatile_server_state.last_log_index,
                prev_log_term: shared_state.volatile_server_state.last_log_term,
                request_id: message_id,
                entry: Some(le)
            };

            let quorum_span = Span::current();
            let task = QuorumTaskType::AppendEntries{
                id: message_id,
                parent_span: quorum_span,
                req,
            };
            task_queue.push(QuorumTask{task_type: task});
        });

        let message_type = OutstandingMessageType::WriteBatch{persistence_done: false, quorum_acks: 0, callback};
        //let response_span = tracing::span!(Level::INFO, "write_batch_outstanding_message_processing");
        //response_span.set_parent(span.context());
        let outstanding_message = OutstandingMessage{
            id: message_id,
            outstanding_responses: 1 + shared_state.quorum_worker_tasks.len() as i32,
            message_type,
            span: Span::current()
        };
        shared_state.outstanding_messages.insert(message_id, outstanding_message);

        shared_state.volatile_server_state.last_log_term = shared_state.server_state.current_term;
        shared_state.volatile_server_state.next_log_index = idx +1;
        shared_state.volatile_server_state.last_log_index = idx;
    }

    fn append_entries(&mut self, append_entries_request: AppendEntries, callback: oneshot::Sender<RaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {

        let span = tracing::span!(Level::INFO, "leader_append_entries", leader=true);
        let _enter = span.enter();
        tracing::debug!("handling append entries for term: {}, current term: {}", append_entries_request.term, shared_state.server_state.current_term);

        if append_entries_request.term > shared_state.server_state.current_term {
            log::info!("recognising leader for new term: {}, leader id: {}", append_entries_request.term, append_entries_request.leader_id);
            // TODO check if prev term or prev index is same as our own, else inform new leader that we need backfill.

            // New leader
            shared_state.server_state.current_term = append_entries_request.term;
            shared_state.server_state.leader_id = append_entries_request.leader_id;
            shared_state.volatile_server_state.next_term = append_entries_request.term +1;

            let message_id = shared_state.next_message_id;
            shared_state.next_message_id += 1;

            shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
                let task = QuorumTaskType::StopHeartbeats{ id: message_id };
                task_queue.push(QuorumTask{task_type: task});
            });
            if let Some(entry) = append_entries_request.entry {
                shared_state.volatile_server_state.last_log_index = entry.index;
                shared_state.volatile_server_state.last_log_term = entry.term;
                log::debug!("updating last log term {} and index {}", entry.term, entry.index);
            }
            callback.send(
                RaftResponseMessage{
                    payload: RaftResponsePayload::AppendEntries(AppendEntriesCallbackResponse::Ok)
                }
            ).expect("sending append_entries callback failed");
            return RaftMessageStateChange::Follower(Box::new(RaftFollowerStateDelegate::new()));

        } else {
            tracing::info!("Received append_entries request from lower term candidate: {}, {}", append_entries_request.term, append_entries_request.leader_id);
            callback.send(
                RaftResponseMessage{
                    payload: RaftResponsePayload::AppendEntries(AppendEntriesCallbackResponse::UnrecognizedLeader)
                }
            ).expect("sending append_entries callback failed");
        }
        RaftMessageStateChange::None
    }

    fn time_step(&mut self, shared_state: &mut SharedState) -> RaftMessageStateChange {
        // Check heartbeat
        // Poll persistence changes

        if !self.initialized {
            let message_id = shared_state.next_message_id;
            shared_state.next_message_id += 1;
            shared_state.volatile_server_state.next_log_index = 0;

            // Tell quorum workers to start sending heartbeats.
            shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
                let task = QuorumTaskType::StartHeartbeats{
                    id: message_id,
                    term: shared_state.server_state.current_term,
                    prev_term: shared_state.volatile_server_state.last_log_term,
                    prev_log_index: shared_state.volatile_server_state.last_log_index
                };
                task_queue.push(QuorumTask{task_type: task});
            });
            self.initialized = true
        }

        // TODO
        // make sure quorum workers are sending heartbeats.

        // TODO
        // If a batch has been persisted, check if data has been persisted to quorum of workers
        // If yes, update commit index and maybe delete outstanding message
        while let Some(task) = shared_state.persistence_response.pop() {
            match task {

                PersistenceResponseType::LogPersisted { id } => {
                    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
                        let span = tracing::span!(Level::INFO, "outstanding_message_log_persisted_processing");
                        span.set_parent(msg.span.context());
                        let _entered = span.enter();
                        tracing::event!(Level::INFO, "handling log persisted");
                        let new_outstanding_resp = msg.outstanding_responses-1; // Maybe remove
                        match msg.message_type {
                            OutstandingMessageType::WriteBatch{persistence_done, quorum_acks, callback} => {
                                if quorum_acks >= shared_state.quorum_size {
                                    // Persistence done and replicated to quorum of nodes
                                    log::info!("Write batch with message id {} has been persisted on quorum of nodes", id);
                                    // TODO update commit index and apply to data structure.
                                    let r = RaftWriteBatchResponse{
                                        responses: vec![], // TODO
                                        err: None
                                    };
                                    let payload = RaftResponsePayload::WriteBatch(r);
                                    let response = RaftResponseMessage{payload};
                                    callback.send(response).unwrap();
                                } else {
                                    msg.outstanding_responses = new_outstanding_resp;
                                    // TODO maybe not recreate this all of the time.
                                    let new_state = OutstandingMessageType::WriteBatch { persistence_done: true, quorum_acks, callback};
                                    msg.message_type = new_state;
                                    shared_state.outstanding_messages.insert(id, msg);
                                }
                            }

                            OutstandingMessageType::RequestVote{callback} => {
                                let payload = RaftResponsePayload::RequestVote(true);
                                let response = RaftResponseMessage{payload};
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
                PersistenceResponseType::VotePersisted { id, term, candidate_id } => {
                    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
                        let _entered = msg.span.enter();
                        match msg.message_type {
                            OutstandingMessageType::RequestVote{callback} => {
                                let payload = RaftResponsePayload::RequestVote(true);
                                let response = RaftResponseMessage{payload};
                                callback.send(response).unwrap();
                            }
                            _ => {
                                log::error!("Received unexpected outstanding_messages type for PersistenceResponseType.VotePersisted {}", id);
                            }
                        }

                    } else {
                        tracing::error!("Received persistence event with no outstanding message registered {}", id);
                    }
                }

                v => {
                    log::error!("Persistence event not valid in current state");
                }
            }
        }

        while let Some(task) = shared_state.quorum_response.pop() {
            match task.response_type {
                QuorumTaskResponseType::AppendEntries { id, ok } => {
                    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
                        let span = tracing::span!(Level::INFO, "outstanding_message_quorum_append_entries_processing");
                        span.set_parent(msg.span.context());
                        let _entered = span.enter();
                        tracing::event!(Level::INFO, "handling quorum append entries");
                        let new_outstanding_resp = msg.outstanding_responses-1;

                        match msg.message_type {
                            OutstandingMessageType::WriteBatch{persistence_done, quorum_acks, callback} => {
                                let new_acks = quorum_acks +1;
                                if new_acks >= shared_state.quorum_size && persistence_done {
                                    // Log replication done
                                    log::info!("Write batch with message id {} has been persisted on quorum of nodes", id);
                                    let r = RaftWriteBatchResponse{
                                        responses: vec![], // TODO
                                        err: None
                                    };
                                    let payload = RaftResponsePayload::WriteBatch(r);
                                    let response = RaftResponseMessage{payload};
                                    callback.send(response).unwrap();
                                } else if new_outstanding_resp > 0 {
                                    msg.outstanding_responses = new_outstanding_resp;
                                    let new_state = OutstandingMessageType::WriteBatch { persistence_done, quorum_acks: new_acks, callback};
                                    msg.message_type = new_state;
                                    shared_state.outstanding_messages.insert(id, msg);
                                } else {
                                    log::info!("All nodes have acked write batch {}", id);
                                }
                            }

                            _ => {
                                log::error!("Received invalid outstanding quorum message in current state {}", id);
                            }
                        }
                    } else {
                        log::error!("Received quorum event with no outstanding message registered {}", id);
                    }
                }

                e => {

                    log::error!("Received invalid quorum response message in current state");
                }
            }
        }

        return RaftMessageStateChange::None;
    }

    fn request_vote(&mut self, request_vote_request: RequestVoteRequest, callback: oneshot::Sender<RaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {
        if !self.should_accept_vote(request_vote_request.term, request_vote_request.last_log_term, request_vote_request.last_log_index, shared_state) {
            callback.send(
                RaftResponseMessage{
                    payload: RaftResponsePayload::RequestVote(false)
                }
            );
            return RaftMessageStateChange::None
        }

        if shared_state.volatile_server_state.next_term <= request_vote_request.term {
            shared_state.volatile_server_state.next_term = request_vote_request.term+1;
        }

        let message_id = shared_state.next_message_id;

        let span = tracing::span!(Level::INFO, "delegate_request_vote");
        let _enter = span.enter();

        let persistence_task = PersistenceTaskType::AppendVote{id: message_id, term: request_vote_request.term, candidate_id: request_vote_request.candidate_id, parent_span: Span::current()};
        shared_state.persistence_work.push(persistence_task);
        shared_state.term_votes.insert(request_vote_request.term, request_vote_request.candidate_id);
        shared_state.next_message_id = message_id+1;
        let message_type = OutstandingMessageType::RequestVote{callback};
        let response_span = tracing::span!(Level::INFO, "request_vote_outstanding_message_processing");
        response_span.set_parent(span.context());
        let outstanding_message = OutstandingMessage{
            id: message_id,
            outstanding_responses: 1,
            message_type,
            span: response_span,
        };
        shared_state.outstanding_messages.insert(message_id, outstanding_message);
        RaftMessageStateChange::None
    }
}