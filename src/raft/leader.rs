use std::sync::Arc;
use crossbeam_queue::SegQueue;
use prost::bytes::{BufMut, Bytes};
use rand::Rng;
use rand::rngs::ThreadRng;
use sbe_kraft_replication_schema::command_codec::CommandEncoder;
use sbe_kraft_replication_schema::log_entry_codec::LogEntryEncoder;
use sbe_kraft_replication_schema::{message_header_codec, WriteBuf};
use sbe_kraft_replication_schema::command_type::CommandType;
use tokio::sync::oneshot;
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::persistence::worker::{PersistenceResponseType, PersistenceTaskType};
use crate::quorum::worker::{LocalQuorumWorkerTask, LocalQuorumTaskResponseType, LocalQuorumWorkerTaskType};
use crate::raft::candidate::RaftCandidateStateDelegate;
use crate::raft::follower::RaftFollowerStateDelegate;
use crate::raft::raft_sm::{LocalAppendEntries, LocalAppendEntriesCallbackResponse, OutstandingMessage, OutstandingMessageType, RaftMessageStateChange, RaftNodeType, RaftProtocol, LocalRaftResponseMessage, LocalRaftResponsePayload, RaftServerState, RaftVolatileState, LocalRaftWriteBatchRequest, LocalRaftWriteBatchResponse, LocalRequestVoteRequest, SharedState, TermVote};
use crate::server::raftproto::{RemoteAppendEntriesRequest, RemoteLogEntry, RemotePutRequest};
use crate::service_utils::app_time::now_millis;
use crate::service_utils::storage_utils::{SerializationData, serialize_data};

pub struct RaftLeaderStateDelegate {
    election_timeout: u128,
    rng: ThreadRng,
    initialized: bool,
}

fn clone_subset<T>(data: &[Arc<T>], start: usize, end: usize) -> Vec<Arc<T>> {
    data[start..end]
        .iter()
        .map(Arc::clone) // increments Arc ref count, not deep copy
        .collect()
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
}

impl RaftProtocol for RaftLeaderStateDelegate {
    fn init(&self) {
        todo!()
    }
    fn get_node_type(&self) -> RaftNodeType {
        RaftNodeType::Leader
    }

    fn write_batch(&mut self, write_batch_request: LocalRaftWriteBatchRequest, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) {
        let message_id = shared_state.next_message_id;
        let mut idx = shared_state.volatile_server_state.next_log_index;
        log::info!("leader handling writing batches {} as message id: {} with index: {}", write_batch_request.requests.len(), message_id, idx);
        tracing::info!("writing batches {} as message id: {} with index: {}", write_batch_request.requests.len(), message_id, idx);
        shared_state.next_message_id += 1;

        let span = tracing::span!(Level::INFO, "delegate_write_batch");
        let _enter = span.enter();
        // TODO validate request
        // TODO serialize valid request bits
        let buffer = vec![0u8; shared_state.state_machine_config.max_message_size_bytes];
        let offset = 0usize;

        let serialization_data = SerializationData{
            index: idx,
            term: shared_state.server_state.current_term,
            timestamp: now_millis() as u64,
            prev_index: shared_state.volatile_server_state.last_log_index,
            prev_term: shared_state.volatile_server_state.last_log_term,
            message_id,
            requests: write_batch_request.requests
        };
        let (limit, mut data) = serialize_data(&serialization_data, buffer, offset);
        tracing::info!("serialized data size: {}", limit);
        data.truncate(limit);
        if idx == 0 {
            shared_state.volatile_server_state.replication_log_term_starts.insert(shared_state.server_state.current_term, shared_state.volatile_server_state.replication_log.len() as u64);
        }
        shared_state.volatile_server_state.replication_log.push(Arc::new(serialization_data));

        // TODO no clone.
        let persistence_task = PersistenceTaskType::AppendLog{id: message_id, data: data.clone(), parent_span: Span::current(), request_id: message_id};
        shared_state.persistence_work.push(persistence_task);

        // TODO log entry command should be just bytes
        shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
            let le = RemoteLogEntry {
                index: idx,
                data: data.clone(), // TODO maybe fundamental problem with the approach, doesn't support IO without copies...
                batch_index: idx,
                term: shared_state.server_state.current_term,
                prev_log_index: shared_state.volatile_server_state.last_log_index,
                prev_log_term: shared_state.volatile_server_state.last_log_term,
                message_id: message_id,
            };
            let req = RemoteAppendEntriesRequest {
                term: shared_state.server_state.current_term,
                leader_id: shared_state.server_state.leader_id,
                entries: vec![],
                prev_log_index: shared_state.volatile_server_state.last_log_index,
                prev_log_term: shared_state.volatile_server_state.last_log_term,
                request_id: message_id,
                entry: Some(le)
            };

            let quorum_span = Span::current();
            let task = LocalQuorumWorkerTaskType::AppendEntries{
                id: message_id,
                parent_span: quorum_span,
                req,
            };
            task_queue.push(LocalQuorumWorkerTask {task_type: task});
        });
        tracing::info!("tasks dispatched");

        let message_type = OutstandingMessageType::WriteBatch{persistence_done: false, quorum_acks: 0, callback, index: idx};
        //let response_span = tracing::span!(Level::INFO, "write_batch_outstanding_message_processing");
        //response_span.set_parent(span.context());
        let outstanding_message = OutstandingMessage{
            id: message_id,
            outstanding_responses: 1 + shared_state.quorum_worker_tasks.len() as i32,
            message_type,
            span: Span::current()
        };
        shared_state.outstanding_messages.insert(message_id, outstanding_message);
        // TODO write log entry to volatile state replication log for backfills.

        shared_state.volatile_server_state.last_log_term = shared_state.server_state.current_term;
        shared_state.volatile_server_state.next_log_index = idx +1;
        shared_state.volatile_server_state.last_log_index = idx;
    }

    fn append_entries(&mut self, append_entries_request: LocalAppendEntries, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {

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

            shared_state.volatile_server_state.replication_log_term_starts.insert(shared_state.server_state.current_term, shared_state.volatile_server_state.replication_log.len() as u64);

            shared_state.quorum_worker_tasks.iter().for_each(|task_queue| {
                let task = LocalQuorumWorkerTaskType::StopHeartbeats{ id: message_id };
                task_queue.push(LocalQuorumWorkerTask {task_type: task});
            });
            if let Some(entry) = append_entries_request.entry {
                shared_state.volatile_server_state.last_log_index = entry.index;
                shared_state.volatile_server_state.last_log_term = entry.term;
                log::debug!("updating last log term {} and index {}", entry.term, entry.index);
            }
            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::Ok)
                }
            ).expect("sending append_entries callback failed");
            return RaftMessageStateChange::Follower(Box::new(RaftFollowerStateDelegate::new()));

        } else {
            tracing::info!("Received append_entries request from lower term candidate: {}, {}", append_entries_request.term, append_entries_request.leader_id);
            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::AppendEntries(LocalAppendEntriesCallbackResponse::UnrecognizedLeader)
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
                let task = LocalQuorumWorkerTaskType::StartHeartbeats{
                    id: message_id,
                    term: shared_state.server_state.current_term,
                    prev_term: shared_state.volatile_server_state.last_log_term,
                    prev_log_index: shared_state.volatile_server_state.last_log_index
                };
                task_queue.push(LocalQuorumWorkerTask {task_type: task});
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
                            OutstandingMessageType::WriteBatch{persistence_done, quorum_acks, callback, index} => {
                                if quorum_acks >= shared_state.quorum_size {
                                    // Persistence done and replicated to quorum of nodes
                                    log::info!("Write batch with message id {} has been persisted on quorum of nodes", id);
                                    shared_state.volatile_server_state.commit_index = index;
                                    let r = LocalRaftWriteBatchResponse{
                                        responses: vec![], // TODO
                                        err: None
                                    };
                                    let payload = LocalRaftResponsePayload::WriteBatch(r);
                                    let response = LocalRaftResponseMessage {payload};
                                    callback.send(response).unwrap();
                                } else {
                                    msg.outstanding_responses = new_outstanding_resp;
                                    // TODO maybe not recreate this all of the time.
                                    let new_state = OutstandingMessageType::WriteBatch { persistence_done: true, quorum_acks, callback, index};
                                    msg.message_type = new_state;
                                    shared_state.outstanding_messages.insert(id, msg);
                                }
                            }

                            OutstandingMessageType::RequestVote{callback} => {
                                let payload = LocalRaftResponsePayload::RequestVote(true);
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
                PersistenceResponseType::VotePersisted { id, term, candidate_id } => {
                    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
                        let _entered = msg.span.enter();
                        match msg.message_type {
                            OutstandingMessageType::RequestVote{callback} => {
                                let payload = LocalRaftResponsePayload::RequestVote(true);
                                let response = LocalRaftResponseMessage {payload};
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

                _ => {
                    log::error!("Persistence event not valid in current state");
                }
            }
        }

        while let Some(task) = shared_state.quorum_response.pop() {
            match task.response_type {
                LocalQuorumTaskResponseType::AppendEntries { id, ok } => {
                    if let Some(mut msg) = shared_state.outstanding_messages.remove(&id) {
                        let span = tracing::span!(Level::INFO, "outstanding_message_quorum_append_entries_processing");
                        span.set_parent(msg.span.context());
                        let _entered = span.enter();
                        tracing::event!(Level::INFO, "handling quorum append entries");
                        let new_outstanding_resp = msg.outstanding_responses-1;

                        match msg.message_type {
                            OutstandingMessageType::WriteBatch{persistence_done, quorum_acks, callback, index} => {
                                let new_acks = quorum_acks +1;
                                if new_acks >= shared_state.quorum_size && persistence_done {
                                    // Log replication done
                                    log::info!("Write batch with message id {} has been persisted on quorum of nodes", id);
                                    shared_state.volatile_server_state.commit_index = index;
                                    let r = LocalRaftWriteBatchResponse{
                                        responses: vec![], // TODO
                                        err: None
                                    };
                                    let payload = LocalRaftResponsePayload::WriteBatch(r);
                                    let response = LocalRaftResponseMessage {payload};
                                    callback.send(response).unwrap();
                                } else if new_outstanding_resp > 0 {
                                    msg.outstanding_responses = new_outstanding_resp;
                                    let new_state = OutstandingMessageType::WriteBatch { persistence_done, quorum_acks: new_acks, callback, index};
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
                        log::warn!("Received quorum event with no outstanding message registered {}, maybe backfill", id);
                    }
                }
                LocalQuorumTaskResponseType::BackfillLog { quorum_node_id, prev_term, prev_index} => {
                    log::info!("Received backfill log request for member {}, prev_term: {}, prev_index: {}", quorum_node_id, prev_term, prev_index);
                    let start_index = if prev_term == 0 && prev_index == 0 {
                        0
                    } else {
                        // TODO slice index starts at 118923 but ends at 118921. Happens if a previous leader which is ahead asks for backfill.
                        // https://trello.com/c/2AEY4e8E/2-bug-fix-issue-when-a-member-who-doesnt-have-the-highest-log-index-is-elected-leader
                        shared_state.volatile_server_state.replication_log_term_starts.get(&prev_term).unwrap().clone() + prev_index + 1
                    };
                    let slice = shared_state.volatile_server_state.replication_log.as_slice();
                    let data_slice = clone_subset(&slice, start_index as usize, slice.len()); // TODO maybe something with prev index
                    let quorum_node_index = if quorum_node_id > shared_state.identity as u64 {
                        quorum_node_id - 2
                    } else {
                        quorum_node_id - 1
                    };
                    let queue = shared_state.quorum_worker_tasks.get(quorum_node_index as usize).unwrap();
                    queue.push(LocalQuorumWorkerTask{task_type: LocalQuorumWorkerTaskType::BackfillLog{
                        data: data_slice,
                    }});
                }
                LocalQuorumTaskResponseType::RequestVoteResponse { id, term, received_vote} => {
                    log::info!("Received vote response for term: {} while already leader for term: {} received_vote: {}", term, shared_state.server_state.current_term, received_vote);
                }

                _ => {

                    log::error!("Received invalid quorum response message in current state");
                }
            }
        }

        return RaftMessageStateChange::None;
    }

    fn request_vote(&mut self, request_vote_request: LocalRequestVoteRequest, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange {
        if !self.should_accept_vote(request_vote_request.term, request_vote_request.last_log_term, request_vote_request.last_log_index, shared_state) {
            callback.send(
                LocalRaftResponseMessage {
                    payload: LocalRaftResponsePayload::RequestVote(false)
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