use std::sync::Arc;
use std::sync::atomic::Ordering;
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
    pub(crate) initialized: bool,
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

    fn send_truncate_signal(&self, shared_state: &mut SharedState, quorum_node_id: u64, prev_term: u64, last_index_for_term: u64) {
        log::info!("Sending truncate signal to member {} for term {} index {}", quorum_node_id, prev_term, last_index_for_term);

        let quorum_node_index = if quorum_node_id == 0 {
            0
        } else if quorum_node_id > shared_state.identity as u64 {
            quorum_node_id.saturating_sub(2)
        } else {
            quorum_node_id.saturating_sub(1)
        };

        if let Some(queue) = shared_state.quorum_worker_tasks.get(quorum_node_index as usize) {
            let message_id = shared_state.next_message_id;
            shared_state.next_message_id += 1;

            let task = LocalQuorumWorkerTaskType::TruncateLog{
                id: message_id,
                prev_term,
                prev_index: last_index_for_term,
                parent_span: tracing::Span::current(),
            };
            queue.push(LocalQuorumWorkerTask {task_type: task});
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
        let batch_size = write_batch_request.requests.len();
        let start_time = std::time::Instant::now();

        log::info!("leader handling writing batches {} as message id: {} with index: {}", batch_size, message_id, idx);
        tracing::info!("writing batches {} as message id: {} with index: {}", batch_size, message_id, idx);
        shared_state.next_message_id += 1;

        let span = tracing::span!(Level::INFO, "delegate_write_batch");
        let _enter = span.enter();
        // TODO validate request
        // TODO serialize valid request bits
        let buffer = vec![0u8; shared_state.state_machine_config.max_message_size_bytes];
        let offset = 0usize;

        if write_batch_request.requests.is_empty() {
            log::warn!("Received empty write batch request, ignoring");
            let r = LocalRaftWriteBatchResponse{
                responses: vec![],
                err: Some("Empty write batch request".to_string())
            };
            let payload = LocalRaftResponsePayload::WriteBatch(r);
            let response = LocalRaftResponseMessage {payload};
            callback.send(response).unwrap();
            return;
        }

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
        let arc_serialization_data = Arc::new(serialization_data);
        shared_state.volatile_server_state.replication_log.push(Arc::clone(&arc_serialization_data));
        shared_state.log_entry_bus.broadcast(arc_serialization_data);

        // TODO no clone.
        let d = Arc::new(data.clone());
        let persistence_task = PersistenceTaskType::AppendLog{id: message_id, data: d, parent_span: Span::current(), request_id: message_id};
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
                entry: Some(le),
                commit_index: shared_state.volatile_server_state.commit_index,
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

        let message_type = OutstandingMessageType::WriteBatch{persistence_done: false, quorum_acks: 0, callback, index: idx, start_time, batch_size};
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

        if !self.initialized {
            let message_id = shared_state.next_message_id;
            shared_state.next_message_id += 1;
            shared_state.volatile_server_state.next_log_index = 0;
            shared_state.volatile_server_state.commit_index = 0;

            shared_state.volatile_server_state.commit_state.commit_index.store(0, Ordering::Release);
            shared_state.volatile_server_state.commit_state.term.store(shared_state.server_state.current_term, Ordering::Release);
            shared_state.volatile_server_state.commit_state.version.fetch_add(1, Ordering::Release);

            // Update metrics for leader initialization
            crate::metrics::update_raft_state_metrics(
                shared_state.volatile_server_state.commit_index,
                shared_state.server_state.leader_id,
                shared_state.server_state.current_term,
                shared_state.volatile_server_state.replication_log.len()
            );

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
                            OutstandingMessageType::WriteBatch{persistence_done, quorum_acks, callback, index, start_time, batch_size} => {
                                if quorum_acks >= shared_state.quorum_size {
                                    // Persistence done and replicated to quorum of nodes
                                    log::info!("Write batch with message id {} has been persisted on quorum of nodes", id);
                                    shared_state.volatile_server_state.commit_index = index;
                                    shared_state.volatile_server_state.commit_state.commit_index.store(index, Ordering::Release);
                                    shared_state.volatile_server_state.commit_state.term.store(shared_state.server_state.current_term, Ordering::Release);
                                    shared_state.volatile_server_state.commit_state.version.fetch_add(1, Ordering::Release);

                                    // Update Raft state metrics
                                    crate::metrics::update_raft_state_metrics(
                                        shared_state.volatile_server_state.commit_index,
                                        shared_state.server_state.leader_id,
                                        shared_state.server_state.current_term,
                                        shared_state.volatile_server_state.replication_log.len()
                                    );

                                    // Record metrics for completed write batch (in microseconds)
                                    let duration = start_time.elapsed();
                                    crate::metrics::record_write_batch_completion(duration.as_micros());
                                    crate::metrics::WRITE_BATCH_SIZE.observe(batch_size as f64);
                                    crate::metrics::WRITE_BATCH_TOTAL.inc();

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
                                    let new_state = OutstandingMessageType::WriteBatch { persistence_done: true, quorum_acks, callback, index, start_time, batch_size};
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
                            OutstandingMessageType::WriteBatch{persistence_done, quorum_acks, callback, index, start_time, batch_size} => {
                                let new_acks = quorum_acks +1;
                                if new_acks >= shared_state.quorum_size && persistence_done {
                                    // Log replication done
                                    log::info!("Write batch with message id {} has been persisted on quorum of nodes", id);
                                    if index > shared_state.volatile_server_state.commit_index {
                                        shared_state.volatile_server_state.commit_index = index;
                                        shared_state.volatile_server_state.commit_state.commit_index.store(index, Ordering::Release);
                                        shared_state.volatile_server_state.commit_state.term.store(shared_state.server_state.current_term, Ordering::Release);
                                        shared_state.volatile_server_state.commit_state.version.fetch_add(1, Ordering::Release);
                                    }

                                    // Update Raft state metrics
                                    crate::metrics::update_raft_state_metrics(
                                        shared_state.volatile_server_state.commit_index,
                                        shared_state.server_state.leader_id,
                                        shared_state.server_state.current_term,
                                        shared_state.volatile_server_state.replication_log.len()
                                    );

                                    // Record metrics for completed write batch (in microseconds)
                                    let duration = start_time.elapsed();
                                    crate::metrics::record_write_batch_completion(duration.as_micros());
                                    crate::metrics::WRITE_BATCH_SIZE.observe(batch_size as f64);
                                    crate::metrics::WRITE_BATCH_TOTAL.inc();

                                    let r = LocalRaftWriteBatchResponse{
                                        responses: vec![], // TODO
                                        err: None
                                    };
                                    let payload = LocalRaftResponsePayload::WriteBatch(r);
                                    let response = LocalRaftResponseMessage {payload};
                                    callback.send(response).unwrap();
                                } else if new_outstanding_resp > 0 {
                                    msg.outstanding_responses = new_outstanding_resp;
                                    let new_state = OutstandingMessageType::WriteBatch { persistence_done, quorum_acks: new_acks, callback, index, start_time, batch_size};
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
                        // Check if we have entries for the requested term
                        match shared_state.volatile_server_state.replication_log_term_starts.get(&prev_term) {
                            Some(term_start) => {
                                // Calculate the end index for this term by looking at the next term's start
                                let term_end = {
                                    let mut next_term_start = shared_state.volatile_server_state.replication_log.len() as u64;
                                    for (&term, &start) in &shared_state.volatile_server_state.replication_log_term_starts {
                                        if term > prev_term && start < next_term_start {
                                            next_term_start = start;
                                        }
                                    }
                                    next_term_start
                                };

                                let requested_absolute_index = term_start + prev_index;

                                // Check if we have the requested entry for this term
                                if requested_absolute_index < term_end {
                                    term_start + prev_index + 1
                                } else {
                                    // We don't have entries up to the requested index for this term
                                    // Find the last entry we have for this term and send truncate signal
                                    let last_index_for_term = if term_end > *term_start { term_end - term_start - 1 } else { 0 };
                                    log::info!("Leader doesn't have entries up to index {} for term {}, last available index for term: {}", prev_index, prev_term, last_index_for_term);
                                    self.send_truncate_signal(shared_state, quorum_node_id, prev_term, last_index_for_term);
                                    continue;
                                }
                            }
                            None => {
                                // We don't have any entries for the requested term
                                log::info!("Leader doesn't have any entries for term {}, sending truncate signal", prev_term);
                                self.send_truncate_signal(shared_state, quorum_node_id, prev_term, 0);
                                continue;
                            }
                        }
                    };

                    let slice = shared_state.volatile_server_state.replication_log.as_slice();
                    log::info!("Backfilling from index: {} to {}", start_index, slice.len());
                    let data_slice = clone_subset(&slice, start_index as usize, slice.len());

                    let quorum_node_index = if quorum_node_id == 0 {
                        0
                    } else if quorum_node_id > shared_state.identity as u64 {
                        quorum_node_id.saturating_sub(2)
                    } else {
                        quorum_node_id.saturating_sub(1)
                    };
                    let queue = shared_state.quorum_worker_tasks.get(quorum_node_index as usize).unwrap();
                    queue.push(LocalQuorumWorkerTask{task_type: LocalQuorumWorkerTaskType::BackfillLog{
                        data: data_slice,
                    }});
                }
                LocalQuorumTaskResponseType::RequestVoteResponse { id, term, received_vote} => {
                    log::info!("Received vote response for term: {} while already leader for term: {} received_vote: {}", term, shared_state.server_state.current_term, received_vote);
                }
                LocalQuorumTaskResponseType::TruncateLogResponse { id, ok } => {
                    log::info!("Received truncate log response for message id: {}, ok: {}", id, ok);
                    // The follower has truncated its log and should now request backfill again
                    // No additional action needed from leader side
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
    use log::info;
    use crate::quorum::worker::LocalQuorumResponse;
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
                commit_state: Arc::new(CommitState{
                    commit_index: AtomicU64::new(0),
                    version: AtomicU64::new(0),
                    term: AtomicU64::new(0),
                })
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
            log_entry_bus: Bus::new(5000)
        }
    }

    #[tokio::test]
    async fn test_append_entries_recognize_new_leader() {
        let mut delegate = RaftLeaderStateDelegate::new();
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
        assert!(matches!(result, RaftMessageStateChange::Follower(..)));
    }

    #[tokio::test]
    async fn test_request_vote_rejects_invalid_vote() {
        let mut delegate = RaftLeaderStateDelegate::new();
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
    async fn test_backfill_request_sends_truncate_signal_when_leader_missing_entries() {
        let mut delegate = RaftLeaderStateDelegate::new();
        delegate.initialized = true;
        let mut shared_state = create_shared_state();

        // Initialize as leader for term 3
        shared_state.server_state.current_term = 3;
        shared_state.server_state.leader_id = 0;
        shared_state.volatile_server_state.next_term = 4;

        // Leader only has entries for term 2 up to index 2 (term_start=10, so absolute indices 10,11,12)
        shared_state.volatile_server_state.replication_log_term_starts.insert(2, 10);
        shared_state.volatile_server_state.replication_log_term_starts.insert(3, 13);

        // Add some dummy log entries
        for _ in 0..15 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: 2,
                timestamp: 0,
                prev_index: 0,
                prev_term: 0,
                index: 0,
                message_id: 0,
            }));
        }

        // Set up quorum worker task queues
        shared_state.quorum_worker_tasks = vec![Arc::new(SegQueue::new()); 3];

        // Follower requests backfill for term 2 index 5 (but leader only has up to index 2)
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                prev_term: 2,
                prev_index: 5, // Leader doesn't have this index for term 2
            }
        });

        let result = delegate.time_step(&mut shared_state);

        // Should remain leader
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should have sent a truncate signal
        let queue = &shared_state.quorum_worker_tasks[0]; // quorum_node_id 1 maps to index 0
        assert_eq!(queue.len(), 1);

        if let Some(task) = queue.pop() {
            match task.task_type {
                LocalQuorumWorkerTaskType::TruncateLog { id, prev_term, prev_index, .. } => {
                    assert_eq!(prev_term, 2);
                    assert_eq!(prev_index, 2); // Last index leader has for term 2
                    assert!(id > 0); // Should have a valid message ID
                }
                _ => panic!("Expected TruncateLog task"),
            }
        } else {
            panic!("Expected task in queue");
        }
    }

    #[tokio::test]
    async fn test_backfill_request_sends_truncate_for_unknown_term() {
        let mut delegate = RaftLeaderStateDelegate::new();
        delegate.initialized = true;
        let mut shared_state = create_shared_state();

        // Initialize as leader for term 3
        shared_state.server_state.current_term = 3;
        shared_state.server_state.leader_id = 0;

        // Leader only has entries for term 3, no entries for term 2
        shared_state.volatile_server_state.replication_log_term_starts.insert(3, 0);

        // Set up quorum worker task queues
        shared_state.quorum_worker_tasks = vec![Arc::new(SegQueue::new()); 3];

        // Follower requests backfill for unknown term 2
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                prev_term: 2,
                prev_index: 3,
            }
        });

        let result = delegate.time_step(&mut shared_state);

        // Should remain leader
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should have sent a truncate signal with index 0 for unknown term
        let queue = &shared_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        if let Some(task) = queue.pop() {
            match task.task_type {
                LocalQuorumWorkerTaskType::TruncateLog { prev_term, prev_index, .. } => {
                    assert_eq!(prev_term, 2);
                    assert_eq!(prev_index, 0); // Should send 0 for unknown term
                }
                _ => panic!("Expected TruncateLog task"),
            }
        }
    }

    #[tokio::test]
    async fn test_backfill_request_succeeds_when_leader_has_entries() {
        let mut delegate = RaftLeaderStateDelegate::new();
        delegate.initialized = true;
        let mut shared_state = create_shared_state();

        // Initialize as leader
        shared_state.server_state.current_term = 3;
        shared_state.server_state.leader_id = 0;

        // Leader has entries for term 2 up to index 5
        shared_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        shared_state.volatile_server_state.replication_log_term_starts.insert(3, 6);

        // Add log entries
        for i in 0..10 {
            shared_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: if i < 6 { 2 } else { 3 },
                timestamp: 0,
                prev_index: 0,
                prev_term: 0,
                index: i,
                message_id: i,
            }));
        }

        // Set up quorum worker task queues
        shared_state.quorum_worker_tasks = vec![Arc::new(SegQueue::new()); 3];

        // Follower requests backfill for term 2 index 3 (leader has this)
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                prev_term: 2,
                prev_index: 3,
            }
        });

        let result = delegate.time_step(&mut shared_state);

        // Should remain leader
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should have sent backfill data, not truncate
        let queue = &shared_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        if let Some(task) = queue.pop() {
            match task.task_type {
                LocalQuorumWorkerTaskType::BackfillLog { data } => {
                    assert!(data.len() > 0); // Should have some data to backfill
                }
                _ => panic!("Expected BackfillLog task"),
            }
        }
    }

    #[tokio::test]
    async fn test_truncate_log_response_handling() {
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state();

        // Leader receives truncate log response
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::TruncateLogResponse {
                id: 123,
                ok: true,
            }
        });

        let result = delegate.time_step(&mut shared_state);

        // Should remain leader and handle response gracefully
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[tokio::test]
    async fn test_write_batch_happy_path() {
        // Test that write_batch correctly appends a batch and dispatches tasks
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state();
        let (tx, rx) = oneshot::channel();

        // Prepare a write batch request
        let put_request = RemotePutRequest {
            id: "put_1".to_string(),
            payload: "payload".to_string(),
            node_id: 1,
        };
        let write_batch_request = LocalRaftWriteBatchRequest {
            requests: vec![put_request.clone()],
        };

        // Add a dummy quorum worker task queue
        shared_state.quorum_worker_tasks.push(Arc::new(SegQueue::new()));

        // Call write_batch
        delegate.write_batch(write_batch_request, tx, &mut shared_state);

        // Check that the replication log was updated
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 1);
        let entry = &shared_state.volatile_server_state.replication_log[0];
        assert_eq!(entry.index, 1); // Indexes start at 1
        assert_eq!(entry.term, shared_state.server_state.current_term);
        assert_eq!(entry.requests.len(), 1);
        assert_eq!(entry.requests[0].id, "put_1");

        // Check that outstanding_messages contains the new message
        assert!(shared_state.outstanding_messages.contains_key(&(shared_state.next_message_id - 1)));

        // Check that persistence_work contains the append log task
        let found = shared_state.persistence_work.pop().is_some();
        assert!(found, "Persistence task should be queued");

        // Check that a quorum worker task was dispatched
        let queue = &shared_state.quorum_worker_tasks[0];
        let found = queue.pop().is_some();
        assert!(found, "Quorum worker task should be queued");
    }

    #[tokio::test]
    async fn test_write_batch_empty_requests_failure() {
        // Test that write_batch does not append when requests are empty
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state();
        let (tx, _rx) = oneshot::channel();

        // Prepare an empty write batch request
        let write_batch_request = LocalRaftWriteBatchRequest {
            requests: vec![],
        };

        // Add a dummy quorum worker task queue
        shared_state.quorum_worker_tasks.push(Arc::new(SegQueue::new()));

        // Call write_batch
        delegate.write_batch(write_batch_request, tx, &mut shared_state);

        // Check that the replication log was still updated (current implementation does not reject empty)
        assert_eq!(shared_state.volatile_server_state.replication_log.len(), 0);
    }


    fn create_shared_state_with_quorum() -> SharedState {
        let mut state = SharedState {
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
                commit_state: Arc::new(CommitState{
                    commit_index: AtomicU64::new(0),
                    term: AtomicU64::new(0),
                    version: AtomicU64::new(0),
                })
            },
            next_message_id: 1,
            outstanding_messages: std::collections::HashMap::new(),
            quorum_size: 2,
            quorum_worker_tasks: vec![Arc::new(SegQueue::new()), Arc::new(SegQueue::new())],
            persistence_work: Arc::new(SegQueue::new()),
            persistence_response: Arc::new(SegQueue::new()),
            quorum_work: Arc::new(SegQueue::new()),
            quorum_response: Arc::new(SegQueue::new()),
            identity: 0,
            term_votes: std::collections::HashMap::new(),
            state_machine_config: StateMachineConfig { max_message_size_bytes: 2048},
            log_entry_bus: Bus::new(5000)
        };
        state
    }

    #[test]
    fn test_time_step_initializes_leader_and_sends_heartbeats() {
        // Test that time_step initializes the leader and sends heartbeats
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state_with_quorum();

        // Initially not initialized
        assert!(!delegate.initialized);

        // Call time_step
        let result = delegate.time_step(&mut shared_state);

        // Should be initialized now
        assert!(delegate.initialized);

        // Should have sent heartbeats to all quorum worker tasks
        for queue in &shared_state.quorum_worker_tasks {
            let task = queue.pop().unwrap();
            match task.task_type {
                LocalQuorumWorkerTaskType::StartHeartbeats { id, term, prev_term, prev_log_index } => {
                    assert_eq!(id, 1);
                    assert_eq!(term, 1);
                    assert_eq!(prev_term, 1);
                    assert_eq!(prev_log_index, 0);
                }
                _ => panic!("Expected StartHeartbeats task"),
            }
        }

        // Should return None state change
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[test]
    fn test_time_step_handles_log_persisted_and_quorum_ack_happy_path() {
        // Test that time_step handles LogPersisted and quorum ack correctly
        let mut delegate = RaftLeaderStateDelegate::new();
        let mut shared_state = create_shared_state_with_quorum();

        // Simulate an outstanding write batch message
        let (callback_tx, mut callback_rx) = oneshot::channel();
        let msg_id = shared_state.next_message_id;
        let outstanding = OutstandingMessage {
            id: msg_id,
            outstanding_responses: 2,
            message_type: OutstandingMessageType::WriteBatch {
                persistence_done: false,
                quorum_acks: 1,
                callback: callback_tx,
                index: 42,
                start_time: Instant::now(),
                batch_size: 0,
            },
            span: tracing::span!(Level::INFO, "test"),
        };
        shared_state.outstanding_messages.insert(msg_id, outstanding);

        shared_state.persistence_response.push(PersistenceResponseType::LogPersisted { id: msg_id });
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::AppendEntries { id: msg_id, ok: true }
        });

        let result = delegate.time_step(&mut shared_state);

        // Should have updated commit_index
        assert_eq!(shared_state.volatile_server_state.commit_index, 42);

        // Should have sent callback response
        let response = callback_rx.try_recv().unwrap();
        match response.payload {
            LocalRaftResponsePayload::WriteBatch(r) => {
                assert!(r.err.is_none());
            }
            _ => panic!("Expected WriteBatch response"),
        }

        // Should return None state change
        assert!(matches!(result, RaftMessageStateChange::None));
    }

    #[test]
    fn test_time_step_handles_backfill_log_request() {
        // Test that time_step handles BackfillLog request
        let mut delegate = RaftLeaderStateDelegate::new();
        delegate.initialized = true;
        let mut shared_state = create_shared_state_with_quorum();

        shared_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
            index: 1,
            term: 1,
            timestamp: 0,
            prev_index: 0,
            prev_term: 0,
            message_id: 1,
            requests: vec![],
        }));

        // Simulate BackfillLog event
        shared_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                prev_term: 0,
                prev_index: 0,
            }
        });

        let result = delegate.time_step(&mut shared_state);

        // Should have dispatched a BackfillLog task to the quorum worker
        let queue = &shared_state.quorum_worker_tasks[0];
        let task = queue.pop().unwrap();
        match task.task_type {
            LocalQuorumWorkerTaskType::BackfillLog { data } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0].index, 1);
                assert_eq!(data[0].term, 1);
                assert_eq!(data[0].requests.len(), 0);
                assert_eq!(data[0].message_id, 1);
                assert_eq!(data[0].prev_index, 0);
                assert_eq!(data[0].prev_term, 0);
            }
            v => panic!("Expected BackfillLog task"),
        }

        // Should return None state change
        assert!(matches!(result, RaftMessageStateChange::None));
    }
}