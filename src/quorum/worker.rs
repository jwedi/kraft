use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc, Condvar};
use std::thread::sleep;
use std::time::Duration;
use arc_swap::ArcSwap;
use crossbeam_queue::SegQueue;
use futures::{FutureExt, StreamExt, TryFuture};
use log::{error, info};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_zipkin::Propagator;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, oneshot};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::task::JoinSet;
use tonic::{Request, Response, Status};
use tonic::codegen::http::StatusCode;
use tonic::transport::{Channel};
use tracing::{Instrument, Level, Span};
use tracing::instrument::Instrumented;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::client::cluster_node_client::SharedGrpcChannel;
use crate::raft::raft_sm::{LocalAppendEntries, LocalAppendEntriesCallbackResponse, LocalRaftMessage, LocalRaftMessagePayload, LocalRaftResponseMessage, LocalRaftResponsePayload, LocalRaftWriteBatchRequest, LocalRequestVoteRequest};
use crate::server::raftproto::raft_client::RaftClient;
use crate::server::raftproto::{RemoteAppendEntriesAcknowledge, RemoteAppendEntriesRequest, RemoteAppendEntriesResponse, RemoteLogEntry, RemotePutBatchRequest, RemotePutBatchResponse, RemoteQuorumMessage, RemoteTruncateLogRequest, RemoteTruncateLogResponse, RemoteVoteRequest, RemoteVoteResponse};
use crate::server::raftproto::remote_quorum_message::MessagePayload;
use crate::service_utils::app_time::{now_millis, now_plus_duration_millis};
use crate::service_utils::storage_utils::{SerializationData, serialize_data, SerializedData};
use crate::service_utils::tracing_utils::{span_to_tracing_context, tracing_context_to_span};
use crate::transport::stream_manager::{StreamManager, StreamManagerImpl};
use crate::transport::stream_manager::raftproto::TracingContext;
use crate::transport::write_proxy::{WriteBatch, WriteResponse};

pub struct LocalQuorumWorkerTask {
    pub task_type: LocalQuorumWorkerTaskType
    // New append entries
    // Request vote
    // Start heartbeats
    // Stop heartbeats
}

pub enum LocalQuorumWorkerTaskType {
    StartHeartbeats{ id: u64, term: u64, prev_term: u64, prev_log_index: u64 },
    StopHeartbeats { id: u64 },
    RequestVote{ id: u64, term: u64, last_term: u64, last_index: u64, parent_span: Span}, // Term, last term, last index,
    AppendEntries{ id: u64, req: RemoteAppendEntriesRequest, parent_span: Span},
    BackfillLog {data: Vec<Arc<SerializationData>>},
    WriteBatch{ write_batch: WriteBatch },
    TruncateLog{ id: u64, prev_term: u64, prev_index: u64, parent_span: Span }
}

pub enum LocalQuorumTaskResponseType {
    RequestVoteResponse{ id: u64, term: u64, received_vote: bool},
    AppendEntries{ id: u64, ok: bool}, // TODO get next index
    BackfillLog{ quorum_node_id: u64, prev_term: u64, prev_index: u64},
    TruncateLog{ quorum_node_id: u64, prev_term: u64, prev_index: u64},
    TruncateLogResponse{ id: u64, ok: bool}
}

pub struct LocalQuorumResponse {
    pub response_type: LocalQuorumTaskResponseType
}

// Responsible for communication with other nodes in the cluster if this node is the leader.
// Sends AppendEntries to followers to achieve quorum and responds back on the response queue.
// Sends RequestVote when asked to.
// Sends AppendEntries heartbeats regardless to keep up constant load.
pub struct QuorumWorker {
    work_queue: Arc<SegQueue<LocalQuorumWorkerTask>>,
    response_queue: Arc<SegQueue<LocalQuorumResponse>>,
    send_heartbeats: bool,
    self_id: u32,
    next_index: u64,
    member_id: u64,
    member_endpoint: String,
    term: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    last_heartbeat: u128,
    propagator: Propagator,
    grpc_channel: SharedGrpcChannel,
    stream_manager: Arc<StreamManagerImpl>,
    task_queue: Arc<SegQueue<LocalRaftMessage>>,
    ongoing_backfill: Option<OngoingBackfill>,
    commit_index: u64,
    pending_write_batches: HashMap<String, PendingWriteBatch>,
    max_message_size_bytes: usize,
    pending_append_entries: HashMap<u64, std::time::Instant>,
}


#[derive(Debug)]
pub enum PendingQuorumTaskEnum {
    Vote(RemoteVoteRequest),
    AppendEntries{request_id: u64, log_index: u64, term: u64, parent_span: Span},
    WriteBatch{ batch_id: String, parent_span: Span}
}
#[derive(Debug)]
struct PendingQuorumTask {
    callback: Receiver<LocalRaftResponseMessage>,
    task: PendingQuorumTaskEnum,
    start_time: std::time::Instant,
}

struct OngoingBackfill {
    from_term: u64,
    from_index: u64,
}

struct PendingWriteBatch {
    callback: Sender<WriteResponse>,
    start_time: std::time::Instant,
}

impl QuorumWorker {
    pub fn new(
        work_queue: Arc<SegQueue<LocalQuorumWorkerTask>>,
        response_queue: Arc<SegQueue<LocalQuorumResponse>>,
        self_id: u32,
        next_index: u64,
        member_id: u64,
        member_endpoint: String,
        stream_manager: Arc<StreamManagerImpl>,
        task_queue: Arc<SegQueue<LocalRaftMessage>>,
        max_message_size_bytes: usize
    ) -> Self {
        Self {
            work_queue,
            response_queue,
            send_heartbeats: false,
            self_id,
            next_index,
            member_id,
            member_endpoint: member_endpoint.clone(),
            term: 0,
            prev_log_term: 0,
            prev_log_index: 0,
            last_heartbeat: 0,
            propagator: opentelemetry_zipkin::Propagator::new(),
            grpc_channel: SharedGrpcChannel::new(member_endpoint.as_str(), member_id as u32),
            stream_manager,
            task_queue,
            ongoing_backfill: None,
            commit_index: 0,
            pending_write_batches: HashMap::new(),
            max_message_size_bytes,
            pending_append_entries: HashMap::new(),
        }
    }



    pub async fn run(&mut self) {
        log::info!("Running quorum worker");
        let mut pending_tasks: Vec<PendingQuorumTask> = vec![];

        loop {
            let current_time = now_millis();
            let mut maybe_receive_stream = self.stream_manager.get_receive_stream(self.member_id as u32).await;
            let mut maybe_send_stream = self.stream_manager.get_send_stream(self.member_id as u32).await;

            if maybe_send_stream.is_none() {
                info!("Trying to connect towards: {}", self.member_id);
                let connect_result = self.stream_manager.try_connect(self.member_id as u32).await;
                match connect_result {
                    Ok(_) => {
                        log::info!("Send stream connect success towards: {}", self.member_endpoint);
                    }
                    Err(e) => {
                        log::error!("Send stream connect error {} towards: {}", e.stringify(), self.member_endpoint);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
                continue;
            }
            if maybe_receive_stream.is_none() {
                log::error!("Receive stream towards {} is none", self.member_endpoint);
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            let rec = maybe_receive_stream.take().unwrap();
            let sen = maybe_send_stream.take().unwrap();

            let mut receive_stream = rec.lock().await;
            let mut send_stream = sen.lock().await;

            let mut pending_remove = vec![];
            for i in 0..pending_tasks.len() {
                let mut maybe_pt = pending_tasks.get_mut(i);
                match maybe_pt {
                    None => {
                        pending_remove.push(i)
                    }
                    Some(pt) => {
                        let resp = pt.callback.try_recv();
                        match resp {
                            Ok(val) => {
                                match val.payload {
                                    LocalRaftResponsePayload::RequestVote(rv) => {
                                        match &pt.task {
                                            PendingQuorumTaskEnum::Vote(task) => {
                                                send_stream.send(
                                                    RemoteQuorumMessage{
                                                        message_payload: Some(MessagePayload::VoteResponseOption(RemoteVoteResponse{vote_granted: rv, request_id: task.request_id, term: task.term})),
                                                        tracing_context: None
                                                    }
                                                ).expect("sending on stream shouldn't fail");
                                            }
                                            default => {
                                                log::error!("unexpected pending task type for request vote")
                                            }
                                        }
                                    }
                                    LocalRaftResponsePayload::AppendEntries(ae) => {

                                        match &pt.task {
                                            PendingQuorumTaskEnum::AppendEntries{request_id, log_index, term, parent_span} => {
                                                let span = tracing::span!(Level::INFO, "processing_append_entries_response");
                                                span.set_parent(parent_span.context());
                                                let _enter = span.enter();
                                                tracing::info!("processing append entries response from member {} as self id {}", self.member_id, self.self_id);

                                                let tracing_context = span_to_tracing_context(&span.context(), &self.propagator);
                                                match ae {
                                                    LocalAppendEntriesCallbackResponse::Ok => {
                                                        send_stream.send(
                                                            RemoteQuorumMessage {
                                                                message_payload: Some(MessagePayload::AppendEntriesAcknowledgeOption(RemoteAppendEntriesAcknowledge { last_log_index: *log_index, last_log_term: *term, request_id: *request_id, ok: true })),
                                                                tracing_context: Some(tracing_context),
                                                            }).expect("sending on stream shouldn't fail");
                                                    }
                                                    LocalAppendEntriesCallbackResponse::UnrecognizedLeader => {
                                                        send_stream.send(
                                                            RemoteQuorumMessage {
                                                                message_payload: Some(MessagePayload::AppendEntriesAcknowledgeOption(RemoteAppendEntriesAcknowledge { last_log_index: *log_index, last_log_term: *term, request_id: *request_id, ok: false })) ,
                                                                tracing_context: None
                                                            }
                                                        ).expect("sending on stream shouldn't fail");
                                                    }
                                                    LocalAppendEntriesCallbackResponse::WantedPreviousEntry { last_term, last_index } => {
                                                        send_stream.send(
                                                            RemoteQuorumMessage{
                                                                message_payload: Some(MessagePayload::AppendEntriesAcknowledgeOption(RemoteAppendEntriesAcknowledge{last_log_index: last_index, last_log_term: last_term, request_id: *request_id, ok: false})),
                                                                tracing_context: None
                                                            }
                                                        ).expect("sending on stream shouldn't fail");
                                                    }
                                                    LocalAppendEntriesCallbackResponse::TruncateLog { prev_term, prev_index } => {
                                                        log::info!("Received truncate log response from leader, truncating to term {} index {}", prev_term, prev_index);
                                                        let resp = LocalQuorumTaskResponseType::TruncateLog{quorum_node_id: self.member_id, prev_term, prev_index};
                                                        self.response_queue.push(LocalQuorumResponse {response_type: resp});
                                                        send_stream.send(
                                                            RemoteQuorumMessage{
                                                                message_payload: Some(MessagePayload::AppendEntriesAcknowledgeOption(RemoteAppendEntriesAcknowledge{last_log_index: prev_index, last_log_term: prev_term, request_id: *request_id, ok: false})),
                                                                tracing_context: None
                                                            }
                                                        ).expect("sending on stream shouldn't fail");
                                                    }
                                                }
                                            }
                                            default => {
                                                log::error!("unexpected pending task type for append entries")
                                            }
                                        } }
                                    LocalRaftResponsePayload::WriteBatch(write_resp) => {
                                        match &pt.task {
                                            PendingQuorumTaskEnum::WriteBatch{batch_id, parent_span} => {
                                                let span = tracing::span!(Level::INFO, "processing_write_batch_response");
                                                span.set_parent(parent_span.context());
                                                let _enter = span.enter();
                                                tracing::info!("processing write batch response from member {} as self id {}", self.member_id, self.self_id);

                                                let tracing_context = span_to_tracing_context(&span.context(), &self.propagator);
                                                send_stream.send(
                                                    RemoteQuorumMessage{
                                                        message_payload: Some(MessagePayload::PutBatchResponseOption(RemotePutBatchResponse{responses: write_resp.responses, batch_id: batch_id.clone()})),
                                                        tracing_context: Some(tracing_context)
                                                    }
                                                ).expect("sending on stream shouldn't fail");
                                            }
                                            default => {
                                                log::error!("unexpected pending task type for write batch")
                                            }
                                        }
                                    }
                                    unknown => {
                                        log::error!("unknown raft response payload")
                                    }
                                }
                                pending_remove.push(i)
                            }
                            Err(err) => {
                                match err {
                                    TryRecvError::Empty => {}
                                    TryRecvError::Closed => {
                                        pending_remove.push(i)
                                    }
                                }
                            }
                        }
                    }
                }

            }

            for (i, el) in pending_remove.iter().enumerate() {
                pending_tasks.remove(el - i);
            }

            let mut iterations = 0;
            let mut receive_error = false;
            let max_smart_batch = 32;
            while iterations < max_smart_batch { // This isn't really smart batching.
                iterations += 1;
                let result = receive_stream.message().now_or_never();
                match result {
                    Some(Ok(Some(val))) => {
                        let span = match val.tracing_context {
                            Some(ctx) => {

                                let parent_cx = tracing_context_to_span(&ctx, &self.propagator);
                                let s = tracing::span!(Level::INFO, "received_remote_quorum_message", member_id=self.member_id);
                                s.set_parent(parent_cx);
                                s
                            }
                            None => {
                                tracing::span!(Level::INFO, "received_quorum_message_no_context", member_id=self.member_id)
                            }
                        };
                        let _enter = span.enter();
                        match val.message_payload {
                            None => {
                                break;
                            }
                            Some(MessagePayload::VoteResponseOption(vote)) => {
                                log::info!("received vote response id: {}, member: {}, term: {}, granted: {}", vote.request_id, self.member_id, vote.term, vote.vote_granted);
                                let resp = LocalQuorumTaskResponseType::RequestVoteResponse{id: vote.request_id, term: vote.term, received_vote: vote.vote_granted};
                                self.response_queue.push(LocalQuorumResponse {response_type: resp})
                            }
                            Some(MessagePayload::AppendEntriesAcknowledgeOption(append_entries)) => {
                                if !append_entries.ok {
                                    log::info!("received append entries acknowledge from: {}, but it was not ok, backfilling log from term {} and index {}", self.member_id, append_entries.last_log_term, append_entries.last_log_index);
                                    if self.ongoing_backfill.is_none() {
                                        self.ongoing_backfill = Some(OngoingBackfill {from_term: append_entries.last_log_term, from_index: append_entries.last_log_index});
                                        let resp = LocalQuorumTaskResponseType::BackfillLog{quorum_node_id: self.member_id, prev_term: append_entries.last_log_term, prev_index: append_entries.last_log_index};
                                        self.response_queue.push(LocalQuorumResponse {response_type: resp})
                                    } else {
                                        log::warn!("Ongoing backfill already in progress, ignoring new backfill request");
                                    }
                                }

                                if append_entries.request_id != 0 {
                                    tracing::info!("received append entries acknowledge from: {}, ok: {}, request id: {}, member: {}", self.member_id, append_entries.ok, append_entries.request_id, self.member_id);

                                    if let Some(start_time) = self.pending_append_entries.remove(&append_entries.request_id) {
                                        let latency_micros = start_time.elapsed().as_micros() as f64;
                                        crate::metrics::QUORUM_APPEND_ENTRIES_LATENCY
                                            .with_label_values(&[&self.member_id.to_string()])
                                            .observe(latency_micros);
                                    }

                                    let resp = LocalQuorumTaskResponseType::AppendEntries{id: append_entries.request_id, ok: append_entries.ok};
                                    self.response_queue.push(LocalQuorumResponse {response_type: resp})

                                }
                            }
                            Some(MessagePayload::AppendEntriesRequestOption(mut append_entries)) => {
                                let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();
                                // TODO get all of the data from the log entry.

                                let index = append_entries.entry.as_ref().map(|entry| entry.index).unwrap_or_else(|| 0);
                                let term = append_entries.entry.as_ref().map(|entry| entry.term).unwrap_or_else(|| 0);
                                tracing::info!("received append entries request from {} as self {}, sending task to local state machine", self.member_id, self.self_id);
                                self.commit_index = append_entries.commit_index;

                                self.task_queue.push(LocalRaftMessage {
                                    payload: LocalRaftMessagePayload::AppendEntries(LocalAppendEntries { leader_id: append_entries.leader_id, term: append_entries.term, prev_term: append_entries.prev_log_term, prev_index: append_entries.prev_log_index, entries: append_entries.entries, request_id: append_entries.request_id, entry: append_entries.entry, commit_index: append_entries.commit_index}),
                                    callback: callback.0,
                                    parent_span: span.clone()

                                }, );

                                let pending_task = PendingQuorumTask {
                                    task: PendingQuorumTaskEnum::AppendEntries{request_id: append_entries.request_id, log_index: term, term: index, parent_span: span.clone()},
                                    callback: callback.1,
                                    start_time: std::time::Instant::now(),
                                };
                                pending_tasks.push(pending_task);
                            }
                            Some(MessagePayload::VoteRequestOption(vote_request)) => {
                                log::info!("received vote request from: {} for term: {}", self.member_id, vote_request.term);
                                // TODO what do we do with the callbacks? Do we add a list of receivers that the quorum worker polls each iteration or does the queue worker send a message instead of invoking a callback?
                                // Adding a list of pending callbacks is probably easiest since we don't need to re-write all state machines.
                                let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();

                                let span = tracing::span!(Level::INFO, "awaiting_vote_response");
                                self.task_queue.push(LocalRaftMessage {
                                    payload: LocalRaftMessagePayload::RequestVote(LocalRequestVoteRequest {term: vote_request.term, candidate_id: vote_request.candidate_id, last_log_index: vote_request.last_log_index, last_log_term: vote_request.last_log_term }),
                                    callback: callback.0,
                                    parent_span: span

                                }, );
                                let pending_task = PendingQuorumTask {
                                    task: PendingQuorumTaskEnum::Vote(vote_request),
                                    callback: callback.1,
                                    start_time: std::time::Instant::now(),
                                };
                                pending_tasks.push(pending_task);
                            }
                            Some(MessagePayload::PutBatchRequestOption(put_batch)) => {
                                let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();

                                tracing::info!("received put batch request from {} as self {}, sending task to local state machine", self.member_id, self.self_id);
                                let pending_task = PendingQuorumTask {
                                    task: PendingQuorumTaskEnum::WriteBatch{ batch_id: put_batch.batch_id.clone(), parent_span: span.clone()},
                                    callback: callback.1,
                                    start_time: std::time::Instant::now(),
                                };
                                pending_tasks.push(pending_task);

                                self.task_queue.push(LocalRaftMessage{
                                    payload: LocalRaftMessagePayload::WriteBatch(LocalRaftWriteBatchRequest{
                                        requests: put_batch.put_request,
                                    }),
                                    callback: callback.0,
                                    parent_span: span.clone()
                                });
                            }
                            Some(MessagePayload::PutBatchResponseOption(put_batch_response)) => {
                                let pending_batch = self.pending_write_batches.remove(&put_batch_response.batch_id);
                                match pending_batch {
                                    Some(batch) => {
                                        let latency_micros = batch.start_time.elapsed().as_micros() as f64;
                                        crate::metrics::QUORUM_PUT_BATCH_LATENCY
                                            .with_label_values(&[&self.member_id.to_string()])
                                            .observe(latency_micros);

                                        let _ = batch.callback.send(WriteResponse{ responses: put_batch_response.responses, status_code: StatusCode::OK});
                                    }
                                    None => {
                                        log::error!("No pending write batch found for batch id: {}", put_batch_response.batch_id);
                                    }
                                }
                            }
                            Some(MessagePayload::TruncateLogRequestOption(truncate_request)) => {
                                log::info!("Received truncate log request from leader {} for term {} index {}", truncate_request.leader_id, truncate_request.prev_term, truncate_request.prev_index);

                                // Send to local task queue for processing
                                let resp = LocalQuorumTaskResponseType::TruncateLog{
                                    quorum_node_id: self.member_id,
                                    prev_term: truncate_request.prev_term,
                                    prev_index: truncate_request.prev_index
                                };
                                self.response_queue.push(LocalQuorumResponse {response_type: resp});

                                // Send acknowledgment back to leader
                                send_stream.send(
                                    RemoteQuorumMessage{
                                        message_payload: Some(MessagePayload::TruncateLogResponseOption(RemoteTruncateLogResponse{
                                            request_id: truncate_request.request_id,
                                            ok: true
                                        })),
                                        tracing_context: None
                                    }
                                ).expect("sending truncate response on stream shouldn't fail");
                            }
                            Some(MessagePayload::TruncateLogResponseOption(truncate_response)) => {
                                log::info!("Received truncate log response for request {}, ok: {}", truncate_response.request_id, truncate_response.ok);
                                let resp = LocalQuorumTaskResponseType::TruncateLogResponse{
                                    id: truncate_response.request_id,
                                    ok: truncate_response.ok
                                };
                                self.response_queue.push(LocalQuorumResponse {response_type: resp});
                            }
                            Some(payload) => {
                                log::error!("Encountered unexpected receive stream message payload: {:?}", payload);
                            }
                        }
                    }
                    Some(Ok(None)) => {
                        break;
                    }
                    Some(Err(err)) => {
                        log::error!("Receive stream message error, invalidating both send and receive streams: {}", err);
                        // Kill receive stream
                        receive_error = true;
                        break;

                    }
                    None => {
                        break;
                    }
                }
            }

            if receive_error {
                log::warn!("Receive stream error detected, resetting streams for member {}", self.member_id);
                drop(receive_stream);
                self.stream_manager.reset_receive_stream(self.member_id as u32).await;
                drop(send_stream);
                self.stream_manager.reset_send_stream(self.member_id as u32).await;
                continue
            }

            let now = now_millis();
            // SOmething here, maybe not...
            if self.send_heartbeats && (now-self.last_heartbeat > 50){
                let request = RemoteAppendEntriesRequest{
                    term: self.term,
                    leader_id: self.self_id,
                    prev_log_index: self.prev_log_index,
                    prev_log_term: self.prev_log_term,
                    entries: vec![],
                    request_id: 0,
                    entry: None,
                    commit_index: self.commit_index,
                };

                let req = MessagePayload::AppendEntriesRequestOption(request);

                let response = send_stream.send(RemoteQuorumMessage{message_payload: Some(req), tracing_context: None});

                match response {
                    Ok(resp) => {
                        //tracing::info!("received append entries response {} for member {}, term {}, request duration: {}", val.ok, self.member_id, self.term, request_time);
                    }
                    Err(err) => {
                        tracing::error!("received heartbeat append entries error {} for member {}", err.to_string(), self.member_id);
                    }
                }

                self.last_heartbeat = now_millis()
            }
            let queue_len = self.work_queue.len();
            if queue_len > 5 {
                log::info!("Long quorum queue len: {}, member {} self id {}", queue_len, self.member_id, self.self_id);
            }
            iterations = 0;
            let mut send_error = false;
            while iterations < max_smart_batch {
                iterations += 1;
                let task = self.work_queue.pop();
                match task {
                    Some(task) => {
                        match task.task_type {
                            LocalQuorumWorkerTaskType::StartHeartbeats{id, term, prev_term, prev_log_index} => {
                                log::info!("received start heartbeat request id: {}, term: {}", id, term);
                                self.send_heartbeats = true;
                                self.term = term;
                                self.prev_log_term = prev_term;
                                self.prev_log_index = prev_log_index;
                            }
                            LocalQuorumWorkerTaskType::StopHeartbeats{id} => {
                                log::info!("received stop heartbeat request id: {}", id);
                                self.send_heartbeats = false;
                            }
                            LocalQuorumWorkerTaskType::RequestVote{id, term, last_index, last_term, parent_span} => {
                                log::info!("sending request vote with request id {} for term {}", id, term);
                                let vote_req = RemoteVoteRequest{
                                    term,
                                    candidate_id: self.self_id,
                                    last_log_index: last_index,
                                    last_log_term: last_term,
                                    request_id: id
                                };
                                match send_stream.send(RemoteQuorumMessage{ message_payload: Some(MessagePayload::VoteRequestOption(vote_req)), tracing_context: None}) {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("Error sending message to send stream for member {}, error: {}", self.member_id, e);
                                        send_error = true;
                                        break
                                    }
                                }
                            }
                            LocalQuorumWorkerTaskType::AppendEntries {id, req, parent_span} => {
                                let s = tracing::span!(Level::INFO, "sending_append_entries");
                                s.set_parent(parent_span.context());
                                let _guard = s.enter();
                                tracing::info!("sending append entries with request id {} to member {}, self id {}", id, self.member_id, self.self_id);

                                self.pending_append_entries.insert(id, std::time::Instant::now());

                                self.prev_log_term = req.entry.as_ref().unwrap().term;
                                self.prev_log_index = req.entry.as_ref().unwrap().index;
                                self.commit_index = req.commit_index;
                                let tracing_context = span_to_tracing_context(&s.context(), &self.propagator);
                                send_stream.send(
                                    RemoteQuorumMessage{
                                        message_payload: Some(MessagePayload::AppendEntriesRequestOption(req)),
                                        tracing_context: Some(tracing_context)
                                    }
                                ).unwrap();
                                self.last_heartbeat = now_millis();
                            }
                            LocalQuorumWorkerTaskType::BackfillLog { data} => {
                                log::info!("received backfill log request with {} entries. First entry: {}", data.len(), data.first().map_or("None".to_string(), |e| format!("prev_term: {}, prev_index: {}", e.prev_term, e.prev_index)));
                                for entry in data {
                                    let buffer = vec![0u8; self.max_message_size_bytes];
                                    let (limit, mut serialized) = serialize_data(&entry, buffer, 0);
                                    serialized.truncate(limit);
                                    let entry = entry.deref();
                                    let remote_entry = RemoteLogEntry {
                                        term: entry.term,
                                        index: entry.index,
                                        data: serialized,
                                        batch_index: 0,
                                        message_id: entry.message_id,
                                        prev_log_index: entry.prev_index,
                                        prev_log_term: entry.prev_term
                                    };
                                    let request = RemoteAppendEntriesRequest{
                                        term: entry.term,
                                        leader_id: self.self_id,
                                        prev_log_index: entry.prev_index,
                                        prev_log_term: entry.prev_term,
                                        entries: vec![],
                                        request_id: entry.message_id,
                                        entry: Some(remote_entry),
                                        commit_index: self.commit_index,
                                    };
                                    send_stream.send(
                                        RemoteQuorumMessage{
                                            message_payload: Some(MessagePayload::AppendEntriesRequestOption(request)),
                                            tracing_context: None
                                        }
                                    ).unwrap();
                                }
                                self.last_heartbeat = now_millis();
                                self.ongoing_backfill = None;
                            }
                            LocalQuorumWorkerTaskType::WriteBatch { write_batch} => {
                                let s = tracing::span!(Level::INFO, "sending_write_batch");
                                write_batch.span_parent.iter().for_each(|p| {
                                    s.set_parent(p.context());
                                });
                                let _guard = s.enter();
                                tracing::info!("sending write batch with id {} to member {}, self id {}, num requests: {}", write_batch.batch_id, self.member_id, self.self_id, write_batch.requests.len());

                                let put_batch_request = RemotePutBatchRequest{
                                    batch_id: write_batch.batch_id,
                                    put_request: write_batch.requests
                                };

                                self.pending_write_batches.insert(
                                    put_batch_request.batch_id.clone(),
                                    PendingWriteBatch {
                                        callback: write_batch.callback,
                                        start_time: std::time::Instant::now(),
                                    }
                                );

                                let tracing_context = span_to_tracing_context(&s.context(), &self.propagator);
                                send_stream.send(
                                    RemoteQuorumMessage{
                                        message_payload: Some(MessagePayload::PutBatchRequestOption(put_batch_request)),
                                        tracing_context: Some(tracing_context)
                                    }
                                ).unwrap();

                            }
                            LocalQuorumWorkerTaskType::TruncateLog { id, prev_term, prev_index, parent_span } => {
                                let s = tracing::span!(Level::INFO, "sending_truncate_log");
                                s.set_parent(parent_span.context());
                                let _guard = s.enter();
                                tracing::info!("sending truncate log with id {} to member {}, self id {}, prev_term: {}, prev_index: {}", id, self.member_id, self.self_id, prev_term, prev_index);

                                let truncate_request = RemoteTruncateLogRequest{
                                    prev_term,
                                    prev_index,
                                    term: self.term,
                                    leader_id: self.self_id,
                                    request_id: id,
                                };

                                let tracing_context = span_to_tracing_context(&s.context(), &self.propagator);
                                send_stream.send(
                                    RemoteQuorumMessage{
                                        message_payload: Some(MessagePayload::TruncateLogRequestOption(truncate_request)),
                                        tracing_context: Some(tracing_context)
                                    }
                                ).unwrap();
                            }

                        }
                    }
                    None => {
                        break;
                    }
                }

            }
            if send_error {
                log::warn!("Send stream error detected, resetting streams for member {}", self.member_id);
                drop(receive_stream);
                self.stream_manager.reset_receive_stream(self.member_id as u32).await;
                drop(send_stream);
                self.stream_manager.reset_send_stream(self.member_id as u32).await;
            }
            let elapsed = now_millis() - current_time;
            if elapsed < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}
