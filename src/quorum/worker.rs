use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc, Condvar};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use arc_swap::ArcSwap;
use crossbeam_queue::SegQueue;
use futures::{FutureExt, StreamExt, TryFuture};
use log::{error, info};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_zipkin::Propagator;
use time::Instant;
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
use crate::transport::raft::raftproto::raft_client::RaftClient;
use crate::transport::raft::raftproto::{RemoteAppendEntriesAcknowledge, RemoteAppendEntriesRequest, RemoteAppendEntriesResponse, RemoteLogEntry, RemotePutBatchRequest, RemotePutBatchResponse, RemoteQuorumMessage, RemoteTruncateLogRequest, RemoteTruncateLogResponse, RemoteVoteRequest, RemoteVoteResponse};
use crate::transport::raft::raftproto::remote_quorum_message::MessagePayload;
use crate::service_utils::app_time::{now_millis, now_plus_duration_millis};
use crate::service_utils::storage_utils::{SerializationData, serialize_data, SerializedData};
use crate::service_utils::tracing_utils::{span_to_tracing_context, tracing_context_to_span};
use crate::transport::stream_manager::{StreamManager, StreamManagerImpl};
use crate::transport::stream_manager::raftproto::TracingContext;
use crate::transport::write_proxy::{WriteBatch, WriteResponse};

// Type aliases for stream types
type SendStream = tokio::sync::mpsc::UnboundedSender<RemoteQuorumMessage>;
type ReceiveStream = tonic::Streaming<RemoteQuorumMessage>;

pub struct LocalQuorumWorkerTask {
    pub task_type: LocalQuorumWorkerTaskType
}

pub enum LocalQuorumWorkerTaskType {
    StartHeartbeats{ id: u64, term: u64, prev_term: u64, prev_log_index: u64, commit_index: u64 },
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

    // ============================================================================
    // Main Worker loop
    // ============================================================================
    pub async fn run(&mut self) {
        log::info!("Running quorum worker");
        let mut pending_tasks: Vec<PendingQuorumTask> = vec![];

        loop {
            let start_time = std::time::Instant::now();

            // Stream setup
            let maybe_receive_stream = self.stream_manager.get_receive_stream(self.member_id as u32).await;
            let maybe_send_stream = self.stream_manager.get_send_stream(self.member_id as u32).await;

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

            let rec = maybe_receive_stream.unwrap();
            let sen = maybe_send_stream.unwrap();

            let mut receive_stream = rec.lock().await;
            let send_stream = sen.lock().await;

            // Process pending task responses
            self.process_pending_task_responses(&mut pending_tasks, &send_stream);

            // Handle incoming messages
            let receive_error = self
                .process_incoming_messages(&mut pending_tasks, &mut receive_stream, &send_stream)
                .await;

            if receive_error {
                log::warn!("Receive stream error detected, resetting streams for member {}", self.member_id);
                drop(receive_stream);
                drop(send_stream);
                self.reset_streams().await;
                continue;
            }

            // Send heartbeat if needed
            self.send_heartbeat_if_needed(&send_stream);

            // Process work queue
            let send_error = self.process_work_queue(&send_stream);

            if send_error {
                log::warn!("Send stream error detected, resetting streams for member {}", self.member_id);
                drop(receive_stream);
                drop(send_stream);
                self.reset_streams().await;
            }

            // CPU throttling
            if start_time.elapsed().as_micros() < 200 {
                thread::yield_now();
            }
        }
    }

    // ============================================================================
    // Utility Functions
    // ============================================================================

    async fn reset_streams(&self) {
        self.stream_manager.reset_receive_stream(self.member_id as u32).await;
        self.stream_manager.reset_send_stream(self.member_id as u32).await;
    }

    // ============================================================================
    // Work Queue State Handlers (on_work_*)
    // ============================================================================

    fn on_work_start_heartbeats(&mut self, id: u64, term: u64, prev_term: u64, prev_log_index: u64, commit_index: u64) {
        log::info!("received start heartbeat request id: {}, term: {}", id, term);
        self.send_heartbeats = true;
        self.term = term;
        self.prev_log_term = prev_term;
        self.prev_log_index = prev_log_index;
        self.commit_index = commit_index;
    }

    fn on_work_stop_heartbeats(&mut self, id: u64) {
        log::info!("received stop heartbeat request id: {}", id);
        self.send_heartbeats = false;
    }

    // ============================================================================
    // Send Functions (send_*)
    // ============================================================================

    fn send_vote_request(
        &self,
        id: u64,
        term: u64,
        last_index: u64,
        last_term: u64,
        send_stream: &SendStream,
    ) -> Result<(), SendError<RemoteQuorumMessage>> {
        log::info!("sending request vote with request id {} for term {}", id, term);
        let vote_req = RemoteVoteRequest {
            term,
            candidate_id: self.self_id,
            last_log_index: last_index,
            last_log_term: last_term,
            request_id: id,
        };
        send_stream.send(RemoteQuorumMessage {
            message_payload: Some(MessagePayload::VoteRequestOption(vote_req)),
            tracing_context: None,
        })
    }

    fn send_append_entries(
        &mut self,
        id: u64,
        req: RemoteAppendEntriesRequest,
        parent_span: Span,
        send_stream: &SendStream,
    ) {
        let s = tracing::span!(Level::INFO, "sending_append_entries");
        s.set_parent(parent_span.context());
        let _guard = s.enter();
        tracing::info!(
            "sending append entries with request id {} to member {}, self id {}",
            id,
            self.member_id,
            self.self_id
        );

        self.pending_append_entries.insert(id, std::time::Instant::now());

        self.prev_log_term = req.entry.as_ref().unwrap().term;
        self.prev_log_index = req.entry.as_ref().unwrap().index;
        self.commit_index = req.commit_index;
        let tracing_context = span_to_tracing_context(&s.context(), &self.propagator);
        send_stream
            .send(RemoteQuorumMessage {
                message_payload: Some(MessagePayload::AppendEntriesRequestOption(req)),
                tracing_context: Some(tracing_context),
            })
            .unwrap();
        self.last_heartbeat = now_millis();
    }

    fn send_backfill_entries(&mut self, data: Vec<Arc<SerializationData>>, send_stream: &SendStream) {
        log::info!(
            "received backfill log request with {} entries. First entry: {}",
            data.len(),
            data.first()
                .map_or("None".to_string(), |e| format!(
                    "prev_term: {}, prev_index: {}",
                    e.prev_term, e.prev_index
                ))
        );
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
                prev_log_term: entry.prev_term,
            };
            let request = RemoteAppendEntriesRequest {
                term: entry.term,
                leader_id: self.self_id,
                prev_log_index: entry.prev_index,
                prev_log_term: entry.prev_term,
                request_id: entry.message_id,
                entry: Some(remote_entry),
                commit_index: self.commit_index,
            };
            send_stream
                .send(RemoteQuorumMessage {
                    message_payload: Some(MessagePayload::AppendEntriesRequestOption(request)),
                    tracing_context: None,
                })
                .unwrap();
        }
        self.last_heartbeat = now_millis();
        self.ongoing_backfill = None;
    }

    fn send_write_batch(&mut self, write_batch: WriteBatch, send_stream: &SendStream) {
        let s = tracing::span!(Level::INFO, "sending_write_batch");
        write_batch.span_parent.iter().for_each(|p| {
            s.set_parent(p.context());
        });
        let _guard = s.enter();
        tracing::info!(
            "sending write batch with id {} to member {}, self id {}, num requests: {}",
            write_batch.batch_id,
            self.member_id,
            self.self_id,
            write_batch.requests.len()
        );

        let put_batch_request = RemotePutBatchRequest {
            batch_id: write_batch.batch_id,
            put_request: write_batch.requests,
        };

        self.pending_write_batches.insert(
            put_batch_request.batch_id.clone(),
            PendingWriteBatch {
                callback: write_batch.callback,
                start_time: std::time::Instant::now(),
            },
        );

        let tracing_context = span_to_tracing_context(&s.context(), &self.propagator);
        send_stream
            .send(RemoteQuorumMessage {
                message_payload: Some(MessagePayload::PutBatchRequestOption(put_batch_request)),
                tracing_context: Some(tracing_context),
            })
            .unwrap();
    }

    fn send_truncate_log(
        &self,
        id: u64,
        prev_term: u64,
        prev_index: u64,
        parent_span: Span,
        send_stream: &SendStream,
    ) {
        let s = tracing::span!(Level::INFO, "sending_truncate_log");
        s.set_parent(parent_span.context());
        let _guard = s.enter();
        tracing::info!(
            "sending truncate log with id {} to member {}, self id {}, prev_term: {}, prev_index: {}",
            id,
            self.member_id,
            self.self_id,
            prev_term,
            prev_index
        );

        let truncate_request = RemoteTruncateLogRequest {
            prev_term,
            prev_index,
            term: self.term,
            leader_id: self.self_id,
            request_id: id,
        };

        let tracing_context = span_to_tracing_context(&s.context(), &self.propagator);
        send_stream
            .send(RemoteQuorumMessage {
                message_payload: Some(MessagePayload::TruncateLogRequestOption(truncate_request)),
                tracing_context: Some(tracing_context),
            })
            .unwrap();
    }

    // ============================================================================
    // Incoming Message Handlers (on_remote_*)
    // ============================================================================

    fn on_remote_vote_response(&self, vote: RemoteVoteResponse) {
        log::info!(
            "received vote response id: {}, member: {}, term: {}, granted: {}",
            vote.request_id,
            self.member_id,
            vote.term,
            vote.vote_granted
        );
        let resp = LocalQuorumTaskResponseType::RequestVoteResponse {
            id: vote.request_id,
            term: vote.term,
            received_vote: vote.vote_granted,
        };
        self.response_queue.push(LocalQuorumResponse { response_type: resp });
    }

    fn on_remote_append_entries_ack(&mut self, append_entries: RemoteAppendEntriesAcknowledge) {
        if !append_entries.ok {
            log::info!(
                "received append entries acknowledge from: {}, but it was not ok, backfilling log from term {} and index {}",
                self.member_id,
                append_entries.last_log_term,
                append_entries.last_log_index
            );
            if self.ongoing_backfill.is_none() {
                self.ongoing_backfill = Some(OngoingBackfill {
                    from_term: append_entries.last_log_term,
                    from_index: append_entries.last_log_index,
                });
                let resp = LocalQuorumTaskResponseType::BackfillLog {
                    quorum_node_id: self.member_id,
                    prev_term: append_entries.last_log_term,
                    prev_index: append_entries.last_log_index,
                };
                self.response_queue.push(LocalQuorumResponse { response_type: resp });
            } else {
                log::warn!("Ongoing backfill already in progress, ignoring new backfill request for term {} and index {}", append_entries.last_log_term, append_entries.last_log_index);
            }
        }

        if append_entries.request_id != 0 {
            tracing::info!(
                "received append entries acknowledge from: {}, ok: {}, request id: {}, member: {}",
                self.member_id,
                append_entries.ok,
                append_entries.request_id,
                self.member_id
            );

            if let Some(start_time) = self.pending_append_entries.remove(&append_entries.request_id) {
                let latency_micros = start_time.elapsed().as_micros() as f64;
                crate::transport::metrics::QUORUM_APPEND_ENTRIES_LATENCY
                    .with_label_values(&[&self.member_id.to_string()])
                    .observe(latency_micros);
            }

            let resp = LocalQuorumTaskResponseType::AppendEntries {
                id: append_entries.request_id,
                ok: append_entries.ok,
            };
            self.response_queue.push(LocalQuorumResponse { response_type: resp });
        }
    }

    fn on_remote_append_entries_request(
        &mut self,
        append_entries: RemoteAppendEntriesRequest,
        span: &Span,
        pending_tasks: &mut Vec<PendingQuorumTask>,
    ) {
        let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();

        let index = append_entries.entry.as_ref().map(|entry| entry.index).unwrap_or(0);
        let term = append_entries.entry.as_ref().map(|entry| entry.term).unwrap_or(0);
        tracing::info!(
            "received append entries request from {} as self {}, sending task to local state machine",
            self.member_id,
            self.self_id
        );
        self.commit_index = append_entries.commit_index;

        self.task_queue.push(LocalRaftMessage {
            payload: LocalRaftMessagePayload::AppendEntries(LocalAppendEntries {
                leader_id: append_entries.leader_id,
                term: append_entries.term,
                prev_term: append_entries.prev_log_term,
                prev_index: append_entries.prev_log_index,
                request_id: append_entries.request_id,
                entry: append_entries.entry,
                commit_index: append_entries.commit_index,
            }),
            callback: callback.0,
            parent_span: span.clone(),
        });

        let pending_task = PendingQuorumTask {
            task: PendingQuorumTaskEnum::AppendEntries {
                request_id: append_entries.request_id,
                log_index: index,
                term: term,
                parent_span: span.clone(),
            },
            callback: callback.1,
            start_time: std::time::Instant::now(),
        };
        pending_tasks.push(pending_task);
    }

    fn on_remote_vote_request(
        &self,
        vote_request: RemoteVoteRequest,
        pending_tasks: &mut Vec<PendingQuorumTask>,
    ) {
        log::info!("received vote request from: {} for term: {}", self.member_id, vote_request.term);
        let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();

        let span = tracing::span!(Level::INFO, "awaiting_vote_response");
        self.task_queue.push(LocalRaftMessage {
            payload: LocalRaftMessagePayload::RequestVote(LocalRequestVoteRequest {
                term: vote_request.term,
                candidate_id: vote_request.candidate_id,
                last_log_index: vote_request.last_log_index,
                last_log_term: vote_request.last_log_term,
            }),
            callback: callback.0,
            parent_span: span,
        });

        let pending_task = PendingQuorumTask {
            task: PendingQuorumTaskEnum::Vote(vote_request),
            callback: callback.1,
            start_time: std::time::Instant::now(),
        };
        pending_tasks.push(pending_task);
    }

    fn on_remote_put_batch_request(
        &self,
        put_batch: RemotePutBatchRequest,
        span: &Span,
        pending_tasks: &mut Vec<PendingQuorumTask>,
    ) {
        let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();

        tracing::info!(
            "received put batch request from {} as self {}, sending task to local state machine",
            self.member_id,
            self.self_id
        );

        let pending_task = PendingQuorumTask {
            task: PendingQuorumTaskEnum::WriteBatch {
                batch_id: put_batch.batch_id.clone(),
                parent_span: span.clone(),
            },
            callback: callback.1,
            start_time: std::time::Instant::now(),
        };
        pending_tasks.push(pending_task);

        self.task_queue.push(LocalRaftMessage {
            payload: LocalRaftMessagePayload::WriteBatch(LocalRaftWriteBatchRequest {
                requests: put_batch.put_request,
            }),
            callback: callback.0,
            parent_span: span.clone(),
        });
    }

    fn on_remote_put_batch_response(&mut self, put_batch_response: RemotePutBatchResponse) {
        let pending_batch = self.pending_write_batches.remove(&put_batch_response.batch_id);
        match pending_batch {
            Some(batch) => {
                let latency_micros = batch.start_time.elapsed().as_micros() as f64;
                crate::transport::metrics::QUORUM_PUT_BATCH_LATENCY
                    .with_label_values(&[&self.member_id.to_string()])
                    .observe(latency_micros);

                let _ = batch.callback.send(WriteResponse {
                    responses: put_batch_response.responses,
                    status_code: StatusCode::OK,
                });
            }
            None => {
                log::error!("No pending write batch found for batch id: {}", put_batch_response.batch_id);
            }
        }
    }

    fn on_remote_truncate_log_request(
        &self,
        truncate_request: RemoteTruncateLogRequest,
        send_stream: &SendStream,
    ) {
        log::info!(
            "Received truncate log request from leader {} for term {} index {}",
            truncate_request.leader_id,
            truncate_request.prev_term,
            truncate_request.prev_index
        );

        // Send to local task queue for processing
        let resp = LocalQuorumTaskResponseType::TruncateLog {
            quorum_node_id: self.member_id,
            prev_term: truncate_request.prev_term,
            prev_index: truncate_request.prev_index,
        };
        self.response_queue.push(LocalQuorumResponse { response_type: resp });

        // Send acknowledgment back to leader
        send_stream
            .send(RemoteQuorumMessage {
                message_payload: Some(MessagePayload::TruncateLogResponseOption(RemoteTruncateLogResponse {
                    request_id: truncate_request.request_id,
                    ok: true,
                })),
                tracing_context: None,
            })
            .expect("sending truncate response on stream shouldn't fail");
    }

    fn on_remote_truncate_log_response(&mut self, truncate_response: RemoteTruncateLogResponse) {
        log::info!(
            "Received truncate log response for request {}, ok: {}",
            truncate_response.request_id,
            truncate_response.ok
        );
        let resp = LocalQuorumTaskResponseType::TruncateLogResponse {
            id: truncate_response.request_id,
            ok: truncate_response.ok,
        };
        self.response_queue.push(LocalQuorumResponse { response_type: resp });
        self.ongoing_backfill = None
    }

    // ============================================================================
    // Main Processing Functions (process_*)
    // ============================================================================

    fn process_pending_task_responses(
        &self,
        pending_tasks: &mut Vec<PendingQuorumTask>,
        send_stream: &SendStream,
    ) {
        let mut pending_remove = vec![];
        for i in 0..pending_tasks.len() {
            let maybe_pt = pending_tasks.get_mut(i);
            match maybe_pt {
                None => {
                    pending_remove.push(i);
                }
                Some(pt) => {
                    let resp = pt.callback.try_recv();
                    match resp {
                        Ok(val) => {
                            match val.payload {
                                LocalRaftResponsePayload::RequestVote(rv) => {
                                    match &pt.task {
                                        PendingQuorumTaskEnum::Vote(task) => {
                                            send_stream
                                                .send(RemoteQuorumMessage {
                                                    message_payload: Some(MessagePayload::VoteResponseOption(
                                                        RemoteVoteResponse {
                                                            vote_granted: rv,
                                                            request_id: task.request_id,
                                                            term: task.term,
                                                        },
                                                    )),
                                                    tracing_context: None,
                                                })
                                                .expect("sending on stream shouldn't fail");
                                        }
                                        _ => {
                                            log::error!("unexpected pending task type for request vote");
                                        }
                                    }
                                }
                                LocalRaftResponsePayload::AppendEntries(ae) => {
                                    match &pt.task {
                                        PendingQuorumTaskEnum::AppendEntries {
                                            request_id,
                                            log_index,
                                            term,
                                            parent_span,
                                        } => {
                                            let span = tracing::span!(Level::INFO, "processing_append_entries_response");
                                            span.set_parent(parent_span.context());
                                            let _enter = span.enter();
                                            tracing::info!(
                                                "processing append entries response from member {} as self id {}",
                                                self.member_id,
                                                self.self_id
                                            );

                                            let tracing_context = span_to_tracing_context(&span.context(), &self.propagator);
                                            match ae {
                                                LocalAppendEntriesCallbackResponse::Ok => {
                                                    send_stream
                                                        .send(RemoteQuorumMessage {
                                                            message_payload: Some(
                                                                MessagePayload::AppendEntriesAcknowledgeOption(
                                                                    RemoteAppendEntriesAcknowledge {
                                                                        last_log_index: *log_index,
                                                                        last_log_term: *term,
                                                                        request_id: *request_id,
                                                                        ok: true,
                                                                    },
                                                                ),
                                                            ),
                                                            tracing_context: Some(tracing_context),
                                                        })
                                                        .expect("sending on stream shouldn't fail");
                                                }
                                                LocalAppendEntriesCallbackResponse::UnrecognizedLeader => {
                                                    send_stream
                                                        .send(RemoteQuorumMessage {
                                                            message_payload: Some(
                                                                MessagePayload::AppendEntriesAcknowledgeOption(
                                                                    RemoteAppendEntriesAcknowledge {
                                                                        last_log_index: *log_index,
                                                                        last_log_term: *term,
                                                                        request_id: *request_id,
                                                                        ok: false,
                                                                    },
                                                                ),
                                                            ),
                                                            tracing_context: None,
                                                        })
                                                        .expect("sending on stream shouldn't fail");
                                                }
                                                LocalAppendEntriesCallbackResponse::WantedPreviousEntry {
                                                    last_term,
                                                    last_index,
                                                } => {
                                                    send_stream
                                                        .send(RemoteQuorumMessage {
                                                            message_payload: Some(
                                                                MessagePayload::AppendEntriesAcknowledgeOption(
                                                                    RemoteAppendEntriesAcknowledge {
                                                                        last_log_index: last_index,
                                                                        last_log_term: last_term,
                                                                        request_id: *request_id,
                                                                        ok: false,
                                                                    },
                                                                ),
                                                            ),
                                                            tracing_context: None,
                                                        })
                                                        .expect("sending on stream shouldn't fail");
                                                }
                                                LocalAppendEntriesCallbackResponse::TruncateLog { prev_term, prev_index } => {
                                                    log::info!(
                                                        "Received truncate log response from leader, truncating to term {} index {}",
                                                        prev_term,
                                                        prev_index
                                                    );
                                                    let resp = LocalQuorumTaskResponseType::TruncateLog {
                                                        quorum_node_id: self.member_id,
                                                        prev_term,
                                                        prev_index,
                                                    };
                                                    self.response_queue.push(LocalQuorumResponse { response_type: resp });
                                                    send_stream
                                                        .send(RemoteQuorumMessage {
                                                            message_payload: Some(
                                                                MessagePayload::AppendEntriesAcknowledgeOption(
                                                                    RemoteAppendEntriesAcknowledge {
                                                                        last_log_index: prev_index,
                                                                        last_log_term: prev_term,
                                                                        request_id: *request_id,
                                                                        ok: false,
                                                                    },
                                                                ),
                                                            ),
                                                            tracing_context: None,
                                                        })
                                                        .expect("sending on stream shouldn't fail");
                                                }
                                            }
                                        }
                                        _ => {
                                            log::error!("unexpected pending task type for append entries");
                                        }
                                    }
                                }
                                LocalRaftResponsePayload::WriteBatch(write_resp) => {
                                    match &pt.task {
                                        PendingQuorumTaskEnum::WriteBatch { batch_id, parent_span } => {
                                            let span = tracing::span!(Level::INFO, "processing_write_batch_response");
                                            span.set_parent(parent_span.context());
                                            let _enter = span.enter();
                                            tracing::info!(
                                                "processing write batch response from member {} as self id {}",
                                                self.member_id,
                                                self.self_id
                                            );

                                            let tracing_context = span_to_tracing_context(&span.context(), &self.propagator);
                                            send_stream
                                                .send(RemoteQuorumMessage {
                                                    message_payload: Some(MessagePayload::PutBatchResponseOption(
                                                        RemotePutBatchResponse {
                                                            responses: write_resp.responses,
                                                            batch_id: batch_id.clone(),
                                                        },
                                                    )),
                                                    tracing_context: Some(tracing_context),
                                                })
                                                .expect("sending on stream shouldn't fail");
                                        }
                                        _ => {
                                            log::error!("unexpected pending task type for write batch");
                                        }
                                    }
                                }
                                _ => {
                                    log::error!("unknown raft response payload");
                                }
                            }
                            pending_remove.push(i);
                        }
                        Err(err) => match err {
                            TryRecvError::Empty => {}
                            TryRecvError::Closed => {
                                pending_remove.push(i);
                            }
                        },
                    }
                }
            }
        }

        for (i, el) in pending_remove.iter().enumerate() {
            pending_tasks.remove(el - i);
        }
    }

    fn send_heartbeat_if_needed(&mut self, send_stream: &SendStream) {
        let now = now_millis();
        if self.send_heartbeats && (now - self.last_heartbeat > 50) {
            let request = RemoteAppendEntriesRequest {
                term: self.term,
                leader_id: self.self_id,
                prev_log_index: self.prev_log_index,
                prev_log_term: self.prev_log_term,
                request_id: 0,
                entry: None,
                commit_index: self.commit_index,
            };

            let req = MessagePayload::AppendEntriesRequestOption(request);

            let response = send_stream.send(RemoteQuorumMessage {
                message_payload: Some(req),
                tracing_context: None,
            });

            match response {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!(
                        "received heartbeat append entries error {} for member {}",
                        err.to_string(),
                        self.member_id
                    );
                }
            }

            self.last_heartbeat = now_millis();
        }
    }

    async fn process_incoming_messages(
        &mut self,
        pending_tasks: &mut Vec<PendingQuorumTask>,
        receive_stream: &mut ReceiveStream,
        send_stream: &SendStream,
    ) -> bool {
        let max_smart_batch = 32;
        let mut iterations = 0;
        let mut receive_error = false;

        while iterations < max_smart_batch {
            iterations += 1;
            let result = receive_stream.message().now_or_never();
            match result {
                Some(Ok(Some(val))) => {
                    let span = match val.tracing_context {
                        Some(ctx) => {
                            let parent_cx = tracing_context_to_span(&ctx, &self.propagator);
                            let s = tracing::span!(Level::INFO, "received_remote_quorum_message", member_id = self.member_id);
                            s.set_parent(parent_cx);
                            s
                        }
                        None => {
                            tracing::span!(Level::INFO, "received_quorum_message_no_context", member_id = self.member_id)
                        }
                    };
                    let _enter = span.enter();

                    match val.message_payload {
                        None => {
                            break;
                        }
                        Some(MessagePayload::VoteResponseOption(vote)) => {
                            self.on_remote_vote_response(vote);
                        }
                        Some(MessagePayload::AppendEntriesAcknowledgeOption(append_entries)) => {
                            self.on_remote_append_entries_ack(append_entries);
                        }
                        Some(MessagePayload::AppendEntriesRequestOption(append_entries)) => {
                            self.on_remote_append_entries_request(append_entries, &span, pending_tasks);
                        }
                        Some(MessagePayload::VoteRequestOption(vote_request)) => {
                            self.on_remote_vote_request(vote_request, pending_tasks);
                        }
                        Some(MessagePayload::PutBatchRequestOption(put_batch)) => {
                            self.on_remote_put_batch_request(put_batch, &span, pending_tasks);
                        }
                        Some(MessagePayload::PutBatchResponseOption(put_batch_response)) => {
                            self.on_remote_put_batch_response(put_batch_response);
                        }
                        Some(MessagePayload::TruncateLogRequestOption(truncate_request)) => {
                            self.on_remote_truncate_log_request(truncate_request, send_stream);
                        }
                        Some(MessagePayload::TruncateLogResponseOption(truncate_response)) => {
                            self.on_remote_truncate_log_response(truncate_response);
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
                    log::error!(
                        "Receive stream message error, invalidating both send and receive streams: {}",
                        err
                    );
                    receive_error = true;
                    break;
                }
                None => {
                    break;
                }
            }
        }

        receive_error
    }

    fn process_work_queue(&mut self, send_stream: &SendStream) -> bool {
        let queue_len = self.work_queue.len();
        if queue_len > 5 {
            log::debug!(
                "Long quorum queue len: {}, member {} self id {}",
                queue_len,
                self.member_id,
                self.self_id
            );
        }

        let max_smart_batch = 32;
        let mut iterations = 0;
        let mut send_error = false;

        while iterations < max_smart_batch {
            iterations += 1;
            let task = self.work_queue.pop();
            match task {
                Some(task) => match task.task_type {
                    LocalQuorumWorkerTaskType::StartHeartbeats { id, term, prev_term, prev_log_index, commit_index } => {
                        self.on_work_start_heartbeats(id, term, prev_term, prev_log_index, commit_index);
                    }
                    LocalQuorumWorkerTaskType::StopHeartbeats { id } => {
                        self.on_work_stop_heartbeats(id);
                    }
                    LocalQuorumWorkerTaskType::RequestVote { id, term, last_index, last_term, parent_span: _ } => {
                        if let Err(e) = self.send_vote_request(id, term, last_index, last_term, send_stream) {
                            error!("Error sending message to send stream for member {}, error: {}", self.member_id, e);
                            send_error = true;
                            break;
                        }
                    }
                    LocalQuorumWorkerTaskType::AppendEntries { id, req, parent_span } => {
                        self.send_append_entries(id, req, parent_span, send_stream);
                    }
                    LocalQuorumWorkerTaskType::BackfillLog { data } => {
                        self.send_backfill_entries(data, send_stream);
                    }
                    LocalQuorumWorkerTaskType::WriteBatch { write_batch } => {
                        self.send_write_batch(write_batch, send_stream);
                    }
                    LocalQuorumWorkerTaskType::TruncateLog { id, prev_term, prev_index, parent_span } => {
                        self.send_truncate_log(id, prev_term, prev_index, parent_span, send_stream);
                    }
                },
                None => {
                    break;
                }
            }
        }

        send_error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crossbeam_queue::SegQueue;
    use tokio::sync::mpsc;

    /// Creates a QuorumWorker instance for testing with mock dependencies
    fn create_test_worker() -> (
        QuorumWorker,
        Arc<SegQueue<LocalQuorumWorkerTask>>,
        Arc<SegQueue<LocalQuorumResponse>>,
        Arc<SegQueue<LocalRaftMessage>>,
    ) {
        let work_queue = Arc::new(SegQueue::new());
        let response_queue = Arc::new(SegQueue::new());
        let task_queue = Arc::new(SegQueue::new());

        // Create a minimal stream manager - we won't use actual connections in tests
        let stream_manager = Arc::new(StreamManagerImpl::new(
            1,
            vec![],
            Arc::new(Mutex::new(std::collections::HashMap::new())),
            Arc::new(Mutex::new(std::collections::HashMap::new())),
        ));

        let worker = QuorumWorker::new(
            Arc::clone(&work_queue),
            Arc::clone(&response_queue),
            1,    // self_id
            1,    // next_index
            2,    // member_id
            "localhost:5001".to_string(),
            stream_manager,
            Arc::clone(&task_queue),
            2048, // max_message_size_bytes
        );

        (worker, work_queue, response_queue, task_queue)
    }

    /// Creates a test send stream (channel sender) for capturing sent messages
    fn create_test_send_stream() -> (SendStream, mpsc::UnboundedReceiver<RemoteQuorumMessage>) {
        mpsc::unbounded_channel()
    }

    // =========================================================================
    // Tests for work queue processing
    // =========================================================================

    #[tokio::test]
    async fn test_process_work_queue_start_heartbeats() {
        let (mut worker, work_queue, _, _) = create_test_worker();
        let (send_stream, _rx) = create_test_send_stream();

        // Add a StartHeartbeats task to the work queue
        work_queue.push(LocalQuorumWorkerTask {
            task_type: LocalQuorumWorkerTaskType::StartHeartbeats {
                id: 1,
                term: 5,
                prev_term: 4,
                prev_log_index: 10,
            },
        });

        // Process the work queue
        let send_error = worker.process_work_queue(&send_stream);

        // Verify no send error occurred
        assert!(!send_error, "Should not have send error for StartHeartbeats");

        // Verify worker state was updated
        assert!(worker.send_heartbeats, "send_heartbeats should be true");
        assert_eq!(worker.term, 5);
        assert_eq!(worker.prev_log_term, 4);
        assert_eq!(worker.prev_log_index, 10);
    }

    #[tokio::test]
    async fn test_process_work_queue_stop_heartbeats() {
        let (mut worker, work_queue, _, _) = create_test_worker();
        let (send_stream, _rx) = create_test_send_stream();

        // First enable heartbeats
        worker.send_heartbeats = true;

        // Add a StopHeartbeats task
        work_queue.push(LocalQuorumWorkerTask {
            task_type: LocalQuorumWorkerTaskType::StopHeartbeats { id: 1 },
        });

        // Process the work queue
        worker.process_work_queue(&send_stream);

        // Verify heartbeats were stopped
        assert!(!worker.send_heartbeats, "send_heartbeats should be false");
    }

    #[tokio::test]
    async fn test_process_work_queue_request_vote_sends_message() {
        let (mut worker, work_queue, _, _) = create_test_worker();
        let (send_stream, mut rx) = create_test_send_stream();

        // Add a RequestVote task
        let span = tracing::span!(tracing::Level::INFO, "test_span");
        work_queue.push(LocalQuorumWorkerTask {
            task_type: LocalQuorumWorkerTaskType::RequestVote {
                id: 42,
                term: 3,
                last_term: 2,
                last_index: 5,
                parent_span: span,
            },
        });

        // Process the work queue
        let send_error = worker.process_work_queue(&send_stream);

        // Verify no send error
        assert!(!send_error);

        // Verify the message was sent
        let msg = rx.try_recv().expect("Should have received a message");
        match msg.message_payload {
            Some(MessagePayload::VoteRequestOption(vote_req)) => {
                assert_eq!(vote_req.request_id, 42);
                assert_eq!(vote_req.term, 3);
                assert_eq!(vote_req.last_log_term, 2);
                assert_eq!(vote_req.last_log_index, 5);
                assert_eq!(vote_req.candidate_id, 1); // self_id
            }
            _ => panic!("Expected VoteRequestOption message"),
        }
    }

    // =========================================================================
    // Tests for heartbeat sending
    // =========================================================================

    #[tokio::test]
    async fn test_send_heartbeat_when_enabled_and_time_elapsed() {
        let (mut worker, _, _, _) = create_test_worker();
        let (send_stream, mut rx) = create_test_send_stream();

        // Enable heartbeats and set last_heartbeat to a time in the past
        worker.send_heartbeats = true;
        worker.term = 5;
        worker.prev_log_index = 10;
        worker.prev_log_term = 4;
        worker.commit_index = 8;
        worker.last_heartbeat = 0; // Long time ago

        // Send heartbeat
        worker.send_heartbeat_if_needed(&send_stream);

        // Verify heartbeat was sent
        let msg = rx.try_recv().expect("Should have received heartbeat message");
        match msg.message_payload {
            Some(MessagePayload::AppendEntriesRequestOption(req)) => {
                assert_eq!(req.term, 5);
                assert_eq!(req.leader_id, 1); // self_id
                assert_eq!(req.prev_log_index, 10);
                assert_eq!(req.prev_log_term, 4);
                assert_eq!(req.commit_index, 8);
                assert_eq!(req.request_id, 0); // Heartbeat has request_id 0
                assert!(req.entry.is_none()); // Heartbeat has no entry
            }
            _ => panic!("Expected AppendEntriesRequestOption for heartbeat"),
        }
    }

    #[tokio::test]
    async fn test_send_heartbeat_not_sent_when_disabled() {
        let (mut worker, _, _, _) = create_test_worker();
        let (send_stream, mut rx) = create_test_send_stream();

        // Heartbeats disabled
        worker.send_heartbeats = false;
        worker.last_heartbeat = 0;

        // Try to send heartbeat
        worker.send_heartbeat_if_needed(&send_stream);

        // Verify no message was sent
        assert!(
            rx.try_recv().is_err(),
            "Should not send heartbeat when disabled"
        );
    }

    // =========================================================================
    // Tests for incoming message handlers
    // =========================================================================

    #[tokio::test]
    async fn test_on_remote_vote_response_pushes_to_response_queue() {
        let (worker, _, response_queue, _) = create_test_worker();

        let vote_response = RemoteVoteResponse {
            request_id: 42,
            term: 5,
            vote_granted: true,
        };

        worker.on_remote_vote_response(vote_response);

        // Verify response was pushed to queue
        let response = response_queue.pop().expect("Should have response in queue");
        match response.response_type {
            LocalQuorumTaskResponseType::RequestVoteResponse { id, term, received_vote } => {
                assert_eq!(id, 42);
                assert_eq!(term, 5);
                assert!(received_vote);
            }
            _ => panic!("Expected RequestVoteResponse"),
        }
    }

    #[tokio::test]
    async fn test_on_remote_append_entries_ack_triggers_backfill_on_failure() {
        let (mut worker, _, response_queue, _) = create_test_worker();

        // Ensure no ongoing backfill
        assert!(worker.ongoing_backfill.is_none());

        let ack = RemoteAppendEntriesAcknowledge {
            request_id: 0, // Non-tracked request
            last_log_index: 5,
            last_log_term: 2,
            ok: false, // Failure triggers backfill
        };

        worker.on_remote_append_entries_ack(ack);

        // Verify backfill was triggered
        assert!(worker.ongoing_backfill.is_some());

        // Verify response was pushed
        let response = response_queue.pop().expect("Should have response in queue");
        match response.response_type {
            LocalQuorumTaskResponseType::BackfillLog { quorum_node_id, prev_term, prev_index } => {
                assert_eq!(quorum_node_id, 2); // member_id
                assert_eq!(prev_term, 2);
                assert_eq!(prev_index, 5);
            }
            _ => panic!("Expected BackfillLog response"),
        }
    }

    #[tokio::test]
    async fn test_on_remote_append_entries_ack_ignores_duplicate_backfill() {
        let (mut worker, _, response_queue, _) = create_test_worker();

        // Set ongoing backfill
        worker.ongoing_backfill = Some(OngoingBackfill {
            from_term: 1,
            from_index: 3,
        });

        let ack = RemoteAppendEntriesAcknowledge {
            request_id: 0,
            last_log_index: 5,
            last_log_term: 2,
            ok: false,
        };

        worker.on_remote_append_entries_ack(ack);

        // Verify no new response was pushed (duplicate ignored)
        assert!(response_queue.pop().is_none(), "Should ignore duplicate backfill request");

        // Original backfill should remain unchanged
        let backfill = worker.ongoing_backfill.as_ref().unwrap();
        assert_eq!(backfill.from_term, 1);
        assert_eq!(backfill.from_index, 3);
    }

    #[tokio::test]
    async fn test_on_remote_vote_request_creates_pending_task() {
        let (worker, _, _, task_queue) = create_test_worker();
        let mut pending_tasks = vec![];

        let vote_request = RemoteVoteRequest {
            request_id: 42,
            term: 5,
            candidate_id: 3,
            last_log_index: 10,
            last_log_term: 4,
        };

        worker.on_remote_vote_request(vote_request, &mut pending_tasks);

        // Verify task was added to task queue
        let task = task_queue.pop().expect("Should have task in queue");
        match task.payload {
            LocalRaftMessagePayload::RequestVote(req) => {
                assert_eq!(req.term, 5);
                assert_eq!(req.candidate_id, 3);
                assert_eq!(req.last_log_index, 10);
                assert_eq!(req.last_log_term, 4);
            }
            _ => panic!("Expected RequestVote payload"),
        }

        // Verify pending task was added
        assert_eq!(pending_tasks.len(), 1);
        match &pending_tasks[0].task {
            PendingQuorumTaskEnum::Vote(req) => {
                assert_eq!(req.request_id, 42);
            }
            _ => panic!("Expected Vote pending task"),
        }
    }

    #[tokio::test]
    async fn test_on_remote_truncate_log_response_pushes_to_queue() {
        let (mut worker, _, response_queue, _) = create_test_worker();

        let response = RemoteTruncateLogResponse {
            request_id: 42,
            ok: true,
        };

        worker.on_remote_truncate_log_response(response);

        // Verify response was pushed
        let resp = response_queue.pop().expect("Should have response");
        match resp.response_type {
            LocalQuorumTaskResponseType::TruncateLogResponse { id, ok } => {
                assert_eq!(id, 42);
                assert!(ok);
            }
            _ => panic!("Expected TruncateLogResponse"),
        }
    }

    // =========================================================================
    // Tests for send functions
    // =========================================================================

    #[tokio::test]
    async fn test_send_vote_request_formats_message_correctly() {
        let (worker, _, _, _) = create_test_worker();
        let (send_stream, mut rx) = create_test_send_stream();

        let result = worker.send_vote_request(42, 5, 10, 4, &send_stream);

        assert!(result.is_ok(), "send_vote_request should succeed");

        let msg = rx.try_recv().expect("Should receive message");
        match msg.message_payload {
            Some(MessagePayload::VoteRequestOption(req)) => {
                assert_eq!(req.request_id, 42);
                assert_eq!(req.term, 5);
                assert_eq!(req.last_log_index, 10);
                assert_eq!(req.last_log_term, 4);
                assert_eq!(req.candidate_id, 1); // self_id
            }
            _ => panic!("Expected VoteRequestOption"),
        }
    }

    #[tokio::test]
    async fn test_send_truncate_log_formats_message_correctly() {
        let (mut worker, _, _, _) = create_test_worker();
        let (send_stream, mut rx) = create_test_send_stream();

        worker.term = 7;
        let span = tracing::span!(tracing::Level::INFO, "test");

        worker.send_truncate_log(42, 5, 10, span, &send_stream);

        let msg = rx.try_recv().expect("Should receive message");
        match msg.message_payload {
            Some(MessagePayload::TruncateLogRequestOption(req)) => {
                assert_eq!(req.request_id, 42);
                assert_eq!(req.prev_term, 5);
                assert_eq!(req.prev_index, 10);
                assert_eq!(req.term, 7);
                assert_eq!(req.leader_id, 1); // self_id
            }
            _ => panic!("Expected TruncateLogRequestOption"),
        }
    }

    // =========================================================================
    // Tests for error handling
    // =========================================================================

    #[tokio::test]
    async fn test_process_work_queue_returns_error_on_closed_channel() {
        let (mut worker, work_queue, _, _) = create_test_worker();
        let (send_stream, rx) = create_test_send_stream();

        // Drop the receiver to close the channel
        drop(rx);

        // Add a task that requires sending
        let span = tracing::span!(tracing::Level::INFO, "test");
        work_queue.push(LocalQuorumWorkerTask {
            task_type: LocalQuorumWorkerTaskType::RequestVote {
                id: 1,
                term: 1,
                last_term: 1,
                last_index: 1,
                parent_span: span,
            },
        });

        // Process should return send_error = true
        let send_error = worker.process_work_queue(&send_stream);

        assert!(send_error, "Should return send_error when channel is closed");
    }
}
