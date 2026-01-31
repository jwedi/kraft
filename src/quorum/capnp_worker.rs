use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use crossbeam_queue::SegQueue;
use log::{debug, error, info, warn};
use tracing::{Level, Span};

use crate::quorum::worker::{
    LocalQuorumWorkerTask, LocalQuorumWorkerTaskType,
    LocalQuorumResponse, LocalQuorumTaskResponseType,
};
use crate::raft::raft_sm::{
    LocalRaftMessage, LocalRaftMessagePayload, LocalAppendEntries, LocalRequestVoteRequest,
    LocalRaftResponseMessage, LocalRaftResponsePayload, LocalAppendEntriesCallbackResponse,
    LocalRaftWriteBatchRequest,
};
use crate::service_utils::app_time::now_millis;
use crate::service_utils::storage_utils::SerializationData;
use crate::transport::capnp::owned_message::OwnedQuorumMessage;
use crate::transport::capnp::conversion::{CapnpProstConverter, InboundMessagePayload};
use crate::transport::capnp_stream_manager::CapnpStreamManager;
use crate::transport::raft::raftproto::{RemoteAppendEntriesRequest, RemoteAppendEntriesAcknowledge, RemoteVoteRequest, RemoteVoteResponse, RemotePutBatchRequest, RemotePutBatchResponse, RemoteTruncateLogRequest, RemoteTruncateLogResponse, RemoteLogEntry, RemoteQuorumMessage};
use crate::transport::write_proxy::{WriteBatch, WriteResponse};
use crate::transport::metrics::{QUORUM_APPEND_ENTRIES_LATENCY, QUORUM_PUT_BATCH_LATENCY};
use tokio::sync::oneshot;
use tokio::task::yield_now;
use crate::transport::raft::raftproto::remote_quorum_message::MessagePayload;

const HEARTBEAT_INTERVAL_MS: u128 = 100;
const MAX_MESSAGES_PER_ITERATION: usize = 32;

struct OngoingBackfill {
    from_term: u64,
    from_index: u64,
}

struct PendingWriteBatch {
    callback: oneshot::Sender<WriteResponse>,
    start_time: std::time::Instant,
}

#[derive(Debug)]
enum PendingQuorumTaskEnum {
    Vote(RemoteVoteRequest),
    AppendEntries { request_id: u64, log_index: u64, term: u64 },
    PutBatch { batch_id: String },
}

struct PendingQuorumTask {
    callback: oneshot::Receiver<LocalRaftResponseMessage>,
    task: PendingQuorumTaskEnum,
    start_time: std::time::Instant,
}

/// Cap'n Proto version of QuorumWorker with zero-copy message handling.
/// Handles communication with a single remote cluster node.
pub struct CapnpQuorumWorker {
    // Queues for inter-process communication (using prost types)
    work_queue: Arc<SegQueue<LocalQuorumWorkerTask>>,
    response_queue: Arc<SegQueue<LocalQuorumResponse>>,
    task_queue: Arc<SegQueue<LocalRaftMessage>>,

    // Cap'n Proto transport
    stream_manager: Arc<CapnpStreamManager>,
    outbound_tx: Option<Sender<OwnedQuorumMessage>>,
    inbound_rx: Option<Receiver<OwnedQuorumMessage>>,
    converter: CapnpProstConverter,

    // Worker identity
    self_id: u32,
    member_id: u64,
    member_endpoint: String,

    // Raft state
    term: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    commit_index: u64,
    next_index: u64,

    // Heartbeat state
    send_heartbeats: bool,
    last_heartbeat: u128,

    // Pending operations
    ongoing_backfill: Option<OngoingBackfill>,
    pending_write_batches: HashMap<String, PendingWriteBatch>,
    pending_append_entries: HashMap<u64, std::time::Instant>,

    // Configuration
    max_message_size_bytes: usize,
}

impl CapnpQuorumWorker {
    pub fn new(
        work_queue: Arc<SegQueue<LocalQuorumWorkerTask>>,
        response_queue: Arc<SegQueue<LocalQuorumResponse>>,
        self_id: u32,
        next_index: u64,
        member_id: u64,
        member_endpoint: String,
        stream_manager: Arc<CapnpStreamManager>,
        task_queue: Arc<SegQueue<LocalRaftMessage>>,
        max_message_size_bytes: usize,
    ) -> Self {
        Self {
            work_queue,
            response_queue,
            task_queue,
            stream_manager,
            outbound_tx: None,
            inbound_rx: None,
            converter: CapnpProstConverter::new(),
            self_id,
            member_id,
            member_endpoint,
            term: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            commit_index: 0,
            next_index,
            send_heartbeats: false,
            last_heartbeat: 0,
            ongoing_backfill: None,
            pending_write_batches: HashMap::new(),
            pending_append_entries: HashMap::new(),
            max_message_size_bytes,
        }
    }

    // =========================================================================
    // Main Worker Loop
    // =========================================================================

    pub async fn run(&mut self) {
        log::info!("Running Cap'n Proto quorum worker for member {}", self.member_id);
        let mut pending_tasks: Vec<PendingQuorumTask> = vec![];

        loop {
            // Ensure we have connections
            if !self.ensure_connections().await {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            // Process incoming messages from remote node
            self.process_incoming_messages(&mut pending_tasks);

            // Process pending task responses
            self.process_pending_task_responses(&mut pending_tasks);

            // Send heartbeats if we're the leader
            self.send_heartbeat_if_needed().await;

            // Process work queue items
            self.process_work_queue().await;

            // Yield to other tasks
            yield_now().await;
        }
    }

    // =========================================================================
    // Connection Management
    // =========================================================================

    async fn ensure_connections(&mut self) -> bool {
        // Check if outbound connection is still valid in stream manager
        if self.outbound_tx.is_some() {
            if !self.stream_manager.has_outbound_connection(self.member_id as u32).await {
                log::info!("Outbound connection to member {} was lost, resetting", self.member_id);
                self.outbound_tx = None;
            }
        }

        // Ensure outbound connection
        if self.outbound_tx.is_none() {
            match self.stream_manager.get_or_connect(self.member_id as u32).await {
                Ok(tx) => {
                    log::info!("Established outbound connection to member {}", self.member_id);
                    self.outbound_tx = Some(tx);
                }
                Err(e) => {
                    log::error!("Failed to connect to member {}: {}", self.member_id, e);
                    return false;
                }
            }
        }

        // Check for inbound connection
        if self.inbound_rx.is_none() {
            if let Some(rx) = self.stream_manager.get_inbound_queue(self.member_id as u32) {
                log::info!("Inbound queue ready for member {}", self.member_id);
                self.inbound_rx = Some(rx);
            }
        }

        // We need at least outbound to proceed
        self.outbound_tx.is_some()
    }

    fn reset_connections(&mut self) {
        self.outbound_tx = None;
        self.inbound_rx = None;
    }

    // =========================================================================
    // Message Sending
    // =========================================================================

    fn send_message(&self, msg: OwnedQuorumMessage) -> bool {
        if let Some(ref tx) = self.outbound_tx {
            if tx.send(msg).is_err() {
                log::warn!("Failed to send message to member {}", self.member_id);
                false
            } else {
                true
            }
        } else {
            warn!("failed to send message to member {}, outbound_tx is empty", self.member_id);
            false
        }
    }

    async fn send_heartbeat_if_needed(&mut self) {
        if !self.send_heartbeats {
            return;
        }

        let now = now_millis();
        if now - self.last_heartbeat < HEARTBEAT_INTERVAL_MS {
            return;
        }

        self.last_heartbeat = now;

        let request = RemoteAppendEntriesRequest {
            term: self.term,
            leader_id: self.self_id,
            prev_log_index: self.prev_log_index,
            prev_log_term: self.prev_log_term,
            request_id: 0, // Heartbeat
            entry: None,
            commit_index: self.commit_index,
        };

        let msg = self.converter.append_entries_to_capnp(&request);
        self.send_message(msg);
    }

    fn send_vote_request(&mut self, id: u64, term: u64, last_index: u64, last_term: u64, _parent_span: Span) {
        log::info!("sending request vote with request id {} for term {}", id, term);
        let request = RemoteVoteRequest {
            term,
            candidate_id: self.self_id,
            last_log_index: last_index,
            last_log_term: last_term,
            request_id: id,
        };

        let msg = self.converter.vote_request_to_capnp(&request);
        self.send_message(msg);
    }

    fn send_append_entries(&mut self, id: u64, req: RemoteAppendEntriesRequest, _parent_span: Span) {
        let request_id = id;
        let prev_idx = req.entry.as_ref().unwrap().prev_log_index;
        if self.prev_log_index != prev_idx {
            error!("Trying to send non-consecutive append entries request, prev index {} request prev index {}, id {}", self.prev_log_index, prev_idx, id);
        }
        if prev_idx != req.prev_log_index {
            error!("Append entry prev log index and entry prev log index doesn't match req {}, entry {}", req.prev_log_index, prev_idx);
        }

        self.prev_log_term = req.entry.as_ref().unwrap().term;
        self.prev_log_index = req.entry.as_ref().unwrap().index;
        self.commit_index = req.commit_index;
        self.pending_append_entries.insert(request_id, std::time::Instant::now());

        let msg = self.converter.append_entries_to_capnp(&req);
        if !self.send_message(msg) {
            error!("Sending append entries message failed")
        }
        self.last_heartbeat = now_millis();
    }

    fn send_write_batch(&mut self, write_batch: WriteBatch) {
        let batch_id = write_batch.batch_id.clone();

        let request = RemotePutBatchRequest {
            put_request: write_batch.requests,
            batch_id: batch_id.clone(),
        };

        self.pending_write_batches.insert(batch_id.clone(), PendingWriteBatch {
            callback: write_batch.callback,
            start_time: std::time::Instant::now(),
        });

        let msg = self.converter.put_batch_request_to_capnp(&request);
        self.send_message(msg);
    }

    fn send_truncate_log(&mut self, prev_term: u64, prev_index: u64, _parent_span: Span) {
        let request = RemoteTruncateLogRequest {
            prev_term,
            prev_index,
            term: self.term,
            leader_id: self.self_id,
            request_id: 0,
        };

        let msg = self.converter.truncate_log_request_to_capnp(&request);
        self.send_message(msg);
    }

    fn send_backfill_entries(&mut self, entries: Vec<Arc<SerializationData>>) {
        use std::ops::Deref;
        use crate::service_utils::storage_utils::serialize_data;

        if entries.is_empty() {
            return;
        }

        let first = entries.first();
        log::info!(
            "received backfill log request with {} entries. First entry: {}",
            entries.len(),
            first
                .map_or("None".to_string(), |e| format!(
                    "prev_term: {}, prev_index: {}",
                    e.prev_term, e.prev_index
                ))
        );

        for entry in entries {
            // Serialize the entry data
            let buffer = vec![0u8; self.max_message_size_bytes];
            let (limit, mut serialized) = serialize_data(&entry, buffer, 0);
            serialized.truncate(limit);

            let entry_data = entry.deref();
            let request = RemoteAppendEntriesRequest {
                term: self.term,
                leader_id: self.self_id,
                prev_log_index: entry_data.prev_index,
                prev_log_term: entry_data.prev_term,
                request_id: entry_data.index, // Use index as request_id for backfill
                entry: Some(RemoteLogEntry {
                    index: entry_data.index,
                    data: serialized,
                    batch_index: 0,
                    term: entry_data.term,
                    prev_log_term: entry_data.prev_term,
                    prev_log_index: entry_data.prev_index,
                    message_id: entry_data.index,
                }),
                commit_index: self.commit_index,
            };

            let msg = self.converter.append_entries_to_capnp(&request);
            if !self.send_message(msg) {
                break;
            }
        }
        self.ongoing_backfill = None;
        self.last_heartbeat = now_millis();
    }

    // =========================================================================
    // Work Queue Processing
    // =========================================================================

    async fn process_work_queue(&mut self) {
        for _ in 0..MAX_MESSAGES_PER_ITERATION {
            let task = match self.work_queue.pop() {
                Some(t) => t,
                None => break,
            };

            match task.task_type {
                LocalQuorumWorkerTaskType::StartHeartbeats { term, prev_term, prev_log_index, commit_index, .. } => {
                    self.send_heartbeats = true;
                    self.term = term;
                    self.prev_log_term = prev_term;
                    self.prev_log_index = prev_log_index;
                    self.commit_index = commit_index;
                    log::info!("Started heartbeats for member {} at term {}", self.member_id, term);
                }
                LocalQuorumWorkerTaskType::StopHeartbeats { .. } => {
                    self.send_heartbeats = false;
                    log::info!("Stopped heartbeats for member {}", self.member_id);
                }
                LocalQuorumWorkerTaskType::RequestVote { term, last_term, last_index, parent_span, id } => {
                    self.send_vote_request(id, term, last_index, last_term, parent_span);
                }
                LocalQuorumWorkerTaskType::AppendEntries { req, parent_span, id } => {
                    self.send_append_entries(id, req, parent_span);
                }
                LocalQuorumWorkerTaskType::BackfillLog { data } => {
                    self.send_backfill_entries(data);
                }
                LocalQuorumWorkerTaskType::WriteBatch { write_batch } => {
                    self.send_write_batch(write_batch);
                }
                LocalQuorumWorkerTaskType::TruncateLog { prev_term, prev_index, parent_span, .. } => {
                    self.send_truncate_log(prev_term, prev_index, parent_span);
                }
            }
        }
    }

    // =========================================================================
    // Incoming Message Processing
    // =========================================================================

    fn process_incoming_messages(&mut self, pending_tasks: &mut Vec<PendingQuorumTask>) {
        // First, collect messages to avoid borrow issues
        let (messages, disconnected) = {
            let rx = match &self.inbound_rx {
                Some(rx) => rx,
                None => return,
            };

            let mut msgs = Vec::new();
            let mut is_disconnected = false;
            for _ in 0..MAX_MESSAGES_PER_ITERATION {
                match rx.try_recv() {
                    Ok(msg) => msgs.push(msg),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        log::warn!("Inbound channel disconnected for member {}", self.member_id);
                        is_disconnected = true;
                        break;
                    }
                }
            }
            (msgs, is_disconnected)
        };

        // Clear inbound_rx if channel was disconnected
        if disconnected {
            self.inbound_rx = None;
        }

        // Now process collected messages
        for msg in messages {
            if let Err(e) = self.handle_inbound_message(&msg, pending_tasks) {
                log::warn!("Failed to handle inbound message: {:?}", e);
            }
        }
    }

    fn handle_inbound_message(&mut self, msg: &OwnedQuorumMessage, pending_tasks: &mut Vec<PendingQuorumTask>) -> capnp::Result<()> {
        let payload = self.converter.process_inbound_message(msg)?;

        match payload {
            InboundMessagePayload::VoteResponse(resp) => {
                self.on_vote_response(resp);
            }
            InboundMessagePayload::AppendEntriesAcknowledge(ack) => {
                self.on_append_entries_ack(ack);
            }
            InboundMessagePayload::PutBatchResponse(resp) => {
                self.on_put_batch_response(resp);
            }
            InboundMessagePayload::TruncateLogResponse(resp) => {
                self.on_truncate_log_response(resp);
            }
            InboundMessagePayload::AppendEntriesRequest(req) => {
                self.on_append_entries_request(req, pending_tasks);
            }
            InboundMessagePayload::VoteRequest(req) => {
                self.on_vote_request(req, pending_tasks);
            }
            InboundMessagePayload::PutBatchRequest(req) => {
                self.on_put_batch_request(req, pending_tasks);
            }
            InboundMessagePayload::TruncateLogRequest(req) => {
                self.on_truncate_log_request(req, pending_tasks);
            }
            _ => {
                warn!("Unexpected inbound message payload")
            }
        }

        Ok(())
    }

    // =========================================================================
    // Response Handlers
    // =========================================================================

    fn on_vote_response(&mut self, resp: RemoteVoteResponse) {
        self.response_queue.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::RequestVoteResponse {
                id: resp.request_id,
                term: resp.term,
                received_vote: resp.vote_granted,
            },
        });
    }

    fn on_append_entries_ack(&mut self, ack: RemoteAppendEntriesAcknowledge) {
        // Record latency if we have a pending request
        if let Some(start_time) = self.pending_append_entries.remove(&ack.request_id) {
            let latency_micros = start_time.elapsed().as_micros() as f64;
            QUORUM_APPEND_ENTRIES_LATENCY
                .with_label_values(&[&self.member_id.to_string()])
                .observe(latency_micros);
        }

        if !ack.ok {
            // Follower is behind, need backfill
            log::info!(
                "received append entries acknowledge from: {}, but it was not ok, backfilling log from term {} and index {}",
                self.member_id,
                ack.last_log_term,
                ack.last_log_index
            );
            if self.ongoing_backfill.is_none() {
                self.ongoing_backfill = Some(OngoingBackfill {
                    from_term: ack.last_log_term,
                    from_index: ack.last_log_index,
                });
                self.response_queue.push(LocalQuorumResponse {
                    response_type: LocalQuorumTaskResponseType::BackfillLog {
                        quorum_node_id: self.member_id,
                        last_log_term: ack.last_log_term,
                        last_log_index: ack.last_log_index,
                    },
                });
            } else {
                log::warn!("Ongoing backfill already in progress, ignoring new backfill request for term {} and index {}", ack.last_log_term, ack.last_log_index);
            }
        } else if ack.request_id != 0 {
            self.response_queue.push(LocalQuorumResponse {
                response_type: LocalQuorumTaskResponseType::AppendEntries {
                    id: ack.request_id,
                    ok: ack.ok,
                },
            });
        }
    }

    fn on_put_batch_response(&mut self, resp: RemotePutBatchResponse) {
        if let Some(pending) = self.pending_write_batches.remove(&resp.batch_id) {
            let latency_micros = pending.start_time.elapsed().as_micros() as f64;
            QUORUM_PUT_BATCH_LATENCY
                .with_label_values(&[&self.member_id.to_string()])
                .observe(latency_micros);

            let write_response = WriteResponse {
                responses: resp.responses,
                status_code: tonic::codegen::http::StatusCode::OK,
            };

            let _ = pending.callback.send(write_response);
        }
    }

    fn on_truncate_log_response(&mut self, resp: RemoteTruncateLogResponse) {
        log::info!(
            "Received truncate log response for request {}, ok: {}",
            resp.request_id,
            resp.ok
        );
        self.response_queue.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::TruncateLogResponse {
                id: resp.request_id,
                ok: resp.ok,
            },
        });
        self.ongoing_backfill = None
    }

    // =========================================================================
    // Request Handlers (for when we receive requests from remote nodes)
    // =========================================================================

    fn on_append_entries_request(&mut self, req: RemoteAppendEntriesRequest, pending_tasks: &mut Vec<PendingQuorumTask>) {
        let (callback_tx, callback_rx) = oneshot::channel();
        let span = tracing::span!(Level::INFO, "capnp_append_entries_request");

        let index = req.entry.as_ref().map(|e| e.index).unwrap_or(0);
        let prev_index = req.entry.as_ref().map(|e| e.prev_log_index).unwrap_or(0);
        let term = req.entry.as_ref().map(|e| e.term).unwrap_or(0);
        let request_id = req.request_id;

        // Forward to local Raft state machine
        self.task_queue.push(LocalRaftMessage {
            payload: LocalRaftMessagePayload::AppendEntries(LocalAppendEntries {
                leader_id: req.leader_id,
                term: req.term,
                prev_term: req.prev_log_term,
                prev_index: req.prev_log_index,
                request_id: req.request_id,
                entry: req.entry,
                commit_index: req.commit_index,
            }),
            callback: callback_tx,
            parent_span: span,
        });

        // Store pending task to poll for response
        pending_tasks.push(PendingQuorumTask {
            callback: callback_rx,
            task: PendingQuorumTaskEnum::AppendEntries {
                request_id,
                log_index: index,
                term,
            },
            start_time: std::time::Instant::now(),
        });
    }

    fn on_truncate_log_request(&mut self, req: RemoteTruncateLogRequest, pending_tasks: &mut Vec<PendingQuorumTask>) {
        log::info!(
            "Received truncate log request from leader {} for term {} index {}",
            req.leader_id,
            req.prev_term,
            req.prev_index
        );

        // Send to local task queue for processing
        let resp = LocalQuorumTaskResponseType::TruncateLog {
            quorum_node_id: self.member_id,
            prev_term: req.prev_term,
            prev_index: req.prev_index,
        };
        self.response_queue.push(LocalQuorumResponse { response_type: resp });
        let resp = RemoteTruncateLogResponse{
            request_id: req.request_id,
            ok: true,
        };

        let msg = self.converter.truncate_log_response_to_capnp(&resp);
        if !self.send_message(msg) {
            warn!("Failed to send truncate log response back to leader")
        }
    }

    fn on_vote_request(&mut self, req: RemoteVoteRequest, pending_tasks: &mut Vec<PendingQuorumTask>) {
        let (callback_tx, callback_rx) = oneshot::channel();
        let span = tracing::span!(Level::INFO, "capnp_vote_request");

        // Forward to local Raft state machine
        self.task_queue.push(LocalRaftMessage {
            payload: LocalRaftMessagePayload::RequestVote(LocalRequestVoteRequest {
                term: req.term,
                candidate_id: req.candidate_id,
                last_log_index: req.last_log_index,
                last_log_term: req.last_log_term,
            }),
            callback: callback_tx,
            parent_span: span,
        });

        // Store pending task to poll for response
        pending_tasks.push(PendingQuorumTask {
            callback: callback_rx,
            task: PendingQuorumTaskEnum::Vote(req),
            start_time: std::time::Instant::now(),
        });
    }

    fn on_put_batch_request(&mut self, req: RemotePutBatchRequest, pending_tasks: &mut Vec<PendingQuorumTask>) {
        let (callback_tx, callback_rx) = oneshot::channel();
        let span = tracing::span!(Level::INFO, "capnp_put_batch_request");
        let batch_id = req.batch_id.clone();

        // Forward to local Raft state machine
        self.task_queue.push(LocalRaftMessage {
            payload: LocalRaftMessagePayload::WriteBatch(LocalRaftWriteBatchRequest {
                requests: req.put_request,
            }),
            callback: callback_tx,
            parent_span: span,
        });

        // Store pending task to poll for response
        pending_tasks.push(PendingQuorumTask {
            callback: callback_rx,
            task: PendingQuorumTaskEnum::PutBatch { batch_id },
            start_time: std::time::Instant::now(),
        });
    }

    // =========================================================================
    // Pending Task Response Processing
    // =========================================================================

    fn process_pending_task_responses(&mut self, pending_tasks: &mut Vec<PendingQuorumTask>) {
        let mut to_remove = vec![];

        for (i, pt) in pending_tasks.iter_mut().enumerate() {
            match pt.callback.try_recv() {
                Ok(response) => {
                    self.handle_pending_task_response(&pt.task, response);
                    to_remove.push(i);
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    // Still waiting for response
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    log::warn!("Pending task callback channel closed without response");
                    to_remove.push(i);
                }
            }
        }

        // Remove completed/closed tasks (reverse order to maintain indices)
        for i in to_remove.into_iter().rev() {
            pending_tasks.remove(i);
        }
    }

    fn handle_pending_task_response(&mut self, task: &PendingQuorumTaskEnum, response: LocalRaftResponseMessage) {
        match response.payload {
            LocalRaftResponsePayload::RequestVote(granted) => {
                if let PendingQuorumTaskEnum::Vote(ref req) = task {
                    let resp = RemoteVoteResponse {
                        vote_granted: granted,
                        request_id: req.request_id,
                        term: req.term,
                    };
                    let msg = self.converter.vote_response_to_capnp(&resp);
                    self.send_message(msg);
                }
            }
            LocalRaftResponsePayload::AppendEntries(ae_resp) => {
                if let PendingQuorumTaskEnum::AppendEntries { request_id, log_index, term } = task {
                    let (ok, last_log_index, last_log_term) = match ae_resp {
                        LocalAppendEntriesCallbackResponse::Ok => (true, *log_index, *term),
                        LocalAppendEntriesCallbackResponse::UnrecognizedLeader => (false, *log_index, *term),
                        LocalAppendEntriesCallbackResponse::WantedPreviousEntry { last_term, last_index } => {
                            (false, last_index, last_term)
                        }
                        LocalAppendEntriesCallbackResponse::TruncateLog { prev_term, prev_index } => {
                            // Also notify local response queue about truncation
                            self.response_queue.push(LocalQuorumResponse {
                                response_type: LocalQuorumTaskResponseType::TruncateLog {
                                    quorum_node_id: self.member_id,
                                    prev_term,
                                    prev_index,
                                },
                            });
                            (false, prev_index, prev_term)
                        }
                    };

                    let ack = RemoteAppendEntriesAcknowledge {
                        last_log_index,
                        last_log_term,
                        request_id: *request_id,
                        ok,
                    };
                    let msg = self.converter.append_entries_ack_to_capnp(&ack);
                    self.send_message(msg);
                } else {
                    error!("Something bricked with LocalRaftResponsePayload::AppendEntries pending task doesn't match response type")
                }
            }
            LocalRaftResponsePayload::WriteBatch(write_resp) => {
                if let PendingQuorumTaskEnum::PutBatch { ref batch_id } = task {
                    let resp = RemotePutBatchResponse {
                        responses: write_resp.responses,
                        batch_id: batch_id.clone(),
                    };
                    let msg = self.converter.put_batch_response_to_capnp(&resp);
                    self.send_message(msg);
                }
            }
            _ => {
                log::warn!("Unexpected response payload type for pending task");
            }
        }
    }
}
