use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use crossbeam_queue::SegQueue;
use log::{error, info, warn};
use tracing::{Level, Span};

use crate::quorum::types::{
    LocalQuorumWorkerTask, LocalQuorumWorkerTaskType,
    LocalQuorumResponse, LocalQuorumTaskResponseType,
};
use crate::raft::raft_sm::{
    LocalRaftMessage, LocalRaftMessagePayload, LocalAppendEntries, LocalRequestVoteRequest,
    LocalRaftResponseMessage, LocalRaftResponsePayload, LocalAppendEntriesCallbackResponse,
    LocalRaftWriteBatchRequest,
};
use crate::service_utils::app_time::now_millis;
use crate::transport::capnp::owned_message::OwnedQuorumMessage;
use crate::transport::capnp::raft_capnp::remote_quorum_message;
use crate::transport::capnp_stream_manager::CapnpStreamManager;
use crate::transport::raft::raftproto::RemoteLogEntry;
use crate::service_utils::storage_utils::SerializationData;
use crate::transport::write_proxy::{WriteBatch, WriteResponse};
use crate::transport::metrics::{QUORUM_APPEND_ENTRIES_LATENCY, QUORUM_PUT_BATCH_LATENCY};
use crate::transport::capnp::{
    build_owned_write_batch, build_owned_write_batch_response,
    build_heartbeat_message, build_vote_request_message, build_vote_response_message,
    build_append_entries_ack_message, build_put_batch_request_message,
    build_put_batch_response_message, build_truncate_log_request_message, build_truncate_log_response_message,
};
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::time::Instant;

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
    Vote { request_id: u64, term: u64 },
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
    // Queues for inter-process communication
    work_queue: Arc<SegQueue<LocalQuorumWorkerTask>>,
    response_queue: Arc<SegQueue<LocalQuorumResponse>>,
    task_queue: Arc<SegQueue<LocalRaftMessage>>,

    // Cap'n Proto transport
    stream_manager: Arc<CapnpStreamManager>,
    outbound_tx: Option<Sender<OwnedQuorumMessage>>,
    inbound_rx: Option<Receiver<OwnedQuorumMessage>>,

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

        let desired_cadence_micros = 25;
        loop {
            let start_time = Instant::now();
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

            let elapsed_micros = start_time.elapsed().as_micros();
            if elapsed_micros < desired_cadence_micros {
                tokio::time::sleep(Duration::from_micros((desired_cadence_micros-elapsed_micros) as u64)).await;
            } else {
                // Yield to other tasks
                yield_now().await;
            }
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

        let msg = build_heartbeat_message(
            self.term,
            self.self_id,
            self.prev_log_index,
            self.prev_log_term,
            self.commit_index,
            0, // request_id = 0 for heartbeat
        );
        self.send_message(msg);
    }

    fn send_vote_request(&mut self, id: u64, term: u64, last_index: u64, last_term: u64, _parent_span: Span) {
        log::info!("sending request vote with request id {} for term {} to member {}", id, term, self.member_id);
        let msg = build_vote_request_message(term, self.self_id, last_index, last_term, id);
        self.send_message(msg);
    }

    fn send_append_entries(&mut self, id: u64, entry: &Arc<crate::transport::capnp::OwnedLogEntry>, commit_index: u64, _parent_span: Span) {
        let request_id = id;
        let prev_idx = entry.prev_log_index();
        if self.prev_log_index != prev_idx {
            error!("Trying to send non-consecutive append entries request, prev index {} request prev index {}, id {}", self.prev_log_index, prev_idx, id);
        }

        self.prev_log_term = entry.term();
        self.prev_log_index = entry.index();
        self.commit_index = commit_index; // Update cached commit_index
        self.pending_append_entries.insert(request_id, std::time::Instant::now());

        // Build append entries message directly using Cap'n Proto from OwnedLogEntry
        let msg = crate::transport::capnp::build_append_entries_message(
            self.term,
            self.self_id,
            prev_idx,
            entry.prev_log_term(),
            request_id,
            commit_index, // Use the passed commit_index
            Some(entry),
        );

        if !self.send_message(msg) {
            error!("Sending append entries message failed")
        }
        self.last_heartbeat = now_millis();
    }

    fn send_write_batch(&mut self, write_batch: WriteBatch) {
        let batch_id = write_batch.batch_id.clone();

        self.pending_write_batches.insert(batch_id.clone(), PendingWriteBatch {
            callback: write_batch.callback,
            start_time: std::time::Instant::now(),
        });

        // Build Cap'n Proto message directly from OwnedWriteBatch
        let msg = build_put_batch_request_message(&write_batch.message);
        self.send_message(msg);
    }

    fn send_truncate_log(&mut self, prev_term: u64, prev_index: u64, _parent_span: Span) {
        let msg = build_truncate_log_request_message(prev_term, prev_index, self.term, self.self_id, 0);
        self.send_message(msg);
    }

    fn send_backfill_entries(&mut self, entries: Vec<Arc<SerializationData>>) {
        use std::ops::Deref;
        use crate::service_utils::storage_utils::serialize_data;
        use crate::transport::capnp::{build_owned_log_entry, build_append_entries_message};

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
            // Serialize the entry data to SBE format
            let buffer = vec![0u8; self.max_message_size_bytes];
            let (limit, mut serialized) = serialize_data(&entry, buffer, 0);
            serialized.truncate(limit);

            let entry_data = entry.deref();

            // Build OwnedLogEntry from SerializationData
            let owned_entry = Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(entry_data.index);
                builder.set_term(entry_data.term);
                builder.set_prev_log_index(entry_data.prev_index);
                builder.set_prev_log_term(entry_data.prev_term);
                builder.set_message_id(entry_data.index); // Use index as message_id for backfill
                builder.set_timestamp(entry_data.timestamp);
                builder.set_data(&serialized);
                // Set commands from the requests
                let mut commands = builder.init_commands(entry_data.requests.len() as u32);
                for (i, req) in entry_data.requests.iter().enumerate() {
                    let mut cmd = commands.reborrow().get(i as u32);
                    cmd.set_id(&req.id);
                    cmd.set_payload(req.payload.as_bytes());
                    cmd.set_node_id(req.node_id);
                }
            }));

            // Build and send append entries message using Cap'n Proto directly
            let msg = build_append_entries_message(
                self.term,
                self.self_id,
                entry_data.prev_index,
                entry_data.prev_term,
                entry_data.index, // Use index as request_id for backfill
                self.commit_index,
                Some(&owned_entry),
            );

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
                LocalQuorumWorkerTaskType::AppendEntries { entry, parent_span, id, commit_index } => {
                    self.send_append_entries(id, &entry, commit_index, parent_span);
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
        use remote_quorum_message::message_payload::Which;

        msg.with_message(|reader| {
            let payload = reader.get_message_payload();
            match payload.which()? {
                Which::VoteResponse(resp) => {
                    let resp = resp?;
                    self.on_vote_response(
                        resp.get_request_id(),
                        resp.get_term(),
                        resp.get_vote_granted(),
                    );
                }
                Which::AppendEntriesAcknowledge(ack) => {
                    let ack = ack?;
                    self.on_append_entries_ack(
                        ack.get_request_id(),
                        ack.get_last_log_index(),
                        ack.get_last_log_term(),
                        ack.get_ok(),
                    );
                }
                Which::PutBatchResponse(resp) => {
                    let resp = resp?;
                    self.on_put_batch_response_capnp(&resp);
                }
                Which::TruncateLogResponse(resp) => {
                    let resp = resp?;
                    self.on_truncate_log_response(resp.get_request_id(), resp.get_ok());
                }
                Which::AppendEntriesRequest(req) => {
                    let req = req?;
                    self.on_append_entries_request_capnp(&req, pending_tasks);
                }
                Which::VoteRequest(req) => {
                    let req = req?;
                    self.on_vote_request(
                        req.get_term(),
                        req.get_candidate_id(),
                        req.get_last_log_index(),
                        req.get_last_log_term(),
                        req.get_request_id(),
                        pending_tasks,
                    );
                }
                Which::PutBatchRequest(req) => {
                    let req = req?;
                    self.on_put_batch_request_capnp(&req, pending_tasks);
                }
                Which::TruncateLogRequest(req) => {
                    let req = req?;
                    self.on_truncate_log_request(
                        req.get_prev_term(),
                        req.get_prev_index(),
                        req.get_term(),
                        req.get_leader_id(),
                        req.get_request_id(),
                    );
                }
                Which::ConnectRequest(_) | Which::ConnectResponse(_) => {
                    // Ignore connection handshake messages
                }
            }
            Ok(())
        })?
    }

    // =========================================================================
    // Response Handlers
    // =========================================================================

    fn on_vote_response(&mut self, request_id: u64, term: u64, vote_granted: bool) {
        info!("Received vote response for term {} with granted {} from member {}", term, vote_granted, self.member_id);
        self.response_queue.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::RequestVoteResponse {
                id: request_id,
                term,
                received_vote: vote_granted,
            },
        });
    }

    fn on_append_entries_ack(&mut self, request_id: u64, last_log_index: u64, last_log_term: u64, ok: bool) {
        // Record latency if we have a pending request
        if let Some(start_time) = self.pending_append_entries.remove(&request_id) {
            let latency_micros = start_time.elapsed().as_micros() as f64;
            QUORUM_APPEND_ENTRIES_LATENCY
                .with_label_values(&[&self.member_id.to_string()])
                .observe(latency_micros);
        }

        if !ok {
            // Follower is behind, need backfill
            log::info!(
                "received append entries acknowledge from: {}, but it was not ok, backfilling log from term {} and index {}",
                self.member_id,
                last_log_term,
                last_log_index
            );
            if self.ongoing_backfill.is_none() {
                self.ongoing_backfill = Some(OngoingBackfill {
                    from_term: last_log_term,
                    from_index: last_log_index,
                });
                self.response_queue.push(LocalQuorumResponse {
                    response_type: LocalQuorumTaskResponseType::BackfillLog {
                        quorum_node_id: self.member_id,
                        last_log_term,
                        last_log_index,
                    },
                });
            } else {
                log::warn!("Ongoing backfill already in progress, ignoring new backfill request for term {} and index {}", last_log_term, last_log_index);
            }
        } else if request_id != 0 {
            self.response_queue.push(LocalQuorumResponse {
                response_type: LocalQuorumTaskResponseType::AppendEntries {
                    id: request_id,
                    ok,
                },
            });
        }
    }

    fn on_put_batch_response_capnp(&mut self, resp: &crate::transport::capnp::raft_capnp::remote_put_batch_response::Reader<'_>) {
        let batch_id = resp.get_batch_id().map(|s| s.to_string().unwrap_or_default()).unwrap_or_default();

        if let Some(pending) = self.pending_write_batches.remove(&batch_id) {
            let latency_micros = pending.start_time.elapsed().as_micros() as f64;
            QUORUM_PUT_BATCH_LATENCY
                .with_label_values(&[&self.member_id.to_string()])
                .observe(latency_micros);

            // Build OwnedWriteBatchResponse directly from Cap'n Proto
            let message = build_owned_write_batch_response(|mut builder| {
                builder.set_batch_id(&batch_id);
                if let Ok(responses) = resp.get_responses() {
                    let mut out_responses = builder.init_responses(responses.len());
                    for (i, r) in responses.iter().enumerate() {
                        let mut out = out_responses.reborrow().get(i as u32);
                        if let Ok(id) = r.get_id() {
                            out.set_id(id);
                        }
                        if let Ok(response_type) = r.get_response_type() {
                            out.set_response_type(response_type);
                        }
                        if let Ok(message) = r.get_message() {
                            out.set_message(message);
                        }
                        out.set_node_id(r.get_node_id());
                        if let Ok(batch_id) = r.get_batch_id() {
                            out.set_batch_id(batch_id);
                        }
                    }
                }
            });

            if let Err(_) = pending.callback.send(WriteResponse::success(message)) {
                info!("Failed to respond to put batch callback, receiver dropped")
            }
        }
    }

    fn on_truncate_log_response(&mut self, request_id: u64, ok: bool) {
        log::info!(
            "Received truncate log response for request {}, ok: {}",
            request_id,
            ok
        );
        self.response_queue.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::TruncateLogResponse {
                id: request_id,
                ok,
            },
        });
        self.ongoing_backfill = None
    }

    // =========================================================================
    // Request Handlers (for when we receive requests from remote nodes)
    // =========================================================================

    fn on_append_entries_request_capnp(
        &mut self,
        req: &crate::transport::capnp::raft_capnp::remote_append_entries_request::Reader<'_>,
        pending_tasks: &mut Vec<PendingQuorumTask>,
    ) {
        let (callback_tx, callback_rx) = oneshot::channel();
        let span = tracing::span!(Level::INFO, "capnp_append_entries_request");

        let request_id = req.get_request_id();
        let req_term = req.get_term();
        let leader_id = req.get_leader_id();
        let prev_log_index = req.get_prev_log_index();
        let prev_log_term = req.get_prev_log_term();
        let commit_index = req.get_commit_index();

        log::debug!("Received AppendEntries request_id={} term={} leader={} prev_idx={} has_entry={}",
            request_id, req_term, leader_id, prev_log_index, req.has_entry());

        // Extract entry info if present
        let (entry_index, entry_term, entry) = if req.has_entry() {
            if let Ok(e) = req.get_entry() {
                // Convert Cap'n Proto RemoteLogEntry to protobuf RemoteLogEntry for LocalAppendEntries
                // (LocalAppendEntries still uses Option<RemoteLogEntry> for the entry field)
                let data = e.get_data().map(|d| d.to_vec()).unwrap_or_default();
                log::debug!("AppendEntries entry: index={} term={} data_size={}",
                    e.get_index(), e.get_term(), data.len());
                let proto_entry = RemoteLogEntry {
                    index: e.get_index(),
                    data,
                    batch_index: e.get_batch_index(),
                    term: e.get_term(),
                    prev_log_term: e.get_prev_log_term(),
                    prev_log_index: e.get_prev_log_index(),
                    message_id: e.get_message_id(),
                };
                (e.get_index(), e.get_term(), Some(proto_entry))
            } else {
                log::warn!("AppendEntries has_entry=true but get_entry failed");
                (0, 0, None)
            }
        } else {
            (0, 0, None)
        };

        // Forward to local Raft state machine
        self.task_queue.push(LocalRaftMessage {
            payload: LocalRaftMessagePayload::AppendEntries(LocalAppendEntries {
                leader_id,
                term: req_term,
                prev_term: prev_log_term,
                prev_index: prev_log_index,
                request_id,
                entry,
                commit_index,
            }),
            callback: callback_tx,
            parent_span: span,
        });

        // Store pending task to poll for response
        pending_tasks.push(PendingQuorumTask {
            callback: callback_rx,
            task: PendingQuorumTaskEnum::AppendEntries {
                request_id,
                log_index: entry_index,
                term: entry_term,
            },
            start_time: std::time::Instant::now(),
        });
    }

    fn on_truncate_log_request(&mut self, prev_term: u64, prev_index: u64, _term: u64, leader_id: u32, request_id: u64) {
        log::info!(
            "Received truncate log request from leader {} for term {} index {}",
            leader_id,
            prev_term,
            prev_index
        );

        // Send to local task queue for processing
        let resp = LocalQuorumTaskResponseType::TruncateLog {
            quorum_node_id: self.member_id,
            prev_term,
            prev_index,
        };
        self.response_queue.push(LocalQuorumResponse { response_type: resp });

        // Send response using direct builder
        let msg = build_truncate_log_response_message(request_id, true);
        if !self.send_message(msg) {
            warn!("Failed to send truncate log response back to leader")
        }
    }

    fn on_vote_request(&mut self, term: u64, candidate_id: u32, last_log_index: u64, last_log_term: u64, request_id: u64, pending_tasks: &mut Vec<PendingQuorumTask>) {
        info!("Received vote request for term {} from member {}", term, candidate_id);
        let (callback_tx, callback_rx) = oneshot::channel();
        let span = tracing::span!(Level::INFO, "capnp_vote_request");

        // Forward to local Raft state machine
        self.task_queue.push(LocalRaftMessage {
            payload: LocalRaftMessagePayload::RequestVote(LocalRequestVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            }),
            callback: callback_tx,
            parent_span: span,
        });

        // Store pending task to poll for response
        pending_tasks.push(PendingQuorumTask {
            callback: callback_rx,
            task: PendingQuorumTaskEnum::Vote { request_id, term },
            start_time: std::time::Instant::now(),
        });
    }

    fn on_put_batch_request_capnp(
        &mut self,
        req: &crate::transport::capnp::raft_capnp::remote_put_batch_request::Reader<'_>,
        pending_tasks: &mut Vec<PendingQuorumTask>,
    ) {
        let (callback_tx, callback_rx) = oneshot::channel();
        let span = tracing::span!(Level::INFO, "capnp_put_batch_request");
        let batch_id = req.get_batch_id().map(|s| s.to_string().unwrap_or_default()).unwrap_or_default();

        let message = build_owned_write_batch(|mut builder| {
            builder.set_batch_id(&batch_id);
            if let Ok(put_requests) = req.get_put_requests() {
                let mut requests = builder.init_requests(put_requests.len());
                for (i, r) in put_requests.iter().enumerate() {
                    let mut out = requests.reborrow().get(i as u32);
                    if let Ok(id) = r.get_id() {
                        out.set_id(id);
                    }
                    // RemotePutRequest uses Text for payload, convert to bytes
                    if let Ok(payload) = r.get_payload() {
                        out.set_payload(payload.as_bytes());
                    }
                    out.set_node_id(r.get_node_id());
                }
            }
        });

        // Forward to local Raft state machine
        self.task_queue.push(LocalRaftMessage {
            payload: LocalRaftMessagePayload::WriteBatch(LocalRaftWriteBatchRequest {
                message,
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
                if let PendingQuorumTaskEnum::Vote { request_id, term } = task {
                    let msg = build_vote_response_message(granted, *request_id, *term);
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

                    log::debug!("Sending AppendEntries ack request_id={} ok={} last_log_index={} last_log_term={}",
                        request_id, ok, last_log_index, last_log_term);
                    let msg = build_append_entries_ack_message(last_log_term, last_log_index, *request_id, ok);
                    self.send_message(msg);
                } else {
                    error!("Something bricked with LocalRaftResponsePayload::AppendEntries pending task doesn't match response type")
                }
            }
            LocalRaftResponsePayload::WriteBatch(write_resp) => {
                if let PendingQuorumTaskEnum::PutBatch { batch_id: id } = task {
                    let msg = build_put_batch_response_message(&write_resp.message, id);
                    self.send_message(msg);
                } else {
                    error!("Something bricked with LocalRaftResponsePayload::WriteBatch pending task doesn't match response type")
                }
            }
            _ => {
                log::warn!("Unexpected response payload type for pending task");
            }
        }
    }
}
