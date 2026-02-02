pub mod inbound;
pub mod outbound;
pub mod pending;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use crossbeam_channel::{Receiver, Sender};
use crossbeam_queue::SegQueue;
use log::info;
use tracing::Span;

use crate::quorum::types::{
    LocalQuorumWorkerTask, LocalQuorumWorkerTaskType,
    LocalQuorumResponse,
};
use crate::raft::raft_sm::{LocalRaftMessage, LocalRaftResponseMessage};
use crate::service_utils::storage_utils::SerializationData;
use crate::transport::capnp::owned_message::OwnedQuorumMessage;
use crate::transport::capnp_stream_manager::CapnpStreamManager;
use crate::transport::write_proxy::{WriteBatch, WriteResponse};
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::time::Instant;

const MAX_MESSAGES_PER_ITERATION: usize = 32;

pub(crate) struct OngoingBackfill {
    pub from_term: u64,
    pub from_index: u64,
}

pub(crate) struct PendingWriteBatch {
    pub callback: oneshot::Sender<WriteResponse>,
    pub start_time: std::time::Instant,
}

#[derive(Debug)]
pub(crate) enum PendingQuorumTaskEnum {
    Vote { request_id: u64, term: u64 },
    AppendEntries { request_id: u64, log_index: u64, term: u64 },
    PutBatch { batch_id: String },
}

pub(crate) struct PendingQuorumTask {
    pub callback: oneshot::Receiver<LocalRaftResponseMessage>,
    pub task: PendingQuorumTaskEnum,
    pub start_time: std::time::Instant,
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
            inbound::process_incoming_messages(
                &mut self.inbound_rx,
                &mut pending_tasks,
                self.member_id,
                self.self_id,
                &self.outbound_tx,
                &self.response_queue,
                &self.task_queue,
                &mut self.pending_append_entries,
                &mut self.pending_write_batches,
                &mut self.ongoing_backfill,
            );

            // Process pending task responses
            pending::process_pending_task_responses(
                &mut pending_tasks,
                &self.outbound_tx,
                &self.response_queue,
                self.member_id,
            );

            // Send heartbeats if we're the leader
            outbound::send_heartbeat_if_needed(
                self.send_heartbeats,
                &mut self.last_heartbeat,
                self.term,
                self.self_id,
                self.prev_log_index,
                self.prev_log_term,
                self.commit_index,
                &self.outbound_tx,
                self.member_id,
            );

            // Process work queue items
            self.process_work_queue();

            let elapsed_micros = start_time.elapsed().as_micros();
            if elapsed_micros < desired_cadence_micros {
                tokio::time::sleep(Duration::from_micros((desired_cadence_micros - elapsed_micros) as u64)).await;
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
    // Work Queue Processing
    // =========================================================================

    fn process_work_queue(&mut self) {
        for _ in 0..MAX_MESSAGES_PER_ITERATION {
            let task = match self.work_queue.pop() {
                Some(t) => t,
                None => break,
            };

            match task.task_type {
                LocalQuorumWorkerTaskType::StartHeartbeats {
                    term,
                    prev_term,
                    prev_log_index,
                    commit_index,
                    ..
                } => {
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
                LocalQuorumWorkerTaskType::RequestVote {
                    term,
                    last_term,
                    last_index,
                    parent_span,
                    id,
                } => {
                    outbound::send_vote_request(
                        &self.outbound_tx,
                        self.member_id,
                        self.self_id,
                        id,
                        term,
                        last_index,
                        last_term,
                        parent_span,
                    );
                }
                LocalQuorumWorkerTaskType::AppendEntries {
                    entry,
                    parent_span,
                    id,
                    commit_index,
                } => {
                    outbound::send_append_entries(
                        &self.outbound_tx,
                        self.member_id,
                        self.self_id,
                        self.term,
                        &mut self.prev_log_index,
                        &mut self.prev_log_term,
                        &mut self.commit_index,
                        &mut self.pending_append_entries,
                        &mut self.last_heartbeat,
                        id,
                        &entry,
                        commit_index,
                        parent_span,
                    );
                }
                LocalQuorumWorkerTaskType::BackfillLog { data } => {
                    outbound::send_backfill_entries(
                        &self.outbound_tx,
                        self.member_id,
                        self.self_id,
                        self.term,
                        self.commit_index,
                        &mut self.last_heartbeat,
                        &mut self.ongoing_backfill,
                        self.max_message_size_bytes,
                        data,
                    );
                }
                LocalQuorumWorkerTaskType::WriteBatch { write_batch } => {
                    outbound::send_write_batch(
                        &self.outbound_tx,
                        self.member_id,
                        &mut self.pending_write_batches,
                        write_batch,
                    );
                }
                LocalQuorumWorkerTaskType::TruncateLog {
                    prev_term,
                    prev_index,
                    parent_span,
                    ..
                } => {
                    outbound::send_truncate_log(
                        &self.outbound_tx,
                        self.member_id,
                        self.self_id,
                        self.term,
                        prev_term,
                        prev_index,
                        parent_span,
                    );
                }
            }
        }
    }
}
