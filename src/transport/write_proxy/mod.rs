pub mod batch;
pub mod remote;

use std::sync::Arc;
use std::time::{Duration, Instant};
use crossbeam_queue::SegQueue;
use opentelemetry::Context;
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinSet;
use tracing::{Instrument, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::config::config::ClusterNode;
use crate::quorum::types::LocalQuorumWorkerTask;
use crate::raft::raft_sm::{
    LocalRaftMessage, LocalRaftMessagePayload, LocalRaftResponseMessage, LocalRaftResponsePayload,
    LocalRaftWriteBatchRequest,
};
use crate::service_utils::app_time::now_millis;
use crate::transport::capnp::{build_owned_write_batch_response, OwnedWriteBatch, OwnedWriteBatchResponse};

/// Status codes for write operations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriteStatus {
    Ok,
    InternalServerError,
}

/// Response from a write batch operation.
/// Contains Cap'n Proto response for zero-copy internal handling.
#[derive(Debug)]
pub struct WriteResponse {
    pub message: OwnedWriteBatchResponse,
    pub status: WriteStatus,
}

impl WriteResponse {
    /// Create an empty error response
    pub fn error() -> Self {
        let message = build_owned_write_batch_response(|_| {});
        Self { message, status: WriteStatus::InternalServerError }
    }

    /// Create a successful response from Cap'n Proto message
    pub fn success(message: OwnedWriteBatchResponse) -> Self {
        Self {
            message,
            status: WriteStatus::Ok,
        }
    }
}

/// A batch of write requests using Cap'n Proto for zero-copy handling.
#[derive(Debug)]
pub struct WriteBatch {
    pub batch_id: String,
    pub message: OwnedWriteBatch,
    pub span_parent: Option<Span>,
    pub callback: oneshot::Sender<WriteResponse>,
}

impl WriteBatch {
    /// Get the total size of all payloads in the batch
    pub fn size(&self) -> u64 {
        self.message
            .with_message(|r| {
                r.get_requests()
                    .map(|reqs| reqs.iter().map(|req| req.get_payload().map(|p| p.len() as u64).unwrap_or(0)).sum())
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    }

    /// Get the number of requests in this batch
    pub fn request_count(&self) -> usize {
        self.message
            .with_message(|r| r.get_requests().map(|reqs| reqs.len() as usize).unwrap_or(0))
            .unwrap_or(0)
    }
}

#[derive(Debug)]
pub struct WriteQueue {
    work_queue: Arc<SegQueue<WriteBatch>>,
    shelved_batch: Option<WriteBatch>,
}

impl WriteQueue {
    pub fn next_write_batch(&mut self, max_batch_size: u64) -> (Vec<WriteBatch>, bool) {
        let mut responses: Vec<WriteBatch> = vec![];
        let mut current_size: u64 = 0u64;
        if let Some(val) = self.shelved_batch.take() {
            current_size += &val.size();
            responses.push(val)
        }

        while current_size < max_batch_size {
            if let Some(next) = self.work_queue.pop() {
                tracing::debug!("received write request");
                let new_size = next.size() + current_size;
                if new_size <= max_batch_size {
                    // Take, otherwise memorise and break;
                    responses.push(next);
                    current_size = new_size;
                } else {
                    // Doesn't fit current batch
                    self.shelved_batch = Some(next);
                    return (responses, true);
                }
            } else {
                // No more pending batches
                return (responses, false);
            }
        }
        (responses, true)
    }
}

#[derive(Debug)]
pub struct WriteProxy {
    write_queue: WriteQueue,
    max_batch_size: u64,
    task_queue: Arc<SegQueue<LocalRaftMessage>>,
    self_id: u32,
    min_batch_interval: u128,
    quorum_worker_tasks: Vec<Arc<SegQueue<LocalQuorumWorkerTask>>>,
}

impl WriteProxy {
    pub fn new(
        work_queue: Arc<SegQueue<WriteBatch>>,
        max_batch_size: u64,
        task_queue: Arc<SegQueue<LocalRaftMessage>>,
        self_id: u32,
        _cluster_nodes: Vec<ClusterNode>,
        min_batch_interval: u64,
        quorum_worker_tasks: Vec<Arc<SegQueue<LocalQuorumWorkerTask>>>,
    ) -> Self {
        let write_queue = WriteQueue {
            work_queue,
            shelved_batch: None,
        };
        Self {
            write_queue,
            max_batch_size,
            task_queue,
            self_id,
            min_batch_interval: min_batch_interval as u128,
            quorum_worker_tasks,
        }
    }

    async fn get_current_leader(&self) -> Option<u32> {
        let span = Span::current();
        let chan: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();
        let payload = LocalRaftMessagePayload::GetRaftState;
        let raft_message = LocalRaftMessage {
            payload,
            callback: chan.0,
            parent_span: span.clone(),
        };
        self.task_queue.push(raft_message);
        let resp = chan.1.instrument(span).await;
        match resp {
            Ok(m) => match &m.payload {
                LocalRaftResponsePayload::RaftState { leader_id } => {
                    if *leader_id == 0 {
                        None
                    } else {
                        Some(*leader_id)
                    }
                }
                _ => {
                    log::error!("Received unexpected raft response payload type, expected RaftState");
                    None
                }
            },
            Err(err) => {
                log::error!("Reading raft response message failed: {}", err);
                None
            }
        }
    }

    fn process_local_batch(
        &self,
        combined_batch: OwnedWriteBatch,
        batch_callbacks: std::collections::HashMap<String, Sender<WriteResponse>>,
        num_requests: u64,
        request_size: u64,
        join_set: &mut JoinSet<()>,
    ) {
        let chan: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();
        let payload = LocalRaftMessagePayload::WriteBatch(LocalRaftWriteBatchRequest {
            message: combined_batch,
        });
        let span = tracing::span!(
            Level::INFO,
            "write_proxy_local_batch_forward_await",
            num_requests = num_requests,
            request_size = request_size
        );
        let raft_message = LocalRaftMessage {
            payload,
            callback: chan.0,
            parent_span: span.clone(),
        };
        self.task_queue.push(raft_message);
        let span_clone = span.clone();
        join_set.spawn(async {
            remote::handle_local_write_response(chan.1, span_clone.clone(), batch_callbacks)
                .instrument(span_clone)
                .await;
        });
    }

    fn process_remote_batch(
        &self,
        batch_id: String,
        combined_batch: OwnedWriteBatch,
        batch_callbacks: std::collections::HashMap<String, Sender<WriteResponse>>,
        num_requests: u64,
        request_size: u64,
        current_leader_id: u32,
        parent_span: &Span,
        join_set: &mut JoinSet<()>,
    ) {
        let put_batch_span = tracing::span!(
            Level::INFO,
            "write_proxy_remote_put_batch",
            num_requests = num_requests,
            request_size = request_size
        );
        put_batch_span.set_parent(parent_span.context());
        let _enter = put_batch_span.enter();

        let p = put_batch_span.clone();
        let leader_quorum_worker_index = if current_leader_id < self.self_id {
            (current_leader_id - 1) as usize
        } else {
            (current_leader_id - 2) as usize
        };
        let leader_quorum_worker = Arc::clone(&self.quorum_worker_tasks[leader_quorum_worker_index]);
        join_set.spawn(async move {
            remote::send_quorum_put_batch_and_handle_response(
                leader_quorum_worker,
                batch_id,
                combined_batch,
                batch_callbacks,
                p.clone(),
            )
            .instrument(p)
            .await
        });
    }

    pub async fn run(&mut self) {
        tracing::info!("Write proxy worker running");
        let mut join_set = JoinSet::new();
        let mut current_leader_id = 0u32;
        let mut leader_resync = Instant::now();

        loop {
            if current_leader_id == 0 || leader_resync.elapsed().as_millis() > 1 {
                match self.get_current_leader().await {
                    Some(leader_id) => {
                        if leader_id != current_leader_id {
                            log::info!("New leader received: {}", leader_id);
                        }
                        current_leader_id = leader_id;
                        leader_resync = Instant::now();
                    }
                    None => {
                        tokio::time::sleep(Duration::from_millis(3)).await;
                        continue;
                    }
                }
            }

            let (batch, full_batch) = self.write_queue.next_write_batch(self.max_batch_size);
            let start_time = now_millis();

            if !batch.is_empty() {
                let batch_requests: usize = batch.iter().map(|b| b.request_count()).sum();
                let num_batches: u64 = batch.len() as u64;
                let mut new_root = tracing::span!(
                    Level::INFO,
                    "write_proxy_batch_process",
                    batch_requests = batch_requests,
                    num_batches = num_batches
                );
                new_root.set_parent(Context::new());
                let _enter = new_root.enter();
                let batch_id = uuid::Uuid::new_v4().to_string();
                let (combined_batch, batch_callbacks) = batch::prepare_batch_and_callbacks(&batch_id, batch);
                let request_size: u64 = combined_batch.size();
                let num_requests = combined_batch
                    .with_message(|r| r.get_requests().map(|reqs| reqs.len() as u64).unwrap_or(0))
                    .unwrap_or(0);

                if request_size == 0 {
                    log::warn!("Request size is 0, batch requests {}, num batches: {}", batch_requests, num_batches)
                }
                if num_requests == 0 {
                    log::warn!("Num requests is 0, batch requests {}, num batches: {}", batch_requests, num_batches)
                }

                if self.self_id == current_leader_id {
                    // Node is the leader
                    self.process_local_batch(combined_batch, batch_callbacks, num_requests, request_size, &mut join_set);
                } else {
                    // Forward to leader via quorum worker
                    self.process_remote_batch(
                        batch_id,
                        combined_batch,
                        batch_callbacks,
                        num_requests,
                        request_size,
                        current_leader_id,
                        &new_root,
                        &mut join_set,
                    );
                }

                let delta = now_millis() - start_time;
                // don't sleep if we've got a full batch to not fall behind.
                if !full_batch && delta < self.min_batch_interval {
                    tokio::time::sleep(Duration::from_millis((self.min_batch_interval - delta) as u64)).await;
                }

                // Clean up completed tasks
                while let Some(result) = join_set.try_join_next() {
                    if let Err(e) = result {
                        log::error!("A task failed: {:?}", e);
                    }
                }
            } else {
                tokio::time::sleep(Duration::from_millis(self.min_batch_interval as u64)).await;
            }
        }
    }
}
