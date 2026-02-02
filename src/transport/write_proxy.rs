use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::future::Future;
use std::io::Read;
use std::iter::Map;
use std::rc::Rc;
use std::sync::{Arc, Condvar};
use std::thread::sleep;
use std::time::{Duration, Instant};
use crossbeam_queue::SegQueue;
use log::{error, info, warn};
use opentelemetry::Context;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{SpanBuilder, TraceContextExt};
use opentelemetry_zipkin::Propagator;
use serde::Deserialize;
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinSet;
use tonic::{Request, Response, Status};
use tonic::codegen::http::StatusCode;
use tonic::transport::Channel;
use tracing::{Id, Instrument, instrument, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::client::cluster_node_client::SharedGrpcChannel;
use crate::config::config::ClusterNode;
use crate::persistence::worker::{PersistenceConfig, PersistenceResponseType, PersistenceTaskType, PersistenceWorker};
use crate::raft::raft_sm::{LocalRaftMessage, LocalRaftMessagePayload, LocalRaftResponseMessage, LocalRaftResponsePayload, LocalRaftWriteBatchRequest};
use crate::transport::raft::raftproto::{RemotePutBatchRequest, RemotePutBatchResponse, RemotePutRequest, RemotePutResponse};
use crate::transport::raft::raftproto::raft_client::RaftClient;
use crate::service_utils::app_time::now_millis;
use crate::quorum::types::{LocalQuorumWorkerTask, LocalQuorumWorkerTaskType};
use crate::transport::capnp::{OwnedWriteBatch, OwnedWriteBatchResponse, build_owned_write_batch, build_owned_write_batch_response};

/// Response from a write batch operation.
/// Contains Cap'n Proto response for zero-copy internal handling.
#[derive(Debug)]
pub struct WriteResponse {
    pub message: OwnedWriteBatchResponse,
    pub status_code: StatusCode
}

impl WriteResponse {
    /// Create an empty error response
    pub fn error(status_code: StatusCode) -> Self {
        let message = build_owned_write_batch_response(|_| {});
        Self { message, status_code }
    }

    /// Create a successful response from Cap'n Proto message
    pub fn success(message: OwnedWriteBatchResponse) -> Self {
        Self { message, status_code: StatusCode::OK }
    }

    /// Convert responses to protobuf for external API boundary
    pub fn to_protobuf_responses(&self) -> Vec<RemotePutResponse> {
        self.message.with_message(|r| {
            r.get_responses().map(|resps| {
                resps.iter().map(|resp| {
                    RemotePutResponse {
                        id: resp.get_id().map(|s| s.to_string().unwrap_or_default()).unwrap_or_default(),
                        response_type: match resp.get_response_type() {
                            Ok(crate::transport::capnp::raft_capnp::RemoteResponseType::Ok) => 1,
                            Ok(crate::transport::capnp::raft_capnp::RemoteResponseType::Invalid) => 2,
                            _ => 0,
                        },
                        message: resp.get_message().map(|s| s.to_string().unwrap_or_default()).unwrap_or_default(),
                        node_id: resp.get_node_id(),
                        batch_id: resp.get_batch_id().map(|s| s.to_string().unwrap_or_default()).unwrap_or_default(),
                    }
                }).collect()
            }).unwrap_or_default()
        }).unwrap_or_default()
    }
}

/// A batch of write requests using Cap'n Proto for zero-copy handling.
#[derive(Debug)]
pub struct WriteBatch {
    pub batch_id: String,
    pub message: OwnedWriteBatch,
    pub span_parent: Option<Span>,
    pub callback: oneshot::Sender<WriteResponse>
}

impl WriteBatch {
    /// Get the total size of all payloads in the batch
    pub fn size(&self) -> u64 {
        self.message.with_message(|r| {
            r.get_requests().map(|reqs| {
                reqs.iter().map(|req| {
                    req.get_payload().map(|p| p.len() as u64).unwrap_or(0)
                }).sum()
            }).unwrap_or(0)
        }).unwrap_or(0)
    }

    /// Get the number of requests in this batch
    pub fn request_count(&self) -> usize {
        self.message.with_message(|r| {
            r.get_requests().map(|reqs| reqs.len() as usize).unwrap_or(0)
        }).unwrap_or(0)
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
                    return (responses, true)
                }
            } else {
                //tracing::info!("no more write requests");
                // No more pending batches
                return (responses, false);
            }
        }
        //tracing::info!("returning batch with {} items", responses.len());
        return (responses, true);
    }
}

#[derive(Debug)]
pub struct WriteProxy {
    write_queue: WriteQueue,
    max_batch_size: u64,
    task_queue: Arc<SegQueue<LocalRaftMessage>>,
    self_id: u32,
    cluster_nodes: Vec<ClusterNode>,
    current_leader_connection: SharedGrpcChannel,
    propagator: Propagator,
    min_batch_interval: u128,
    quorum_worker_tasks: Vec<Arc<SegQueue<LocalQuorumWorkerTask>>>,
    useQuorumForWrites: bool
}

impl WriteProxy {
    pub fn new(
        work_queue: Arc<SegQueue<WriteBatch>>,
        max_batch_size: u64,
        task_queue: Arc<SegQueue<LocalRaftMessage>>,
        self_id: u32,
        cluster_nodes: Vec<ClusterNode>,
        min_batch_interval: u64,
        quorum_worker_tasks: Vec<Arc<SegQueue<LocalQuorumWorkerTask>>>
    ) -> Self {
        let write_queue = WriteQueue {
            work_queue,
            shelved_batch: None,
        };
        let default_leader_connection = SharedGrpcChannel::new("placeholder", 0); // Lazy connection so this doesn't matter. Don't wanna juggle Options.
        Self {
            write_queue,
            max_batch_size,
            task_queue,
            self_id,
            cluster_nodes,
            current_leader_connection: default_leader_connection,
            propagator: opentelemetry_zipkin::Propagator::new(),
            min_batch_interval: min_batch_interval as u128,
            quorum_worker_tasks,
            useQuorumForWrites: true
        }
    }


    async fn get_client(&mut self, node_id: u32) -> Result<RaftClient<Channel>, String> {
        if node_id == self.current_leader_connection.node_id {
            self.current_leader_connection.get_client().await
        } else {
            // New leader
            tracing::info!("Establishing write proxy connection to new leader {}", node_id);
            let cn = self.cluster_nodes.iter().find(|node| node.node_id == node_id).unwrap(); // TODO
            self.current_leader_connection = SharedGrpcChannel::new(cn.endpoint.as_str(), cn.node_id);
            self.current_leader_connection.get_client().await
        }

    }

    fn respond_to_batch_callbacks(response: OwnedWriteBatchResponse, batch_callbacks: HashMap<String, oneshot::Sender<WriteResponse>>) {
        // For now, send the same response to all callbacks
        // TODO: Group responses by batch_id if needed
        for (batch_id, callback) in batch_callbacks {
            let batch_response = build_owned_write_batch_response(|mut builder| {
                builder.set_batch_id(&batch_id);
                // Copy relevant responses from the combined response
                let _ = response.with_message(|r| {
                    if let Ok(resps) = r.get_responses() {
                        let mut out_resps = builder.init_responses(resps.len() as u32);
                        for (i, resp) in resps.iter().enumerate() {
                            let mut out = out_resps.reborrow().get(i as u32);
                            if let Ok(id) = resp.get_id() {
                                out.set_id(id);
                            }
                            if let Ok(rt) = resp.get_response_type() {
                                out.set_response_type(rt);
                            }
                            if let Ok(msg) = resp.get_message() {
                                out.set_message(msg);
                            }
                            out.set_node_id(resp.get_node_id());
                            if let Ok(bid) = resp.get_batch_id() {
                                out.set_batch_id(bid);
                            }
                        }
                    }
                });
            });
            let msg = WriteResponse::success(batch_response);
            if callback.send(msg).is_err() {
                warn!("Sending write response callback failed because the receiver dropped");
            }
        }
    }

    fn handle_insert_completed(msg: LocalRaftResponseMessage, batch_callbacks: HashMap<String, oneshot::Sender<WriteResponse>>) {
        match msg.payload {
            LocalRaftResponsePayload::WriteBatch(response) => {
                WriteProxy::respond_to_batch_callbacks(response.message, batch_callbacks)
            }
            _ => {
                batch_callbacks.into_iter().for_each(|callback| {
                    let msg = WriteResponse::error(StatusCode::INTERNAL_SERVER_ERROR);
                    if callback.1.send(msg).is_err() {
                        log::warn!("Sending write response callback failed because the receiver dropped");
                    }
                });
            }
        }
    }

    async fn handle_local_write_response(receiver: Receiver<LocalRaftResponseMessage>, parent_span: Span, batch_callbacks: HashMap<String, oneshot::Sender<WriteResponse>>) {
        let span = tracing::span!(Level::INFO, "write_proxy_local_batch_forward_response_await");
        span.set_parent(parent_span.context());
        let resp = receiver.instrument(span).await;
        match resp {
            Ok(ok_runtime_resp) => {
                tracing::debug!("raft batch handling completed");
                WriteProxy::handle_insert_completed(ok_runtime_resp, batch_callbacks)
            }
            Err(e) => {
                tracing::error!("receiving raft message in proxy failed with error, {}", e);
            }
        }
    }


    /// Combines multiple WriteBatches into a single OwnedWriteBatch for processing.
    /// This involves extracting data from each batch and building a new combined message.
    /// The callbacks are collected for later response routing.
    fn prepare_batch_and_callbacks(batch_id: &str, batches: Vec<WriteBatch>) -> (OwnedWriteBatch, HashMap<String, Sender<WriteResponse>>) {
        let mut batch_callbacks: HashMap<String, oneshot::Sender<WriteResponse>> = HashMap::new();

        // First, collect all request data from all batches
        struct RequestData {
            id: String,
            payload: Vec<u8>,
            node_id: u32,
        }
        let mut all_requests: Vec<RequestData> = vec![];

        for b in batches.into_iter() {
            batch_callbacks.insert(b.batch_id.clone(), b.callback);

            // Extract requests from this batch's OwnedWriteBatch
            let _ = b.message.with_message(|reader| {
                if let Ok(reqs) = reader.get_requests() {
                    for req in reqs.iter() {
                        all_requests.push(RequestData {
                            id: req.get_id().map(|s| s.to_string().unwrap_or_default()).unwrap_or_default(),
                            payload: req.get_payload().map(|p| p.to_vec()).unwrap_or_default(),
                            node_id: req.get_node_id(),
                        });
                    }
                }
            });
        }

        // Build combined OwnedWriteBatch
        let combined = build_owned_write_batch(|mut builder| {
            builder.set_batch_id(batch_id);
            let mut reqs = builder.init_requests(all_requests.len() as u32);
            for (i, req_data) in all_requests.iter().enumerate() {
                let mut req = reqs.reborrow().get(i as u32);
                req.set_id(&req_data.id);
                req.set_payload(&req_data.payload);
                req.set_node_id(req_data.node_id);
            }
        });

        (combined, batch_callbacks)
    }

    /// Convert OwnedWriteBatch to protobuf RemotePutBatchRequest for gRPC fallback
    fn owned_batch_to_protobuf(batch_id: &str, batch: &OwnedWriteBatch) -> RemotePutBatchRequest {
        let put_requests = batch.with_message(|r| {
            r.get_requests().map(|reqs| {
                reqs.iter().map(|req| {
                    RemotePutRequest {
                        id: req.get_id().map(|s| s.to_string().unwrap_or_default()).unwrap_or_default(),
                        payload: req.get_payload().map(|p| String::from_utf8_lossy(p).to_string()).unwrap_or_default(),
                        node_id: req.get_node_id(),
                    }
                }).collect()
            }).unwrap_or_default()
        }).unwrap_or_default();

        RemotePutBatchRequest {
            batch_id: batch_id.to_string(),
            put_request: put_requests,
        }
    }

    fn prepare_put_batch_request(batch_id: &str, batch: &OwnedWriteBatch, span_parent: Span, propagator: &Propagator) -> Request<RemotePutBatchRequest> {
        let req = WriteProxy::owned_batch_to_protobuf(batch_id, batch);
        let mut request = tonic::Request::new(req);

        // Tracing stuff
        let mut carrier = std::collections::HashMap::new();
        let ctx = span_parent.context();
        propagator.inject_context(&ctx, &mut carrier);

        // Add tracing metadata to the request
        let metadata = request.metadata_mut();
        for (key, value) in carrier {
            if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
                metadata.insert(key, value.parse().unwrap());
            }
        }
        request
    }

    async fn send_quorum_put_batch_and_handle_response(
        leader_quorum_worker: Arc<SegQueue<LocalQuorumWorkerTask>>,
        batch_id: String,
        batch: OwnedWriteBatch,
        batch_callbacks: HashMap<String, Sender<WriteResponse>>,
        span: Span
    ) {
        let s = span.clone();
        let callback: (Sender<WriteResponse>, Receiver<WriteResponse>) = oneshot::channel();
        let wb = LocalQuorumWorkerTaskType::WriteBatch {
            write_batch: WriteBatch {
                batch_id: batch_id.clone(),
                message: batch,
                span_parent: Some(span.clone()),
                callback: callback.0
            }
        };
        leader_quorum_worker.push(LocalQuorumWorkerTask { task_type: wb });
        let resp = callback.1.instrument(s).await;
        match resp {
            Ok(put_response) => {
                let write_callback_span = tracing::span!(Level::INFO, "write_proxy_remote_success_callbacks");
                write_callback_span.set_parent(span.context());
                write_callback_span.in_scope(|| {
                    tracing::event!(Level::INFO, "sending successful callback");
                    WriteProxy::respond_to_batch_callbacks(put_response.message, batch_callbacks)
                })
            }
            Err(e) => {
                tracing::error!("Remote PutBatch request failed with error: {}", e);
                error!("Remote PutBatch request failed with error: {}", e);
                for (_, callback) in batch_callbacks {
                    let msg = WriteResponse::error(StatusCode::INTERNAL_SERVER_ERROR);
                    if let Err(_) = callback.send(msg) {
                        warn!("Failed to send write batch response on channel, receiver dropped")
                    }
                }
            }
        }
    }

    /// Convert protobuf responses to OwnedWriteBatchResponse
    fn protobuf_responses_to_owned(batch_id: &str, responses: Vec<RemotePutResponse>) -> OwnedWriteBatchResponse {
        build_owned_write_batch_response(|mut builder| {
            builder.set_batch_id(batch_id);
            let mut out_resps = builder.init_responses(responses.len() as u32);
            for (i, resp) in responses.iter().enumerate() {
                let mut out = out_resps.reborrow().get(i as u32);
                out.set_id(&resp.id);
                out.set_response_type(match resp.response_type {
                    1 => crate::transport::capnp::raft_capnp::RemoteResponseType::Ok,
                    2 => crate::transport::capnp::raft_capnp::RemoteResponseType::Invalid,
                    _ => crate::transport::capnp::raft_capnp::RemoteResponseType::Unspecified,
                });
                out.set_message(&resp.message);
                out.set_node_id(resp.node_id);
                out.set_batch_id(&resp.batch_id);
            }
        })
    }

    async fn send_remote_put_batch_and_handle_response(
        mut client: RaftClient<Channel>,
        batch_id: String,
        request: Request<RemotePutBatchRequest>,
        batch_callbacks: HashMap<String, Sender<WriteResponse>>,
        span: Span
    ) {
        let s = span.clone();
        let resp = client.put_batch(request).instrument(s).await;
        match resp {
            Ok(put_response) => {
                let write_callback_span = tracing::span!(Level::INFO, "write_proxy_remote_success_callbacks");
                write_callback_span.set_parent(span.context());
                write_callback_span.in_scope(|| {
                    tracing::event!(Level::INFO, "sending successful callback");
                    let owned_response = WriteProxy::protobuf_responses_to_owned(&batch_id, put_response.into_inner().responses);
                    WriteProxy::respond_to_batch_callbacks(owned_response, batch_callbacks)
                })
            }
            Err(e) => {
                tracing::error!("Remote PutBatch request failed with error: {}", e);
                for (_, callback) in batch_callbacks {
                    let msg = WriteResponse::error(StatusCode::INTERNAL_SERVER_ERROR);
                    let _ = callback.send(msg);
                }
            }
        }
    }

    pub async fn run(&mut self) {
        tracing::info!("Write proxy worker running");
        let mut join_set = JoinSet::new();
        let mut current_leader_id = 0u32;
        let mut leader_resync = Instant::now();

        loop {
            // TODO this is a bit funky to just re-check leader every N milliseconds. Would be good to get notified instead.
            // Checking once per batch shouldn't be a big deal as well, but will test out if it's better.
            // Realistically, this is just stupid.
            // Something like a atomic integer that's shared between the write proxy and the state machine could also work.

            if current_leader_id <= 0 || leader_resync.elapsed().as_millis() > 1 {
                let span = Span::current();
                let chan: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();
                let payload = LocalRaftMessagePayload::GetRaftState;
                let raft_message = LocalRaftMessage {
                    payload,
                    callback: chan.0,
                    parent_span: span.clone()
                };
                self.task_queue.push(raft_message);
                let resp = chan.1.instrument(span).await;
                match resp {
                    Ok(m) => {
                        match &m.payload {
                            LocalRaftResponsePayload::RaftState { leader_id } => {
                                if *leader_id <= 0 {
                                    //log::warn!("No leader elected, retrying later");
                                    tokio::time::sleep(Duration::from_millis(3)).await;
                                    continue
                                } else if *leader_id == current_leader_id {
                                    leader_resync = Instant::now()
                                } else {
                                    log::info!("New leader received: {}", *leader_id);
                                    current_leader_id = *leader_id;
                                    leader_resync = Instant::now()
                                }
                            }
                            default => {
                                log::error!("Received unexpected raft response payload type, expected RaftState");
                                tokio::time::sleep(Duration::from_millis(3)).await;
                                continue
                            }
                        }
                    }
                    Err(err) => {
                        log::error!("Reading raft response message failed: {}", err);
                        tokio::time::sleep(Duration::from_millis(3)).await;
                        continue
                    }
                }
            }

            let (batch, full_batch) = self.write_queue.next_write_batch(self.max_batch_size);
            let start_time = now_millis();
            if !batch.is_empty() {
                let batch_requests: usize = batch.iter().map(|b| b.request_count()).sum();
                let num_batches: u64 = batch.len() as u64;
                let mut new_root = tracing::span!(Level::INFO, "write_proxy_batch_process", batch_requests=batch_requests, num_batches=num_batches);
                new_root.set_parent(Context::new());
                let _enter = new_root.enter();
                let batch_id = uuid::Uuid::new_v4().to_string();
                let (combined_batch, batch_callbacks) = WriteProxy::prepare_batch_and_callbacks(&batch_id, batch);
                let request_size: u64 = combined_batch.size();
                let num_requests = combined_batch.with_message(|r| {
                    r.get_requests().map(|reqs| reqs.len() as u64).unwrap_or(0)
                }).unwrap_or(0);
                if request_size == 0 {
                    log::warn!("Request size is 0, batch requests {}, num batches: {}", batch_requests, num_batches)
                }
                if num_requests == 0 {
                    log::warn!("Num requests is 0, batch requests {}, num batches: {}", batch_requests, num_batches)
                }

                if self.self_id == current_leader_id {
                    // Node is the leader
                    let chan: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();
                    let payload = LocalRaftMessagePayload::WriteBatch(
                        LocalRaftWriteBatchRequest {
                            message: combined_batch,
                        }
                    );
                    let span = tracing::span!(Level::INFO, "write_proxy_local_batch_forward_await", num_requests=num_requests, request_size=request_size);
                    let raft_message = LocalRaftMessage {
                        payload,
                        callback: chan.0,
                        parent_span: span.clone()
                    };
                    self.task_queue.push(raft_message);
                    let span_clone = span.clone();
                    join_set.spawn(async {
                        WriteProxy::handle_local_write_response(chan.1, span_clone.clone(), batch_callbacks).instrument(span_clone).await;
                    });
                } else {
                    if self.useQuorumForWrites {
                        let put_batch_span = tracing::span!(Level::INFO, "write_proxy_remote_put_batch", num_requests=num_requests, request_size=request_size);
                        put_batch_span.set_parent(new_root.context());
                        let _enter = put_batch_span.enter();

                        let p = put_batch_span.clone();
                        let leader_quorum_worker_index = if current_leader_id < self.self_id { (current_leader_id - 1) as usize } else { (current_leader_id - 2) as usize };
                        let leader_quorum_worker = Arc::clone(&self.quorum_worker_tasks[leader_quorum_worker_index]);
                        let batch_id_clone = batch_id.clone();
                        join_set.spawn(async move {
                            WriteProxy::send_quorum_put_batch_and_handle_response(leader_quorum_worker, batch_id_clone, combined_batch, batch_callbacks, p.clone()).instrument(p).await
                        });
                    } else {
                        match self.get_client(current_leader_id).instrument(new_root.clone()).await {
                            Ok(client) => {
                                let put_batch_span = tracing::span!(Level::INFO, "write_proxy_remote_put_batch", num_requests=num_requests, request_size=request_size);
                                put_batch_span.set_parent(new_root.context());
                                let _enter = put_batch_span.enter();
                                let request = WriteProxy::prepare_put_batch_request(&batch_id, &combined_batch, put_batch_span.clone(), &self.propagator);
                                let batch_id_clone = batch_id.clone();

                                let p = put_batch_span.clone();
                                join_set.spawn(async move {
                                    WriteProxy::send_remote_put_batch_and_handle_response(client, batch_id_clone, request, batch_callbacks, p.clone()).instrument(p).await
                                });
                            }
                            Err(e) => {
                                tracing::error!("Setting up remote client failed with error: {}", e);
                                for (_, callback) in batch_callbacks {
                                    let msg = WriteResponse::error(StatusCode::INTERNAL_SERVER_ERROR);
                                    let _ = callback.send(msg);
                                }
                            }
                        }
                    }
                }
                let delta = now_millis() - start_time;
                // don't sleep if we've got a full batch to not fall behind.
                if !full_batch && delta < self.min_batch_interval {
                    tokio::time::sleep(Duration::from_millis((self.min_batch_interval - delta) as u64)).await;
                }
                //  Clean up completed tasks
                while let Some(result) = join_set.try_join_next() {
                    match result {
                        Ok(_) => {
                            // Handle successful completion if needed
                        }
                        Err(e) => {
                            log::error!("A task failed: {:?}", e);
                        }
                    }
                }
            } else {
                tokio::time::sleep(Duration::from_millis(self.min_batch_interval as u64)).await;
            }
        }
    }
}