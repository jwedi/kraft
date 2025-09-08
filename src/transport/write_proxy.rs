use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::future::Future;
use std::iter::Map;
use std::rc::Rc;
use std::sync::{Arc, Condvar};
use std::thread::sleep;
use std::time::{Duration, Instant};
use crossbeam_queue::SegQueue;
use opentelemetry::Context;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::SpanBuilder;
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
use crate::server::raftproto::{RemotePutBatchRequest, RemotePutBatchResponse, RemotePutRequest, RemotePutResponse};
use crate::server::raftproto::raft_client::RaftClient;
use crate::service_utils::app_time::now_millis;
use sbe_kraft_replication_schema::{log_entry_codec, message_header_codec, WriteBuf};
use sbe_kraft_replication_schema::command_codec::CommandEncoder;
use sbe_kraft_replication_schema::log_entry_codec::LogEntryEncoder;

#[derive(Debug)]
pub struct WriteResponse {
    pub responses: Vec<RemotePutResponse>,
    pub status_code: StatusCode
}

#[derive(Debug)]
pub struct WriteBatch {
    pub batch_id: String,
    pub requests: Vec<RemotePutRequest>,
    pub span_parent: Option<Span>,
    pub callback: oneshot::Sender<WriteResponse>
}

impl WriteBatch {
    pub fn size(&self) -> u64 {
        self.requests.iter().map(|v| v.get_size()).sum()
    }
}

impl RemotePutRequest {
    pub fn get_size(&self) -> u64 {
        self.payload.len() as u64
    }
}

#[derive(Debug)]
pub struct WriteQueue {
    work_queue: Arc<SegQueue<WriteBatch>>,
    shelved_batch: Option<WriteBatch>,
}

impl WriteQueue {

    pub fn next_write_batch(&mut self, max_batch_size: u64) -> Vec<WriteBatch> {
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
                    break;
                }
            } else {
                //tracing::info!("no more write requests");
                // No more pending batches
                break;
            }
        }
        //tracing::info!("returning batch with {} items", responses.len());
        return responses;
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
    min_batch_interval: u128
}

impl WriteProxy {
    pub fn new(
        work_queue: Arc<SegQueue<WriteBatch>>,
        max_batch_size: u64,
        task_queue: Arc<SegQueue<LocalRaftMessage>>,
        self_id: u32,
        cluster_nodes: Vec<ClusterNode>,
        min_batch_interval: u64
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
            min_batch_interval: min_batch_interval as u128
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

    fn respond_to_put_responses(responses: Vec<RemotePutResponse>, batch_callbacks: HashMap<String, oneshot::Sender<WriteResponse>>) {
        let mut grouped: HashMap<String, Vec<RemotePutResponse>> = HashMap::new();

        for resp in responses {
            let entry = grouped.entry(resp.id.clone()); // TODO clone
            entry.or_insert(vec![]).push(resp);
        }

        for b in batch_callbacks {
            let maybe_responses = grouped.remove(&b.0).unwrap_or_else(|| vec![]);
            let msg = WriteResponse {
                responses: maybe_responses,
                status_code: StatusCode::OK
            };
            match b.1.send(msg) {
                Ok(_) => {}
                Err(_) => {
                    tracing::info!("Sending write response callback failed because the receiver dropped");
                }
            }
        }
    }

    fn handle_insert_completed(msg: LocalRaftResponseMessage, batch_callbacks: HashMap<String, oneshot::Sender<WriteResponse>>) {

        match msg.payload {
            LocalRaftResponsePayload::WriteBatch (responses ) => {
                WriteProxy::respond_to_put_responses(responses.responses, batch_callbacks)
            }
            _ => {
                batch_callbacks.into_iter().for_each(|callback| {
                    let msg = WriteResponse {
                        responses: vec![],
                        status_code: StatusCode::INTERNAL_SERVER_ERROR
                    };
                    match callback.1.send(msg) {
                        Ok(_) => {}
                        Err(e) => {
                            log::warn!("Sending write response callback failed because the receiver dropped");
                        }
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


    fn prepare_batch_and_callbacks(batches: Vec<WriteBatch>) -> (Vec<RemotePutRequest>, HashMap<String, Sender<WriteResponse>>) {
        let mut all_requests: Vec<RemotePutRequest> = vec![];
        let mut batch_callbacks: HashMap<String, oneshot::Sender<WriteResponse>> = HashMap::new();
        for mut b in batches.into_iter() {
            all_requests.append(&mut b.requests);
            batch_callbacks.insert(b.batch_id, b.callback);
        }
        (all_requests, batch_callbacks)
    }

    fn prepare_put_batch_request(put_requests: Vec<RemotePutRequest>, span_parent: Span, propagator: &Propagator) -> Request<RemotePutBatchRequest> {
        let req = RemotePutBatchRequest {
            put_request: put_requests
        };

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

    async fn send_remote_put_batch_and_handle_response(mut client: RaftClient<Channel>, request: Request<RemotePutBatchRequest>, batch_callbacks: HashMap<String, Sender<WriteResponse>>, span: Span) {

        let s = span.clone();
        let resp = client.put_batch(request).instrument(s).await;
        match resp {
            Ok(put_response) => {
                let write_callback_span = tracing::span!(Level::INFO, "write_proxy_remote_success_callbacks");
                write_callback_span.set_parent(span.context());
                write_callback_span.in_scope(|| {
                    tracing::event!(Level::INFO, "sending successful callback");
                    WriteProxy::respond_to_put_responses(put_response.into_inner().responses, batch_callbacks)
                })
            }
            Err(e) => {
                tracing::error!("Remote PutBatch request failed with error: {}", e);
                for b in batch_callbacks {
                    let msg = WriteResponse {
                        responses: vec![],
                        status_code: StatusCode::INTERNAL_SERVER_ERROR
                    };
                    b.1.send(msg);
                }
            }
        }
    }

    async fn handle_write_batch_with_known_leader(&mut self, leader_id: u32, batches: Vec<WriteBatch>) {
        let parent = Span::current();
        let mut preparation = WriteProxy::prepare_batch_and_callbacks(batches);
        let all_requests = preparation.0;
        let mut batch_callbacks = preparation.1;

        if leader_id == self.self_id {
            tracing::debug!("local batch forward");
            // Node is the leader
            let chan: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();
            let payload = LocalRaftMessagePayload::WriteBatch(
                LocalRaftWriteBatchRequest {
                    requests: all_requests,
                }
            );
            let span = tracing::span!(Level::INFO, "write_proxy_local_batch_forward_await");
            let raft_message = LocalRaftMessage {
                payload,
                callback: chan.0,
                parent_span: span.clone()
            };
            self.task_queue.push(raft_message);
            WriteProxy::handle_local_write_response(chan.1, span.clone(), batch_callbacks).instrument(span).await;
        } else {
            let span = tracing::span!(Level::INFO, "write_proxy_remote_batch_forward_client");
            tracing::debug!("remote batch forward");

            // Get RPC client and do a remote PutBatch request.
            match self.get_client(leader_id).instrument(span).await {
                Ok(mut client) => {
                    let put_batch_span = tracing::span!(Level::INFO, "write_proxy_remote_put_batch");
                    put_batch_span.set_parent(parent.context());

                    let rpc_put_batch_span = tracing::span!(Level::INFO, "write_proxy_remote_put_batch_send_rpc");
                    rpc_put_batch_span.set_parent(put_batch_span.context());

                    let request = WriteProxy::prepare_put_batch_request(all_requests, rpc_put_batch_span.clone(), &self.propagator);

                    WriteProxy::send_remote_put_batch_and_handle_response(client, request, batch_callbacks, rpc_put_batch_span).instrument(put_batch_span).await;
                }
                Err(e) => {
                    tracing::error!("Setting up remote client failed with error: {}", e);
                    for b in batch_callbacks {
                        let msg = WriteResponse {
                            responses: vec![],
                            status_code: StatusCode::INTERNAL_SERVER_ERROR
                        };
                        b.1.send(msg);
                    }
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

            //if current_leader_id <= 0 || leader_resync.elapsed().as_millis() > 10 {
            if true {
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
                                    log::warn!("No leader elected, retrying later");
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

            let mut batch = self.write_queue.next_write_batch(self.max_batch_size);
            let start_time = now_millis();
            if !batch.is_empty() {
                let mut new_root = batch.first_mut().unwrap().span_parent.take().unwrap_or_else( || {
                    log::error!("unexpected empty parent span from write queue batch");
                    Span::none()
                });

                let batch_requests: u64 = batch.iter().map(|b| b.requests.len() as u64).sum();
                let num_batches: u64 = batch.len() as u64;
                let mut preparation = WriteProxy::prepare_batch_and_callbacks(batch);
                let all_requests = preparation.0;
                let mut batch_callbacks = preparation.1;
                let request_size: u64 = all_requests.iter().map(|req| req.get_size()).sum();
                let num_requests: u64 = all_requests.len() as u64;
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
                            requests: all_requests,
                        }
                    );
                    let span = tracing::span!(Level::INFO, "write_proxy_local_batch_forward_await", num_requests=num_requests, request_size=request_size);
                    span.set_parent(new_root.context());
                    let raft_message = LocalRaftMessage {
                        payload,
                        callback: chan.0,
                        parent_span: span.clone()
                    };
                    self.task_queue.push(raft_message);
                    join_set.spawn(async {
                        WriteProxy::handle_local_write_response(chan.1, span.clone(), batch_callbacks).instrument(span).await;
                    });
                } else {
                    match self.get_client(current_leader_id).await {
                        Ok(mut client) => {
                            let put_batch_span = tracing::span!(Level::INFO, "write_proxy_remote_put_batch", num_requests=num_requests, request_size=request_size);
                            put_batch_span.set_parent(new_root.context());
                            let request = WriteProxy::prepare_put_batch_request(all_requests, put_batch_span.clone(), &self.propagator);

                            let p = new_root.clone();
                            join_set.spawn(async {
                                WriteProxy::send_remote_put_batch_and_handle_response(client, request, batch_callbacks, p).instrument(put_batch_span).await
                            });
                        }
                        Err(e) => {
                            tracing::error!("Setting up remote client failed with error: {}", e);
                            for b in batch_callbacks {
                                let msg = WriteResponse {
                                    responses: vec![],
                                    status_code: StatusCode::INTERNAL_SERVER_ERROR
                                };
                                b.1.send(msg);
                            }
                        }
                    }
                }
                let delta = now_millis() - start_time;
                if delta < self.min_batch_interval {
                    tokio::time::sleep(Duration::from_millis(delta as u64)).await;
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
                tokio::time::sleep(Duration::from_micros(300)).await;
            }
        }

        // Solution would only be scalable in terms of writes if leadership can be sharded?
        // Alternatively 3 nodes are made into a mini-quorum for a key range. So N*3 nodes means N*100.000 RPS, that's decent.
        // Cassandra does around 10.000 replicated RPS for a 3 node cluster.

        // 100.000 RPS
        // means 1000 requests per ms
        // means 33.000 RPS per node in 3 node cluster
        // means 333 requests per millisecond per node.

        // means 1 request per millisecond for batches of 1000.
        // means 4 requests per millisecond for batches of 250.
        // 2 serial RPCs and 2 potentially concurrent disk IO per batch. Throughput of 100 batches per second.
        // This is within the realm of reasonable
    }
}