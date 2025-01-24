use std::error::Error;
use std::future::Future;
use std::rc::Rc;
use std::sync::{Arc, Condvar};
use std::thread::sleep;
use std::time::Duration;
use crossbeam_queue::SegQueue;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_zipkin::Propagator;
use tokio::task::JoinSet;
use tonic::{Request, Response, Status};
use tonic::transport::{Channel};
use tracing::{Instrument, Level, Span};
use tracing::instrument::Instrumented;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::client::cluster_node_client::SharedGrpcChannel;
use crate::raft::raft_sm::{AppendEntries, RequestVoteRequest};
use crate::server::raftproto::raft_client::RaftClient;
use crate::server::raftproto::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use crate::service_utils::app_time::{now_millis, now_plus_duration_millis};

pub struct QuorumTask {
    pub task_type: QuorumTaskType
    // New append entries
    // Request vote
    // Start heartbeats
    // Stop heartbeats
}

pub enum QuorumTaskType {
    StartHeartbeats{ id: u64, term: u64, prev_term: u64, prev_log_index: u64 },
    StopHeartbeats { id: u64 },
    RequestVote{ id: u64, term: u64, last_term: u64, last_index: u64, parent_span: Span}, // Term, last term, last index,
    AppendEntries{ id: u64, req: AppendEntriesRequest, parent_span: Span},
}

pub enum QuorumTaskResponseType {
    RequestVoteResponse{ id: u64, term: u64, received_vote: bool},
    AppendEntries{ id: u64, ok: bool} // TODO get next index
}

pub struct QuorumResponse {
    pub response_type: QuorumTaskResponseType
}

// Responsible for communication with other nodes in the cluster if this node is the leader.
// Sends AppendEntries to followers to achieve quorum and responds back on the response queue.
// Sends RequestVote when asked to.
// Sends AppendEntries heartbeats regardless to keep up constant load.
pub struct QuorumWorker {
    work_queue: Arc<SegQueue<QuorumTask>>,
    response_queue: Arc<SegQueue<QuorumResponse>>,
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
    grpc_channel: SharedGrpcChannel

    // gRPC client to quorum member
    // quorum member id.
    // id of this node.
    // the next term and index of the quorum member
}

impl QuorumWorker {
    pub fn new(
        work_queue: Arc<SegQueue<QuorumTask>>,
        response_queue: Arc<SegQueue<QuorumResponse>>,
        self_id: u32,
        next_index: u64,
        member_id: u64,
        member_endpoint: String
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
            grpc_channel: SharedGrpcChannel::new(member_endpoint.as_str(), member_id as u32)
        }
    }

    async fn try_connect(&mut self) -> Option<Channel> {
        let endpoint_clone = self.member_endpoint.clone();
        let maybe_connect = Channel::from_shared(endpoint_clone)
            .expect("quorum worker endpoint parsing failed")
            .connect().await;
        match maybe_connect {
            Ok(channel) => {
                tracing::info!("Connected to cluster member {} succeeded", self.member_id);
                Some(channel.clone())
            }
            Err(e) => {
                tracing::error!("Connecting to cluster member failed: {}", e);
                None
            }
        }
    }

    async fn send_append_entries_and_respond(message_id: u64, mut client: RaftClient<Channel>, req: Request<AppendEntriesRequest>, response_channel: Arc<SegQueue<QuorumResponse>>, span: Span) {
        let response = client.append_entries(req).instrument(span).await;
        //let request_time = now_millis() - now;
        match response {
            Ok(resp) => {
                let val = resp.into_inner();
                //tracing::info!("received append entries response: {}, {}, {}, request time {}", id, self.member_id, val.ok, request_time);
                let resp = QuorumTaskResponseType::AppendEntries{id: message_id, ok: val.ok};
                response_channel.push(QuorumResponse{response_type: resp})
            }
            Err(err) => {
                let msg = err.to_string();
                // TODO send error response
                tracing::error!("sending append entries with id {} failed with error {}", message_id, msg);
            }
        }
    }

    pub async fn run(&mut self) {
        tracing::info!("Running quorum worker");
        let mut maybe_channel: Option<Channel> = None;
        let mut client: Option<Box<RaftClient<Channel>>> = None;
        let mut join_set = JoinSet::new();

        loop {
            if client.is_none() {
                maybe_channel = self.try_connect().await;

                client = match maybe_channel {
                    None => {
                        tracing::error!("none channel for member, retrying later {}", self.member_id);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    Some(channel) => {
                        tracing::info!("client set up for {}", self.member_id);
                        Some(Box::new(RaftClient::new(channel)))
                    }
                };
            }

            // Clean up pending tasks
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


            let now = now_millis();
            // SOmething here, maybe not...
            if self.send_heartbeats && (now-self.last_heartbeat > 50){
                if let Some(mut c) = client.as_mut() {
                    //tracing::info!("sending append entries for member {}", self.member_id);

                    // Call a method on the client, for example, `your_method`.
                    let request = tonic::Request::new(AppendEntriesRequest{
                        term: self.term,
                        leader_id: self.self_id,
                        prev_log_index: self.prev_log_index,
                        prev_log_term: self.prev_log_term,
                        entries: vec![],
                    });

                    let response = c.append_entries(request).await;
                    let request_time = now_millis() - now;
                    match response {
                        Ok(resp) => {
                            let val = resp.into_inner();
                            //tracing::info!("received append entries response {} for member {}, term {}, request duration: {}", val.ok, self.member_id, self.term, request_time);
                        }
                        Err(err) => {
                            tracing::error!("received append entries error {} for member {} after {}", err.to_string(), self.member_id, request_time);
                        }
                    }
                } else {
                    tracing::error!("Setting up quorum worker client failed");
                }
                self.last_heartbeat = now_millis()
            }
            let queue_len = self.work_queue.len();
            if queue_len > 5 {
                tracing::info!("Long quorum queue len: {}", queue_len);
            }
            let task = self.work_queue.pop();
            match task {
                Some(task) => {
                    tracing::info!("Received quorum task");
                    match task.task_type {
                        QuorumTaskType::StartHeartbeats{id, term, prev_term, prev_log_index} => {
                            tracing::info!(message="received start heartbeat request id: {}", id);
                            self.send_heartbeats = true;
                            self.term = term;
                            self.prev_log_term = prev_term;
                            self.prev_log_index = prev_log_index;
                        }
                        QuorumTaskType::StopHeartbeats{id} => {
                            tracing::info!(message="received stop heartbeat request id: {}", id);
                            self.send_heartbeats = false;
                        }
                        QuorumTaskType::RequestVote{id, term, last_index, last_term, parent_span} => {
                            if let Some(mut c) = client.as_mut() {
                                let request_span = tracing::span!(Level::INFO, "request_vote_remote");
                                request_span.set_parent(parent_span.context());
                                let _guard = request_span.enter();
                                let mut request = tonic::Request::new(VoteRequest{
                                    term,
                                    candidate_id: self.self_id,
                                    last_log_index: last_index,
                                    last_log_term: last_term

                                });
                                request.set_timeout(Duration::from_millis(250));
                                tracing::info!("sending request vote id: {}, member: {}, term: {}", id, self.member_id, term);
                                let response_await_span = tracing::span!(Level::INFO, "request_vote_remote_await");
                                let response = c.request_vote(request).instrument(response_await_span).await;
                                let request_time = now_millis() - now;
                                match response {
                                    Ok(resp) => {
                                        let val = resp.into_inner();
                                        tracing::info!("received vote response id: {}, member: {}, term: {}, granted: {} after {}", id, self.member_id, term, val.vote_granted, request_time);
                                        let resp = QuorumTaskResponseType::RequestVoteResponse{id, term, received_vote: val.vote_granted};
                                        self.response_queue.push(QuorumResponse{response_type: resp})
                                    }
                                    Err(err) => {
                                        let msg = err.to_string();
                                        // TODO send error response
                                        tracing::error!("sending request vote id: {}, member: {}, term: {} failed with error {} after {}", id, self.member_id, term, msg, request_time);
                                    }
                                }
                            }
                        }
                        QuorumTaskType::AppendEntries {id, req, parent_span} => {


                            if let Ok(mut c) = self.grpc_channel.get_client().await {
                                let request_span = tracing::span!(Level::INFO, "append_entries_remote", member_id=self.member_id, work_backlog=self.work_queue.len());
                                request_span.set_parent(parent_span.context());
                                let _guard = request_span.enter();

                                let response_await_span = tracing::span!(Level::INFO, "append_entries_remote_await", member_id=self.member_id);
                                let mut request = tonic::Request::new(req);
                                request.set_timeout(Duration::from_millis(250));

                                // Tracing stuff
                                let mut carrier = std::collections::HashMap::new();
                                let ctx = response_await_span.context();

                                self.propagator.inject_context(&ctx, &mut carrier);

                                // Add tracing metadata to the request
                                let metadata = request.metadata_mut();
                                for (key, value) in carrier {
                                    if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
                                        metadata.insert(key, value.parse().unwrap());
                                    }
                                }

                                tracing::info!("sending append entries request: {}, member: {}", id, self.member_id);

                                let queue_clone = Arc::clone(&self.response_queue);
                                // TODO some client fix here.... Maybe copy from write proxy
                                //let future = c.append_entries(request).instrument(response_await_span);

                                let span_clone = response_await_span.clone();
                                let id_clone = id;
                                join_set.spawn(async move {
                                    QuorumWorker::send_append_entries_and_respond(id_clone, c, request, queue_clone, span_clone).await
                                });


                                /*
                                let response = c.append_entries(request).instrument(response_await_span).await;
                                let request_time = now_millis() - now;
                                match response {
                                    Ok(resp) => {
                                        let val = resp.into_inner();
                                        tracing::info!("received append entries response: {}, {}, {}, request time {}", id, self.member_id, val.ok, request_time);
                                        let resp = QuorumTaskResponseType::AppendEntries{id, ok: val.ok};
                                        self.response_queue.push(QuorumResponse{response_type: resp})
                                    }
                                    Err(err) => {
                                        let msg = err.to_string();
                                        // TODO send error response
                                        tracing::error!("sending append entries with id {} to member {} failed with error {} after {}", id, self.member_id, msg, request_time);
                                    }
                                }*/
                                self.last_heartbeat = now_millis()
                            }
                        }
                    }
                }
                None => {
                    // Condvar wait or sleep
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }
        }
    }
}
