use std::sync::Arc;
use std::thread;
use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::{mpsc, oneshot};

use ping::ping_pong_server::{PingPong, PingPongServer};
use ping::{PingRequest, PingResponse};
use raftproto::raft_server::{Raft, RaftServer};
use log::{info, warn};
use tokio::sync::oneshot::{Receiver, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use crate::runtime_core::task_buffer::TaskBufferImpl;
use crate::runtime_core::types::{Command, RuntimeTask, RuntimeTaskResponse, CommandType};
use crate::server::raftproto::{RemoteAppendEntriesRequest, RemoteAppendEntriesResponse, RemoteVoteRequest, RemoteVoteResponse, RemotePutBatchRequest, RemotePutBatchResponse, RemotePutResponse, RemoteQuorumMessage, StreamResponse};
use crate::service_utils::errors::ServiceError;
use crate::raft::raft_sm::{RaftStateMachineExecutor, StateMachineExecutorImpl, RaftServerState, TermVote, RaftVolatileState, SharedState, LocalRaftMessage, LocalRaftMessagePayload, LocalRequestVoteRequest, LocalRaftResponseMessage, LocalRaftResponsePayload, LocalAppendEntries, LocalRaftWriteBatchRequest, LocalAppendEntriesCallbackResponse};
use crossbeam_queue::SegQueue;
use futures::FutureExt;
use tokio::sync::oneshot::error::RecvError;
use tracing::{instrument, Instrument, Level, Span};
use crate::persistence::worker::PersistenceTaskType;
use crate::server::raftproto::remote_quorum_message::MessagePayload;
use crate::transport::stream_manager::StreamManagerImpl;

pub mod ping {
    tonic::include_proto!("ping"); // The string specified here must match the proto package name
}

pub mod raftproto {
    tonic::include_proto!("raftproto"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct PingServer {}

#[tonic::async_trait]
impl PingPong for PingServer {
    async fn ping(
        &self,
        request: Request<PingRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<PingResponse>, Status> { // Return an instance of type HelloReply
        tracing::info!("received request");

        let reply = PingResponse {
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }
}

pub struct QueueWorker {
    work_queue: Arc<SegQueue<LocalRaftMessage>>,
    state_machine: Box<StateMachineExecutorImpl>,
}

impl QueueWorker {
    pub fn new(work_queue: Arc<SegQueue<LocalRaftMessage>>, shared_state: SharedState) -> Self {
        Self {
            work_queue,
            state_machine: Box::new(StateMachineExecutorImpl::new(shared_state))
        }
    }

    pub fn run(&mut self) {
        log::info!("Running worker");
        loop {
            self.state_machine.time_step();
            let task = self.work_queue.pop();

            match task {
                Some(task) => {
                    self.state_machine.accept(task)
                }
                None => {
                    let sleep_duration = core::time::Duration::from_micros(50);
                    thread::sleep(sleep_duration)
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct RaftServerImpl {
    pub task_queue: Arc<SegQueue<LocalRaftMessage>>,
    pub self_id: u32,
    pub stream_manager: Arc<StreamManagerImpl>
}

#[tonic::async_trait]
impl Raft for RaftServerImpl {

    async fn messages(&self, req: Request<tonic::Streaming<RemoteQuorumMessage>>) -> Result<Response<StreamResponse>, Status> {
        log::info!("Received stream connect request");
        let n_id = req.metadata().get("x-node-id");
        let node_id = n_id.unwrap().to_str().unwrap().parse::<u32>().unwrap();
        let mut stream = req.into_inner();
        log::info!("Accepting stream from {}", node_id);
        self.stream_manager.accept_stream(stream, node_id).await;
        log::info!("Stream from {} added", node_id);

        let stream_response = StreamResponse{};

        Ok(Response::new(stream_response))
    }

    #[instrument]
    async fn put_batch(&self, request: Request<RemotePutBatchRequest>) -> Result<Response<RemotePutBatchResponse>, Status> {
        tracing::info!("received put_batch request");

        let req = request.into_inner();
        let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();
        let payload = LocalRaftMessagePayload::WriteBatch(LocalRaftWriteBatchRequest {
            requests: req.put_request,
        });

        let span = tracing::span!(Level::INFO, "awaiting_put_batch");
        let msg = LocalRaftMessage {
            payload,
            callback: callback.0,
            parent_span: span.clone()
        };
        self.task_queue.push(msg);
        let receive_resp = callback.1.instrument(span).await;

        let ok: bool = match receive_resp {
            Ok(resp) => {
                match resp.payload {
                    LocalRaftResponsePayload::AppendEntries(_) => {
                        tracing::error!("put batch unexpected response type append entries");
                        false
                    }
                    LocalRaftResponsePayload::None => {
                        tracing::error!("put batch unexpected response type none");
                        false
                    }
                    LocalRaftResponsePayload::RequestVote(_) => {
                        tracing::error!("put batch unexpected response type, request vote");
                        false
                    }
                    LocalRaftResponsePayload::RaftState { .. } => {
                        tracing::error!("put batch unexpected response type, raft state");
                        false
                    }
                    LocalRaftResponsePayload::WriteBatch(resp) => {
                        tracing::info!("write batch responses {}", resp.responses.len());
                        true
                    }
                }
            }
            Err(e) => {
                tracing::error!("put batch receive error {}", e);
                false
            }
        };

        let response = RemotePutBatchResponse {
            responses: vec![
                RemotePutResponse{
                    id: "1234".to_string(),
                    message: "".to_string(),
                    response_type: 0,
                    batch_id: "111".to_string(),
                    node_id: self.self_id,
                }
            ]
        };
        Ok(Response::new(response))
    }

    #[instrument]
    async fn append_entries(&self, request: Request<RemoteAppendEntriesRequest>) -> Result<Response<RemoteAppendEntriesResponse>, Status> {
        tracing::debug!("received append_entries request");
        let req = request.into_inner();
        let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();
        let payload = LocalRaftMessagePayload::AppendEntries(LocalAppendEntries {
            term: req.term,
            leader_id: req.leader_id,
            prev_index: req.prev_log_index,
            prev_term: req.prev_log_term,
            entries: req.entries,
            request_id: req.request_id,
            entry: req.entry
        });
        let span = tracing::span!(Level::INFO, "awaiting_append_entries");
        let msg = LocalRaftMessage {
            payload,
            callback: callback.0,
            parent_span: span.clone()
        };
        self.task_queue.push(msg);
        let receive_resp = callback.1.instrument(span).await;

        let ok: bool = match receive_resp {
            Ok(resp) => {
                match resp.payload {
                    LocalRaftResponsePayload::AppendEntries(append_entries_resp) => {
                        match append_entries_resp {
                            LocalAppendEntriesCallbackResponse::Ok => {
                                true
                            }
                            LocalAppendEntriesCallbackResponse::UnrecognizedLeader => {
                                tracing::warn!("append entries response from channel received unrecognized leader: term: {}, leader {}", req.term, req.leader_id);
                                false
                            }
                            LocalAppendEntriesCallbackResponse::WantedPreviousEntry { last_term, last_index } => {
                                tracing::warn!("append entries response from channel received wanted previous entry: last term {} last index {}, req term {} req index {}", last_term, last_index, req.prev_log_term, req.prev_log_index);
                                false
                            }
                        }
                    }
                    LocalRaftResponsePayload::None => {
                        tracing::error!("append entries unexpected response type {} {}, None", req.term, req.leader_id);
                        false
                    }
                    LocalRaftResponsePayload::RequestVote(_) => {
                        tracing::error!("append entries unexpected response type {} {}, RequestVote", req.term, req.leader_id);
                        false
                    }
                    LocalRaftResponsePayload::RaftState { .. } => {
                        tracing::error!("append entries unexpected response type {} {}, RaftState", req.term, req.leader_id);
                        false
                    }
                    LocalRaftResponsePayload::WriteBatch(_) => {
                        tracing::error!("append entries unexpected response type {} {}, WriteBatch", req.term, req.leader_id);
                        false
                    }
                }
            }
            Err(e) => {
                tracing::error!("append entries receive error {} {} {}", e, req.term, req.leader_id);
                false
            }
        };
        tracing::debug!("append entries response ok: {} term: {} leader: {}", ok, req.term, req.leader_id);
        let response = RemoteAppendEntriesResponse {
            ok
        };
        Ok(Response::new(response))
    }

    #[instrument]
    async fn request_vote(&self, request: Request<RemoteVoteRequest>) -> Result<Response<RemoteVoteResponse>, Status> {
        tracing::info!("received request_vote request");

        let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();
        let req = request.into_inner();
        let payload = LocalRaftMessagePayload::RequestVote(LocalRequestVoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        });
        let span = tracing::span!(Level::INFO, "awaiting_request_vote");
        let msg = LocalRaftMessage {
            payload,
            callback: callback.0,
            parent_span: span.clone()
        };
        self.task_queue.push(msg);
        let receive_resp = callback.1.instrument(span).await;

        let vote_granted: bool = match receive_resp {
            Ok(resp) => {
                match resp.payload {
                    LocalRaftResponsePayload::None => {
                        false
                    }
                    LocalRaftResponsePayload::RequestVote(vote_granted) => {
                        vote_granted
                    }
                    default => {
                        // Error
                        false
                    }
                }
            }
            Err(e) => {
                false
            }
        };

        let response = RemoteVoteResponse {
            vote_granted,
            term: req.term,
            request_id: req.request_id
        };

        Ok(Response::new(response))
    }
}