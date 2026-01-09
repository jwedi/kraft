use std::sync::Arc;
use std::thread;
use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::{mpsc, oneshot};

use raftproto::raft_server::{Raft, RaftServer};
use log::{info, warn};
use tokio::sync::oneshot::{Receiver, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use crate::runtime_core::task_buffer::TaskBufferImpl;
use crate::runtime_core::types::{Command, RuntimeTask, RuntimeTaskResponse, CommandType};
use crate::transport::raft::raftproto::{RemoteAppendEntriesRequest, RemoteAppendEntriesResponse, RemoteVoteRequest, RemoteVoteResponse, RemotePutBatchRequest, RemotePutBatchResponse, RemotePutResponse, RemoteQuorumMessage, StreamResponse};
use crate::service_utils::errors::ServiceError;
use crate::raft::raft_sm::{RaftStateMachineExecutor, StateMachineExecutorImpl, RaftServerState, TermVote, RaftVolatileState, SharedState, LocalRaftMessage, LocalRaftMessagePayload, LocalRequestVoteRequest, LocalRaftResponseMessage, LocalRaftResponsePayload, LocalAppendEntries, LocalRaftWriteBatchRequest, LocalAppendEntriesCallbackResponse};
use crossbeam_queue::SegQueue;
use futures::FutureExt;
use tokio::sync::oneshot::error::RecvError;
use tracing::{instrument, Instrument, Level, Span};
use crate::persistence::worker::PersistenceTaskType;
use crate::transport::raft::raftproto::remote_quorum_message::MessagePayload;
use crate::transport::stream_manager::StreamManagerImpl;


pub mod raftproto {
    tonic::include_proto!("raftproto"); // The string specified here must match the proto package name
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
        let _timing_guard = crate::transport::metrics::record_rpc_request("put_batch");
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
            batch_id: req.batch_id,
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
        let _timing_guard = crate::transport::metrics::record_rpc_request("append_entries");
        crate::transport::metrics::record_append_entries();
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
            entry: req.entry,
            commit_index: req.commit_index,
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
                            LocalAppendEntriesCallbackResponse::TruncateLog { prev_term, prev_index } => {
                                tracing::warn!("append entries response from channel received truncate log: prev term {} prev index {}", prev_term, prev_index);
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
        let _timing_guard = crate::transport::metrics::record_rpc_request("request_vote");
        crate::transport::metrics::record_vote_request();
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