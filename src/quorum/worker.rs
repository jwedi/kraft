use std::any::Any;
use std::cell::RefCell;
use std::error::Error;
use std::future::Future;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc, Condvar};
use std::thread::sleep;
use std::time::Duration;
use arc_swap::ArcSwap;
use crossbeam_queue::SegQueue;
use futures::{FutureExt, StreamExt, TryFuture};
use log::{error, info};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_zipkin::Propagator;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, oneshot};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::task::JoinSet;
use tonic::{Request, Response, Status};
use tonic::transport::{Channel};
use tracing::{Instrument, Level, Span};
use tracing::instrument::Instrumented;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::client::cluster_node_client::SharedGrpcChannel;
use crate::raft::raft_sm::{LocalAppendEntries, LocalRaftMessage, LocalRaftMessagePayload, LocalRaftResponseMessage, LocalRaftResponsePayload, LocalRequestVoteRequest};
use crate::server::raftproto::raft_client::RaftClient;
use crate::server::raftproto::{RemoteAppendEntriesAcknowledge, RemoteAppendEntriesRequest, RemoteAppendEntriesResponse, RemoteLogEntry, RemoteQuorumMessage, RemoteVoteRequest, RemoteVoteResponse};
use crate::server::raftproto::remote_quorum_message::MessagePayload;
use crate::service_utils::app_time::{now_millis, now_plus_duration_millis};
use crate::service_utils::storage_utils::{SerializationData, SerializedData};
use crate::transport::stream_manager::{StreamManager, StreamManagerImpl};
use crate::transport::write_proxy::WriteResponse;

pub struct LocalQuorumWorkerTask {
    pub task_type: LocalQuorumWorkerTaskType
    // New append entries
    // Request vote
    // Start heartbeats
    // Stop heartbeats
}

pub enum LocalQuorumWorkerTaskType {
    StartHeartbeats{ id: u64, term: u64, prev_term: u64, prev_log_index: u64 },
    StopHeartbeats { id: u64 },
    RequestVote{ id: u64, term: u64, last_term: u64, last_index: u64, parent_span: Span}, // Term, last term, last index,
    AppendEntries{ id: u64, req: RemoteAppendEntriesRequest, parent_span: Span},
    // TODO backfill
}

pub enum LocalQuorumTaskResponseType {
    RequestVoteResponse{ id: u64, term: u64, received_vote: bool},
    AppendEntries{ id: u64, ok: bool}, // TODO get next index
    BackfillLog{member_id: u64, prev_term: u64, prev_index: u64}
}

pub struct LocalQuorumResponse {
    pub response_type: LocalQuorumTaskResponseType
}

// Responsible for communication with other nodes in the cluster if this node is the leader.
// Sends AppendEntries to followers to achieve quorum and responds back on the response queue.
// Sends RequestVote when asked to.
// Sends AppendEntries heartbeats regardless to keep up constant load.
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

    // TODO needs work task queue since all requests will now come through the quorum worker instead of the gRPC server
    // VoteRequest comes in, is sent to task worker queue
    // Task worker does the job and then sends the response to the quorum member queue and the quorum worker responds over the stream.
    // Append entries comes in, is sent to task worker queue
    // Task worker does the job and then sends the response to the quorum member quorum queue
}


#[derive(Debug)]
pub enum PendingQuorumTaskEnum {
    Vote(RemoteVoteRequest),
    AppendEntries{request_id: u64, log_index: u64, term: u64}
}
#[derive(Debug)]
struct PendingQuorumTask {
    callback: Receiver<LocalRaftResponseMessage>,
    task: PendingQuorumTaskEnum
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
        }
    }



    pub async fn run(&mut self) {
        log::info!("Running quorum worker");
        let mut pending_tasks: Vec<PendingQuorumTask> = vec![];

        loop {
            let mut maybe_receive_stream = self.stream_manager.get_receive_stream(self.member_id as u32).await;
            let mut maybe_send_stream = self.stream_manager.get_send_stream(self.member_id as u32).await;

            if maybe_send_stream.is_none() {
                info!("Trying to connect towards: {}", self.member_id);
                let connect_result = self.stream_manager.try_connect(self.member_id as u32).await;
                match connect_result {
                    Ok(_) => {
                        log::info!("Send stream connect success towards: {}", self.member_endpoint);
                    }
                    Err(e) => {
                        log::error!("Send stream connect error {} towards: {}", e.stringify(), self.member_endpoint);
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            if maybe_receive_stream.is_none() {
                log::error!("Receive stream towards {} is none", self.member_endpoint);
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            let rec = maybe_receive_stream.take().unwrap();
            let sen = maybe_send_stream.take().unwrap();

            let mut receive_stream = rec.lock().await;
            let mut send_stream = sen.lock().await;

            let mut pending_remove = vec![];
            for i in 0..pending_tasks.len() {
                let mut maybe_pt = pending_tasks.get_mut(i);
                match maybe_pt {
                    None => {
                        pending_remove.push(i)
                    }
                    Some(pt) => {
                        let resp = pt.callback.try_recv();
                        match resp {
                            Ok(val) => {
                                match val.payload {
                                    LocalRaftResponsePayload::RequestVote(rv) => {
                                        match &pt.task {
                                            PendingQuorumTaskEnum::Vote(task) => {
                                                send_stream.send(RemoteQuorumMessage{message_payload: Some(MessagePayload::VoteResponseOption(RemoteVoteResponse{vote_granted: rv, request_id: task.request_id, term: task.term}))}).expect("sending on stream shouldn't fail");
                                            }
                                            default => {
                                                log::error!("unexpected pending task type for request vote")
                                            }
                                        }
                                    }
                                    LocalRaftResponsePayload::AppendEntries(ae) => {
                                        // TODO check if OK or not

                                        match &pt.task {
                                            PendingQuorumTaskEnum::AppendEntries{request_id, log_index, term} => {
                                                send_stream.send(RemoteQuorumMessage{message_payload: Some(MessagePayload::AppendEntriesAcknowledgeOption(RemoteAppendEntriesAcknowledge{log_index: *log_index, term: *term, request_id: *request_id, ok: true}))}).expect("sending on stream shouldn't fail");
                                            }
                                            default => {
                                                log::error!("unexpected pending task type for append entries")
                                            }
                                        } }
                                    unknown => {
                                        log::error!("unknown raft response payload")
                                    }
                                }
                                pending_remove.push(i)
                            }
                            Err(err) => {
                                match err {
                                    TryRecvError::Empty => {}
                                    TryRecvError::Closed => {
                                        pending_remove.push(i)
                                    }
                                }
                            }
                        }
                    }
                }

            }

            for (i, el) in pending_remove.iter().enumerate() {
                pending_tasks.remove(el - i);
            }

            let mut iterations = 0;
            let mut receive_error = false;
            let max_smart_batch = 32;
            while iterations < max_smart_batch { // This isn't really smart batching.
                let result = receive_stream.message().now_or_never();
                match result {
                    Some(Ok(Some(val))) => {
                        match val.message_payload {
                            None => {
                                break;
                            }
                            Some(MessagePayload::VoteResponseOption(vote)) => {
                                log::info!("received vote response id: {}, member: {}, term: {}, granted: {}", vote.request_id, self.member_id, vote.term, vote.vote_granted);
                                let resp = LocalQuorumTaskResponseType::RequestVoteResponse{id: vote.request_id, term: vote.term, received_vote: vote.vote_granted};
                                self.response_queue.push(LocalQuorumResponse {response_type: resp})
                            }
                            Some(MessagePayload::AppendEntriesAcknowledgeOption(append_entries)) => {
                                if append_entries.log_index > 0 {
                                    log::info!("received append entries acknowledge from: {}", self.member_id);
                                }
                                if append_entries.request_id != 0 {
                                    // TODO
                                    let resp = LocalQuorumTaskResponseType::AppendEntries{id: append_entries.request_id, ok: append_entries.ok};
                                    self.response_queue.push(LocalQuorumResponse {response_type: resp})
                                }
                            }
                            Some(MessagePayload::AppendEntriesRequestOption(mut append_entries)) => {
                                //log::info!("received append entries request from: {}", self.member_id);
                                let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();

                                let index = append_entries.entry.as_ref().map(|entry| entry.index).unwrap_or_else(|| 0);
                                let term = append_entries.entry.as_ref().map(|entry| entry.term).unwrap_or_else(|| 0);

                                let span = tracing::span!(Level::INFO, "awaiting_append_entries_response");

                                self.task_queue.push(LocalRaftMessage {
                                    payload: LocalRaftMessagePayload::AppendEntries(LocalAppendEntries { leader_id: append_entries.leader_id, term: append_entries.term, prev_term: append_entries.prev_log_term, prev_index: append_entries.prev_log_index, entries: append_entries.entries, request_id: append_entries.request_id, entry: append_entries.entry}),
                                    callback: callback.0,
                                    parent_span: span

                                }, );

                                let pending_task = PendingQuorumTask {
                                    task: PendingQuorumTaskEnum::AppendEntries{request_id: append_entries.request_id, log_index: term, term: index},
                                    callback: callback.1
                                };
                                pending_tasks.push(pending_task);
                            }
                            Some(MessagePayload::VoteRequestOption(vote_request)) => {
                                log::info!("received vote request from: {} for term: {}", self.member_id, vote_request.term);
                                // TODO what do we do with the callbacks? Do we add a list of receivers that the quorum worker polls each iteration or does the queue worker send a message instead of invoking a callback?
                                // Adding a list of pending callbacks is probably easiest since we don't need to re-write all state machines.
                                let callback: (Sender<LocalRaftResponseMessage>, Receiver<LocalRaftResponseMessage>) = oneshot::channel();

                                let span = tracing::span!(Level::INFO, "awaiting_vote_response");
                                self.task_queue.push(LocalRaftMessage {
                                    payload: LocalRaftMessagePayload::RequestVote(LocalRequestVoteRequest {term: vote_request.term, candidate_id: vote_request.candidate_id, last_log_index: vote_request.last_log_index, last_log_term: vote_request.last_log_term }),
                                    callback: callback.0,
                                    parent_span: span

                                }, );
                                let pending_task = PendingQuorumTask {
                                    task: PendingQuorumTaskEnum::Vote(vote_request),
                                    callback: callback.1
                                };
                                pending_tasks.push(pending_task);
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
                        log::error!("Receive stream message error, invalidating both send and receive streams: {}", err);
                        // Kill receive stream
                        receive_error = true;
                        break;

                    }
                    None => {
                        break;
                    }
                }
            }

            if receive_error {
                drop(receive_stream);
                self.stream_manager.reset_receive_stream(self.member_id as u32).await;
                drop(send_stream);
                self.stream_manager.reset_send_stream(self.member_id as u32).await;
                continue
            }

            let now = now_millis();
            // SOmething here, maybe not...
            if self.send_heartbeats && (now-self.last_heartbeat > 50){
                let request = RemoteAppendEntriesRequest{
                    term: self.term,
                    leader_id: self.self_id,
                    prev_log_index: self.prev_log_index,
                    prev_log_term: self.prev_log_term,
                    entries: vec![],
                    request_id: 0,
                    entry: None
                };

                let req = MessagePayload::AppendEntriesRequestOption(request);

                let response = send_stream.send(RemoteQuorumMessage{message_payload: Some(req)});

                match response {
                    Ok(resp) => {
                        //tracing::info!("received append entries response {} for member {}, term {}, request duration: {}", val.ok, self.member_id, self.term, request_time);
                    }
                    Err(err) => {
                        tracing::error!("received heartbeat append entries error {} for member {}", err.to_string(), self.member_id);
                    }
                }

                self.last_heartbeat = now_millis()
            }
            let queue_len = self.work_queue.len();
            if queue_len > 5 {
                log::info!("Long quorum queue len: {}", queue_len);
            }
            let task = self.work_queue.pop();
            match task {
                Some(task) => {
                    log::info!("Received quorum task");
                    match task.task_type {
                        LocalQuorumWorkerTaskType::StartHeartbeats{id, term, prev_term, prev_log_index} => {
                            log::info!("received start heartbeat request id: {}, term: {}", id, term);
                            self.send_heartbeats = true;
                            self.term = term;
                            self.prev_log_term = prev_term;
                            self.prev_log_index = prev_log_index;
                        }
                        LocalQuorumWorkerTaskType::StopHeartbeats{id} => {
                            log::info!("received stop heartbeat request id: {}", id);
                            self.send_heartbeats = false;
                        }
                        LocalQuorumWorkerTaskType::RequestVote{id, term, last_index, last_term, parent_span} => {
                            log::info!("sending request vote with request id {} for term {}", id, term);
                            let vote_req = RemoteVoteRequest{
                                term,
                                candidate_id: self.self_id,
                                last_log_index: last_index,
                                last_log_term: last_term,
                                request_id: id
                            };
                            // TODO not unwrap
                            /*
                            thread 'tokio-runtime-worker' panicked at src/quorum/worker.rs:369:129:
                            called `Result::unwrap()` on an `Err` value: SendError { .. }
                            stack backtrace:
                            note: Some details are omitted, run with `RUST_BACKTRACE=full` for a verbose backtrace.
                             */
                            match send_stream.send(RemoteQuorumMessage{message_payload: Some(MessagePayload::VoteRequestOption(vote_req))}) {
                                Ok(_) => {}
                                Err(e) => {
                                    drop(send_stream);
                                    error!("Error sending message to send stream for member {}, error: {}", self.member_id, e);
                                    self.stream_manager.reset_send_stream(self.member_id as u32).await;
                                    info!("After reset send stream")
                                }
                            }
                        }
                        LocalQuorumWorkerTaskType::AppendEntries {id, req, parent_span} => {
                            // TODO benchmark copying log entries 3 times and writing to segqueues vs cloning Vectors.
                            send_stream.send(RemoteQuorumMessage{message_payload: Some(MessagePayload::AppendEntriesRequestOption(req))}).unwrap();
                            self.last_heartbeat = now_millis();
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
