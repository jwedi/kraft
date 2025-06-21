use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use tokio::sync::oneshot;
use crossbeam_queue::SegQueue;
use std::sync::{Arc, Mutex};
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::persistence::worker::{PersistenceResponseType, PersistenceTaskType};
use crate::quorum::worker::{QuorumResponse, QuorumTask};
use crate::raft::follower::RaftFollowerStateDelegate;
use crate::server::raftproto::{LogEntry, PutRequest, PutResponse};
use crate::server::raftproto::raft_server::Raft;
use crate::service_utils::app_time::now_millis;
use crate::transport::write_proxy::{WriteBatch, WriteResponse};

#[derive(Copy, Clone)]

pub struct RaftServerState {
    pub current_term: u64,
    pub leader_id: u32,
}


#[derive(Copy, Clone)]
pub struct TermVote {
    pub term: u64,
    pub node_id: u64
}

#[derive(Copy, Clone)]
pub struct RaftVolatileState {
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub next_term: u64,// 1 more than the largest term value seen.
    pub next_log_index: u64,
}

pub enum OutstandingMessageType {
    RequestVote{callback: oneshot::Sender<RaftResponseMessage>}, // Request vote from someone else
    CandidateElection{persistence_done: bool, quorum_votes: u32}, // Candidate election by this node.
    AppendLog{callback: oneshot::Sender<RaftResponseMessage>},
    WriteBatch{persistence_done: bool, quorum_acks: u32, callback: oneshot::Sender<RaftResponseMessage>}
}

pub struct OutstandingMessage {
    pub id: u64,
    pub outstanding_responses: i32,
    pub message_type: OutstandingMessageType,
    pub span: Span
}

pub struct SharedState {
    pub server_state: RaftServerState,
    pub volatile_server_state: RaftVolatileState,
    pub persistence_work: Arc<SegQueue<PersistenceTaskType>>,
    pub persistence_response: Arc<SegQueue<PersistenceResponseType>>,
    pub quorum_work: Arc<SegQueue<QuorumTask>>,
    pub quorum_response: Arc<SegQueue<QuorumResponse>>,
    pub identity: u32,
    pub term_votes: HashMap<u64, u32>, // Term -> candidate_id that was voted for.
    pub next_message_id: u64,
    pub outstanding_messages: HashMap<u64, OutstandingMessage>,
    pub quorum_size: u32,
    pub quorum_worker_tasks: Vec<Arc<SegQueue<QuorumTask>>>,
}

pub struct StateMachineExecutorImpl {
    state_delegate: Box<dyn RaftProtocol>,
    shared_state: SharedState,
}

impl StateMachineExecutorImpl {
    pub fn new(shared_state: SharedState) -> Self {
        Self {
            state_delegate: Box::new(RaftFollowerStateDelegate::new()),
            shared_state
        }
    }
}

impl RaftStateMachineExecutor for StateMachineExecutorImpl {
    fn time_step(&mut self) {
        let state_change = self.state_delegate.time_step(&mut self.shared_state);
        match state_change {
            RaftMessageStateChange::None => {}
            RaftMessageStateChange::Candidate(new_state) => {
                log::info!("State change to Candidate.");
                self.state_delegate = new_state
            }
            RaftMessageStateChange::Follower(new_state) => {
                log::info!("State change to Follower.");
                self.state_delegate = new_state
            }
            RaftMessageStateChange::Leader(new_state) => {
                log::info!("State change to Leader.");
                self.state_delegate = new_state
            }
        }
    }

    fn accept(&mut self, raft_message: RaftMessage) {

        //log::debug!("delegating raft message to: {:?}", self.state_delegate.get_node_type());
        match raft_message.payload {

            RaftMessagePayload::RequestVote(r) => {
                let span = tracing::span!(Level::INFO, "sm_accept_request_vote");
                let _enter_span = span.enter();
                let state_change = self.state_delegate.request_vote(r, raft_message.callback, &mut self.shared_state);
                match state_change {
                    RaftMessageStateChange::None => {}
                    RaftMessageStateChange::Candidate(new_state) => {
                        self.state_delegate = new_state
                    }
                    RaftMessageStateChange::Follower(new_state) => {
                        self.state_delegate = new_state
                    }
                    RaftMessageStateChange::Leader(new_state) => {
                        self.state_delegate = new_state
                    }
                }
            }
            RaftMessagePayload::AppendEntries(r) => {
                let span = tracing::span!(Level::INFO, "sm_append_entries");
                span.set_parent(raft_message.parent_span.context());
                let _enter_span = span.enter();
                let state_change = self.state_delegate.append_entries(r, raft_message.callback, &mut self.shared_state);
                match state_change {
                    RaftMessageStateChange::None => {}
                    RaftMessageStateChange::Candidate(new_state) => {
                        self.state_delegate = new_state
                    }
                    RaftMessageStateChange::Follower(new_state) => {
                        self.state_delegate = new_state
                    }
                    RaftMessageStateChange::Leader(new_state) => {
                        self.state_delegate = new_state
                    }
                }
            }
            RaftMessagePayload::GetRaftState => {
                let span = tracing::span!(Level::INFO, "sm_get_raft_state");
                span.set_parent(raft_message.parent_span.context());
                span.in_scope(|| {
                    let resp_payload = RaftResponsePayload::RaftState {leader_id: self.shared_state.server_state.leader_id};
                    let resp = RaftResponseMessage {
                        payload: resp_payload
                    };
                    raft_message.callback.send(resp).expect("Sending raft callback should not fail")
                })
            }
            RaftMessagePayload::WriteBatch(batch) => {
                let span = tracing::span!(Level::INFO, "sm_write_batch");
                span.set_parent(raft_message.parent_span.context());
                let _enter_span = span.enter();
                // If state machine is leader, forward, otherwise reject.
                if self.state_delegate.get_node_type() == RaftNodeType::Leader {
                    self.state_delegate.write_batch(batch, raft_message.callback, &mut self.shared_state)
                } else {
                    log::error!("Received write batch as non-leader. Leader is {}", self.shared_state.server_state.leader_id);
                    let resp = RaftWriteBatchResponse{
                        responses: vec![],
                        err: Some("Received write batch as non-leader".to_string())
                    };
                    let msg = RaftResponsePayload::WriteBatch(resp);
                    raft_message.callback.send(RaftResponseMessage{payload: msg}).expect("Sending raft callback should not fail")
                }

            }
        }
    }
}

pub trait RaftStateMachineExecutor {
    fn accept(&mut self, raft_message: RaftMessage);

    fn time_step(&mut self);
}

// Per-state implementation of the Raft protocol. Handles requests, returns responses through callbacks and notifies about state changes.
pub trait RaftProtocol {

    fn init(&self);

    fn get_node_type(&self) -> RaftNodeType;
    fn request_vote(&mut self, request_vote_request: RequestVoteRequest, callback: oneshot::Sender<RaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange;
    fn append_entries(&mut self, append_entries_request: AppendEntries, callback: oneshot::Sender<RaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange;

    fn write_batch(&mut self, write_batch_request: RaftWriteBatchRequest, callback: oneshot::Sender<RaftResponseMessage>, shared_state: &mut SharedState) {
        tracing::error!(message = "write batch not allowed in this node state");
    }

    fn time_step(&mut self, shared_state: &mut SharedState) -> RaftMessageStateChange;

    fn validate_append_entries(&mut self, append_entries_request: &AppendEntries, prev_index: u64, prev_term: u64) -> bool {
        if append_entries_request.prev_term != prev_term {
            return false
        }
        if append_entries_request.prev_index != prev_index {
            return false
        }
        return true;
    }

    fn should_accept_vote(&mut self, term: u64, last_log_term: u64, last_log_index: u64, shared_state: &mut SharedState) -> bool {
        if let Some(voted_for) = shared_state.term_votes.get(&term) {
            // Node have already voted for someone this term.
            tracing::info!(message = "Rejecting vote, node has already voted this term.", term);
            return false
        }

        if term <= shared_state.server_state.current_term {
            tracing::info!(message = "Rejecting vote, proposed term is lower than current.", term);
            return false
        }
        if last_log_term < shared_state.volatile_server_state.last_log_term || last_log_index < shared_state.volatile_server_state.last_log_index {
            tracing::info!(message = "Rejecting vote, proposer has fewer log entries than node.", term);
            return false
        }
        true
    }
}

pub struct StateMachineResponse {

}

pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: u32,
    pub last_log_index: u64,
    pub last_log_term: u64
}

pub struct AppendEntries {
    pub term: u64,
    pub leader_id: u32,
    pub entries: Vec<LogEntry>,
    pub prev_index: u64,
    pub prev_term: u64,
    pub request_id: u64,
    pub entry: Option<LogEntry>
}

pub struct RaftMessage {
    pub payload: RaftMessagePayload,
    pub parent_span: Span,
    pub callback: oneshot::Sender<RaftResponseMessage>,
}

#[derive(Debug)]
pub enum RaftResponsePayload {
    None,
    RequestVote(bool), // granted
    AppendEntries(AppendEntriesCallbackResponse), // OK
    RaftState{leader_id: u32},
    WriteBatch(RaftWriteBatchResponse)
}

#[derive(Debug)]
pub enum AppendEntriesCallbackResponse {
    Ok,
    UnrecognizedLeader,
    WantedPreviousEntry{ last_term: u64, last_index: u64} // Want the entry that follows the given term and index
}

#[derive(Debug)]
pub struct RaftWriteBatchResponse {
    pub responses: Vec<PutResponse>,
    pub err: Option<String>
}

#[derive(Debug)]
pub struct RaftResponseMessage {
    pub payload: RaftResponsePayload,
}

pub struct RaftWriteBatchRequest {
    pub requests: Vec<PutRequest>,
}

pub enum RaftMessagePayload {
    RequestVote(RequestVoteRequest),
    AppendEntries(AppendEntries),
    GetRaftState,
    WriteBatch(RaftWriteBatchRequest)
}

pub enum RaftMessageStateChange {
    None,
    Follower(Box<dyn RaftProtocol>),
    Candidate(Box<dyn RaftProtocol>),
    Leader(Box<dyn RaftProtocol>)
}

#[derive(Debug, PartialEq)]
pub enum RaftNodeType {
    Follower,
    Candidate,
    Leader
}

struct RaftState {
    pub leader_id: u64
}

pub struct FollowerState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>
}

pub struct LeaderState {
    pub current_term: u64,
    pub log : Vec<LogEntry>,
    pub max_heartbeat_cadence: u64,
}

pub struct RaftSM {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>

}

/*
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: String
}
 */




