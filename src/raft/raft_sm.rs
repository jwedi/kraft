use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use tokio::sync::oneshot;
use crossbeam_queue::SegQueue;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::persistence::worker::{PersistenceResponseType, PersistenceTaskType};
use crate::quorum::worker::{LocalQuorumResponse, LocalQuorumWorkerTask};
use crate::raft::follower::RaftFollowerStateDelegate;
use crate::transport::raft::raftproto::{RemoteLogEntry, RemotePutRequest, RemotePutResponse};
use crate::transport::raft::raftproto::raft_server::Raft;
use crate::service_utils::app_time::now_millis;
use crate::service_utils::storage_utils::SerializationData;
use crate::transport::write_proxy::{WriteBatch, WriteResponse};
use std::time::Instant;
use bus::Bus;

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

//#[derive(Copy, Clone)]
pub struct RaftVolatileState {
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub next_term: u64,// 1 more than the largest term value seen.
    pub next_log_index: u64,
    pub replication_log: Vec<Arc<SerializationData>>,
    pub replication_log_term_starts: HashMap<u64, u64>,
    pub commit_state: Arc<CommitState>
}

pub struct CommitState {
    pub commit_index: AtomicU64,
    pub term: AtomicU64,
    pub version: AtomicU64,
}

pub enum OutstandingMessageType {
    RequestVote{callback: oneshot::Sender<LocalRaftResponseMessage>}, // Request vote from someone else
    CandidateElection{persistence_done: bool, quorum_votes: u32}, // Candidate election by this node.
    AppendLog{callback: oneshot::Sender<LocalRaftResponseMessage>},
    WriteBatch{persistence_done: bool, quorum_acks: u32, callback: oneshot::Sender<LocalRaftResponseMessage>, index: u64, start_time: Instant, batch_size: usize}
}

pub struct OutstandingMessage {
    pub id: u64,
    pub outstanding_responses: i32,
    pub message_type: OutstandingMessageType,
    pub span: Span
}

pub struct StateMachineConfig {
    pub max_message_size_bytes: usize,
}

pub struct SharedState {
    pub server_state: RaftServerState,
    pub volatile_server_state: RaftVolatileState,
    pub persistence_work: Arc<SegQueue<PersistenceTaskType>>,
    pub persistence_response: Arc<SegQueue<PersistenceResponseType>>,
    pub quorum_work: Arc<SegQueue<LocalQuorumWorkerTask>>,
    pub quorum_response: Arc<SegQueue<LocalQuorumResponse>>,
    pub identity: u32,
    pub term_votes: HashMap<u64, u32>, // Term -> candidate_id that was voted for.
    pub next_message_id: u64,
    pub outstanding_messages: HashMap<u64, OutstandingMessage>,
    pub quorum_size: u32,
    pub quorum_worker_tasks: Vec<Arc<SegQueue<LocalQuorumWorkerTask>>>,
    pub state_machine_config: StateMachineConfig,
    pub log_entry_bus: Bus<Arc<SerializationData>>
}

pub struct StateMachineExecutorImpl {
    state_delegate: Box<dyn RaftProtocol>,
    shared_state: SharedState,
}

impl StateMachineExecutorImpl {
    pub fn new(shared_state: SharedState) -> Self {
        // Initialize metrics with current state
        crate::transport::metrics::update_raft_state_metrics(
            shared_state.volatile_server_state.commit_index,
            shared_state.server_state.leader_id,
            shared_state.server_state.current_term,
            shared_state.volatile_server_state.replication_log.len()
        );

        Self {
            state_delegate: Box::new(RaftFollowerStateDelegate::new()),
            shared_state
        }
    }
}

impl RaftStateMachineExecutor for StateMachineExecutorImpl {

    fn initialize(&mut self) {
        log::info!("Starting state machine initialize");
        for entry in &self.shared_state.volatile_server_state.replication_log {
            self.shared_state.log_entry_bus.broadcast(Arc::clone(entry));
        }
        log::info!("State machine initialize completed");
    }

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

    fn accept(&mut self, raft_message: LocalRaftMessage) {

        //log::debug!("delegating raft message to: {:?}", self.state_delegate.get_node_type());
        match raft_message.payload {

            LocalRaftMessagePayload::RequestVote(r) => {
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
            LocalRaftMessagePayload::AppendEntries(r) => {
                let span = tracing::span!(Level::INFO, "sm_append_entries");
                span.set_parent(raft_message.parent_span.context());
                let _enter_span = span.enter();
                tracing::info!("starting append entries processing of {} entries", r.entries.len());
                let state_change = self.state_delegate.append_entries(r, raft_message.callback, &mut self.shared_state);
                tracing::info!("finished append entries processing");
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
            LocalRaftMessagePayload::GetRaftState => {
                let span = tracing::span!(Level::INFO, "sm_get_raft_state");
                span.set_parent(raft_message.parent_span.context());
                span.in_scope(|| {
                    let resp_payload = LocalRaftResponsePayload::RaftState {leader_id: self.shared_state.server_state.leader_id};
                    let resp = LocalRaftResponseMessage {
                        payload: resp_payload
                    };
                    raft_message.callback.send(resp).expect("Sending raft callback should not fail")
                })
            }
            LocalRaftMessagePayload::WriteBatch(batch) => {
                let span = tracing::span!(Level::INFO, "sm_write_batch");
                span.set_parent(raft_message.parent_span.context());
                let _enter_span = span.enter();
                // If state machine is leader, forward, otherwise reject.
                if self.state_delegate.get_node_type() == RaftNodeType::Leader {
                    tracing::info!("Processing write batch as leader.");
                    self.state_delegate.write_batch(batch, raft_message.callback, &mut self.shared_state)
                } else {
                    log::error!("Received write batch as non-leader. Leader is {}", self.shared_state.server_state.leader_id);
                    let resp = LocalRaftWriteBatchResponse {
                        responses: vec![],
                        err: Some("Received write batch as non-leader".to_string())
                    };
                    let msg = LocalRaftResponsePayload::WriteBatch(resp);
                    raft_message.callback.send(LocalRaftResponseMessage {payload: msg}).expect("Sending raft callback should not fail")
                }

            }
        }
    }
}

pub trait RaftStateMachineExecutor {
    fn accept(&mut self, raft_message: LocalRaftMessage);

    fn time_step(&mut self);

    fn initialize(&mut self);
}

// Per-state implementation of the Raft protocol. Handles requests, returns responses through callbacks and notifies about state changes.
pub trait RaftProtocol {

    fn init(&self);

    fn get_node_type(&self) -> RaftNodeType;
    fn request_vote(&mut self, request_vote_request: LocalRequestVoteRequest, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange;
    fn append_entries(&mut self, append_entries_request: LocalAppendEntries, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange;

    fn write_batch(&mut self, write_batch_request: LocalRaftWriteBatchRequest, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) {
        tracing::error!(message = "write batch not allowed in this node state");
    }

    fn time_step(&mut self, shared_state: &mut SharedState) -> RaftMessageStateChange;

    fn validate_append_entries(&mut self, append_entries_request: &LocalAppendEntries, prev_index: u64, prev_term: u64) -> bool {
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

pub struct LocalRequestVoteRequest {
    pub term: u64,
    pub candidate_id: u32,
    pub last_log_index: u64,
    pub last_log_term: u64
}

pub struct LocalAppendEntries {
    pub term: u64,
    pub leader_id: u32,
    pub entries: Vec<RemoteLogEntry>,
    pub prev_index: u64,
    pub prev_term: u64,
    pub request_id: u64,
    pub entry: Option<RemoteLogEntry>,
    pub commit_index: u64
}

pub struct LocalRaftMessage {
    pub payload: LocalRaftMessagePayload,
    pub parent_span: Span,
    pub callback: oneshot::Sender<LocalRaftResponseMessage>,
}

#[derive(Debug)]
pub enum LocalRaftResponsePayload {
    None,
    RequestVote(bool), // granted
    AppendEntries(LocalAppendEntriesCallbackResponse), // OK
    RaftState{leader_id: u32},
    WriteBatch(LocalRaftWriteBatchResponse)
}

#[derive(Debug)]
pub enum LocalAppendEntriesCallbackResponse {
    Ok,
    UnrecognizedLeader,
    WantedPreviousEntry{ last_term: u64, last_index: u64}, // Want the entry that follows the given term and index
    TruncateLog{ prev_term: u64, prev_index: u64} // Leader is telling follower to truncate log to this point
}

#[derive(Debug)]
pub struct LocalRaftWriteBatchResponse {
    pub responses: Vec<RemotePutResponse>,
    pub err: Option<String>
}

#[derive(Debug)]
pub struct LocalRaftResponseMessage {
    pub payload: LocalRaftResponsePayload,
}

pub struct LocalRaftWriteBatchRequest {
    pub requests: Vec<RemotePutRequest>,
}

pub enum LocalRaftMessagePayload {
    RequestVote(LocalRequestVoteRequest),
    AppendEntries(LocalAppendEntries),
    GetRaftState,
    WriteBatch(LocalRaftWriteBatchRequest),
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
    pub log: Vec<RemoteLogEntry>
}

pub struct LeaderState {
    pub current_term: u64,
    pub log : Vec<RemoteLogEntry>,
    pub max_heartbeat_cadence: u64,
}

pub struct RaftSM {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<RemoteLogEntry>
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::raft::leader::RaftLeaderStateDelegate;
    use crate::raft::follower::RaftFollowerStateDelegate;
    use crate::quorum::worker::{LocalQuorumResponse, LocalQuorumTaskResponseType, LocalQuorumWorkerTask, LocalQuorumWorkerTaskType};
    use crate::service_utils::storage_utils::SerializationData;
    use std::collections::HashMap;
    use std::sync::Arc;
    use crossbeam_queue::SegQueue;

    fn create_shared_state() -> SharedState {
        SharedState {
            server_state: RaftServerState {
                current_term: 1,
                leader_id: 1,
            },
            volatile_server_state: RaftVolatileState {
                next_term: 2,
                last_log_term: 1,
                last_log_index: 0,
                commit_index: 0,
                replication_log: vec![],
                replication_log_term_starts: std::collections::HashMap::new(),
                last_applied: 0,
                next_log_index: 1,
                commit_state: Arc::new(CommitState{
                    commit_index: AtomicU64::new(0),
                    term: AtomicU64::new(0),
                    version: AtomicU64::new(0),
                })
            },
            next_message_id: 1,
            outstanding_messages: std::collections::HashMap::new(),
            quorum_size: 3,
            quorum_worker_tasks: vec![Arc::new(SegQueue::new()); 3],
            persistence_work: Arc::new(SegQueue::new()),
            persistence_response: Arc::new(SegQueue::new()),
            quorum_work: Arc::new(SegQueue::new()),
            quorum_response: Arc::new(SegQueue::new()),
            identity: 0,
            term_votes: HashMap::new(),
            state_machine_config: StateMachineConfig { max_message_size_bytes: 2048},
            log_entry_bus: Bus::new(5000)
        }
    }

    #[tokio::test]
    async fn test_complete_truncate_flow_leader_missing_entries() {
        // This test simulates the exact scenario described in the bug:
        // 1. Follower requests backfill for term 2 and index 5
        // 2. Leader is leader of term 3 and only has term 2 index 3
        // 3. Leader finds last entry of term 2, sees it's less than requested index 5
        // 4. Leader sends a truncate signal with prev term 2 and prev index 3
        // 5. Follower truncates its local log to term 2 index 3
        // 6. Follower should then request backfill again

        let mut leader_delegate = RaftLeaderStateDelegate::new();
        leader_delegate.initialized = true;
        let mut leader_state = create_shared_state();

        // Set up leader state: leader of term 3, only has term 2 up to index 3
        leader_state.server_state.current_term = 3;
        leader_state.server_state.leader_id = 0;
        leader_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        leader_state.volatile_server_state.replication_log_term_starts.insert(3, 4);

        // Leader has term 2 entries 0,1,2,3 and term 3 entries 4,5,6
        for i in 0..7 {
            leader_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: if i < 4 { 2 } else { 3 },
                timestamp: 0,
                prev_index: i,
                prev_term: if i < 4 { 1 } else { 2 },
                index: i,
                message_id: i,
            }));
        }

        // Step 1: Follower requests backfill for term 2 index 5 (which leader doesn't have)
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                prev_term: 2,
                prev_index: 5, // Leader only has up to index 3 for term 2
            }
        });

        // Step 2 & 3: Leader processes the request and detects it doesn't have the entries
        let result = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Step 4: Verify leader sent truncate signal
        let queue = &leader_state.quorum_worker_tasks[0]; // quorum_node_id 1 maps to index 0
        assert_eq!(queue.len(), 1);

        let truncate_task = queue.pop().expect("Should have truncate task");
        let (truncate_prev_term, truncate_prev_index) = match truncate_task.task_type {
            LocalQuorumWorkerTaskType::TruncateLog { prev_term, prev_index, .. } => {
                assert_eq!(prev_term, 2);
                assert_eq!(prev_index, 3); // Last index leader has for term 2
                (prev_term, prev_index)
            }
            _ => panic!("Expected TruncateLog task"),
        };

        // Now simulate the follower side
        let mut follower_delegate = RaftFollowerStateDelegate::new();
        let mut follower_state = create_shared_state();

        // Set up follower state: has term 2 entries up to index 5 (the problematic entries)
        follower_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        for i in 0..6 {
            follower_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: 2,
                timestamp: 0,
                prev_index: 0,
                prev_term: 0,
                index: i,
                message_id: i,
            }));
        }
        follower_state.volatile_server_state.last_log_term = 2;
        follower_state.volatile_server_state.last_log_index = 5;
        follower_state.volatile_server_state.next_log_index = 6;

        // Step 5: Follower receives truncate signal
        follower_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::TruncateLog {
                quorum_node_id: 0, // From leader
                prev_term: truncate_prev_term,
                prev_index: truncate_prev_index,
            }
        });

        let result = follower_delegate.time_step(&mut follower_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Verify follower truncated correctly to term 2 index 3
        assert_eq!(follower_state.volatile_server_state.replication_log.len(), 4); // 0,1,2,3
        assert_eq!(follower_state.volatile_server_state.last_log_term, 2);
        assert_eq!(follower_state.volatile_server_state.last_log_index, 3);
        assert_eq!(follower_state.volatile_server_state.next_log_index, 4);

        // Step 6: Now if follower requests backfill again, leader should be able to provide it
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                prev_term: 2,
                prev_index: 3, // Now leader has this
            }
        });

        let result = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should have sent backfill data, not another truncate
        let queue = &leader_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        let backfill_task = queue.pop().expect("Should have backfill task");
        match backfill_task.task_type {
            LocalQuorumWorkerTaskType::BackfillLog { data } => {
                assert!(data.len() > 0); // Should have data to backfill
            }
            _ => panic!("Expected BackfillLog task"),
        }
    }

    #[tokio::test]
    async fn test_truncate_flow_with_new_leader_entries() {
        // Test scenario where new leader has written additional entries in a higher term
        let mut leader_delegate = RaftLeaderStateDelegate::new();
        leader_delegate.initialized = true;
        let mut leader_state = create_shared_state();

        // Leader is in term 4, has entries for terms 2, 3, and 4
        leader_state.server_state.current_term = 4;
        leader_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        leader_state.volatile_server_state.replication_log_term_starts.insert(3, 3);
        leader_state.volatile_server_state.replication_log_term_starts.insert(4, 7);

        // Leader has: term 2 (0,1,2), term 3 (0,1,2), term 4 (0,1,2,3)
        for i in 0..10 {
            leader_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: if i < 3 { 2 } else if i < 6 { 3 } else { 4 },
                timestamp: 0,
                prev_index: 0,
                prev_term: 0,
                index: i % 3,
                message_id: i,
            }));
        }

        // Follower requests backfill for term 3 index 5 (leader has term 3 up to index 6)
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                prev_term: 3,
                prev_index: 1,
            }
        });

        let result = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should send backfill, not truncate, since leader has the requested entries
        let queue = &leader_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        let task = queue.pop().expect("Should have task");
        match task.task_type {
            LocalQuorumWorkerTaskType::BackfillLog { data } => {
                assert_eq!(data.len(), 5)
            }
            default => panic!("Expected BackfillLog task"),
        }
    }

    #[tokio::test]
    async fn test_truncate_flow_boundary_conditions() {
        // Test edge cases like requesting the last entry of a term
        let mut leader_delegate = RaftLeaderStateDelegate::new();
        leader_delegate.initialized = true;
        let mut leader_state = create_shared_state();

        leader_state.server_state.current_term = 3;
        leader_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        leader_state.volatile_server_state.replication_log_term_starts.insert(3, 5);

        // Leader has term 2 entries 0,1,2,3,4 and term 3 entries 5,6
        for i in 0..7 {
            leader_state.volatile_server_state.replication_log.push(Arc::new(SerializationData {
                requests: vec![],
                term: if i < 5 { 2 } else { 3 },
                timestamp: 0,
                prev_index: 0,
                prev_term: 0,
                index: i,
                message_id: i,
            }));
        }

        // Request exactly the last entry of term 2 (index 4)
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                prev_term: 2,
                prev_index: 4, // Last entry of term 2
            }
        });

        let result = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should send backfill since leader has this entry
        let queue = &leader_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        let task = queue.pop().expect("Should have task");
        match task.task_type {
            LocalQuorumWorkerTaskType::BackfillLog { .. } => {
                // Success - leader has the entry
            }
            _ => panic!("Expected BackfillLog task"),
        }

        // Now request beyond the last entry of term 2 (index 5, but term 2 only goes to 4)
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                prev_term: 2,
                prev_index: 5, // Beyond last entry of term 2
            }
        });

        let result = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should send truncate since leader doesn't have term 2 index 5
        let queue = &leader_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        let task = queue.pop().expect("Should have task");
        match task.task_type {
            LocalQuorumWorkerTaskType::TruncateLog { prev_term, prev_index, .. } => {
                assert_eq!(prev_term, 2);
                assert_eq!(prev_index, 4); // Last entry leader has for term 2
            }
            _ => panic!("Expected TruncateLog task"),
        }
    }
}




