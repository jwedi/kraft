use std::collections::HashMap;
use std::fmt::Debug;
use std::thread;
use tokio::sync::oneshot;
use crossbeam_channel::Sender;
use crossbeam_queue::SegQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::persistence::worker::{PersistenceResponseType, PersistenceTaskType};
use crate::quorum::types::{LocalQuorumResponse, LocalQuorumWorkerTask};
use crate::raft::follower::RaftFollowerStateDelegate;
use crate::transport::capnp::{OwnedWriteBatch, OwnedWriteBatchResponse, OwnedLogEntry, build_owned_write_batch_response};
use crate::transport::write_proxy::{WriteBatch, WriteResponse};
use std::time::{Duration, Instant};
use bus::Bus;
use log::{info, warn};

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
    pub replication_log: Vec<Arc<OwnedLogEntry>>,
    pub replication_log_term_starts: HashMap<u64, u64>,
    pub commit_state: Arc<CommitState>,
    /// Tracks the highest log index that has been broadcast to the bus.
    /// None means no entries have been broadcast yet.
    /// Only committed entries are broadcast to ensure query workers never see truncated data.
    pub last_broadcast_index: Option<u64>,
    /// Indicates that there are deferred broadcasts pending due to bus full condition.
    /// When true, time_step will retry broadcasting remaining entries.
    pub has_deferred_broadcast: bool,
}

pub struct CommitState {
    pub commit_index: AtomicU64,
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
    pub persistence_work: Sender<PersistenceTaskType>,
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
    pub log_entry_bus: Bus<Arc<OwnedLogEntry>>
}

impl SharedState {
    pub fn broadcast_committed_entries(&mut self, new_commit_index: u64) {
        let broadcast_start = Instant::now();
        let start_index = match self.volatile_server_state.last_broadcast_index {
            Some(last) => last + 1,
            None => 0,
        };

        if start_index > new_commit_index {
            return;
        }

        let mut last_broadcast_index = 0u64;
        for i in start_index..=new_commit_index {
            let idx = i as usize;
            if let Some(entry) = self.volatile_server_state.replication_log.get(idx) {
                let e = Arc::clone(entry);
                match self.log_entry_bus.try_broadcast(e) {
                    Ok(_) => {
                        last_broadcast_index = i;
                    }
                    Err(_) => {
                        last_broadcast_index = self.broadcast_committed_entries_when_bus_full(new_commit_index as usize, idx) as u64;
                        break;
                    }
                }
            } else {
                warn!("Didnt find index {} in replication log of length {}", idx, self.volatile_server_state.replication_log.len())
            }
        }

        let entries_broadcasted = last_broadcast_index - start_index;
        let elapsed_millis = broadcast_start.elapsed().as_millis();
        if elapsed_millis > 5 || self.volatile_server_state.last_broadcast_index.is_none() {
            log::info!("Broadcasted {} entries in {} ms", entries_broadcasted, elapsed_millis)
        }
        self.volatile_server_state.last_broadcast_index = Some(last_broadcast_index);
        self.volatile_server_state.has_deferred_broadcast = last_broadcast_index < new_commit_index;
        self.volatile_server_state.commit_state.version.fetch_add(1, Ordering::Release);
    }

    /// Broadcasts committed entries when the bus is full.
    ///
    /// 1. Attempts to broadcast with a limited number of retries per entry
    /// 2. Returns early if the bus remains full after max retries
    /// 3. The caller will retry on the next time_step, allowing other Raft operations to proceed
    ///
    /// This prevents blocking elections and heartbeats when consumers are slow.
    fn broadcast_committed_entries_when_bus_full(&mut self, new_absolute_commit_index: usize, start_index: usize) -> usize {
        const MAX_RETRIES_PER_ENTRY: u32 = 100;
        const YIELD_INTERVAL: u32 = 10; // Yield every N retries

        self.volatile_server_state.commit_state.version.fetch_add(1, Ordering::Release);
        let mut idx = start_index;

        while idx <= new_absolute_commit_index {
            if let Some(entry) = self.volatile_server_state.replication_log.get(idx) {
                let e = Arc::clone(entry);
                let mut retries = 0u32;

                loop {
                    match self.log_entry_bus.try_broadcast(e.clone()) {
                        Ok(_) => {
                            idx += 1;
                            break;
                        }
                        Err(_) => {
                            retries += 1;
                            if retries >= MAX_RETRIES_PER_ENTRY {
                                // Bus is persistently full - return early to allow other operations
                                // The next call to broadcast_committed_entries will resume from here
                                log::warn!(
                                    "Bus full after {} retries at index {}, deferring remaining {} entries",
                                    retries,
                                    idx,
                                    new_absolute_commit_index - idx
                                );
                                return if idx > start_index { idx - 1 } else { start_index };
                            }
                            if retries % YIELD_INTERVAL == 0 {
                                self.volatile_server_state.commit_state.version.fetch_add(1, Ordering::Release);
                                thread::yield_now();
                            }
                        }
                    }
                }
            } else {
                log::warn!("No replication log entry at index: {}, returning", idx);
                return if idx > start_index { idx - 1 } else { start_index };
            }
        }
        new_absolute_commit_index
    }

    /// Maximum entries to broadcast per time_step to avoid blocking Raft operations.
    const MAX_BROADCAST_PER_TIMESTEP: u64 = 1000;

    /// Retries broadcasting deferred entries. Called from time_step.
    /// Returns the number of entries successfully broadcast.
    pub fn retry_deferred_broadcast(&mut self) -> u64 {
        if !self.volatile_server_state.has_deferred_broadcast {
            return 0;
        }

        let commit_index = self.volatile_server_state.commit_index;
        let start_index = self.volatile_server_state.last_broadcast_index
            .map(|i| i + 1)
            .unwrap_or(0);

        if start_index > commit_index {
            self.volatile_server_state.has_deferred_broadcast = false;
            return 0;
        }

        let mut count = 0u64;
        let end_index = std::cmp::min(commit_index, start_index + Self::MAX_BROADCAST_PER_TIMESTEP - 1);

        for i in start_index..=end_index {
            if let Some(entry) = self.volatile_server_state.replication_log.get(i as usize) {
                match self.log_entry_bus.try_broadcast(Arc::clone(entry)) {
                    Ok(_) => {
                        self.volatile_server_state.last_broadcast_index = Some(i);
                        count += 1;
                    }
                    Err(_) => {
                        // Bus still full, will retry next time_step
                        return count;
                    }
                }
            }
        }

        // Check if done
        if self.volatile_server_state.last_broadcast_index == Some(commit_index) {
            self.volatile_server_state.has_deferred_broadcast = false;
        }

        if count > 0 {
            self.volatile_server_state.commit_state.version.fetch_add(1, Ordering::Release);
        }
        count
    }
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

    fn time_step(&mut self) -> u64 {
        let (state_change, work_done) = self.state_delegate.time_step(&mut self.shared_state);
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
        work_done
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
                tracing::info!("starting append entries processing");
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
                    raft_message.callback.send(resp).unwrap_or_else(|_| log::warn!("Failed to send raft callback: receiver dropped"))
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
                        message: build_owned_write_batch_response(|_| {}),
                        err: Some("Received write batch as non-leader".to_string())
                    };
                    let msg = LocalRaftResponsePayload::WriteBatch(resp);
                    raft_message.callback.send(LocalRaftResponseMessage {payload: msg}).unwrap_or_else(|_| log::warn!("Failed to send raft callback: receiver dropped"))
                }

            }
        }
    }
}

pub trait RaftStateMachineExecutor {
    fn accept(&mut self, raft_message: LocalRaftMessage);

    /// Returns the number of work items processed in this time step
    fn time_step(&mut self) -> u64;
}

// Per-state implementation of the Raft protocol. Handles requests, returns responses through callbacks and notifies about state changes.
pub trait RaftProtocol {
    fn init(&self) {}

    fn get_node_type(&self) -> RaftNodeType;
    fn request_vote(&mut self, request_vote_request: LocalRequestVoteRequest, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange;
    fn append_entries(&mut self, append_entries_request: LocalAppendEntries, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) -> RaftMessageStateChange;

    fn write_batch(&mut self, write_batch_request: LocalRaftWriteBatchRequest, callback: oneshot::Sender<LocalRaftResponseMessage>, shared_state: &mut SharedState) {
        tracing::error!(message = "write batch not allowed in this node state");
    }

    /// Returns (state_change, work_done_count)
    fn time_step(&mut self, shared_state: &mut SharedState) -> (RaftMessageStateChange, u64);

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
            info!("Rejecting vote, node has already voted for candidate {} this term {}", voted_for, term);
            return false
        }

        if term <= shared_state.server_state.current_term {
            info!("Rejecting vote, proposed term {} is lower than current {}.", term, shared_state.server_state.current_term);
            return false
        }
        if last_log_index < shared_state.volatile_server_state.last_log_index {
            info!("Rejecting vote, proposer has fewer log entries than node {}.", term);
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

/// Log entry for replication - replaces the protobuf RemoteLogEntry
#[derive(Clone, Debug)]
pub struct LogEntry {
    pub index: u64,
    pub data: Vec<u8>,
    pub batch_index: u64,
    pub term: u64,
    pub prev_log_term: u64,
    pub prev_log_index: u64,
    pub message_id: u64,
}

pub struct LocalAppendEntries {
    pub term: u64,
    pub leader_id: u32,
    pub prev_index: u64,
    pub prev_term: u64,
    pub request_id: u64,
    pub entry: Option<LogEntry>,
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

/// Response from a write batch operation, using Cap'n Proto for zero-copy.
pub struct LocalRaftWriteBatchResponse {
    pub message: OwnedWriteBatchResponse,
    pub err: Option<String>
}

impl std::fmt::Debug for LocalRaftWriteBatchResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalRaftWriteBatchResponse {{ message: {:?}, err: {:?} }}", self.message, self.err)
    }
}

#[derive(Debug)]
pub struct LocalRaftResponseMessage {
    pub payload: LocalRaftResponsePayload,
}

/// Write batch request using Cap'n Proto for zero-copy internal handling.
pub struct LocalRaftWriteBatchRequest {
    pub message: OwnedWriteBatch,
}

impl std::fmt::Debug for LocalRaftWriteBatchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalRaftWriteBatchRequest {{ message: {:?} }}", self.message)
    }
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

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::raft::leader::RaftLeaderStateDelegate;
    use crate::raft::follower::RaftFollowerStateDelegate;
    use crate::quorum::types::{LocalQuorumResponse, LocalQuorumTaskResponseType, LocalQuorumWorkerTask, LocalQuorumWorkerTaskType};
    use crate::transport::capnp::build_owned_log_entry;
    use std::collections::HashMap;
    use std::sync::Arc;
    use crossbeam_channel::unbounded;
    use crossbeam_queue::SegQueue;

    fn create_shared_state() -> SharedState {
        let (persistence_tx, _persistence_rx) = unbounded();
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
                    version: AtomicU64::new(0),
                }),
                last_broadcast_index: None,
                has_deferred_broadcast: false,
            },
            next_message_id: 1,
            outstanding_messages: std::collections::HashMap::new(),
            quorum_size: 3,
            quorum_worker_tasks: vec![Arc::new(SegQueue::new()); 3],
            persistence_work: persistence_tx,
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
        // This test simulates log divergence scenario with absolute indexing:
        // 1. Leader has: indexes 0-3 (term 2), indexes 4-6 (term 3)
        // 2. Follower has: indexes 0-5 (all term 2) - diverged from leader at index 4
        // 3. Follower requests backfill at prev_term=2, prev_index=5 (its last entry)
        // 4. Leader checks index 5: term 3 != term 2 (mismatch!)
        // 5. Leader walks back to find last matching point (index 3, term 2)
        // 6. Leader sends truncate signal with prev_term=2, prev_index=3
        // 7. Follower truncates to index 3
        // 8. Follower requests backfill again at index 3, leader provides entries 4-6

        let mut leader_delegate = RaftLeaderStateDelegate::new();
        leader_delegate.initialized = true;
        let mut leader_state = create_shared_state();

        // Set up leader state with absolute indexes
        leader_state.server_state.current_term = 3;
        leader_state.server_state.leader_id = 0;
        leader_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        leader_state.volatile_server_state.replication_log_term_starts.insert(3, 4);

        // Leader has 7 entries with absolute indexes 0-6
        // Entries 0-3: term 2, entries 4-6: term 3
        for i in 0u64..7 {
            let term = if i < 4 { 2 } else { 3 };
            let prev_term = if i == 0 { 0 } else if i <= 4 { 2 } else { 3 };
            let prev_index = if i == 0 { 0 } else { i - 1 };
            leader_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(i);
                builder.set_term(term);
                builder.set_prev_log_index(prev_index);
                builder.set_prev_log_term(prev_term);
                builder.set_message_id(i);
                builder.set_timestamp(0);
            })));
        }
        leader_state.volatile_server_state.last_log_index = 6;
        leader_state.volatile_server_state.last_log_term = 3;
        leader_state.volatile_server_state.next_log_index = 7;

        // Step 1: Follower requests backfill for prev_term=2, prev_index=5
        // (follower's last entry is at absolute index 5 with term 2)
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                last_log_term: 2,
                last_log_index: 5, // Follower claims term 2 at index 5, but leader has term 3 there
            }
        });

        // Step 2: Leader processes and detects term mismatch
        let (result, _work_done) = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Step 3: Verify leader sent truncate signal to last matching point
        let queue = &leader_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        let truncate_task = queue.pop().expect("Should have truncate task");
        let (truncate_prev_term, truncate_prev_index) = match truncate_task.task_type {
            LocalQuorumWorkerTaskType::TruncateLog { prev_term, prev_index, .. } => {
                assert_eq!(prev_term, 2);
                assert_eq!(prev_index, 3); // Last index with term 2 in leader's log
                (prev_term, prev_index)
            }
            _ => panic!("Expected TruncateLog task"),
        };

        // Now simulate the follower side
        let mut follower_delegate = RaftFollowerStateDelegate::new();
        let mut follower_state = create_shared_state();

        // Follower has 6 entries (indexes 0-5), all term 2
        follower_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        for i in 0u64..6 {
            let prev_index = if i == 0 { 0 } else { i - 1 };
            let prev_term = if i == 0 { 0 } else { 2 };
            follower_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(i);
                builder.set_term(2);
                builder.set_prev_log_index(prev_index);
                builder.set_prev_log_term(prev_term);
                builder.set_message_id(i);
                builder.set_timestamp(0);
            })));
        }
        follower_state.volatile_server_state.last_log_term = 2;
        follower_state.volatile_server_state.last_log_index = 5;
        follower_state.volatile_server_state.next_log_index = 6;

        // Step 4: Follower receives truncate signal
        follower_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::TruncateLog {
                quorum_node_id: 0,
                prev_term: truncate_prev_term,
                prev_index: truncate_prev_index,
            }
        });

        let (result, _work_done) = follower_delegate.time_step(&mut follower_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Step 5: Verify follower truncated correctly
        assert_eq!(follower_state.volatile_server_state.replication_log.len(), 4); // indexes 0,1,2,3
        assert_eq!(follower_state.volatile_server_state.last_log_term, 2);
        assert_eq!(follower_state.volatile_server_state.last_log_index, 3);
        assert_eq!(follower_state.volatile_server_state.next_log_index, 4);

        // Step 6: Follower requests backfill again at the truncation point
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                last_log_term: 2,
                last_log_index: 3, // Now matches leader's log
            }
        });

        let (result, _work_done) = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Step 7: Verify leader sent backfill data (not truncate)
        let queue = &leader_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        let backfill_task = queue.pop().expect("Should have backfill task");
        match backfill_task.task_type {
            LocalQuorumWorkerTaskType::BackfillLog { data } => {
                assert_eq!(data.len(), 3); // Entries 4, 5, 6
            }
            _ => panic!("Expected BackfillLog task"),
        }
    }

    #[tokio::test]
    async fn test_truncate_flow_with_new_leader_entries() {
        // Test scenario where follower and leader agree on an entry, and leader provides backfill
        // With absolute indexing:
        // - Leader has 10 entries (indexes 0-9)
        // - Entries 0-2: term 2, entries 3-5: term 3, entries 6-9: term 4
        // - Follower requests backfill at last_log_term=3, prev_index=4 (entry at index 4 has term 3)
        // - Leader verifies term matches, sends backfill from index 5 onwards
        let mut leader_delegate = RaftLeaderStateDelegate::new();
        leader_delegate.initialized = true;
        let mut leader_state = create_shared_state();

        // Leader is in term 4
        leader_state.server_state.current_term = 4;
        leader_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        leader_state.volatile_server_state.replication_log_term_starts.insert(3, 3);
        leader_state.volatile_server_state.replication_log_term_starts.insert(4, 6);

        // Leader has 10 entries with absolute indexes
        // Entries 0-2: term 2, entries 3-5: term 3, entries 6-9: term 4
        for i in 0u64..10 {
            let term = if i < 3 { 2 } else if i < 6 { 3 } else { 4 };
            let prev_term = if i == 0 { 0 } else if i <= 3 { 2 } else if i <= 6 { 3 } else { 4 };
            let prev_index = if i == 0 { 0 } else { i - 1 };
            leader_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(i);
                builder.set_term(term);
                builder.set_prev_log_index(prev_index);
                builder.set_prev_log_term(prev_term);
                builder.set_message_id(i);
                builder.set_timestamp(0);
            })));
        }
        leader_state.volatile_server_state.last_log_index = 9;
        leader_state.volatile_server_state.last_log_term = 4;
        leader_state.volatile_server_state.next_log_index = 10;

        // Follower requests backfill at last_log_term=3, prev_index=4
        // Entry at index 4 in leader's log has term 3, so this should match
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                last_log_term: 3,
                last_log_index: 4, // Absolute index 4 has term 3 in leader's log
            }
        });

        let (result, _work_done) = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should send backfill (not truncate) since term matches at index 4
        let queue = &leader_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        let task = queue.pop().expect("Should have task");
        match task.task_type {
            LocalQuorumWorkerTaskType::BackfillLog { data } => {
                // Should send entries 5, 6, 7, 8, 9 (5 entries)
                assert_eq!(data.len(), 5);
            }
            _ => panic!("Expected BackfillLog task"),
        }
    }

    #[tokio::test]
    async fn test_truncate_flow_boundary_conditions() {
        // Test edge cases: requesting exactly the last entry of a term
        // With absolute indexing:
        // - Leader has 7 entries (indexes 0-6)
        // - Entries 0-4: term 2, entries 5-6: term 3
        // - Request at prev_term=2, prev_index=4 → entry at index 4 has term 2 → match, backfill
        // - Request at prev_term=2, prev_index=5 → entry at index 5 has term 3 ≠ 2 → truncate
        let mut leader_delegate = RaftLeaderStateDelegate::new();
        leader_delegate.initialized = true;
        let mut leader_state = create_shared_state();

        leader_state.server_state.current_term = 3;
        leader_state.volatile_server_state.replication_log_term_starts.insert(2, 0);
        leader_state.volatile_server_state.replication_log_term_starts.insert(3, 5);

        // Leader has 7 entries with absolute indexes
        for i in 0u64..7 {
            let term = if i < 5 { 2 } else { 3 };
            let prev_term = if i == 0 { 0 } else if i <= 5 { 2 } else { 3 };
            let prev_index = if i == 0 { 0 } else { i - 1 };
            leader_state.volatile_server_state.replication_log.push(Arc::new(build_owned_log_entry(|mut builder| {
                builder.set_index(i);
                builder.set_term(term);
                builder.set_prev_log_index(prev_index);
                builder.set_prev_log_term(prev_term);
                builder.set_message_id(i);
                builder.set_timestamp(0);
            })));
        }
        leader_state.volatile_server_state.last_log_index = 6;
        leader_state.volatile_server_state.last_log_term = 3;
        leader_state.volatile_server_state.next_log_index = 7;

        // Request exactly the last entry of term 2 (absolute index 4)
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                last_log_term: 2,
                last_log_index: 4, // Entry at index 4 has term 2
            }
        });

        let (result, _work_done) = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should send backfill since term matches at index 4
        let queue = &leader_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        let task = queue.pop().expect("Should have task");
        match task.task_type {
            LocalQuorumWorkerTaskType::BackfillLog { data } => {
                // Should send entries 5, 6
                assert_eq!(data.len(), 2);
            }
            _ => panic!("Expected BackfillLog task"),
        }

        // Request index 5 with term 2 (but index 5 has term 3)
        leader_state.quorum_response.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::BackfillLog {
                quorum_node_id: 1,
                last_log_term: 2,
                last_log_index: 5, // Entry at index 5 has term 3, not term 2
            }
        });

        let (result, _work_done) = leader_delegate.time_step(&mut leader_state);
        assert!(matches!(result, RaftMessageStateChange::None));

        // Should send truncate since term mismatch at index 5
        let queue = &leader_state.quorum_worker_tasks[0];
        assert_eq!(queue.len(), 1);

        let task = queue.pop().expect("Should have task");
        match task.task_type {
            LocalQuorumWorkerTaskType::TruncateLog { prev_term, prev_index, .. } => {
                assert_eq!(prev_term, 2);
                assert_eq!(prev_index, 4); // Last index where term 2 exists
            }
            _ => panic!("Expected TruncateLog task"),
        }
    }
}




