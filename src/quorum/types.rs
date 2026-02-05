use std::sync::Arc;
use tracing::Span;
use crate::transport::capnp::OwnedLogEntry;
use crate::transport::write_proxy::WriteBatch;

/// Task wrapper for quorum worker tasks
pub struct LocalQuorumWorkerTask {
    pub task_type: LocalQuorumWorkerTaskType
}

/// Task types for quorum worker
pub enum LocalQuorumWorkerTaskType {
    /// Start sending heartbeats to this quorum member.
    StartHeartbeats { id: u64, term: u64, prev_term: u64, prev_log_index: u64, commit_index: u64 },
    /// Stop sending heartbeats to this quorum member.
    StopHeartbeats { id: u64 },
    /// Request a vote from this quorum member.
    RequestVote { id: u64, term: u64, last_term: u64, last_index: u64, parent_span: Span },
    /// Append a log entry to this quorum member.
    AppendEntries { id: u64, entry: Arc<OwnedLogEntry>, commit_index: u64, parent_span: Span },
    /// Backfill log entries to this quorum member.
    BackfillLog { data: Vec<Arc<OwnedLogEntry>> },
    /// Write a batch of requests.
    WriteBatch { write_batch: WriteBatch },
    /// Truncate log to specified term/index on the quorum member.
    TruncateLog { id: u64, prev_term: u64, prev_index: u64, parent_span: Span },
    /// Notify follower of new commit index via immediate heartbeat.
    UpdateCommitIndex { commit_index: u64 },
}

/// Response types from quorum worker
pub enum LocalQuorumTaskResponseType {
    RequestVoteResponse { id: u64, term: u64, received_vote: bool },
    AppendEntries { id: u64, ok: bool },
    BackfillLog { quorum_node_id: u64, last_log_term: u64, last_log_index: u64 },
    TruncateLog { quorum_node_id: u64, prev_term: u64, prev_index: u64 },
    TruncateLogResponse { id: u64, ok: bool }
}

/// Response wrapper for quorum worker responses
pub struct LocalQuorumResponse {
    pub response_type: LocalQuorumTaskResponseType
}
