//! Queue and channel creation for inter-component communication.

use std::sync::Arc;
use crossbeam_queue::SegQueue;

use crate::persistence::worker::{PersistenceResponseType, PersistenceTaskType};
use crate::query::worker::QueryRequest;
use crate::quorum::types::{LocalQuorumResponse, LocalQuorumWorkerTask};
use crate::raft::raft_sm::LocalRaftMessage;
use crate::transport::write_proxy::WriteBatch;

/// Container for all the queues used by the Raft state machine and workers.
pub struct AppQueues {
    /// Main task queue for the Raft state machine.
    pub task_queue: Arc<SegQueue<LocalRaftMessage>>,
    /// Write batch queue for the API to submit writes.
    pub write_work_queue: Arc<SegQueue<WriteBatch>>,
    /// Query queue for read requests.
    pub query_queue: Arc<SegQueue<QueryRequest>>,
    /// Persistence work queue for log/vote persistence tasks.
    pub persistence_work_queue: Arc<SegQueue<PersistenceTaskType>>,
    /// Persistence response queue for completion notifications.
    pub persistence_response_queue: Arc<SegQueue<PersistenceResponseType>>,
    /// Quorum work queue (used for general quorum tasks).
    pub quorum_work_queue: Arc<SegQueue<LocalQuorumWorkerTask>>,
    /// Quorum response queue for acknowledgments from cluster members.
    pub quorum_response_queue: Arc<SegQueue<LocalQuorumResponse>>,
}

impl AppQueues {
    /// Creates all application queues.
    pub fn new() -> Self {
        Self {
            task_queue: Arc::new(SegQueue::new()),
            write_work_queue: Arc::new(SegQueue::new()),
            query_queue: Arc::new(SegQueue::new()),
            persistence_work_queue: Arc::new(SegQueue::new()),
            persistence_response_queue: Arc::new(SegQueue::new()),
            quorum_work_queue: Arc::new(SegQueue::new()),
            quorum_response_queue: Arc::new(SegQueue::new()),
        }
    }
}

impl Default for AppQueues {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_queues_creation() {
        let queues = AppQueues::new();

        // All queues should be empty initially
        assert!(queues.task_queue.is_empty());
        assert!(queues.write_work_queue.is_empty());
        assert!(queues.query_queue.is_empty());
        assert!(queues.persistence_work_queue.is_empty());
        assert!(queues.persistence_response_queue.is_empty());
        assert!(queues.quorum_work_queue.is_empty());
        assert!(queues.quorum_response_queue.is_empty());
    }
}
