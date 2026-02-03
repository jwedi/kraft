//! Server state initialization for Raft consensus.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bus::Bus;
use crossbeam_queue::SegQueue;

use crate::quorum::types::LocalQuorumWorkerTask;
use crate::raft::raft_sm::{
    CommitState, OutstandingMessage, RaftServerState, RaftVolatileState, SharedState,
    StateMachineConfig,
};
use crate::transport::capnp::OwnedLogEntry;
use crate::startup::queues::AppQueues;
use crate::startup::recovery::RecoveredData;

/// Creates the commit state that tracks the committed log index.
pub fn create_commit_state() -> Arc<CommitState> {
    Arc::new(CommitState {
        commit_index: AtomicU64::new(0),
        version: AtomicU64::new(0),
    })
}

/// Creates the initial Raft server state (persistent state).
pub fn create_server_state() -> RaftServerState {
    RaftServerState {
        current_term: 0,
        leader_id: 0,
    }
}

/// Creates the volatile server state from recovered data.
pub fn create_volatile_state(
    recovered: RecoveredData,
    commit_state: Arc<CommitState>,
) -> RaftVolatileState {
    RaftVolatileState {
        commit_index: 0,
        last_applied: 0,
        last_log_index: recovered.last_log_index,
        last_log_term: recovered.last_log_term,
        next_term: recovered.next_term,
        next_log_index: recovered.last_log_index + 1,
        replication_log: recovered.replication_log,
        replication_log_term_starts: recovered.term_start_index,
        commit_state,
        last_broadcast_index: None,
    }
}

/// Creates the shared state used by the Raft state machine.
pub fn create_shared_state(
    queues: &AppQueues,
    recovered: RecoveredData,
    commit_state: Arc<CommitState>,
    quorum_worker_tasks: Vec<Arc<SegQueue<LocalQuorumWorkerTask>>>,
    node_id: u32,
    quorum_size: u32,
    max_message_size_bytes: usize,
) -> (SharedState, bus::BusReader<Arc<OwnedLogEntry>>) {
    let server_state = create_server_state();
    let term_votes = recovered.term_votes.clone();
    let volatile_server_state = create_volatile_state(recovered, commit_state);

    let outstanding_messages: HashMap<u64, OutstandingMessage> = HashMap::new();

    let mut log_entry_bus: Bus<Arc<OwnedLogEntry>> = Bus::new(5000);
    let query_bus_reader = log_entry_bus.add_rx();

    let shared_state = SharedState {
        server_state,
        volatile_server_state,
        persistence_work: queues.persistence_work_sender.clone(),
        persistence_response: Arc::clone(&queues.persistence_response_queue),
        quorum_work: Arc::clone(&queues.quorum_work_queue),
        quorum_response: Arc::clone(&queues.quorum_response_queue),
        identity: node_id,
        term_votes,
        next_message_id: 0,
        outstanding_messages,
        quorum_size,
        quorum_worker_tasks,
        state_machine_config: StateMachineConfig { max_message_size_bytes },
        log_entry_bus,
    };

    (shared_state, query_bus_reader)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_queues() -> AppQueues {
        AppQueues::new()
    }

    fn create_test_recovered_data() -> RecoveredData {
        RecoveredData {
            replication_log: vec![],
            term_start_index: HashMap::new(),
            last_log_index: 0,
            last_log_term: 0,
            term_votes: HashMap::new(),
            next_term: 1,
        }
    }

    #[test]
    fn test_create_commit_state() {
        let commit_state = create_commit_state();

        assert_eq!(
            commit_state.commit_index.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            commit_state.version.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_create_server_state() {
        let state = create_server_state();

        assert_eq!(state.current_term, 0);
        assert_eq!(state.leader_id, 0);
    }

    #[test]
    fn test_create_volatile_state() {
        let recovered = RecoveredData {
            replication_log: vec![],
            term_start_index: HashMap::new(),
            last_log_index: 5,
            last_log_term: 2,
            term_votes: HashMap::new(),
            next_term: 3,
        };
        let commit_state = create_commit_state();

        let volatile = create_volatile_state(recovered, commit_state);

        assert_eq!(volatile.last_log_index, 5);
        assert_eq!(volatile.last_log_term, 2);
        assert_eq!(volatile.next_term, 3);
        assert_eq!(volatile.next_log_index, 6);
        assert_eq!(volatile.commit_index, 0);
        assert_eq!(volatile.last_applied, 0);
    }

    #[test]
    fn test_create_shared_state() {
        let queues = create_test_queues();
        let recovered = create_test_recovered_data();
        let commit_state = create_commit_state();
        let quorum_tasks = vec![Arc::new(SegQueue::new())];

        let (shared_state, _bus_reader) = create_shared_state(
            &queues,
            recovered,
            commit_state,
            quorum_tasks,
            1,
            2,
            1024,
        );

        assert_eq!(shared_state.identity, 1);
        assert_eq!(shared_state.quorum_size, 2);
        assert_eq!(shared_state.next_message_id, 0);
        assert_eq!(shared_state.state_machine_config.max_message_size_bytes, 1024);
        assert!(shared_state.outstanding_messages.is_empty());
    }
}
