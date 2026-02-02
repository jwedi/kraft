use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{Level, Span};

use crate::persistence::worker::PersistenceTaskType;
use crate::quorum::types::{LocalQuorumWorkerTask, LocalQuorumWorkerTaskType};
use crate::raft::raft_sm::{
    LocalRaftResponseMessage, LocalRaftResponsePayload, LocalRaftWriteBatchRequest,
    LocalRaftWriteBatchResponse, OutstandingMessage, OutstandingMessageType, SharedState,
};
use crate::service_utils::app_time::now_millis;
use crate::service_utils::storage_utils::{serialize_data, SerializationData};
use crate::transport::capnp::{build_owned_log_entry, build_owned_write_batch_response};

/// Validates a write batch request.
/// Returns None if valid, or an error message if invalid.
pub fn validate_write_batch(
    write_batch_request: &LocalRaftWriteBatchRequest,
) -> Option<&'static str> {
    let requests = write_batch_request.message.to_protobuf_requests();
    if requests.is_empty() {
        return Some("Empty write batch request");
    }
    None
}

/// Creates SerializationData from a write batch request.
pub fn create_serialization_data(
    write_batch_request: &LocalRaftWriteBatchRequest,
    idx: u64,
    message_id: u64,
    shared_state: &SharedState,
) -> SerializationData {
    let requests = write_batch_request.message.to_protobuf_requests();
    SerializationData {
        index: idx,
        term: shared_state.server_state.current_term,
        timestamp: now_millis() as u64,
        prev_index: shared_state.volatile_server_state.last_log_index,
        prev_term: shared_state.volatile_server_state.last_log_term,
        message_id,
        requests,
    }
}

/// Serializes data and updates the replication log.
pub fn serialize_and_append_to_log(
    serialization_data: SerializationData,
    shared_state: &mut SharedState,
) -> (Arc<SerializationData>, Arc<Vec<u8>>) {
    let buffer = vec![0u8; shared_state.state_machine_config.max_message_size_bytes];
    let (limit, mut data) = serialize_data(&serialization_data, buffer, 0);
    tracing::info!("serialized data size: {}", limit);
    data.truncate(limit);

    if serialization_data.prev_term != serialization_data.term {
        // First entry of new term.
        shared_state.volatile_server_state.replication_log_term_starts.insert(
            shared_state.server_state.current_term,
            shared_state.volatile_server_state.replication_log.len() as u64,
        );
    }

    let arc_serialization_data = Arc::new(serialization_data);
    shared_state
        .volatile_server_state
        .replication_log
        .push(Arc::clone(&arc_serialization_data));

    let shared_data = Arc::new(data);
    (arc_serialization_data, shared_data)
}

/// Queues a persistence task for the write batch.
pub fn queue_persistence_task(message_id: u64, shared_data: Arc<Vec<u8>>, shared_state: &SharedState) {
    let persistence_task = PersistenceTaskType::AppendLog {
        id: message_id,
        data: Arc::clone(&shared_data),
        parent_span: Span::current(),
        request_id: message_id,
    };
    shared_state.persistence_work.push(persistence_task);
}

/// Builds an OwnedLogEntry for the write batch.
pub fn build_log_entry(
    idx: u64,
    message_id: u64,
    shared_data: &Arc<Vec<u8>>,
    arc_serialization_data: &Arc<SerializationData>,
    shared_state: &SharedState,
) -> Arc<crate::transport::capnp::OwnedLogEntry> {
    Arc::new(build_owned_log_entry(|mut builder| {
        builder.set_index(idx);
        builder.set_term(shared_state.server_state.current_term);
        builder.set_prev_log_index(shared_state.volatile_server_state.last_log_index);
        builder.set_prev_log_term(shared_state.volatile_server_state.last_log_term);
        builder.set_message_id(message_id);
        builder.set_timestamp(now_millis() as u64);
        builder.set_data(&Arc::clone(shared_data));
        let mut commands = builder.init_commands(arc_serialization_data.requests.len() as u32);
        for (i, req) in arc_serialization_data.requests.iter().enumerate() {
            let mut cmd = commands.reborrow().get(i as u32);
            cmd.set_id(&req.id);
            cmd.set_payload(req.payload.as_bytes());
            cmd.set_node_id(req.node_id);
        }
    }))
}

/// Dispatches append entries tasks to all quorum workers.
pub fn dispatch_to_quorum(
    message_id: u64,
    owned_entry: Arc<crate::transport::capnp::OwnedLogEntry>,
    shared_state: &SharedState,
) {
    shared_state
        .quorum_worker_tasks
        .iter()
        .for_each(|task_queue| {
            let quorum_span = Span::current();
            let task = LocalQuorumWorkerTaskType::AppendEntries {
                id: message_id,
                parent_span: quorum_span,
                entry: owned_entry.clone(),
                commit_index: shared_state.volatile_server_state.commit_index,
            };
            task_queue.push(LocalQuorumWorkerTask { task_type: task });
        });
}

/// Creates and registers an outstanding message for tracking the write batch.
pub fn register_outstanding_message(
    message_id: u64,
    idx: u64,
    batch_size: usize,
    callback: oneshot::Sender<LocalRaftResponseMessage>,
    shared_state: &mut SharedState,
) {
    let message_type = OutstandingMessageType::WriteBatch {
        persistence_done: false,
        quorum_acks: 0,
        callback,
        index: idx,
        start_time: std::time::Instant::now(),
        batch_size,
    };
    let outstanding_message = OutstandingMessage {
        id: message_id,
        outstanding_responses: 1 + shared_state.quorum_worker_tasks.len() as i32,
        message_type,
        span: Span::current(),
    };
    shared_state
        .outstanding_messages
        .insert(message_id, outstanding_message);
}

/// Updates volatile state after appending a log entry.
pub fn update_volatile_state(idx: u64, shared_state: &mut SharedState) {
    shared_state.volatile_server_state.last_log_term = shared_state.server_state.current_term;
    shared_state.volatile_server_state.next_log_index = idx + 1;
    shared_state.volatile_server_state.last_log_index = idx;
}

/// Sends an error response for an invalid write batch.
pub fn send_error_response(
    error_msg: &str,
    callback: oneshot::Sender<LocalRaftResponseMessage>,
) {
    log::warn!("{}", error_msg);
    let r = LocalRaftWriteBatchResponse {
        message: build_owned_write_batch_response(|_| {}),
        err: Some(error_msg.to_string()),
    };
    let payload = LocalRaftResponsePayload::WriteBatch(r);
    let response = LocalRaftResponseMessage { payload };
    let _ = callback.send(response);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::raft_sm::{
        CommitState, RaftServerState, RaftVolatileState, SharedState, StateMachineConfig,
    };
    use crate::transport::capnp::build_owned_write_batch;
    use bus::Bus;
    use crossbeam_queue::SegQueue;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;

    fn create_test_shared_state() -> SharedState {
        let quorum_tasks: Vec<Arc<SegQueue<crate::quorum::types::LocalQuorumWorkerTask>>> =
            vec![Arc::new(SegQueue::new()), Arc::new(SegQueue::new())];

        SharedState {
            server_state: RaftServerState {
                current_term: 2,
                leader_id: 1,
            },
            volatile_server_state: RaftVolatileState {
                next_term: 3,
                last_log_term: 1,
                last_log_index: 5,
                commit_index: 3,
                replication_log: vec![],
                replication_log_term_starts: HashMap::new(),
                last_applied: 0,
                next_log_index: 6,
                commit_state: Arc::new(CommitState {
                    commit_index: AtomicU64::new(3),
                    version: AtomicU64::new(0),
                }),
                last_broadcast_index: None,
            },
            next_message_id: 100,
            outstanding_messages: HashMap::new(),
            quorum_size: 2,
            quorum_worker_tasks: quorum_tasks,
            persistence_work: Arc::new(SegQueue::new()),
            persistence_response: Arc::new(SegQueue::new()),
            quorum_work: Arc::new(SegQueue::new()),
            quorum_response: Arc::new(SegQueue::new()),
            identity: 1,
            term_votes: HashMap::new(),
            state_machine_config: StateMachineConfig {
                max_message_size_bytes: 2048,
            },
            log_entry_bus: Bus::new(5000),
        }
    }

    fn create_write_batch_request_with_data() -> LocalRaftWriteBatchRequest {
        let message = build_owned_write_batch(|mut builder| {
            builder.set_batch_id("test-batch-1");
            let mut requests = builder.init_requests(2);
            {
                let mut req = requests.reborrow().get(0);
                req.set_id("req1");
                req.set_payload(b"payload1");
                req.set_node_id(1);
            }
            {
                let mut req = requests.reborrow().get(1);
                req.set_id("req2");
                req.set_payload(b"payload2");
                req.set_node_id(1);
            }
        });
        LocalRaftWriteBatchRequest { message }
    }

    fn create_empty_write_batch_request() -> LocalRaftWriteBatchRequest {
        let message = build_owned_write_batch(|mut builder| {
            builder.set_batch_id("empty-batch");
            builder.init_requests(0);
        });
        LocalRaftWriteBatchRequest { message }
    }

    #[test]
    fn test_validate_write_batch_valid_request() {
        let request = create_write_batch_request_with_data();
        let result = validate_write_batch(&request);
        assert!(result.is_none());
    }

    #[test]
    fn test_validate_write_batch_empty_request() {
        let request = create_empty_write_batch_request();
        let result = validate_write_batch(&request);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), "Empty write batch request");
    }

    #[test]
    fn test_create_serialization_data_populates_fields_correctly() {
        let shared_state = create_test_shared_state();
        let request = create_write_batch_request_with_data();

        let data = create_serialization_data(&request, 10, 200, &shared_state);

        assert_eq!(data.index, 10);
        assert_eq!(data.term, 2); // current_term
        assert_eq!(data.prev_index, 5); // last_log_index
        assert_eq!(data.prev_term, 1); // last_log_term
        assert_eq!(data.message_id, 200);
        assert_eq!(data.requests.len(), 2);
        assert_eq!(data.requests[0].id, "req1");
        assert_eq!(data.requests[1].id, "req2");
    }

    #[test]
    fn test_serialize_and_append_to_log_adds_to_replication_log() {
        let mut shared_state = create_test_shared_state();

        let serialization_data = SerializationData {
            index: 6,
            term: 2,
            timestamp: 12345,
            prev_index: 5,
            prev_term: 1, // Different from term, so first entry of new term
            message_id: 100,
            requests: vec![],
        };

        let initial_log_len = shared_state.volatile_server_state.replication_log.len();

        let (arc_data, serialized) = serialize_and_append_to_log(serialization_data, &mut shared_state);

        // Log should have grown
        assert_eq!(
            shared_state.volatile_server_state.replication_log.len(),
            initial_log_len + 1
        );

        // Arc data should match
        assert_eq!(arc_data.index, 6);
        assert_eq!(arc_data.term, 2);

        // Serialized data should not be empty
        assert!(!serialized.is_empty());

        // Term start should be recorded since prev_term != term
        assert!(shared_state
            .volatile_server_state
            .replication_log_term_starts
            .contains_key(&2));
    }

    #[test]
    fn test_queue_persistence_task_adds_to_queue() {
        let shared_state = create_test_shared_state();
        let data = Arc::new(vec![1, 2, 3, 4, 5]);

        queue_persistence_task(42, data, &shared_state);

        // Should have one task in the persistence queue
        let task = shared_state.persistence_work.pop();
        assert!(task.is_some());

        match task.unwrap() {
            crate::persistence::worker::PersistenceTaskType::AppendLog { id, data, .. } => {
                assert_eq!(id, 42);
                assert_eq!(data.len(), 5);
            }
            _ => panic!("Expected AppendLog task"),
        }
    }

    #[test]
    fn test_dispatch_to_quorum_sends_to_all_workers() {
        let shared_state = create_test_shared_state();

        let owned_entry = Arc::new(build_owned_log_entry(|mut builder| {
            builder.set_index(10);
            builder.set_term(2);
            builder.set_prev_log_index(9);
            builder.set_prev_log_term(2);
            builder.set_message_id(100);
            builder.set_timestamp(12345);
            builder.set_data(&[1, 2, 3]);
            builder.init_commands(0);
        }));

        dispatch_to_quorum(100, owned_entry, &shared_state);

        // Both quorum workers should have received a task
        for queue in &shared_state.quorum_worker_tasks {
            let task = queue.pop();
            assert!(task.is_some());

            match task.unwrap().task_type {
                LocalQuorumWorkerTaskType::AppendEntries { id, commit_index, .. } => {
                    assert_eq!(id, 100);
                    assert_eq!(commit_index, 3);
                }
                _ => panic!("Expected AppendEntries task"),
            }
        }
    }

    #[test]
    fn test_register_outstanding_message_adds_to_map() {
        let mut shared_state = create_test_shared_state();
        let (tx, _rx) = oneshot::channel();

        register_outstanding_message(42, 10, 5, tx, &mut shared_state);

        assert!(shared_state.outstanding_messages.contains_key(&42));

        let msg = shared_state.outstanding_messages.get(&42).unwrap();
        assert_eq!(msg.id, 42);
        // outstanding_responses = 1 (persistence) + 2 (quorum workers) = 3
        assert_eq!(msg.outstanding_responses, 3);
    }

    #[test]
    fn test_update_volatile_state_updates_indices() {
        let mut shared_state = create_test_shared_state();

        update_volatile_state(10, &mut shared_state);

        assert_eq!(shared_state.volatile_server_state.last_log_index, 10);
        assert_eq!(shared_state.volatile_server_state.next_log_index, 11);
        assert_eq!(shared_state.volatile_server_state.last_log_term, 2); // current_term
    }

    #[tokio::test]
    async fn test_send_error_response_sends_error() {
        let (tx, rx) = oneshot::channel();

        send_error_response("Test error message", tx);

        let response = rx.await.unwrap();
        match response.payload {
            LocalRaftResponsePayload::WriteBatch(r) => {
                assert!(r.err.is_some());
                assert_eq!(r.err.unwrap(), "Test error message");
            }
            _ => panic!("Expected WriteBatch response"),
        }
    }
}
