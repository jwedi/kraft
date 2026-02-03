//! Worker creation and spawning for the Kraft server.

use std::sync::Arc;
use bus::BusReader;
use crossbeam_channel::Receiver;
use crossbeam_queue::SegQueue;
use tokio::task::{JoinHandle, spawn_blocking};

use crate::config::config::ClusterNode;
use crate::persistence::worker::{PersistenceConfig, PersistenceTaskType, PersistenceWorker};
use crate::persistence::worker::PersistenceResponseType;
use crate::query::worker::QueryRequest;
use crate::quorum::capnp_worker::CapnpQuorumWorker;
use crate::quorum::types::LocalQuorumWorkerTask;
use crate::raft::raft_sm::{CommitState, LocalRaftMessage, SharedState};
use crate::transport::capnp::OwnedLogEntry;
use crate::startup::queues::AppQueues;
use crate::transport::capnp_stream_manager::CapnpStreamManager;
use crate::transport::tcp_datastore::TcpDatastoreServer;
use crate::transport::write_proxy::{WriteBatch, WriteProxy};

/// Creates and initializes the persistence worker.
pub fn create_persistence_worker(
    persistence_rx: Receiver<PersistenceTaskType>,
    response_queue: Arc<SegQueue<PersistenceResponseType>>,
    persistence_dir: String,
) -> PersistenceWorker {
    PersistenceWorker::new(
        persistence_rx,
        response_queue,
        PersistenceConfig { out_dir: persistence_dir },
    )
}

/// Creates Cap'n Proto quorum workers for each cluster node.
///
/// Returns a tuple of (workers, per-worker task queues).
pub fn create_capnp_quorum_workers(
    queues: &AppQueues,
    cluster_nodes: &[ClusterNode],
    node_id: u32,
    capnp_stream_manager: Arc<CapnpStreamManager>,
    max_message_size_bytes: usize,
) -> (Vec<CapnpQuorumWorker>, Vec<Arc<SegQueue<LocalQuorumWorkerTask>>>) {
    let mut workers = Vec::new();
    let mut quorum_send_channels = Vec::new();

    for node in cluster_nodes {
        let task_queue: Arc<SegQueue<LocalQuorumWorkerTask>> = Arc::new(SegQueue::new());
        quorum_send_channels.push(Arc::clone(&task_queue));

        let worker = CapnpQuorumWorker::new(
            task_queue,
            Arc::clone(&queues.quorum_response_queue),
            node_id,
            0,
            node.node_id.into(),
            node.capnp_endpoint.clone(),
            Arc::clone(&capnp_stream_manager),
            Arc::clone(&queues.task_queue),
            max_message_size_bytes,
        );
        workers.push(worker);
    }

    (workers, quorum_send_channels)
}

/// Spawns the write proxy worker.
pub fn spawn_write_proxy(
    write_work_queue: Arc<SegQueue<WriteBatch>>,
    task_queue: Arc<SegQueue<LocalRaftMessage>>,
    cluster_nodes: Vec<ClusterNode>,
    node_id: u32,
    max_batch_size: u64,
    min_batch_interval_ms: u64,
    quorum_send_channels: Vec<Arc<SegQueue<LocalQuorumWorkerTask>>>,
) -> JoinHandle<bool> {
    tokio::spawn(async move {
        let mut wp = WriteProxy::new(
            write_work_queue,
            max_batch_size,
            task_queue,
            node_id,
            cluster_nodes,
            min_batch_interval_ms,
            quorum_send_channels,
        );
        tracing::info!(message = "Spawning write proxy handler");
        wp.run().await;
        true
    })
}

/// Spawns the query worker.
pub fn spawn_query_worker(
    bus_reader: BusReader<Arc<OwnedLogEntry>>,
    commit_state: Arc<CommitState>,
    query_queue: Arc<SegQueue<QueryRequest>>,
) -> JoinHandle<bool> {
    spawn_blocking(move || {
        let mut worker = crate::query::worker::QueryWorker::new(
            bus_reader,
            commit_state,
            query_queue,
        );
        worker.run();
        true
    })
}

/// Spawns the Raft state machine worker.
pub fn spawn_state_machine_worker(
    task_queue: Arc<SegQueue<LocalRaftMessage>>,
    shared_state: SharedState,
) -> JoinHandle<bool> {
    spawn_blocking(move || {
        let mut worker = crate::raft::state_machine_worker::StateMachineWorker::new(
            task_queue,
            shared_state,
        );
        worker.run();
        true
    })
}

/// Spawns the persistence worker.
pub fn spawn_persistence_worker(mut worker: PersistenceWorker) -> JoinHandle<bool> {
    spawn_blocking(move || {
        tracing::info!(message = "Spawning persistence worker");
        worker.run();
        true
    })
}

/// Spawns the Cap'n Proto listener.
pub fn spawn_capnp_listener(
    stream_manager: Arc<CapnpStreamManager>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        log::info!("Starting Cap'n Proto listener");
        if let Err(e) = stream_manager.start_listener().await {
            log::error!("Cap'n Proto listener failed: {}", e);
        }
    })
}

/// Spawns all Cap'n Proto quorum workers.
pub fn spawn_capnp_quorum_workers(workers: Vec<CapnpQuorumWorker>) -> Vec<JoinHandle<bool>> {
    workers
        .into_iter()
        .map(|mut cw| {
            tokio::spawn(async move {
                tracing::info!(message = "Spawning Cap'n Proto quorum worker");
                cw.run().await;
                true
            })
        })
        .collect()
}

/// Spawns the metrics server.
pub fn spawn_metrics_server(metrics_port: u32) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = crate::transport::metrics_server::start_metrics_server(metrics_port as u16).await {
            log::error!("Failed to start metrics server: {}", e);
        }
    })
}

/// Spawns the TCP datastore server.
pub fn spawn_tcp_datastore_server(
    port: u32,
    task_queue: Arc<SegQueue<WriteBatch>>,
    query_queue: Arc<SegQueue<QueryRequest>>,
    commit_state: Arc<CommitState>,
) -> JoinHandle<()> {
    let addr: std::net::SocketAddr = format!("[::1]:{}", port)
        .parse()
        .expect("Invalid tcp_datastore_port");

    tokio::spawn(async move {
        log::info!("Starting TCP datastore server on port {}", port);
        let server = TcpDatastoreServer::new(addr, task_queue, query_queue, commit_state);
        if let Err(e) = server.run().await {
            log::error!("TCP datastore server failed: {}", e);
        }
    })
}
