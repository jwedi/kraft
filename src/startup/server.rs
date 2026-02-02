//! gRPC server building and configuration.

use std::collections::HashMap;
use std::sync::Arc;
use std::net::SocketAddr;

use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use opentelemetry::propagation::TextMapPropagator;

use crate::transport::datastore::datastoreproto::datastore_server::DatastoreServer;
use crate::transport::datastore::DatastoreServerImpl;
use crate::transport::ping::ping::ping_pong_server::PingPongServer;
use crate::transport::ping::PingServer;
use crate::transport::raft::raftproto::raft_server::RaftServer;
use crate::transport::raft::raftproto::RemoteQuorumMessage;
use crate::transport::raft::RaftServerImpl;
use crate::transport::stream_manager::StreamManagerImpl;
use crate::config::config::ClusterNode;
use crate::startup::queues::AppQueues;
use crate::raft::raft_sm::CommitState;

/// Creates the gRPC stream manager for Raft communication.
pub fn create_stream_manager(
    node_id: u32,
    cluster_nodes: Vec<ClusterNode>,
) -> Arc<StreamManagerImpl> {
    let send_streams: Arc<Mutex<HashMap<u32, Arc<Mutex<mpsc::UnboundedSender<RemoteQuorumMessage>>>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let receive_streams: Arc<Mutex<HashMap<u32, Arc<Mutex<tonic::Streaming<RemoteQuorumMessage>>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    Arc::new(StreamManagerImpl::new(
        node_id,
        cluster_nodes,
        send_streams,
        receive_streams,
    ))
}

/// Creates the Raft gRPC server implementation.
pub fn create_raft_server(
    queues: &AppQueues,
    node_id: u32,
    stream_manager: Arc<StreamManagerImpl>,
) -> RaftServerImpl {
    RaftServerImpl {
        task_queue: Arc::clone(&queues.task_queue),
        self_id: node_id,
        stream_manager,
    }
}

/// Creates the Datastore gRPC server implementation.
pub fn create_datastore_server(
    queues: &AppQueues,
    commit_state: Arc<CommitState>,
) -> DatastoreServerImpl {
    DatastoreServerImpl {
        task_queue: Arc::clone(&queues.write_work_queue),
        query_queue: Arc::clone(&queues.query_queue),
        commit_state,
    }
}

/// Starts the gRPC server with OpenTelemetry tracing and Cap'n Proto transport (no RaftServer).
pub async fn start_server_with_tracing_capnp(
    addr: SocketAddr,
    node_id: u32,
    pinger: PingServer,
    datastore_server: DatastoreServerImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    let propagator = opentelemetry_zipkin::Propagator::new();

    Server::builder()
        .trace_fn(move |req| {
            create_tracing_span(req, node_id, &propagator)
        })
        .add_service(PingPongServer::new(pinger))
        .add_service(DatastoreServer::new(datastore_server))
        .serve(addr)
        .await?;

    Ok(())
}

/// Starts the gRPC server with OpenTelemetry tracing and gRPC transport (includes RaftServer).
pub async fn start_server_with_tracing_grpc(
    addr: SocketAddr,
    node_id: u32,
    pinger: PingServer,
    raft_server: RaftServerImpl,
    datastore_server: DatastoreServerImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    let propagator = opentelemetry_zipkin::Propagator::new();

    Server::builder()
        .trace_fn(move |req| {
            create_tracing_span(req, node_id, &propagator)
        })
        .add_service(PingPongServer::new(pinger))
        .add_service(RaftServer::new(raft_server))
        .add_service(DatastoreServer::new(datastore_server))
        .serve(addr)
        .await?;

    Ok(())
}

/// Starts the gRPC server without tracing, with Cap'n Proto transport (no RaftServer).
pub async fn start_server_capnp(
    addr: SocketAddr,
    pinger: PingServer,
    datastore_server: DatastoreServerImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    Server::builder()
        .add_service(PingPongServer::new(pinger))
        .add_service(DatastoreServer::new(datastore_server))
        .serve(addr)
        .await?;

    Ok(())
}

/// Starts the gRPC server without tracing, with gRPC transport (includes RaftServer).
pub async fn start_server_grpc(
    addr: SocketAddr,
    pinger: PingServer,
    raft_server: RaftServerImpl,
    datastore_server: DatastoreServerImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    Server::builder()
        .add_service(PingPongServer::new(pinger))
        .add_service(RaftServer::new(raft_server))
        .add_service(DatastoreServer::new(datastore_server))
        .serve(addr)
        .await?;

    Ok(())
}

/// Creates a tracing span for a gRPC request with context propagation.
fn create_tracing_span(
    req: &tonic::codegen::http::Request<()>,
    node_id: u32,
    propagator: &opentelemetry_zipkin::Propagator,
) -> tracing::Span {
    let method = req.uri().path().to_string();
    let carrier: HashMap<String, String> = req
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
        .collect();

    let ctx = propagator.extract(&carrier);
    let span = tracing::info_span!(
        "grpc_request",
        method = %method,
        request_id = %uuid::Uuid::new_v4(),
        node_id = node_id
    );
    span.set_parent(ctx);
    span
}

/// Starts the appropriate gRPC server based on configuration.
pub async fn start_grpc_server(
    addr: SocketAddr,
    node_id: u32,
    enable_otel: bool,
    use_capnp_transport: bool,
    pinger: PingServer,
    raft_server: RaftServerImpl,
    datastore_server: DatastoreServerImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Starting gRPC server at {}", addr);

    match (enable_otel, use_capnp_transport) {
        (true, true) => {
            start_server_with_tracing_capnp(addr, node_id, pinger, datastore_server).await
        }
        (true, false) => {
            start_server_with_tracing_grpc(addr, node_id, pinger, raft_server, datastore_server).await
        }
        (false, true) => {
            start_server_capnp(addr, pinger, datastore_server).await
        }
        (false, false) => {
            start_server_grpc(addr, pinger, raft_server, datastore_server).await
        }
    }
}
