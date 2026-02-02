use std::collections::HashMap;
use std::sync::{Arc, Condvar};
use std::sync::atomic::AtomicU64;
use bus::Bus;
use transport::ping::PingServer;
use transport::raft::RaftServerImpl;
use tonic::{Request, Response, Status, transport::Server};
use transport::ping::ping::ping_pong_server::PingPongServer;
use crate::transport::raft::raftproto::raft_server::RaftServer;
use tokio::sync::{mpsc, Mutex};
use log::{info, log, warn};
use tokio::sync::mpsc::{Receiver, Sender};
use crate::runtime_core::task_buffer;
use crate::runtime_core::task_buffer::TaskBufferImpl;
use crate::runtime_core::types::RuntimeTask;
use crossbeam_queue::SegQueue;
use futures::future::join_all;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{FutureExt, Tracer};
use opentelemetry_sdk::{Resource, trace};
use opentelemetry_sdk::trace::{Sampler, Tracer as SDKTracer};
use tokio::{join, spawn};
use tokio::task::{JoinHandle, spawn_blocking};
use tonic::codegen::InterceptedService;
use tracing::{callsite, field, Level, Metadata, Span};
use tracing::field::ValueSet;
use tracing::metadata::Kind;
use tracing_opentelemetry::{OpenTelemetryLayer, OpenTelemetrySpanExt};
use tracing_subscriber::{Layer, layer::SubscriberExt, registry, Registry};
use crate::config::config::{ClusterNode, read_config};
use crate::persistence::worker::{PersistenceConfig, PersistenceResponseType, PersistenceTaskType, PersistenceWorker, VoteRow};
use crate::query::worker::QueryRequest;
use crate::quorum::types::{LocalQuorumResponse, LocalQuorumWorkerTask};
use crate::quorum::capnp_worker::CapnpQuorumWorker;
use crate::transport::capnp_stream_manager::CapnpStreamManager;
use crate::raft::raft_sm::{CommitState, LocalRaftMessage, OutstandingMessage, RaftServerState, RaftVolatileState, SharedState, StateMachineConfig};
use crate::transport::raft::raftproto::RemoteQuorumMessage;
use crate::service_utils::app_time::now_millis;
use crate::service_utils::storage_utils;
use crate::service_utils::storage_utils::SerializationData;
use crate::transport::datastore::datastoreproto::datastore_server::DatastoreServer;
use crate::transport::datastore::DatastoreServerImpl;
use crate::transport::stream_manager::StreamManagerImpl;
use crate::transport::write_proxy::{WriteBatch, WriteProxy};

pub mod service_utils {
    pub mod errors;
    pub mod app_time;
    pub mod storage_utils;
    pub mod tracing_utils;
}

mod runtime_core {
    pub mod task_buffer;
    pub mod types;
}

mod raft {
    pub mod raft_sm;
    pub mod follower;
    pub mod candidate;
    pub mod leader;
    pub mod state_machine_worker;
}

mod persistence {
    pub mod worker;
}

mod quorum {
    pub mod capnp_worker;
    pub mod types;
}

mod query {
    pub mod worker;
}

mod config {
    pub mod config;
}

mod transport {
    pub mod datastore;
    pub mod write_proxy;
    pub mod stream_manager;
    pub mod raft;
    pub mod ping;
    pub mod metrics_server;
    pub mod metrics;
    pub mod capnp_stream_manager;
    pub mod capnp;
}

mod client {
    pub mod cluster_node_client;
}

/// Initialize OpenTelemetry tracing if enabled.
/// When disabled, tracing macros become no-ops to reduce performance overhead.
fn init_tracing(node_id: u32, enable_otel: bool) -> Option<SDKTracer> {
    if !enable_otel {
        log::info!("OpenTelemetry tracing is disabled");
        return None;
    }

    log::info!("Initializing OpenTelemetry tracing for node {}", node_id);
    let name = format!("kraft_node_{}", node_id);
    let tracer = opentelemetry_zipkin::new_pipeline()
        .with_service_name(name)
        .with_collector_endpoint("http://localhost:9411/api/v2/spans")
        .with_trace_config(
            trace::Config::default()
                .with_sampler(Sampler::TraceIdRatioBased(0.01f64))
                .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default())
                .with_max_attributes_per_span(8)
                .with_max_events_per_span(16)
                .with_resource(Resource::new(vec![opentelemetry::KeyValue::new("node_id", node_id.to_string())]))
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("Failed to initialize tracing pipeline");

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer.clone()).with_filter(tracing_subscriber::filter::LevelFilter::INFO);

    tracing::subscriber::set_global_default(Registry::default().with(otel_layer))
        .expect("setting default subscriber failed");

    Some(tracer)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder().filter_level(log::LevelFilter::Info).init();

    // Initialize metrics
    transport::metrics::register_metrics();
    transport::metrics::initialize_metrics();

    let cfg = read_config()?;
    let node_id = cfg.node_id;
    let enable_otel = cfg.enable_otel_tracing;
    let _tracer = init_tracing(cfg.node_id, enable_otel);
    let addr = format!("[::1]:{}", cfg.port).parse()?;
    let pinger = PingServer::default();

    let task_queue = Arc::new(SegQueue::<LocalRaftMessage>::new());
    let worker_queue = Arc::clone(&task_queue);
    let api_raft_queue = Arc::clone(&task_queue);
    let api_write_work_queue = Arc::new(SegQueue::<WriteBatch>::new());
    let proxy_write_work_queue = Arc::clone(&api_write_work_queue);
    let query_queue = Arc::new(SegQueue::<QueryRequest>::new());

    let commit_state = Arc::new(CommitState{
        commit_index: AtomicU64::new(0),
        version: AtomicU64::new(0)
    });

    let datastore_server = DatastoreServerImpl{
        task_queue: api_write_work_queue,
        query_queue: Arc::clone(&query_queue),
        commit_state: Arc::clone(&commit_state)
    };

    let persistence_work_queue = Arc::new(SegQueue::<PersistenceTaskType>::new());
    let sm_persistence_work = Arc::clone(&persistence_work_queue);
    let persistence_response_queue = Arc::new(SegQueue::<PersistenceResponseType>::new());
    let sm_persistence_response = Arc::clone(&persistence_response_queue);
    let persistence_condvar = Arc::new(Condvar::new());
    let mut persistence_worker = PersistenceWorker::new(
        persistence_work_queue,
        persistence_response_queue,
        persistence_condvar,
        PersistenceConfig{out_dir: cfg.persistence_dir}
    );
    let votes = persistence_worker.read_votes()?;

    log::info!("Recovered {} votes from disk, last one for {:?}", votes.len(), votes.last());

    let replication_log_bytes = persistence_worker.read_log()?;
    let start_deserialization = now_millis();
    let replication_log: Vec<Arc<SerializationData>> = if replication_log_bytes.is_empty() {
        log::warn!("Replication log is empty, starting with an empty log");
        vec![]
    } else {
        log::info!("Recovered replication log with {} bytes", replication_log_bytes.len());
        storage_utils::deserialize_all_data_as_arc(replication_log_bytes.as_slice()).unwrap_or_else(|_| {
            log::warn!("Failed to deserialize replication log, starting with empty log");
            vec![]
        })
    };
    let mut term_start_index: HashMap<u64, u64> = HashMap::new();

    for (i, entry) in replication_log.iter().enumerate() {
        if entry.index == 0 {
            term_start_index.insert(entry.term, i as u64);
        }
    }
    let last_entry = replication_log.last().clone();
    let last_log_index = last_entry.map_or(0, |e| e.index);
    let last_log_term = last_entry.map_or(0, |e| e.term);
    log::info!("Recovered replication log with {} entries, last index: {}, last term: {}. Deserialization took {} ms", replication_log.len(), last_log_index, last_log_term, now_millis()-start_deserialization);

    // TODO read these from append entries? last entry would be the current term, but votes might be higher...
    let default_vote = VoteRow{term: 0, candidate_id: 0};
    let mut maybe_max_vote = votes.iter().max_by(|x, y|  x.term.cmp(&y.term));
    let highest_term_vote = maybe_max_vote.get_or_insert(&default_vote);
    let server_state = RaftServerState {
        current_term: 0,
        leader_id: 0,
    };
    let next_term = std::cmp::max(highest_term_vote.term, last_log_term) + 1;
    let volatile_server_state = RaftVolatileState {
        commit_index: 0,
        last_applied: 0,
        last_log_index: last_log_index,
        last_log_term: last_log_term,
        next_term: next_term,
        next_log_index: last_log_index + 1, // TODO this is probably stupid.
        replication_log,
        replication_log_term_starts: term_start_index,
        commit_state: Arc::clone(&commit_state),
        last_broadcast_index: None,
    };


    let quorum_queue: Arc<SegQueue<LocalQuorumWorkerTask>> = Arc::new(SegQueue::<LocalQuorumWorkerTask>::new());
    let sm_quorum_work = Arc::clone(&quorum_queue);
    let quorum_response_queue = Arc::new(SegQueue::<LocalQuorumResponse>::new());
    let sm_quorum_response = Arc::clone(&quorum_response_queue);
    let quorum_size = (cfg.cluster_nodes.len() as u32).div_ceil(2);
    let cluster_nodes_without_self: Vec<ClusterNode> = cfg.cluster_nodes.into_iter().filter(|item| item.node_id != cfg.node_id).map(|c| c).collect();

    let term_votes: HashMap<u64, u32> = votes
        .into_iter()
        .map(|s| (s.term, s.candidate_id))
        .collect();

    let outstanding_messages: HashMap<u64, OutstandingMessage> = HashMap::new();

    // Store transport choice for later use
    let use_capnp_transport = cfg.use_capnp_transport;

    // Create stream managers and workers based on transport choice
    let mut quorum_send_channels: Vec<Arc<SegQueue<LocalQuorumWorkerTask>>> = vec![];

    // gRPC stream manager (needed for RaftServer even if not using gRPC for quorum)
    let send_streams: Arc<Mutex<HashMap<u32, Arc<Mutex<mpsc::UnboundedSender<RemoteQuorumMessage>>>>>> = Arc::new(Mutex::new(HashMap::new()));
    let receive_streams: Arc<Mutex<HashMap<u32, Arc<Mutex<tonic::Streaming<RemoteQuorumMessage>>>>>> = Arc::new(Mutex::new(HashMap::new()));
    let sm: StreamManagerImpl = StreamManagerImpl::new(
        node_id,
        Vec::clone(&cluster_nodes_without_self),
        send_streams,
        receive_streams
    );
    let sm_arc = Arc::new(sm);
    let raft_server = RaftServerImpl{task_queue: api_raft_queue, self_id: cfg.node_id, stream_manager: Arc::clone(&sm_arc)};

    // Create Cap'n Proto stream manager if enabled
    let capnp_stream_manager: Option<Arc<CapnpStreamManager>> = if use_capnp_transport {
        let capnp_listen_addr: std::net::SocketAddr = format!("[::1]:{}", cfg.capnp_port).parse()
            .expect("Invalid capnp_port");
        Some(Arc::new(CapnpStreamManager::new(
            node_id,
            Vec::clone(&cluster_nodes_without_self),
            capnp_listen_addr,
        )))
    } else {
        None
    };

    // Create Cap'n Proto workers for cluster communication
    log::info!("Using Cap'n Proto transport for cluster communication");
    let capnp_sm = capnp_stream_manager.as_ref().expect("Cap'n Proto stream manager is required");
    let cluster_workers: Vec<CapnpQuorumWorker> = cluster_nodes_without_self.iter().map(|node| {
        let c_queue: Arc<SegQueue<LocalQuorumWorkerTask>> = Arc::new(SegQueue::<LocalQuorumWorkerTask>::new());
        let c_queue_clone = Arc::clone(&c_queue);
        quorum_send_channels.push(c_queue);
        let response_c = Arc::clone(&sm_quorum_response);
        CapnpQuorumWorker::new(
            c_queue_clone,
            response_c,
            cfg.node_id,
            0,
            node.node_id.into(),
            node.capnp_endpoint.clone(),
            Arc::clone(capnp_sm),
            Arc::clone(&task_queue),
            cfg.max_message_size_bytes
        )
    }).collect();

    let mut log_entry_bus: Bus<Arc<SerializationData>> = Bus::new(5000);
    let mut query_bus_reader = log_entry_bus.add_rx();

    let shared_state = SharedState{
        server_state,
        volatile_server_state,
        persistence_work: sm_persistence_work,
        persistence_response: sm_persistence_response,
        quorum_work: sm_quorum_work,
        quorum_response: sm_quorum_response,
        identity: cfg.node_id.into(),
        term_votes,
        next_message_id: 0,
        outstanding_messages,
        quorum_size,
        quorum_worker_tasks: quorum_send_channels.clone(),
        state_machine_config: StateMachineConfig{ max_message_size_bytes: cfg.max_message_size_bytes},
        log_entry_bus: log_entry_bus
    };

    let write_proxy_handle = spawn(
        async move {
            let proxy_raft_queue = Arc::clone(&task_queue);
            let mut wp = WriteProxy::new(
                proxy_write_work_queue,
                cfg.max_batch_size,
                proxy_raft_queue,
                cfg.node_id,
                cluster_nodes_without_self,
                cfg.min_batch_interval_ms,
                quorum_send_channels
            );
            tracing::info!(message = "Spawning write proxy handler");
            wp.run().await;
            true
        }
    );

    let query_commit_state = Arc::clone(&commit_state);
    let query_pending_queue = Arc::clone(&query_queue);
    let query_worker_handle = spawn_blocking(move ||{
        let mut worker = query::worker::QueryWorker::new(query_bus_reader, query_commit_state, query_pending_queue);
        worker.run();
        true
    });

    let worker_handle = spawn_blocking(move ||{
        let mut worker = raft::state_machine_worker::StateMachineWorker::new(worker_queue, shared_state);
        worker.run();
        true
    });

    let persistence_handle = spawn_blocking(move ||{
        tracing::info!(message = "Spawning persistence worker");
        persistence_worker.run();
        true
    });

    // Start Cap'n Proto listener if enabled
    let capnp_listener_handle: Option<JoinHandle<()>> = if use_capnp_transport {
        let capnp_sm = capnp_stream_manager.clone().unwrap();
        Some(tokio::spawn(async move {
            log::info!("Starting Cap'n Proto listener");
            if let Err(e) = capnp_sm.start_listener().await {
                log::error!("Cap'n Proto listener failed: {}", e);
            }
        }))
    } else {
        None
    };

    // Spawn Cap'n Proto cluster workers
    let mut cw_join_handles: Vec<JoinHandle<bool>> = vec![];
    for mut cw in cluster_workers {
        let handle = spawn(async move {
            tracing::info!(message = "Spawning Cap'n Proto quorum worker");
            cw.run().await;
            true
        });
        cw_join_handles.push(handle);
    }

    info!("Starting server {}", addr);

    // Start metrics server
    let metrics_handle = tokio::spawn(async move {
        if let Err(e) = transport::metrics_server::start_metrics_server(cfg.metrics_port as u16).await {
            log::error!("Failed to start metrics server: {}", e);
        }
    });

    // Build server with or without tracing based on config
    // Tracing can add significant performance overhead
    // RaftServer is only registered when using gRPC transport (not Cap'n Proto)
    match (enable_otel, use_capnp_transport) {
        (true, true) => {
            // OpenTelemetry + Cap'n Proto: trace but no RaftServer
            let propagator = opentelemetry_zipkin::Propagator::new();
            Server::builder()
                .trace_fn(move |req| {
                    let n_id = node_id;
                    let method = req.uri().path().to_string();
                    let carrier = req.headers().iter()
                        .map(|pair| (pair.0.to_string(), pair.1.to_str().unwrap_or_default().to_string()))
                        .collect::<std::collections::HashMap<String, String>>();
                    let ctx = propagator.extract(&carrier);
                    let span = tracing::info_span!("grpc_request", method = %method, request_id = %uuid::Uuid::new_v4(), node_id = n_id);
                    span.set_parent(ctx);
                    span
                })
                .add_service(PingPongServer::new(pinger))
                .add_service(DatastoreServer::new(datastore_server))
                .serve(addr)
                .await?;
        }
        (true, false) => {
            // OpenTelemetry + gRPC: trace and include RaftServer
            let propagator = opentelemetry_zipkin::Propagator::new();
            Server::builder()
                .trace_fn(move |req| {
                    let n_id = node_id;
                    let method = req.uri().path().to_string();
                    let carrier = req.headers().iter()
                        .map(|pair| (pair.0.to_string(), pair.1.to_str().unwrap_or_default().to_string()))
                        .collect::<std::collections::HashMap<String, String>>();
                    let ctx = propagator.extract(&carrier);
                    let span = tracing::info_span!("grpc_request", method = %method, request_id = %uuid::Uuid::new_v4(), node_id = n_id);
                    span.set_parent(ctx);
                    span
                })
                .add_service(PingPongServer::new(pinger))
                .add_service(RaftServer::new(raft_server))
                .add_service(DatastoreServer::new(datastore_server))
                .serve(addr)
                .await?;
        }
        (false, true) => {
            // No tracing + Cap'n Proto: no RaftServer
            Server::builder()
                .add_service(PingPongServer::new(pinger))
                .add_service(DatastoreServer::new(datastore_server))
                .serve(addr)
                .await?;
        }
        (false, false) => {
            // No tracing + gRPC: include RaftServer
            Server::builder()
                .add_service(PingPongServer::new(pinger))
                .add_service(RaftServer::new(raft_server))
                .add_service(DatastoreServer::new(datastore_server))
                .serve(addr)
                .await?;
        }
    }


    let _ = join!(worker_handle, persistence_handle, write_proxy_handle, metrics_handle, query_worker_handle);
    join_all(cw_join_handles);
    if enable_otel {
        opentelemetry::global::shutdown_tracer_provider();
    }
    Ok(())
}
