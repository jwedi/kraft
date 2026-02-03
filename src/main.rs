use std::sync::Arc;

use futures::future::join_all;
use log::info;
use opentelemetry_sdk::trace::{Sampler, Tracer as SDKTracer};
use opentelemetry_sdk::{trace, Resource};
use tokio::join;
use tracing_subscriber::{Layer, layer::SubscriberExt, Registry};

use kraft_lib::config::config::{read_config, ClusterNode};
use kraft_lib::transport::capnp_stream_manager::CapnpStreamManager;
use kraft_lib::{startup, transport};

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
                .with_resource(Resource::new(vec![opentelemetry::KeyValue::new(
                    "node_id",
                    node_id.to_string(),
                )])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("Failed to initialize tracing pipeline");

    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer.clone())
        .with_filter(tracing_subscriber::filter::LevelFilter::INFO);

    tracing::subscriber::set_global_default(Registry::default().with(otel_layer))
        .expect("setting default subscriber failed");

    Some(tracer)
}

/// Creates the Cap'n Proto stream manager for cluster communication.
fn create_capnp_stream_manager(
    node_id: u32,
    cluster_nodes: &[ClusterNode],
    cluster_port: u32,
) -> Arc<CapnpStreamManager> {
    let capnp_addr: std::net::SocketAddr = format!("[::1]:{}", cluster_port)
        .parse()
        .expect("Invalid cluster_port");

    Arc::new(CapnpStreamManager::new(
        node_id,
        cluster_nodes.to_vec(),
        capnp_addr,
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Initialize metrics
    transport::metrics::register_metrics();
    transport::metrics::initialize_metrics();

    // Load configuration
    let cfg = read_config()?;
    let node_id = cfg.node_id;
    let enable_otel = cfg.enable_otel_tracing;

    // Initialize tracing
    let _tracer = init_tracing(node_id, enable_otel);

    // Create all queues
    let queues = startup::AppQueues::new();

    // Create and initialize persistence worker, recover data
    let mut persistence_worker = startup::create_persistence_worker(
        queues.persistence_work_receiver.clone(),
        Arc::clone(&queues.persistence_response_queue),
        cfg.persistence_dir.clone(),
    );
    let votes = persistence_worker.read_votes()?;
    let log_bytes = persistence_worker.read_log()?;

    // Recover persisted data (replication log and votes)
    let recovered = startup::recover_persisted_data(log_bytes, votes);

    // Create commit state
    let commit_state = startup::create_commit_state();

    // Filter out self from cluster nodes
    let cluster_nodes_without_self: Vec<ClusterNode> = cfg
        .cluster_nodes
        .iter()
        .filter(|n| n.node_id != node_id)
        .cloned()
        .collect();

    // Create Cap'n Proto stream manager for cluster communication
    let capnp_stream_manager = create_capnp_stream_manager(
        node_id,
        &cluster_nodes_without_self,
        cfg.cluster_port,
    );

    // Create quorum workers
    log::info!("Using Cap'n Proto transport for cluster communication");

    let (cluster_workers, quorum_send_channels) = startup::create_capnp_quorum_workers(
        &queues,
        &cluster_nodes_without_self,
        node_id,
        Arc::clone(&capnp_stream_manager),
        cfg.max_message_size_bytes,
    );

    // Calculate quorum size
    let quorum_size = (cfg.cluster_nodes.len() as u32).div_ceil(2);

    // Create shared state for the Raft state machine
    let (shared_state, query_bus_reader) = startup::create_shared_state(
        &queues,
        recovered,
        Arc::clone(&commit_state),
        quorum_send_channels.clone(),
        node_id,
        quorum_size,
        cfg.max_message_size_bytes,
    );

    // Spawn workers
    let write_proxy_handle = startup::spawn_write_proxy(
        Arc::clone(&queues.write_work_queue),
        Arc::clone(&queues.task_queue),
        cluster_nodes_without_self,
        node_id,
        cfg.max_batch_size,
        cfg.min_batch_interval_ms,
        quorum_send_channels,
    );

    let query_worker_handle = startup::spawn_query_worker(
        query_bus_reader,
        Arc::clone(&commit_state),
        Arc::clone(&queues.query_queue),
    );

    let worker_handle =
        startup::spawn_state_machine_worker(Arc::clone(&queues.task_queue), shared_state);

    let persistence_handle = startup::spawn_persistence_worker(persistence_worker);

    // Start Cap'n Proto listener
    let capnp_listener_handle = startup::spawn_capnp_listener(Arc::clone(&capnp_stream_manager));

    // Spawn cluster workers
    let cluster_worker_handles = startup::spawn_capnp_quorum_workers(cluster_workers);

    // Start metrics server
    let metrics_handle = startup::spawn_metrics_server(cfg.metrics_port);

    // Start TCP datastore server
    let tcp_datastore_handle = if cfg.tcp_datastore_port > 0 {
        Some(startup::spawn_tcp_datastore_server(
            cfg.tcp_datastore_port,
            Arc::clone(&queues.write_work_queue),
            Arc::clone(&queues.query_queue),
            Arc::clone(&commit_state),
        ))
    } else {
        None
    };

    info!("Kraft server started - node_id={}, cluster_port={}, tcp_datastore_port={}",
          node_id, cfg.cluster_port, cfg.tcp_datastore_port);

    // Wait for all workers to complete
    let _ = join!(
        worker_handle,
        persistence_handle,
        write_proxy_handle,
        metrics_handle,
        query_worker_handle,
        capnp_listener_handle
    );
    join_all(cluster_worker_handles).await;

    if let Some(handle) = tcp_datastore_handle {
        let _ = handle.await;
    }

    // Shutdown tracing
    if enable_otel {
        opentelemetry::global::shutdown_tracer_provider();
    }

    Ok(())
}
