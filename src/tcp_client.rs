//! TCP datastore client with pipelining and connection pooling support.
//!
//! Features:
//! - Pipelined requests using request IDs
//! - Lock-free pending request tracking with DashMap
//! - Connection pooling with multiple connections per endpoint
//! - Metrics collection matching client.rs (percentiles, throughput)
//! - Load test functions with bounded concurrency

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use log::info;
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Semaphore};
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

mod transport {
    pub mod tcp_datastore {
        pub mod codec;
        #[allow(dead_code)]
        pub mod rpc {
            pub const RPC_PUT_RECORD: u8 = 1;
            pub const RPC_GET_RECORD: u8 = 2;
        }
    }
    pub mod capnp {
        pub mod raft_capnp {
            include!(concat!(env!("OUT_DIR"), "/src/transport/capnp/raft_capnp.rs"));
        }
    }
}

use transport::capnp::raft_capnp;
use transport::tcp_datastore::codec::{read_frame, write_frame};
use transport::tcp_datastore::rpc::{RPC_GET_RECORD, RPC_PUT_RECORD};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for load test runs
#[derive(Clone)]
pub struct LoadTestConfig {
    /// Maximum number of concurrent requests at any time
    pub concurrency: usize,
    /// Total number of requests to complete before stopping
    pub max_requests: u64,
    /// Interval between checkpoint logs (in seconds)
    pub checkpoint_interval_secs: u64,
    /// Server endpoints to distribute load across
    pub endpoints: Vec<SocketAddr>,
    /// Request timeout
    pub timeout: Duration,
    /// Number of TCP connections per endpoint
    pub connections_per_endpoint: usize,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            concurrency: 1000,
            max_requests: 500_000,
            checkpoint_interval_secs: 5,
            endpoints: vec![
                "[::1]:49011".parse().unwrap(),
                "[::1]:49012".parse().unwrap(),
                "[::1]:49013".parse().unwrap(),
            ],
            timeout: Duration::from_secs(5),
            connections_per_endpoint: 4,
        }
    }
}

// ============================================================================
// Metrics Collection (matching client.rs)
// ============================================================================

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub completed: u64,
    pub errors: u64,
    pub elapsed: Duration,
    pub p50_micros: i64,
    pub p75_micros: i64,
    pub p95_micros: i64,
    pub p99_micros: i64,
    pub min_micros: i64,
    pub max_micros: i64,
    pub avg_micros: f64,
    pub throughput_rps: f64,
}

/// Thread-safe metrics collector for latency tracking
pub struct MetricsCollector {
    latencies: Mutex<Vec<i64>>,
    completed: AtomicU64,
    errors: AtomicU64,
    start_time: Instant,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            latencies: Mutex::new(Vec::with_capacity(100_000)),
            completed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    pub fn record_latency(&self, latency_micros: i64) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        let mut latencies = self.latencies.lock().unwrap();
        latencies.push(latency_micros);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let completed = self.completed.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed();

        let mut latencies = self.latencies.lock().unwrap();
        latencies.sort_unstable();

        let (p50, p75, p95, p99, min, max, avg) = if latencies.is_empty() {
            (0, 0, 0, 0, 0, 0, 0.0)
        } else {
            let p50 = calculate_percentile(&latencies, 50.0);
            let p75 = calculate_percentile(&latencies, 75.0);
            let p95 = calculate_percentile(&latencies, 95.0);
            let p99 = calculate_percentile(&latencies, 99.0);
            let min = *latencies.first().unwrap_or(&0);
            let max = *latencies.last().unwrap_or(&0);
            let sum: i64 = latencies.iter().sum();
            let avg = sum as f64 / latencies.len() as f64;
            (p50, p75, p95, p99, min, max, avg)
        };

        let throughput_rps = if elapsed.as_secs_f64() > 0.0 {
            completed as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        MetricsSnapshot {
            completed,
            errors,
            elapsed,
            p50_micros: p50,
            p75_micros: p75,
            p95_micros: p95,
            p99_micros: p99,
            min_micros: min,
            max_micros: max,
            avg_micros: avg,
            throughput_rps,
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

fn calculate_percentile(sorted_data: &[i64], percentile: f64) -> i64 {
    if sorted_data.is_empty() {
        return 0;
    }
    let index = ((sorted_data.len() as f64) * percentile / 100.0) as usize;
    sorted_data[index.min(sorted_data.len() - 1)]
}

// ============================================================================
// TCP Connection with Pipelining
// ============================================================================

/// A single TCP connection with pipelining support.
struct TcpConnection {
    writer: Arc<TokioMutex<WriteHalf<TcpStream>>>,
    pending: Arc<DashMap<u64, oneshot::Sender<Bytes>>>,
    next_request_id: AtomicU64,
    _reader_handle: JoinHandle<()>,
}

impl TcpConnection {
    async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = tokio::io::split(stream);

        let pending: Arc<DashMap<u64, oneshot::Sender<Bytes>>> = Arc::new(DashMap::new());
        let pending_clone = Arc::clone(&pending);

        let reader_handle = tokio::spawn(async move {
            Self::reader_loop(reader, pending_clone).await;
        });

        Ok(Self {
            writer: Arc::new(TokioMutex::new(writer)),
            pending,
            next_request_id: AtomicU64::new(1),
            _reader_handle: reader_handle,
        })
    }

    async fn reader_loop(
        mut reader: ReadHalf<TcpStream>,
        pending: Arc<DashMap<u64, oneshot::Sender<Bytes>>>,
    ) {
        loop {
            match read_frame(&mut reader).await {
                Ok((_rpc_type, request_id, payload)) => {
                    if let Some((_, sender)) = pending.remove(&request_id) {
                        let _ = sender.send(payload);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(_) => {
                    break;
                }
            }
        }
    }

    async fn send_request(&self, rpc_type: u8, payload: &[u8]) -> io::Result<oneshot::Receiver<Bytes>> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();

        self.pending.insert(request_id, tx);

        let mut writer = self.writer.lock().await;
        if let Err(e) = write_frame(&mut *writer, rpc_type, request_id, payload).await {
            self.pending.remove(&request_id);
            return Err(e);
        }

        Ok(rx)
    }
}

// ============================================================================
// Connection Pool
// ============================================================================

/// A pool of TCP connections for a single endpoint.
struct EndpointPool {
    connections: Vec<Arc<TcpConnection>>,
    next_conn: AtomicU64,
}

impl EndpointPool {
    async fn new(addr: SocketAddr, pool_size: usize) -> io::Result<Self> {
        let mut connections = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let conn = TcpConnection::connect(addr).await?;
            connections.push(Arc::new(conn));
        }
        Ok(Self {
            connections,
            next_conn: AtomicU64::new(0),
        })
    }

    fn get_connection(&self) -> Arc<TcpConnection> {
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) as usize % self.connections.len();
        Arc::clone(&self.connections[idx])
    }
}

/// Connection pool across multiple endpoints.
pub struct TcpConnectionPool {
    pools: Vec<EndpointPool>,
    next_pool: AtomicU64,
}

impl TcpConnectionPool {
    pub async fn new(endpoints: &[SocketAddr], connections_per_endpoint: usize) -> io::Result<Self> {
        let mut pools = Vec::with_capacity(endpoints.len());
        for &addr in endpoints {
            let pool = EndpointPool::new(addr, connections_per_endpoint).await?;
            pools.push(pool);
        }
        Ok(Self {
            pools,
            next_pool: AtomicU64::new(0),
        })
    }

    pub fn get_connection(&self) -> Arc<TcpConnection> {
        let pool_idx = self.next_pool.fetch_add(1, Ordering::Relaxed) as usize % self.pools.len();
        self.pools[pool_idx].get_connection()
    }

    pub fn total_connections(&self) -> usize {
        self.pools.iter().map(|p| p.connections.len()).sum()
    }

    pub async fn close(&self) {
        for pool in &self.pools {
            for conn in &pool.connections {
                let mut writer = conn.writer.lock().await;
                let _ = writer.shutdown().await;
            }
        }
    }
}

// ============================================================================
// Request Helpers
// ============================================================================

fn build_put_request(key: &str, value: &[u8]) -> Vec<u8> {
    let mut builder = capnp::message::Builder::new_default();
    {
        let mut msg = builder.init_root::<raft_capnp::internal_put_request::Builder>();
        msg.set_id(key);
        msg.set_payload(value);
        msg.set_node_id(0);
    }
    let mut bytes = Vec::new();
    capnp::serialize::write_message(&mut bytes, &builder).expect("write should not fail");
    bytes
}

fn build_get_request(key: &str) -> Vec<u8> {
    let mut builder = capnp::message::Builder::new_default();
    {
        let mut msg = builder.init_root::<raft_capnp::datastore_get_request::Builder>();
        msg.set_id(key);
    }
    let mut bytes = Vec::new();
    capnp::serialize::write_message(&mut bytes, &builder).expect("write should not fail");
    bytes
}

// ============================================================================
// Load Test Runner
// ============================================================================

/// Determines if a request should be a write based on the request_id and write_ratio.
#[allow(dead_code)]
fn should_write(request_id: u64, write_ratio: f64) -> bool {
    let mut hasher = DefaultHasher::new();
    request_id.hash(&mut hasher);
    let hash = hasher.finish();
    (hash % 10000) as f64 / 10000.0 < write_ratio
}

async fn do_write_batch_async(config: &LoadTestConfig) -> io::Result<()> {
    let pool = Arc::new(TcpConnectionPool::new(&config.endpoints, config.connections_per_endpoint).await?);
    let metrics = Arc::new(MetricsCollector::new());
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let next_request_id = Arc::new(AtomicU64::new(0));
    let max_requests = config.max_requests;

    log::info!(
        "Starting TCP WRITE load test: concurrency={}, max_requests={}, endpoints={}, connections={}",
        config.concurrency,
        config.max_requests,
        config.endpoints.len(),
        pool.total_connections()
    );

    // Spawn checkpoint logger
    let metrics_clone = Arc::clone(&metrics);
    let checkpoint_interval = config.checkpoint_interval_secs;
    let checkpoint_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(checkpoint_interval));
        loop {
            interval.tick().await;
            let snapshot = metrics_clone.snapshot();
            log::info!(
                "[TCP WRITE] CHECKPOINT: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
                 p50={:.2}ms, p75={:.2}ms, p95={:.2}ms, p99={:.2}ms, min={:.2}ms, max={:.2}ms",
                snapshot.completed,
                snapshot.errors,
                snapshot.elapsed,
                snapshot.throughput_rps,
                snapshot.p50_micros as f64 / 1000.0,
                snapshot.p75_micros as f64 / 1000.0,
                snapshot.p95_micros as f64 / 1000.0,
                snapshot.p99_micros as f64 / 1000.0,
                snapshot.min_micros as f64 / 1000.0,
                snapshot.max_micros as f64 / 1000.0,
            );
            if metrics_clone.completed() >= max_requests {
                break;
            }
        }
    });

    let mut handles = JoinSet::new();

    loop {
        let request_id = next_request_id.fetch_add(1, Ordering::Relaxed);
        if request_id >= max_requests {
            break;
        }

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let conn = pool.get_connection();
        let metrics = Arc::clone(&metrics);
        let timeout = config.timeout;

        handles.spawn(async move {
            let _permit = permit;
            let start = Instant::now();

            let payload = build_put_request(&request_id.to_string(), format!("value-{}", request_id).as_bytes());

            let result = tokio::time::timeout(timeout, async {
                let rx = conn.send_request(RPC_PUT_RECORD, &payload).await?;
                rx.await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
                Ok::<_, io::Error>(())
            }).await;

            match result {
                Ok(Ok(())) => {
                    metrics.record_latency(start.elapsed().as_micros() as i64);
                }
                _ => {
                    metrics.record_error();
                }
            }
        });
    }

    while handles.join_next().await.is_some() {}
    checkpoint_task.abort();

    let snapshot = metrics.snapshot();
    log::info!(
        "FINAL TCP WRITE SUMMARY: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
         p50={:.2}ms, p75={:.2}ms, p95={:.2}ms, p99={:.2}ms, min={:.2}ms, max={:.2}ms",
        snapshot.completed,
        snapshot.errors,
        snapshot.elapsed,
        snapshot.throughput_rps,
        snapshot.p50_micros as f64 / 1000.0,
        snapshot.p75_micros as f64 / 1000.0,
        snapshot.p95_micros as f64 / 1000.0,
        snapshot.p99_micros as f64 / 1000.0,
        snapshot.min_micros as f64 / 1000.0,
        snapshot.max_micros as f64 / 1000.0,
    );

    pool.close().await;
    Ok(())
}

#[allow(dead_code)]
async fn do_get_record_async(config: &LoadTestConfig) -> io::Result<()> {
    let pool = Arc::new(TcpConnectionPool::new(&config.endpoints, config.connections_per_endpoint).await?);
    let metrics = Arc::new(MetricsCollector::new());
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let next_request_id = Arc::new(AtomicU64::new(0));
    let max_requests = config.max_requests;

    log::info!(
        "Starting TCP READ load test: concurrency={}, max_requests={}, endpoints={}, connections={}",
        config.concurrency,
        config.max_requests,
        config.endpoints.len(),
        pool.total_connections()
    );

    let metrics_clone = Arc::clone(&metrics);
    let checkpoint_interval = config.checkpoint_interval_secs;
    let checkpoint_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(checkpoint_interval));
        loop {
            interval.tick().await;
            let snapshot = metrics_clone.snapshot();
            log::info!(
                "[TCP READ] CHECKPOINT: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
                 p50={:.2}ms, p75={:.2}ms, p95={:.2}ms, p99={:.2}ms, min={:.2}ms, max={:.2}ms",
                snapshot.completed,
                snapshot.errors,
                snapshot.elapsed,
                snapshot.throughput_rps,
                snapshot.p50_micros as f64 / 1000.0,
                snapshot.p75_micros as f64 / 1000.0,
                snapshot.p95_micros as f64 / 1000.0,
                snapshot.p99_micros as f64 / 1000.0,
                snapshot.min_micros as f64 / 1000.0,
                snapshot.max_micros as f64 / 1000.0,
            );
            if metrics_clone.completed() >= max_requests {
                break;
            }
        }
    });

    let mut handles = JoinSet::new();

    loop {
        let request_id = next_request_id.fetch_add(1, Ordering::Relaxed);
        if request_id >= max_requests {
            break;
        }

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let conn = pool.get_connection();
        let metrics = Arc::clone(&metrics);
        let timeout = config.timeout;

        handles.spawn(async move {
            let _permit = permit;
            let start = Instant::now();

            let payload = build_get_request(&request_id.to_string());

            let result = tokio::time::timeout(timeout, async {
                let rx = conn.send_request(RPC_GET_RECORD, &payload).await?;
                rx.await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
                Ok::<_, io::Error>(())
            }).await;

            match result {
                Ok(Ok(())) => {
                    metrics.record_latency(start.elapsed().as_micros() as i64);
                }
                _ => {
                    metrics.record_error();
                }
            }
        });
    }

    while handles.join_next().await.is_some() {}
    checkpoint_task.abort();

    let snapshot = metrics.snapshot();
    log::info!(
        "FINAL TCP READ SUMMARY: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
         p50={:.2}ms, p75={:.2}ms, p95={:.2}ms, p99={:.2}ms, min={:.2}ms, max={:.2}ms",
        snapshot.completed,
        snapshot.errors,
        snapshot.elapsed,
        snapshot.throughput_rps,
        snapshot.p50_micros as f64 / 1000.0,
        snapshot.p75_micros as f64 / 1000.0,
        snapshot.p95_micros as f64 / 1000.0,
        snapshot.p99_micros as f64 / 1000.0,
        snapshot.min_micros as f64 / 1000.0,
        snapshot.max_micros as f64 / 1000.0,
    );

    pool.close().await;
    Ok(())
}

#[allow(dead_code)]
async fn do_mixed_operations_async(config: &LoadTestConfig, write_ratio: f64) -> io::Result<()> {
    let pool = Arc::new(TcpConnectionPool::new(&config.endpoints, config.connections_per_endpoint).await?);
    let metrics = Arc::new(MetricsCollector::new());
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let next_request_id = Arc::new(AtomicU64::new(0));
    let max_requests = config.max_requests;

    let read_pct = (1.0 - write_ratio) * 100.0;
    let write_pct = write_ratio * 100.0;
    log::info!(
        "Starting TCP MIXED load test: concurrency={}, max_requests={}, write_ratio={:.0}% writes / {:.0}% reads, endpoints={}, connections={}",
        config.concurrency,
        config.max_requests,
        write_pct,
        read_pct,
        config.endpoints.len(),
        pool.total_connections()
    );

    let metrics_clone = Arc::clone(&metrics);
    let checkpoint_interval = config.checkpoint_interval_secs;
    let test_name = format!("TCP MIXED {:.0}w/{:.0}r", write_pct, read_pct);
    let checkpoint_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(checkpoint_interval));
        loop {
            interval.tick().await;
            let snapshot = metrics_clone.snapshot();
            log::info!(
                "[{}] CHECKPOINT: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
                 p50={:.2}ms, p75={:.2}ms, p95={:.2}ms, p99={:.2}ms, min={:.2}ms, max={:.2}ms",
                test_name,
                snapshot.completed,
                snapshot.errors,
                snapshot.elapsed,
                snapshot.throughput_rps,
                snapshot.p50_micros as f64 / 1000.0,
                snapshot.p75_micros as f64 / 1000.0,
                snapshot.p95_micros as f64 / 1000.0,
                snapshot.p99_micros as f64 / 1000.0,
                snapshot.min_micros as f64 / 1000.0,
                snapshot.max_micros as f64 / 1000.0,
            );
            if metrics_clone.completed() >= max_requests {
                break;
            }
        }
    });

    let mut handles = JoinSet::new();

    loop {
        let request_id = next_request_id.fetch_add(1, Ordering::Relaxed);
        if request_id >= max_requests {
            break;
        }

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let conn = pool.get_connection();
        let metrics = Arc::clone(&metrics);
        let timeout = config.timeout;

        handles.spawn(async move {
            let _permit = permit;
            let start = Instant::now();

            let result = tokio::time::timeout(timeout, async {
                if should_write(request_id, write_ratio) {
                    let payload = build_put_request(&request_id.to_string(), format!("value-{}", request_id).as_bytes());
                    let rx = conn.send_request(RPC_PUT_RECORD, &payload).await?;
                    rx.await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
                } else {
                    let payload = build_get_request(&request_id.to_string());
                    let rx = conn.send_request(RPC_GET_RECORD, &payload).await?;
                    rx.await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
                }
                Ok::<_, io::Error>(())
            }).await;

            match result {
                Ok(Ok(())) => {
                    metrics.record_latency(start.elapsed().as_micros() as i64);
                }
                _ => {
                    metrics.record_error();
                }
            }
        });
    }

    while handles.join_next().await.is_some() {}
    checkpoint_task.abort();

    let snapshot = metrics.snapshot();
    log::info!(
        "FINAL TCP MIXED SUMMARY: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
         p50={:.2}ms, p75={:.2}ms, p95={:.2}ms, p99={:.2}ms, min={:.2}ms, max={:.2}ms",
        snapshot.completed,
        snapshot.errors,
        snapshot.elapsed,
        snapshot.throughput_rps,
        snapshot.p50_micros as f64 / 1000.0,
        snapshot.p75_micros as f64 / 1000.0,
        snapshot.p95_micros as f64 / 1000.0,
        snapshot.p99_micros as f64 / 1000.0,
        snapshot.min_micros as f64 / 1000.0,
        snapshot.max_micros as f64 / 1000.0,
    );

    pool.close().await;
    Ok(())
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let config = LoadTestConfig {
        concurrency: 1000,
        max_requests: 1_500_000,
        checkpoint_interval_secs: 5,
        connections_per_endpoint: 4,
        ..Default::default()
    };

    info!("TCP Datastore Client - Load Test");
    info!("Endpoints: {:?}", config.endpoints);

    // Run write load test
    //do_write_batch_async(&config).await?;

    // Uncomment to run other tests:
    do_get_record_async(&config).await?;
    // do_mixed_operations_async(&config, 0.2).await?;

    info!("Load test complete");
    Ok(())
}
