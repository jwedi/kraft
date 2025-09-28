use std::time::Instant;

use lazy_static::lazy_static;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};

lazy_static! {
    pub static ref METRICS_REGISTRY: Registry = Registry::new();

    // RPC handling time and count metrics with labels (in microseconds)
    pub static ref RPC_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "kraft_rpc_latency_microseconds",
            "RPC latency distribution in microseconds by endpoint"
        )
        .buckets(vec![250.0, 500.0, 1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 7500.0, 10000.0, 15000.0, 20000.0, 30000.0, 40000.0, 50000.0, 75000.0, 100000.0, 150000.0, 200000.0]),
        &["rpc_name"]
    ).expect("Failed to create RPC latency histogram");

    pub static ref RPC_REQUEST_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("kraft_rpc_requests_total", "Total number of RPC requests handled by endpoint"),
        &["rpc_name"]
    ).expect("Failed to create RPC total counter");

    // Write batch handling time metrics
    pub static ref WRITE_BATCH_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "kraft_write_batch_latency_microseconds",
            "Write batch latency distribution from start to commit in microseconds"
        )
        .buckets(vec![250.0, 500.0, 1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 7500.0, 10000.0, 15000.0, 20000.0, 30000.0, 40000.0, 50000.0, 75000.0, 100000.0, 150000.0, 200000.0])
    ).expect("Failed to create write batch latency histogram");

    pub static ref WRITE_BATCH_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("kraft_write_batches_total", "Total number of write batches processed")
    ).expect("Failed to create write batch total counter");

    // Batch size metrics
    pub static ref WRITE_BATCH_SIZE: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "kraft_write_batch_size",
            "Number of requests within a write batch"
        )
        .buckets(vec![1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0])
    ).expect("Failed to create write batch size histogram");

    // Raft state gauges
    pub static ref RAFT_COMMIT_INDEX: IntGauge = IntGauge::with_opts(
        Opts::new("kraft_raft_commit_index", "Current Raft commit index")
    ).expect("Failed to create commit index gauge");

    pub static ref RAFT_LEADER_ID: IntGauge = IntGauge::with_opts(
        Opts::new("kraft_raft_leader_id", "Current Raft leader ID")
    ).expect("Failed to create leader ID gauge");

    pub static ref RAFT_CURRENT_TERM: IntGauge = IntGauge::with_opts(
        Opts::new("kraft_raft_current_term", "Current Raft term")
    ).expect("Failed to create current term gauge");

    pub static ref RAFT_LOG_SIZE: IntGauge = IntGauge::with_opts(
        Opts::new("kraft_raft_log_size", "Current size of the Raft log")
    ).expect("Failed to create log size gauge");

    // Additional useful metrics
    pub static ref RAFT_APPEND_ENTRIES_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("kraft_raft_append_entries_total", "Total number of append entries operations")
    ).expect("Failed to create append entries counter");

    pub static ref RAFT_VOTE_REQUESTS_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("kraft_raft_vote_requests_total", "Total number of vote requests")
    ).expect("Failed to create vote requests counter");

    pub static ref RAFT_TRUNCATE_OPERATIONS_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("kraft_raft_truncate_operations_total", "Total number of log truncate operations")
    ).expect("Failed to create truncate operations counter");

    // Quorum member latency metrics with labels
    pub static ref QUORUM_APPEND_ENTRIES_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "kraft_quorum_append_entries_latency_microseconds",
            "Latency of append entries requests to quorum members in microseconds"
        )
        .buckets(vec![100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0, 500000.0, 1000000.0, 5000000.0]),
        &["member_id"]
    ).expect("Failed to create quorum append entries latency histogram");

    pub static ref QUORUM_PUT_BATCH_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "kraft_quorum_put_batch_latency_microseconds",
            "Latency of put batch requests to quorum members in microseconds"
        )
        .buckets(vec![100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0, 500000.0, 1000000.0, 5000000.0]),
        &["member_id"]
    ).expect("Failed to create quorum put batch latency histogram");
}

pub fn register_metrics() {
    METRICS_REGISTRY
        .register(Box::new(RPC_LATENCY.clone()))
        .expect("Failed to register RPC latency histogram");

    METRICS_REGISTRY
        .register(Box::new(RPC_REQUEST_TOTAL.clone()))
        .expect("Failed to register RPC total counter");

    METRICS_REGISTRY
        .register(Box::new(WRITE_BATCH_LATENCY.clone()))
        .expect("Failed to register write batch latency histogram");

    METRICS_REGISTRY
        .register(Box::new(WRITE_BATCH_TOTAL.clone()))
        .expect("Failed to register write batch total counter");

    METRICS_REGISTRY
        .register(Box::new(WRITE_BATCH_SIZE.clone()))
        .expect("Failed to register write batch size histogram");

    METRICS_REGISTRY
        .register(Box::new(RAFT_COMMIT_INDEX.clone()))
        .expect("Failed to register commit index gauge");

    METRICS_REGISTRY
        .register(Box::new(RAFT_LEADER_ID.clone()))
        .expect("Failed to register leader ID gauge");

    METRICS_REGISTRY
        .register(Box::new(RAFT_CURRENT_TERM.clone()))
        .expect("Failed to register current term gauge");

    METRICS_REGISTRY
        .register(Box::new(RAFT_LOG_SIZE.clone()))
        .expect("Failed to register log size gauge");

    METRICS_REGISTRY
        .register(Box::new(RAFT_APPEND_ENTRIES_TOTAL.clone()))
        .expect("Failed to register append entries counter");

    METRICS_REGISTRY
        .register(Box::new(RAFT_VOTE_REQUESTS_TOTAL.clone()))
        .expect("Failed to register vote requests counter");

    METRICS_REGISTRY
        .register(Box::new(RAFT_TRUNCATE_OPERATIONS_TOTAL.clone()))
        .expect("Failed to register truncate operations counter");

    METRICS_REGISTRY
        .register(Box::new(QUORUM_APPEND_ENTRIES_LATENCY.clone()))
        .expect("Failed to register quorum append entries latency histogram");

    METRICS_REGISTRY
        .register(Box::new(QUORUM_PUT_BATCH_LATENCY.clone()))
        .expect("Failed to register quorum put batch latency histogram");
}

/// Initialize gauge metrics to 0 so they appear in Prometheus immediately
/// Note: Histograms don't need initialization as they appear automatically
pub fn initialize_metrics() {
    // Initialize Raft state gauges to 0
    RAFT_COMMIT_INDEX.set(0);
    RAFT_LEADER_ID.set(0);
    RAFT_CURRENT_TERM.set(0);
    RAFT_LOG_SIZE.set(0);
}

/// Helper struct to measure execution time and automatically record it
pub struct RpcTimingGuard {
    start: Instant,
    histogram: Histogram,
}

impl RpcTimingGuard {
    pub fn new(histogram: Histogram) -> Self {
        Self {
            start: Instant::now(),
            histogram,
        }
    }

    pub fn observe_and_drop(self) {
        let duration = self.start.elapsed();
        self.histogram.observe(duration.as_micros() as f64);
    }
}

impl Drop for RpcTimingGuard {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        self.histogram.observe(duration.as_micros() as f64);
    }
}

/// Helper struct for write batch timing
pub struct WriteBatchTimingGuard {
    start: Instant,
}

impl WriteBatchTimingGuard {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn observe_and_drop(self) {
        let duration = self.start.elapsed();
        WRITE_BATCH_LATENCY.observe(duration.as_micros() as f64);
    }

    pub fn get_duration_micros(&self) -> u128 {
        self.start.elapsed().as_micros()
    }
}

/// Update Raft state metrics
pub fn update_raft_state_metrics(commit_index: u64, leader_id: u32, current_term: u64, log_size: usize) {
    RAFT_COMMIT_INDEX.set(commit_index as i64);
    RAFT_LEADER_ID.set(leader_id as i64);
    RAFT_CURRENT_TERM.set(current_term as i64);
    RAFT_LOG_SIZE.set(log_size as i64);
}

/// Record RPC request with endpoint label
pub fn record_rpc_request(rpc_name: &str) -> RpcTimingGuard {
    RPC_REQUEST_TOTAL.with_label_values(&[rpc_name]).inc();
    RpcTimingGuard::new(RPC_LATENCY.with_label_values(&[rpc_name]))
}

/// Record write batch
pub fn record_write_batch(batch_size: usize) -> WriteBatchTimingGuard {
    WRITE_BATCH_TOTAL.inc();
    WRITE_BATCH_SIZE.observe(batch_size as f64);
    WriteBatchTimingGuard::new()
}

/// Record write batch completion with duration
pub fn record_write_batch_completion(duration_micros: u128) {
    WRITE_BATCH_LATENCY.observe(duration_micros as f64);
}

/// Record append entries operation
pub fn record_append_entries() {
    RAFT_APPEND_ENTRIES_TOTAL.inc();
}

/// Record vote request
pub fn record_vote_request() {
    RAFT_VOTE_REQUESTS_TOTAL.inc();
}

/// Record truncate operation
pub fn record_truncate_operation() {
    RAFT_TRUNCATE_OPERATIONS_TOTAL.inc();
}