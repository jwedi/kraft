#![feature(future_join)]

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::future::{join, Future};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use futures::future::join_all;
use log::{error, info};
use tokio::io::join;
use datastoreproto::datastore_client::DatastoreClient;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tonic::Response;
use tonic::transport::Channel;

use crate::datastoreproto::{GetDataStoreRecordRequest, GetDataStoreRecordResponse, PutDataStoreRecordRequest};

pub mod ping {
    tonic::include_proto!("ping");
}

pub mod raftproto {
    tonic::include_proto!("raftproto");
}

pub mod datastoreproto {
    tonic::include_proto!("datastoreproto");
}

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
    pub endpoints: Vec<String>,
    /// Request timeout
    pub timeout: Duration,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            concurrency: 1000,
            max_requests: 500_000,
            checkpoint_interval_secs: 5,
            endpoints: vec![
                "http://[::1]:50051".to_string(),
                "http://[::1]:50052".to_string(),
                "http://[::1]:50053".to_string(),
            ],
            timeout: Duration::from_secs(5),
        }
    }
}

// ============================================================================
// Metrics Collection
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
    latencies: Mutex<Vec<i64>>, // latencies in microseconds
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

    /// Record a successful request latency (in microseconds)
    pub fn record_latency(&self, latency_micros: i64) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        let mut latencies = self.latencies.lock().unwrap();
        latencies.push(latency_micros);
    }

    /// Record an error
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get completed count (lock-free)
    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    /// Get current stats snapshot
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

fn calculate_percentile(sorted_data: &[i64], percentile: f64) -> i64 {
    if sorted_data.is_empty() {
        return 0;
    }
    let index = ((sorted_data.len() as f64) * percentile / 100.0) as usize;
    sorted_data[index.min(sorted_data.len() - 1)]
}

// ============================================================================
// Bounded Concurrency Runner
// ============================================================================

async fn run_load_test<F, Fut>(
    config: &LoadTestConfig,
    channels: Vec<Channel>,
    metrics: Arc<MetricsCollector>,
    test_name: &str,
    request_fn: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(DatastoreClient<Channel>, u64) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<i64, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
{
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let next_request_id = Arc::new(AtomicU64::new(0));
    let max_requests = config.max_requests;

    // Spawn checkpoint logger task
    let metrics_clone = Arc::clone(&metrics);
    let checkpoint_interval = config.checkpoint_interval_secs;
    let test_name_owned = test_name.to_string();
    let checkpoint_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(checkpoint_interval));
        loop {
            interval.tick().await;
            let snapshot = metrics_clone.snapshot();
            log::info!(
                "[{}] CHECKPOINT: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
                 p50={:.2}ms, p75={:.2}ms, p95={:.2}ms, p99={:.2}ms, min={:.2}ms, max={:.2}ms",
                test_name_owned,
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

    // Main request spawning loop
    let mut handles = JoinSet::new();

    loop {
        // Check if we've started enough requests
        let request_id = next_request_id.fetch_add(1, Ordering::Relaxed);
        if request_id >= max_requests {
            break;
        }

        // Acquire semaphore permit (blocks if at concurrency limit)
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let channel = channels[request_id as usize % channels.len()].clone();
        let client = DatastoreClient::new(channel);
        let metrics = Arc::clone(&metrics);
        let request_fn = request_fn.clone();
        let timeout = config.timeout;

        handles.spawn(async move {
            let _permit = permit; // Hold permit until request completes

            match tokio::time::timeout(timeout, request_fn(client, request_id)).await {
                Ok(Ok(latency_micros)) => {
                    metrics.record_latency(latency_micros);
                }
                Ok(Err(_e)) => {
                    //error!("Error {}", _e);
                    metrics.record_error();
                }
                Err(_) => {
                    //error!("Timeout error");
                    // Timeout
                    metrics.record_error();
                }
            }
        });
    }

    // Wait for all in-flight requests to complete
    while handles.join_next().await.is_some() {}

    // Stop checkpoint logger
    checkpoint_task.abort();

    Ok(())
}

// ============================================================================
// Load Test Functions
// ============================================================================

async fn do_write_batch_async(config: &LoadTestConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Create channels
    let mut channels = Vec::new();
    for endpoint in &config.endpoints {
        let channel = Channel::from_shared(endpoint.clone())?
            .connect()
            .await?;
        channels.push(channel);
    }

    let metrics = Arc::new(MetricsCollector::new());

    log::info!(
        "Starting WRITE load test: concurrency={}, max_requests={}",
        config.concurrency,
        config.max_requests
    );

    run_load_test(
        config,
        channels,
        Arc::clone(&metrics),
        "WRITE",
        |mut client, request_id| async move {
            let start = Instant::now();
            let mut request = tonic::Request::new(PutDataStoreRecordRequest {
                key: request_id.to_string(),
                value: format!("value-{}", request_id),
            });
            request.set_timeout(Duration::from_secs(5));

            client.put_record(request).await.map_err(|e| {
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
            Ok(start.elapsed().as_micros() as i64)
        },
    )
    .await?;

    // Final summary
    let snapshot = metrics.snapshot();
    log::info!(
        "FINAL WRITE SUMMARY: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
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

    Ok(())
}

async fn do_get_one_record_async(config: &LoadTestConfig) -> Result<Response<GetDataStoreRecordResponse>, Box<dyn Error + Send + Sync>> {
    let mut channels = Vec::new();
    for endpoint in &config.endpoints {
        let channel = Channel::from_shared(endpoint.clone()).unwrap()
        .connect()
        .await.unwrap();
        channels.push(channel);
    };

    let mut request = tonic::Request::new(GetDataStoreRecordRequest {
        id: "123".to_string(),
    });

    let mut client = DatastoreClient::new(channels.first().unwrap().clone());
    info!("Sending Get DataStoreRecordRequest");
    client.get_record(request).await.map_err(|e| {
        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
    })
}

async fn do_get_record_async(config: &LoadTestConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Create channels
    let mut channels = Vec::new();
    for endpoint in &config.endpoints {
        let channel = Channel::from_shared(endpoint.clone())?
            .connect()
            .await?;
        channels.push(channel);
    }

    let metrics = Arc::new(MetricsCollector::new());

    log::info!(
        "Starting READ load test: concurrency={}, max_requests={}",
        config.concurrency,
        config.max_requests
    );

    run_load_test(
        config,
        channels,
        Arc::clone(&metrics),
        "READ",
        |mut client, request_id| async move {
            let start = Instant::now();
            let mut request = tonic::Request::new(GetDataStoreRecordRequest {
                id: request_id.to_string(),
            });
            request.set_timeout(Duration::from_secs(5));

            client.get_record(request).await.map_err(|e| {
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
            Ok(start.elapsed().as_micros() as i64)
        },
    )
    .await?;

    // Final summary
    let snapshot = metrics.snapshot();
    log::info!(
        "FINAL READ SUMMARY: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
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

    Ok(())
}

/// Determines if a request should be a write based on the request_id and write_ratio.
/// Uses hashing to get a pseudo-random but deterministic distribution.
fn should_write(request_id: u64, write_ratio: f64) -> bool {
    let mut hasher = DefaultHasher::new();
    request_id.hash(&mut hasher);
    let hash = hasher.finish();
    (hash % 10000) as f64 / 10000.0 < write_ratio
}

async fn do_mixed_operations_async(
    config: &LoadTestConfig,
    write_ratio: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create channels
    let mut channels = Vec::new();
    for endpoint in &config.endpoints {
        let channel = Channel::from_shared(endpoint.clone())?
            .connect()
            .await?;
        channels.push(channel);
    }

    let metrics = Arc::new(MetricsCollector::new());

    let read_pct = (1.0 - write_ratio) * 100.0;
    let write_pct = write_ratio * 100.0;
    log::info!(
        "Starting MIXED load test: concurrency={}, max_requests={}, write_ratio={:.0}% writes / {:.0}% reads",
        config.concurrency,
        config.max_requests,
        write_pct,
        read_pct
    );

    run_load_test(
        config,
        channels,
        Arc::clone(&metrics),
        &format!("MIXED {:.0}w/{:.0}r", write_pct, read_pct),
        move |mut client, request_id| async move {
            let start = Instant::now();

            if should_write(request_id, write_ratio) {
                // Write operation
                let mut request = tonic::Request::new(PutDataStoreRecordRequest {
                    key: request_id.to_string(),
                    value: format!("value-{}", request_id),
                });
                request.set_timeout(Duration::from_secs(5));

                client.put_record(request).await.map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                })?;
            } else {
                // Read operation
                let mut request = tonic::Request::new(GetDataStoreRecordRequest {
                    id: request_id.to_string(),
                });
                request.set_timeout(Duration::from_secs(5));

                client.get_record(request).await.map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                })?;
            }

            Ok(start.elapsed().as_micros() as i64)
        },
    )
    .await?;

    // Final summary
    let snapshot = metrics.snapshot();
    log::info!(
        "FINAL MIXED SUMMARY: completed={}, errors={}, elapsed={:.2?}, throughput={:.0} rps, \
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

    Ok(())
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let config = LoadTestConfig {
        concurrency: 1500,
        max_requests: 1_500_000,
        checkpoint_interval_secs: 5,
        ..Default::default()
    };

    let mixed_handle = do_mixed_operations_async(&config, 0.2);
    //let mixed_handle = do_get_record_async(&config);
    //let mixed_handle = do_write_batch_async(&config);

    join!(mixed_handle).await;
    //do_get_one_record_async(&config).await;

    Ok(())
}
