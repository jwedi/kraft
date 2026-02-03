//! Integration test for a 3-node Kraft cluster.
//!
//! This test validates:
//! - Leader election
//! - Log replication across all nodes
//! - Consensus on committed entries
//! - Leader forwarding (non-leader nodes forward writes to leader)

use std::fs;
use std::fs::File;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;

use capnp;
use kraft_lib::transport::capnp::OwnedLogEntry;
use tonic::transport::Channel;

// Include generated proto code
pub mod ping {
    tonic::include_proto!("ping");
}
pub mod datastoreproto {
    tonic::include_proto!("datastoreproto");
}

use datastoreproto::datastore_client::DatastoreClient;
use datastoreproto::{GetDataStoreRecordRequest, PutDataStoreRecordRequest};
use ping::ping_pong_client::PingPongClient;
use ping::PingRequest;

/// Port configuration for test nodes (using high ports to avoid conflicts)
const BASE_GRPC_PORT: u32 = 60051;
const BASE_CAPNP_PORT: u32 = 61151;
const BASE_METRICS_PORT: u32 = 62051;

/// Timeouts
const NODE_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const LEADER_ELECTION_TIMEOUT: Duration = Duration::from_secs(60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Handle for a running Kraft node
struct NodeHandle {
    process: Child,
    temp_dir: TempDir,
    endpoint: String,
    node_id: u32,
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

impl NodeHandle {
    /// Spawn a new Kraft node with the given configuration
    async fn spawn(node_id: u32, cluster_size: u32) -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let config = generate_config(node_id, cluster_size, temp_dir.path().to_str().unwrap());

        std::fs::write(temp_dir.path().join("application.toml"), &config)?;
        std::fs::create_dir(temp_dir.path().join("data"))?;

        let stderr_file = File::create(temp_dir.path().join("stderr.log"))?;
        let binary = env!("CARGO_BIN_EXE_kraft");

        let process = Command::new(binary)
            .current_dir(temp_dir.path())
            .env("RESOURCE_DIR", temp_dir.path())
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_file))
            .spawn()?;

        let endpoint = format!("http://[::1]:{}", BASE_GRPC_PORT + node_id - 1);

        Ok(NodeHandle {
            process,
            temp_dir,
            endpoint,
            node_id,
        })
    }

    async fn is_ready(&self) -> bool {
        let Ok(channel) = Channel::from_shared(self.endpoint.clone())
            .and_then(|c| Ok(c.connect_timeout(Duration::from_secs(1))))
        else {
            return false;
        };

        let Ok(channel) = channel.connect().await else {
            return false;
        };

        PingPongClient::new(channel).ping(PingRequest {}).await.is_ok()
    }

    fn read_stderr(&self) -> String {
        std::fs::read_to_string(self.temp_dir.path().join("stderr.log"))
            .unwrap_or_else(|_| "<no stderr>".to_string())
    }

    fn log_file_path(&self) -> std::path::PathBuf {
        self.temp_dir.path().join("data").join("log.capnp")
    }
}

fn generate_config(node_id: u32, cluster_size: u32, temp_dir: &str) -> String {
    let grpc_port = BASE_GRPC_PORT + node_id - 1;
    let capnp_port = BASE_CAPNP_PORT + node_id - 1;
    let metrics_port = BASE_METRICS_PORT + node_id - 1;

    let mut config = format!(
        r#"port = {grpc_port}
node_id = {node_id}
persistence_dir = "{temp_dir}/data/"
max_message_size_bytes = 4096
max_batch_size = 512
min_batch_interval_ms = 2
metrics_port = {metrics_port}
enable_otel_tracing = false
use_capnp_transport = true
capnp_port = {capnp_port}
"#
    );

    for i in 1..=cluster_size {
        let node_grpc_port = BASE_GRPC_PORT + i - 1;
        let node_capnp_port = BASE_CAPNP_PORT + i - 1;
        config.push_str(&format!(
            r#"
[[cluster_nodes]]
endpoint = "http://[::1]:{node_grpc_port}"
capnp_endpoint = "[::1]:{node_capnp_port}"
node_id = {i}
"#
        ));
    }

    config
}

struct TestCluster {
    nodes: Vec<NodeHandle>,
}

impl TestCluster {
    async fn start(node_count: u32) -> Result<Self, Box<dyn std::error::Error>> {
        let mut nodes = Vec::with_capacity(node_count as usize);
        for node_id in 1..=node_count {
            nodes.push(NodeHandle::spawn(node_id, node_count).await?);
        }
        Ok(TestCluster { nodes })
    }

    async fn wait_for_ready(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Wait for all nodes to respond to ping
        let deadline = tokio::time::Instant::now() + NODE_STARTUP_TIMEOUT;
        for node in &self.nodes {
            while tokio::time::Instant::now() < deadline {
                if node.is_ready().await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if !node.is_ready().await {
                eprintln!("Node {} stderr:\n{}", node.node_id, node.read_stderr());
                return Err(format!("Node {} failed to start", node.node_id).into());
            }
        }

        // Wait for leader election
        let deadline = tokio::time::Instant::now() + LEADER_ELECTION_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if self.put_record(0, "__leader_check__", "test", REQUEST_TIMEOUT).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err("Leader election did not complete within timeout".into())
    }

    async fn put_record(
        &self,
        node_idx: usize,
        key: &str,
        value: &str,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = self.nodes[node_idx].endpoint.clone();
        tokio::time::timeout(timeout, async {
            let channel = Channel::from_shared(endpoint)?.connect().await?;
            DatastoreClient::new(channel)
                .put_record(PutDataStoreRecordRequest {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await
        .map_err(|_| "Request timed out")?
    }

    async fn get_record(
        &self,
        node_idx: usize,
        key: &str,
        timeout: Duration,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = self.nodes[node_idx].endpoint.clone();
        tokio::time::timeout(timeout, async {
            let channel = Channel::from_shared(endpoint)?.connect().await?;
            let response = DatastoreClient::new(channel)
                .get_record(GetDataStoreRecordRequest { id: key.to_string() })
                .await?;
            let record = response.into_inner().record.ok_or("Record not found")?;
            Ok::<String, Box<dyn std::error::Error + Send + Sync>>(record.payload)
        })
        .await
        .map_err(|_| "Request timed out")?
    }

    async fn get_record_with_retry(
        &self,
        node_idx: usize,
        key: &str,
        timeout: Duration,
        max_retries: u32,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut last_error = None;
        for attempt in 0..max_retries {
            match self.get_record(node_idx, key, timeout).await {
                Ok(value) => return Ok(value),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < max_retries - 1 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| "No attempts made".into()))
    }

    fn read_transaction_log(&self, node_idx: usize) -> Vec<OwnedLogEntry> {
        let log_path = self.nodes[node_idx].log_file_path();
        let log_bytes = fs::read(&log_path)
            .expect(&format!("Failed to read log file for node {}", node_idx + 1));

        if log_bytes.is_empty() {
            return Vec::new();
        }

        // Parse concatenated Cap'n Proto messages
        let mut entries = Vec::new();
        let mut offset = 0;
        while offset < log_bytes.len() {
            let mut slice = &log_bytes[offset..];
            match capnp::serialize::read_message_from_flat_slice(&mut slice, Default::default()) {
                Ok(_) => {
                    let bytes_consumed = log_bytes.len() - offset - slice.len();
                    let entry_bytes = log_bytes[offset..offset + bytes_consumed].to_vec();
                    match OwnedLogEntry::from_bytes(entry_bytes) {
                        Ok(entry) => entries.push(entry),
                        Err(e) => panic!("Failed to parse log entry at offset {}: {:?}", offset, e),
                    }
                    offset += bytes_consumed;
                }
                Err(e) => panic!("Failed to read capnp message at offset {}: {:?}", offset, e),
            }
        }
        entries
    }

    /// Wait for a key to replicate to all nodes
    async fn wait_for_replication(&self, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = tokio::time::Instant::now() + REPLICATION_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            let mut all_have_key = true;
            for node_idx in 0..self.nodes.len() {
                if self.get_record(node_idx, key, REQUEST_TIMEOUT).await.is_err() {
                    all_have_key = false;
                    break;
                }
            }
            if all_have_key {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(format!("{} did not replicate to all nodes within timeout", key).into())
    }
}

#[tokio::test]
async fn test_three_node_cluster_consensus() {
    // Start 3-node cluster
    let cluster = TestCluster::start(3).await.expect("Failed to start cluster");
    cluster.wait_for_ready().await.expect("Cluster failed to elect leader");

    let entry_count = 500;

    // Write entries distributed across all nodes (round-robin)
    for i in 0..entry_count {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);
        let node_idx = i % 3;
        cluster
            .put_record(node_idx, &key, &value, REQUEST_TIMEOUT)
            .await
            .expect(&format!("Failed to write {}", key));
    }

    // Wait for last key to replicate to all nodes
    // With the UpdateCommitIndex fix, followers should learn about commits
    // promptly without needing additional writes to flush the batch
    let last_key = format!("key-{}", entry_count - 1);
    cluster
        .wait_for_replication(&last_key)
        .await
        .expect("Replication timeout");

    // Verify all entries on all nodes
    for node_idx in 0..3 {
        for i in 0..entry_count {
            let key = format!("key-{}", i);
            let expected = format!("value-{}", i);
            let value = cluster
                .get_record_with_retry(node_idx, &key, REQUEST_TIMEOUT, 10)
                .await
                .expect(&format!("Failed to read {} from node {}", key, node_idx + 1));
            assert_eq!(
                value, expected,
                "Value mismatch for {} on node {}",
                key, node_idx + 1
            );
        }
    }

    // ===== Transaction Log Verification =====
    let logs: Vec<Vec<OwnedLogEntry>> = (0..3)
        .map(|i| cluster.read_transaction_log(i))
        .collect();

    // Verify all logs have the same length (identical entry count)
    assert!(logs.windows(2).all(|w| w[0].len() == w[1].len()), "Transaction logs have different lengths");

    // Verify all logs have the same indices and terms
    for (i, entry_pair) in logs.windows(2).enumerate() {
        for (j, (e1, e2)) in entry_pair[0].iter().zip(entry_pair[1].iter()).enumerate() {
            assert_eq!(e1.index(), e2.index(), "Index mismatch at position {} between nodes {} and {}", j, i, i+1);
            assert_eq!(e1.term(), e2.term(), "Term mismatch at position {} between nodes {} and {}", j, i, i+1);
        }
    }

    // Verify all written entries appear in the log with correct values
    let mut log_entries: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    for entry in &logs[0] {
        let _ = entry.for_each_command(|id, payload, _node_id| {
            log_entries.insert(id.to_string(), payload.to_vec());
        });
    }

    for i in 0..entry_count {
        let key = format!("key-{}", i);
        let expected = format!("value-{}", i);
        let actual = log_entries.get(&key).map(|v| String::from_utf8_lossy(v).to_string());
        assert_eq!(actual.as_deref(), Some(expected.as_str()), "Key {} mismatch in log", key);
    }

    println!("Transaction log verification passed: {} entries across all nodes", logs[0].len());
}
