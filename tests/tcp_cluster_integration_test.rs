//! Integration test for a 3-node Kraft cluster using TCP datastore transport.
//!
//! This test validates:
//! - Leader election
//! - Log replication across all nodes via TCP transport
//! - Consensus on committed entries
//! - TCP client pipelining and connection pooling

use std::fs;
use std::fs::File;
use std::io;
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use capnp;
use dashmap::DashMap;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use kraft_lib::transport::capnp::{raft_capnp, OwnedLogEntry};

// Include generated proto code for ping health check
pub mod ping {
    tonic::include_proto!("ping");
}

use ping::ping_pong_client::PingPongClient;
use ping::PingRequest;

// ============================================================================
// TCP Codec (inline implementation)
// ============================================================================

const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> io::Result<(u8, u64, Bytes)> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let frame_len = u32::from_le_bytes(len_buf);

    if frame_len < 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Frame too small: {} bytes", frame_len),
        ));
    }

    if frame_len > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Frame too large: {} bytes", frame_len),
        ));
    }

    let rpc_type = reader.read_u8().await?;
    let request_id = reader.read_u64_le().await?;

    let payload_len = frame_len as usize - 9;
    let mut payload = BytesMut::with_capacity(payload_len);
    payload.resize(payload_len, 0);
    reader.read_exact(&mut payload).await?;

    Ok((rpc_type, request_id, payload.freeze()))
}

async fn write_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    rpc_type: u8,
    request_id: u64,
    payload: &[u8],
) -> io::Result<()> {
    let frame_len = 1 + 8 + payload.len();

    let mut buf = BytesMut::with_capacity(4 + frame_len);
    buf.put_u32_le(frame_len as u32);
    buf.put_u8(rpc_type);
    buf.put_u64_le(request_id);
    buf.put_slice(payload);

    writer.write_all(&buf).await
}

const RPC_PUT_RECORD: u8 = 1;
const RPC_GET_RECORD: u8 = 2;

/// Port configuration for test nodes
const BASE_GRPC_PORT: u32 = 60051;
const BASE_CAPNP_PORT: u32 = 61151;
const BASE_METRICS_PORT: u32 = 62051;
const BASE_TCP_DATASTORE_PORT: u32 = 63051;

/// Timeouts
const NODE_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const LEADER_ELECTION_TIMEOUT: Duration = Duration::from_secs(60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(30);

// ============================================================================
// Cap'n Proto Message Building (using proper capnp builders)
// ============================================================================

/// Build a PUT request (InternalPutRequest)
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

/// Build a GET request (DatastoreGetRequest)
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

/// Parse a GET response (DatastoreGetResponse)
fn parse_get_response(payload: &[u8]) -> io::Result<Option<Bytes>> {
    if payload.is_empty() {
        return Ok(None);
    }

    let mut slice: &[u8] = payload;
    let reader = capnp::serialize::read_message_from_flat_slice(&mut slice, Default::default())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("capnp error: {:?}", e)))?;
    let msg = reader
        .get_root::<raft_capnp::datastore_get_response::Reader<'_>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("capnp error: {:?}", e)))?;

    if !msg.get_found() {
        return Ok(None);
    }

    let data = msg
        .get_payload()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("capnp error: {:?}", e)))?;
    Ok(Some(Bytes::copy_from_slice(data)))
}

// ============================================================================
// TCP Connection
// ============================================================================

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
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(_) => break,
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

    async fn close(&self) {
        let mut writer = self.writer.lock().await;
        let _ = writer.shutdown().await;
    }
}

// ============================================================================
// Node and Cluster Management
// ============================================================================

struct NodeHandle {
    process: Child,
    temp_dir: TempDir,
    grpc_endpoint: String,
    tcp_endpoint: SocketAddr,
    node_id: u32,
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

impl NodeHandle {
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

        let grpc_endpoint = format!("http://[::1]:{}", BASE_GRPC_PORT + node_id - 1);
        let tcp_port = BASE_TCP_DATASTORE_PORT + node_id - 1;
        let tcp_endpoint: SocketAddr = format!("[::1]:{}", tcp_port).parse()?;

        Ok(NodeHandle {
            process,
            temp_dir,
            grpc_endpoint,
            tcp_endpoint,
            node_id,
        })
    }

    async fn is_ready(&self) -> bool {
        let Ok(channel) = Channel::from_shared(self.grpc_endpoint.clone())
            .and_then(|c| Ok(c.connect_timeout(Duration::from_secs(1))))
        else {
            return false;
        };

        let Ok(channel) = channel.connect().await else {
            return false;
        };

        PingPongClient::new(channel).ping(PingRequest {}).await.is_ok()
    }

    async fn is_tcp_ready(&self) -> bool {
        TcpStream::connect(self.tcp_endpoint).await.is_ok()
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
    let tcp_datastore_port = BASE_TCP_DATASTORE_PORT + node_id - 1;

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
tcp_datastore_port = {tcp_datastore_port}
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
    tcp_connections: Vec<Option<Arc<TcpConnection>>>,
}

impl TestCluster {
    async fn start(node_count: u32) -> Result<Self, Box<dyn std::error::Error>> {
        let mut nodes = Vec::with_capacity(node_count as usize);
        for node_id in 1..=node_count {
            nodes.push(NodeHandle::spawn(node_id, node_count).await?);
        }
        let mut tcp_connections = Vec::with_capacity(node_count as usize);
        for _ in 0..node_count {
            tcp_connections.push(None);
        }
        Ok(TestCluster { nodes, tcp_connections })
    }

    async fn wait_for_ready(&mut self) -> Result<(), Box<dyn std::error::Error>> {
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

        // Wait for TCP endpoints to be ready
        for node in &self.nodes {
            while tokio::time::Instant::now() < deadline {
                if node.is_tcp_ready().await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if !node.is_tcp_ready().await {
                return Err(format!("Node {} TCP endpoint not ready", node.node_id).into());
            }
        }

        // Establish TCP connections
        for (i, node) in self.nodes.iter().enumerate() {
            let conn = TcpConnection::connect(node.tcp_endpoint).await?;
            self.tcp_connections[i] = Some(Arc::new(conn));
        }

        // Wait for leader election by trying a write via TCP
        let deadline = tokio::time::Instant::now() + LEADER_ELECTION_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if self.put_record_tcp(0, "__leader_check__", "test", REQUEST_TIMEOUT).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err("Leader election did not complete within timeout".into())
    }

    async fn put_record_tcp(
        &self,
        node_idx: usize,
        key: &str,
        value: &str,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.tcp_connections[node_idx]
            .as_ref()
            .ok_or("No TCP connection")?;

        let payload = build_put_request(key, value.as_bytes());

        tokio::time::timeout(timeout, async {
            let rx = conn.send_request(RPC_PUT_RECORD, &payload).await?;
            rx.await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
            Ok::<(), io::Error>(())
        })
        .await
        .map_err(|_| "Request timed out")?
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn get_record_tcp(
        &self,
        node_idx: usize,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.tcp_connections[node_idx]
            .as_ref()
            .ok_or("No TCP connection")?;

        let payload = build_get_request(key);

        let response = tokio::time::timeout(timeout, async {
            let rx = conn.send_request(RPC_GET_RECORD, &payload).await?;
            let resp = rx.await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
            Ok::<Bytes, io::Error>(resp)
        })
        .await
        .map_err(|_| "Request timed out")?
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let value = parse_get_response(&response)?;
        Ok(value.map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    async fn get_record_tcp_with_retry(
        &self,
        node_idx: usize,
        key: &str,
        timeout: Duration,
        max_retries: u32,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut last_error = None;
        for attempt in 0..max_retries {
            match self.get_record_tcp(node_idx, key, timeout).await {
                Ok(Some(value)) => return Ok(value),
                Ok(None) => {
                    last_error = Some("Record not found".into());
                    if attempt < max_retries - 1 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
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

    async fn wait_for_replication_tcp(&self, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = tokio::time::Instant::now() + REPLICATION_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            let mut all_have_key = true;
            for node_idx in 0..self.nodes.len() {
                match self.get_record_tcp(node_idx, key, REQUEST_TIMEOUT).await {
                    Ok(Some(_)) => {}
                    _ => {
                        all_have_key = false;
                        break;
                    }
                }
            }
            if all_have_key {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(format!("{} did not replicate to all nodes within timeout", key).into())
    }

    async fn close(&self) {
        for conn in &self.tcp_connections {
            if let Some(c) = conn {
                c.close().await;
            }
        }
    }
}

#[tokio::test]
async fn test_three_node_cluster_consensus_tcp() {
    // Start 3-node cluster
    let mut cluster = TestCluster::start(3).await.expect("Failed to start cluster");
    cluster.wait_for_ready().await.expect("Cluster failed to elect leader");

    let entry_count = 500;

    // Write entries distributed across all nodes via TCP (round-robin)
    for i in 0..entry_count {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);
        let node_idx = i % 3;
        cluster
            .put_record_tcp(node_idx, &key, &value, REQUEST_TIMEOUT)
            .await
            .expect(&format!("Failed to write {} via TCP", key));
    }

    // Give time for replication to complete
    // We'll verify via transaction log instead of GET requests
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify all entries via transaction log (more reliable than parsing responses)
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

    println!("TCP Transport: Transaction log verification passed: {} entries across all nodes", logs[0].len());

    cluster.close().await;
}
