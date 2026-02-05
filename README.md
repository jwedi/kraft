# Kraft

A high-performance, distributed, durable key-value store built on the Raft consensus algorithm and log replication.

This project is created for learning purposes and is not ready for production usage.

Kraft is designed for throughput over latency, using an actor-based architecture with lock-free queues, intelligent batching, and zero-copy serialization to achieve high write throughput while maintaining strong consistency guarantees.

All core logic and initial implementation is handwritten but refactorings, migrations and tests are AI assisted.

## Features

- **Zero-Copy I/O**: Arc-wrapped Cap'n Proto bytes flow through the system without copying
- **Strong Consistency**: Built on Raft consensus with leader election and log replication
- **Durability**: All writes are persisted to disk before acknowledgment
- **Read-Your-Own-Writes**: Clients can immediately read values they've written
- **High Throughput**: Smart batching of writes and lock-free inter-component communication
- **Observability**: Built-in distributed tracing (Zipkin) and Prometheus metrics

## Rough Benchmark
On a 3x Kraft node cluster (~1 core each) running on a 2021 Apple M1 Pro Kraft does:
- ~ 100.000 durable quorum writes per second.
- ~ 150.000 commited reads per second.

[TCP WRITE] CHECKPOINT: throughput=99343 rps, p50=9.56ms, p75=11.31ms, p95=14.31ms, p99=17.00ms

[TCP READ] CHECKPOINT: throughput=148410 rps, p50=4.51ms, p75=7.28ms, p95=13.68ms, p99=24.27ms

## Architecture Overview

Kraft uses an **actor-based architecture** where independent workers communicate via lock-free queues. This design eliminates contention and allows each component to operate at its own pace.

```
┌──────────┐                    ┌──────────────────────────────────────────┐
│  Client  │◄──── TCP ─────────►│                KRAFT NODE                │
│  (TCP)   │                    │                                          │
└──────────┘                    │  ┌───────────┐                           │
                                │  │    TCP    │                           │
                                │  │ Datastore ├───────────────┐           │
                                │  │  Server   │               │           │
                                │  └─────┬─────┘               │           │
                                │        │                     │           │
                                │        ▼                     ▼           │
                                │  ┌───────────┐        ┌─────────────┐    │
                                │  │   Write   ├───────►│    Query    │    │
                                │  │   Proxy   │        │    Worker   │    │
                                │  └─────┬─────┘        └──────▲──────┘    │
                                │        │                     │           │
                                │        ├─────────────────────┼─────┐     │
                                │        │                           │     │
                                │        ▼                           ▼     │       ┌ ─ ─ ─ ─ ─ ─ ┐
                                │  ┌────────────────────┐  ┌───┴────────┐  │       │  Other Node │
                                │  │ Raft State Machine │  │  Quorum   ─┼──┼──────►│ ┌─────────┐ │
                                │  │ Follower/Candidate/├─►│  Workers   │  │       │ │ Quorum  │ │
                                │  │       Leader       │  │ (per peer) │◄─┼───────│ │ Worker  │ │
                                │  └──────────┬─────────┘  └────────────┘  │       │ └─────────┘ │
                                │             │                            │        ─ ─ ─ ─ ─ ─ ─┘
                                │             ▼                            │
                                │      ┌─────────────┐                     │
                                │      │ Persistence │                     │
                                │      │   Worker    │                     │
                                │      └──────┬──────┘                     │
                                │             │                            │
                                └─────────────┼────────────────────────────┘
                                              ▼
                                        ┌───────────┐
                                        │   Disk    │
                                        │ log.capnp │
                                        │ votes.csv │
                                        └───────────┘
```

## Core Components

### Raft State Machine (`src/raft/`)

The heart of the system. Implements the Raft consensus protocol as a state machine that transitions between **Follower**, **Candidate**, and **Leader** states. Responds to client write requests, persistence confirmations, replication acknowledgments, and election timeouts.

### Write Proxy (`src/transport/write_proxy/`)

Smart batching for high throughput. Collects incoming write requests and forwards them when either `max_batch_size` bytes accumulate or `min_batch_interval_ms` elapses. Routes batches to the current Raft leader (locally or remotely).

### Persistence Worker (`src/persistence/worker.rs`)

Handles all disk I/O on a dedicated thread. Appends log entries to `log.capnp`, records vote decisions to `votes.csv`, batches multiple writes before `fsync()`, and recovers state on startup by replaying the log.

### Quorum Workers (`src/quorum/quorum_worker/`)

One worker per peer node. Sends heartbeats, replicates log entries over TCP using a custom capn proto protocol, handles vote requests during elections, manages backfill when followers fall behind, and processes truncation when logs diverge.

### Query Worker (`src/query/worker.rs`)

Handles client read requests by subscribing to committed log entries via a broadcast bus. Ensures read-your-own-writes by waiting for the relevant commit index.

### ClusterStreamManager (`src/transport/cluster_stream_manager.rs`)

Manages TCP connections for cluster communication. Provides per-peer outbound send channels and inbound receive queues.

## Zero-Copy Architecture

A key design goal is minimizing memory copies as data flows through the system:

```
┌──────────────┐     Arc<[u8]>      ┌──────────────┐     Arc<[u8]>      ┌──────────────┐
│   Network    │ ──────────────────►│   SegQueue   │ ──────────────────►│    Worker    │
│  (receive)   │   zero-copy        │  (lock-free) │   zero-copy        │  (process)   │
└──────────────┘                    └──────────────┘                    └──────────────┘
```

**Zero-copy message types** (`src/transport/capnp/internal_message.rs`):
- `OwnedWriteBatch` - Client write requests wrapped in Arc
- `OwnedLogEntry` - Replication log entries wrapped in Arc
- `OwnedQuorumMessage` - Cluster protocol messages wrapped in Arc

**Cap'n Proto benefits**:
- Zero-copy deserialization - no parsing step, read directly from wire format
- Messages can be stored and transmitted without re-serialization
- Arc wrapping allows sharing across queues without copying

## Performance Optimizations

| Optimization         | Description                                                         |
|----------------------|---------------------------------------------------------------------|
| **Zero-Copy I/O**    | Arc-wrapped serialized bytes flow without copying                   |
| **Smart Batching**   | Multiple writes combined into single log entries to ammortise costs |
| **Lock-Free queues** | Crossbeam SegQueue, zero contention                                 |
| **Non-blocking**     | Lock free and non-blocking in hot paths                             |
| **Custom transport** | Reduced overhead from a custom protocol built on raw TCP            |
| **Async + Blocking** | Tokio for I/O, dedicated threads for CPU work                       |

## Consistency Guarantees

- **Read-Your-Own-Writes**: A client will always see its own previous writes
- **Durability**: Acknowledged writes survive node failures (persisted to quorum before ack)
- **Linearizability**: (Coming soon) All operations appear to execute atomically

## Getting Started

### Prerequisites

- Rust 1.70+
- Cap'n Proto compiler (`capnp`)

### Configuration

Create `application.toml` in your resource directory:

```toml
node_id = 1
persistence_dir = "out/"
metrics_port = 51051
cluster_port = 51151        # Cluster communication (Cap'n Proto)
tcp_datastore_port = 49011  # Client API (TCP)
max_message_size_bytes = 4096
max_batch_size = 512
min_batch_interval_ms = 2
enable_otel_tracing = false

[[cluster_nodes]]
endpoint = "[::1]:51151"
node_id = 1

[[cluster_nodes]]
endpoint = "[::1]:51152"
node_id = 2

[[cluster_nodes]]
endpoint = "[::1]:51153"
node_id = 3
```

### Running a Cluster

```bash
# Terminal 1 - Node 1
RESOURCE_DIR=./node1 RUSTFLAGS="--emit=asm" cargo run --release --bin kraft

# Terminal 2 - Node 2
RESOURCE_DIR=./node2 RUSTFLAGS="--emit=asm" cargo run --release --bin kraft

# Terminal 3 - Node 3
RESOURCE_DIR=./node3 RUSTFLAGS="--emit=asm" cargo run --release --bin kraft
```

### TCP Client

```bash
RUSTFLAGS="--emit=asm" cargo run --release --bin tcp-client
```

### Running Tests

```bash
cargo test
```

## Observability

### Distributed Tracing

Kraft integrates with **Zipkin** for distributed tracing. Start Zipkin:

```bash
docker run -d -p 9411:9411 openzipkin/zipkin
```

Enable tracing in config: `enable_otel_tracing = true`

### Metrics

Prometheus metrics are exposed at `http://localhost:{metrics_port}/metrics`:

- `raft_commit_index` - Current commit index
- `raft_term` - Current Raft term
- `raft_state` - Current state (follower/candidate/leader)

## Project Structure

```
src/
├── main.rs                     # Application entry point
├── raft/
│   ├── raft_sm.rs              # Shared state and broadcast logic
│   ├── state_machine_worker.rs # Main event loop
│   ├── leader/                 # Leader state handling
│   ├── follower/               # Follower state handling
│   └── candidate.rs            # Candidate state handling
├── persistence/
│   └── worker.rs               # Disk I/O worker
├── quorum/
│   └── quorum_worker/           # Per-peer replication workers
├── query/
│   └── worker.rs               # Read request handler
├── transport/
│   ├── write_proxy/            # Write batching
│   ├── datastore/              # Client-facing TCP API
│   ├── capnp/                  # Cap'n Proto message types
│   │   ├── raft.capnp          # Protocol schema
│   │   ├── internal_message.rs # Zero-copy owned types
│   │   └── owned_message.rs    # Quorum message wrapper
│   └── cluster_stream_manager.rs # TCP connection management
├── config/
│   └── config.rs               # Configuration loading
└── startup/                    # Initialization and recovery
```

## TODO

- [ ] Log compaction / snapshotting
- [ ] Linearizable reads
- [ ] Membership changes
- [ ] Checksum validation
- [ ] Graceful shutdown
- [ ] Try a io_uring thread-per-core runtime like monoio instead of Tokio.

## License

MIT
