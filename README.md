# Kraft

A high-performance, distributed, durable key-value store built on the Raft consensus algorithm.
The project is created for learning purposes and it's not ready for prod usage.

Kraft is designed for throughput over latency, using an actor-based architecture with lock-free queues and intelligent batching to achieve high write throughput while maintaining strong consistency guarantees.

## Features

- **Strong Consistency**: Built on Raft consensus with leader election and log replication
- **Durability**: All writes are persisted to disk before acknowledgment
- **Read-Your-Own-Writes**: Clients can immediately read values they've written
- **High Throughput**: Smart batching of writes and lock-free inter-component communication
- **Observability**: Built-in distributed tracing (Zipkin) and Prometheus metrics

## Architecture Overview

Kraft uses an **actor-based architecture** where independent workers communicate via lock-free queues. This design eliminates contention and allows each component to operate at its own pace.

```
                                    ┌─────────────────────────────────────────────────────────────┐
                                    │                         KRAFT NODE                          │
                                    │                                                             │
    ┌──────────┐                    │  ┌─────────────┐      ┌─────────────────────────────────┐  │
    │  Client  │◄──────────────────►│  │   gRPC      │      │      Raft State Machine         │  │
    │  (gRPC)  │   Put/Get          │  │   Server    │      │  ┌───────┐ ┌─────────┐ ┌──────┐ │  │
    └──────────┘                    │  │             │      │  │Follower│ │Candidate│ │Leader│ │  │
                                    │  └──────┬──────┘      │  └───┬───┘ └────┬────┘ └──┬───┘ │  │
                                    │         │             │      └──────────┴─────────┘     │  │
                                    │         ▼             │              ▲                  │  │
                                    │  ┌─────────────┐      │              │                  │  │
                                    │  │   Write     │      │     Lock-Free Queue             │  │
                                    │  │   Proxy     ├──────┼──────────────┘                  │  │
                                    │  │  (Batching) │      │                                 │  │
                                    │  └─────────────┘      └─────────────────────────────────┘  │
                                    │                                 │                          │
                                    │         ┌───────────────────────┼───────────────────────┐  │
                                    │         │                       │                       │  │
                                    │         ▼                       ▼                       ▼  │
                                    │  ┌─────────────┐      ┌─────────────────┐      ┌─────────┐ │
                                    │  │ Persistence │      │  Quorum Workers │      │  Query  │ │
                                    │  │   Worker    │      │  (per peer)     │      │  Worker │ │
                                    │  └──────┬──────┘      └────────┬────────┘      └────┬────┘ │
                                    │         │                      │                    │      │
                                    └─────────┼──────────────────────┼────────────────────┼──────┘
                                              │                      │                    │
                                              ▼                      ▼                    ▼
                                    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐
                                    │   Disk (WAL)    │    │   Other Kraft   │    │    Read     │
                                    │   votes.csv     │    │     Nodes       │    │   Results   │
                                    │   log.sbe       │    │                 │    │             │
                                    └─────────────────┘    └─────────────────┘    └─────────────┘
```

## Core Components

### 1. Raft State Machine (`src/raft/`)

The heart of the system. Implements the Raft consensus protocol as a **state machine** that responds to:
- Incoming write requests from clients
- Responses from the persistence worker (disk writes completed)
- Responses from quorum workers (replication acknowledgments)
- Timeouts for leader election

The state machine transitions between three states:
- **Follower**: Receives log entries from the leader
- **Candidate**: Requests votes to become leader
- **Leader**: Accepts client writes and replicates to followers

```
┌──────────┐  timeout   ┌───────────┐  majority votes  ┌────────┐
│ Follower ├───────────►│ Candidate ├─────────────────►│ Leader │
└────┬─────┘            └─────┬─────┘                  └───┬────┘
     │                        │                            │
     │◄───────────────────────┴────────────────────────────┘
     │         discovers higher term / new leader
```

### 2. Write Proxy (`src/transport/write_proxy.rs`)

**Smart batching** for high throughput. Instead of processing writes one-by-one:

1. Collects incoming write requests into batches
2. Waits for either:
   - `max_batch_size` bytes accumulated, OR
   - `min_batch_interval_ms` elapsed
3. Gets the current Raft leader from the Raft State Machine.
4. Forwards the entire batch to the current Raft leader, either locally or remotely.

This dramatically improves throughput by amortizing the cost of disk fsync and network round-trips across many writes.

### 3. Persistence Worker (`src/persistence/worker.rs`)

Handles all disk I/O on a dedicated thread:
- Appends log entries to the write-ahead log (`log.sbe`)
- Records vote decisions (`votes.csv`)
- Batches multiple log writes before calling `fsync()` for efficiency
- Recovers state on startup by replaying the log

### 4. Quorum Workers (`src/quorum/worker.rs`)

One worker per peer node, responsible for:
- Sending heartbeats (leader only)
- Replicating log entries via AppendEntries RPCs
- Handling vote requests during elections
- Managing backfill when a follower falls behind
- Processing truncation when logs diverge

Uses **bidirectional gRPC streaming** for efficient communication between nodes and in-order delivery.

### 5. Query Worker (`src/query/worker.rs`)

Handles client read requests by subscribing to committed log entries via a broadcast bus. Ensures clients can read their own writes by waiting for the relevant commit index.

## Communication Model

### Lock-Free Queues

All inter-component communication uses **crossbeam's lock-free SegQueue**:

```
┌─────────────┐                     ┌─────────────┐
│   Writer    │  ───push()────►     │  SegQueue   │  ───pop()────►  │   Reader    │
│   (fast)    │  (non-blocking)     │  (lock-free)│  (non-blocking) │   (fast)    │
└─────────────┘                     └─────────────┘                 └─────────────┘
```

Benefits:
- No mutex contention between producers and consumers
- Writers never block waiting for readers
- Predictable latency without lock acquisition

### Callback Channels

For request-response patterns, Kraft uses **oneshot channels**:

```rust
// Client sends request with callback
let (tx, rx) = oneshot::channel();
queue.push(WriteBatch { callback: tx, ... });

// Worker processes and responds
callback.send(WriteResponse { ... });

// Client awaits response
let response = rx.await?;
```

## Performance Optimizations

| Optimization | Description |
|-------------|-------------|
| **Write Batching** | Multiple client writes combined into single log entries |
| **Persistence Batching** | Multiple log entries fsynced together |
| **Lock-Free Queues** | Zero contention between components |
| **gRPC Streaming** | Persistent connections between nodes, no connection overhead |
| **SBE Encoding** | Simple Binary Encoding for compact, zero-copy log serialization |
| **Async I/O** | Tokio runtime for efficient async networking |

## Consistency Guarantees

- **Linearizability**: (Coming soon) All operations appear to execute atomically at some point between invocation and response
- **Read-Your-Own-Writes**: A client will always see its own previous writes
- **Durability**: Acknowledged writes survive node failures (persisted to quorum before ack)

## Getting Started

### Prerequisites

- Rust 1.70+
- Protobuf compiler (`protoc`)

### Configuration

Create `application.toml`:

```toml
persistence_dir = "./data/"
port = 50051
metrics_port = 9090
node_id = 1
max_message_size_bytes = 4194304
min_batch_interval_ms = 5
max_batch_size = 1048576

[[cluster_nodes]]
endpoint = "http://[::1]:50051"
node_id = 1

[[cluster_nodes]]
endpoint = "http://[::1]:50052"
node_id = 2

[[cluster_nodes]]
endpoint = "http://[::1]:50053"
node_id = 3
```

### Running a Cluster

```bash
# Terminal 1 - Node 1
RESOURCE_DIR=./node1 cargo run --bin kraft

# Terminal 2 - Node 2
RESOURCE_DIR=./node2 cargo run --bin kraft

# Terminal 3 - Node 3
RESOURCE_DIR=./node3 cargo run --bin kraft
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

Traces show the full path of a write request across batching, persistence, and replication.

### Metrics

Prometheus metrics are exposed at `http://localhost:{metrics_port}/metrics`:

- `raft_commit_index` - Current commit index
- `raft_term` - Current Raft term
- `raft_state` - Current state (follower/candidate/leader)

## Project Structure

```
src/
├── main.rs                 # Application entry point
├── raft/
│   ├── raft_sm.rs          # Shared state and broadcast logic
│   ├── state_machine_worker.rs  # Main event loop
│   ├── leader.rs           # Leader state handling
│   ├── follower.rs         # Follower state handling
│   └── candidate.rs        # Candidate state handling
├── persistence/
│   └── worker.rs           # Disk I/O worker
├── quorum/
│   └── worker.rs           # Per-peer replication worker
├── query/
│   └── worker.rs           # Read request handler
├── transport/
│   ├── write_proxy.rs      # Write batching
│   ├── raft.rs             # gRPC service implementation
│   ├── datastore.rs        # Client-facing API
│   └── stream_manager.rs   # gRPC stream management
└── config/
    └── config.rs           # Configuration loading
```

## TODO / Future Improvements

### High Priority
- [ ] **Zero copy IO** - Prost gRPC streaming doesn't support Zero copy IO, should migrate to something like cap'n proto / SBE over raw TCP streams instead.
- [ ] **Log compaction / snapshotting** - Truncate persisted log after snapshotting state
- [ ] **Linearizable reads** - Implement read index or lease-based reads
- [ ] **Membership changes** - Dynamic cluster reconfiguration
- [ ] **Checksum validation** - Verify log integrity on recovery
- [ ] **Graceful shutdown** - Drain pending writes before stopping

## License

MIT
