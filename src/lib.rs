//! Kraft - A distributed key-value store using Raft consensus.
//!
//! This library provides the core components for building a distributed
//! key-value store with Cap'n Proto transport and Raft-based replication.

pub mod service_utils {
    pub mod app_time;
    pub mod errors;
    pub mod tracing_utils;
}

pub mod runtime_core {
    pub mod task_buffer;
    pub mod types;
}

pub mod raft {
    pub mod candidate;
    pub mod follower;
    pub mod leader;
    pub mod raft_sm;
    pub mod state_machine_worker;
}

pub mod persistence {
    pub mod worker;
}

pub mod quorum {
    pub mod quorum_worker;
    pub mod types;
}

pub mod query {
    pub mod worker;
}

pub mod config {
    pub mod config;
}

pub mod transport {
    pub mod capnp;
    pub mod cluster_stream_manager;
    pub mod datastore;
    pub mod metrics;
    pub mod metrics_server;
    pub mod write_proxy;
}

pub mod startup;
