//! Startup module containing initialization helpers for the Kraft server.
//!
//! This module breaks down the server startup process into focused components:
//! - `queues`: Queue and channel creation
//! - `recovery`: Persistence recovery (votes, replication log)
//! - `state`: Server state initialization
//! - `workers`: Worker creation and spawning
//! - `server`: gRPC server building

pub mod queues;
pub mod recovery;
pub mod state;
pub mod workers;
pub mod server;

pub use queues::*;
pub use recovery::*;
pub use state::*;
pub use workers::*;
pub use server::*;
