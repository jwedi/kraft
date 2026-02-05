//! TCP-based datastore server with custom length-prefixed framing and Cap'n Proto serialization.
//!
//! Wire Protocol:
//! ```text
//! +----------------+----------+---------------+---------------------------+
//! | Length (4B LE) | RPC Type | Request ID    | Cap'n Proto Payload       |
//! | u32            | u8       | u64           | Variable                  |
//! +----------------+----------+---------------+---------------------------+
//! ```
//!
//! Length includes RPC type (1) + request ID (8) + payload.
//! Request ID enables pipelining - responses matched to requests by ID.

pub mod codec;
pub mod connection;
pub mod server;

pub use server::DatastoreServer;

/// RPC type constants for the wire protocol
pub mod rpc {
    /// Put a record into the datastore
    pub const RPC_PUT_RECORD: u8 = 1;
    /// Get a record from the datastore
    pub const RPC_GET_RECORD: u8 = 2;
}
