use std::fmt;

/// Comprehensive error types for the distributed key-value store.
///
/// Organized by subsystem to enable targeted error handling and proper
/// error propagation through the system.
#[derive(Debug)]
pub enum ServiceError {
    // =========================================================================
    // Concurrency Errors
    // =========================================================================
    /// Operation was throttled due to resource constraints
    ThrottlingError(String),
    /// A race condition was detected during a concurrent operation
    RaceConditionError,

    // =========================================================================
    // Network Errors
    // =========================================================================
    /// Failed to connect to a remote node
    ConnectionError { node_id: u32, message: String },
    /// Connection to a remote node was lost
    ConnectionLost { node_id: u32, message: String },
    /// Network timeout occurred
    TimeoutError { operation: String, timeout_ms: u64 },
    /// Message was too large to process
    MessageTooLarge { size: usize, max_size: usize },

    // =========================================================================
    // Consensus Errors
    // =========================================================================
    /// Operation rejected because this node is not the leader
    NotLeader { leader_id: Option<u32> },
    /// Failed to achieve quorum for an operation
    QuorumNotReached { required: u32, achieved: u32 },
    /// Term mismatch during Raft operation
    TermMismatch { expected: u64, actual: u64 },
    /// Log entry was rejected (e.g., during replication)
    LogEntryRejected { index: u64, reason: String },

    // =========================================================================
    // Persistence Errors
    // =========================================================================
    /// Failed to write to persistent storage
    PersistenceWriteError { message: String },
    /// Failed to read from persistent storage
    PersistenceReadError { message: String },
    /// Data corruption detected in persistent storage
    DataCorruption { message: String },

    // =========================================================================
    // Configuration Errors
    // =========================================================================
    /// Invalid configuration value
    ConfigError { field: String, message: String },
    /// Missing required configuration
    MissingConfig { field: String },

    // =========================================================================
    // Serialization Errors
    // =========================================================================
    /// Failed to serialize a message
    SerializationError { message: String },
    /// Failed to deserialize a message
    DeserializationError { message: String },

    // =========================================================================
    // Channel Errors
    // =========================================================================
    /// A channel send operation failed (receiver dropped)
    ChannelSendError { channel: String },
    /// A channel receive operation failed (sender dropped)
    ChannelRecvError { channel: String },
    /// Channel is full and cannot accept more messages
    ChannelFull { channel: String },

    // =========================================================================
    // Internal Errors
    // =========================================================================
    /// An internal invariant was violated (programming error)
    InternalError { message: String },
    /// Operation was cancelled
    Cancelled { operation: String },
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Concurrency
            ServiceError::ThrottlingError(msg) => write!(f, "Throttling: {}", msg),
            ServiceError::RaceConditionError => write!(f, "Race condition detected"),

            // Network
            ServiceError::ConnectionError { node_id, message } => {
                write!(f, "Connection error to node {}: {}", node_id, message)
            }
            ServiceError::ConnectionLost { node_id, message } => {
                write!(f, "Connection lost to node {}: {}", node_id, message)
            }
            ServiceError::TimeoutError { operation, timeout_ms } => {
                write!(f, "Timeout after {}ms during: {}", timeout_ms, operation)
            }
            ServiceError::MessageTooLarge { size, max_size } => {
                write!(f, "Message size {} exceeds maximum {}", size, max_size)
            }

            // Consensus
            ServiceError::NotLeader { leader_id } => match leader_id {
                Some(id) => write!(f, "Not leader, current leader is node {}", id),
                None => write!(f, "Not leader, leader unknown"),
            },
            ServiceError::QuorumNotReached { required, achieved } => {
                write!(f, "Quorum not reached: need {}, got {}", required, achieved)
            }
            ServiceError::TermMismatch { expected, actual } => {
                write!(f, "Term mismatch: expected {}, got {}", expected, actual)
            }
            ServiceError::LogEntryRejected { index, reason } => {
                write!(f, "Log entry at index {} rejected: {}", index, reason)
            }

            // Persistence
            ServiceError::PersistenceWriteError { message } => {
                write!(f, "Persistence write error: {}", message)
            }
            ServiceError::PersistenceReadError { message } => {
                write!(f, "Persistence read error: {}", message)
            }
            ServiceError::DataCorruption { message } => {
                write!(f, "Data corruption: {}", message)
            }

            // Configuration
            ServiceError::ConfigError { field, message } => {
                write!(f, "Config error in '{}': {}", field, message)
            }
            ServiceError::MissingConfig { field } => {
                write!(f, "Missing required config: {}", field)
            }

            // Serialization
            ServiceError::SerializationError { message } => {
                write!(f, "Serialization error: {}", message)
            }
            ServiceError::DeserializationError { message } => {
                write!(f, "Deserialization error: {}", message)
            }

            // Channel
            ServiceError::ChannelSendError { channel } => {
                write!(f, "Channel send error on '{}'", channel)
            }
            ServiceError::ChannelRecvError { channel } => {
                write!(f, "Channel receive error on '{}'", channel)
            }
            ServiceError::ChannelFull { channel } => {
                write!(f, "Channel '{}' is full", channel)
            }

            // Internal
            ServiceError::InternalError { message } => {
                write!(f, "Internal error: {}", message)
            }
            ServiceError::Cancelled { operation } => {
                write!(f, "Operation cancelled: {}", operation)
            }
        }
    }
}

impl std::error::Error for ServiceError {}

// Implement PartialEq for the variants that need it (for backward compatibility)
impl PartialEq for ServiceError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ServiceError::ThrottlingError(a), ServiceError::ThrottlingError(b)) => a == b,
            (ServiceError::RaceConditionError, ServiceError::RaceConditionError) => true,
            // For other variants, use discriminant comparison
            _ => std::mem::discriminant(self) == std::mem::discriminant(other),
        }
    }
}

/// Result type alias using ServiceError
pub type ServiceResult<T> = Result<T, ServiceError>;

/// Helper trait for converting channel errors to ServiceError
pub trait ChannelErrorExt<T> {
    fn map_channel_err(self, channel_name: &str) -> ServiceResult<T>;
}

impl<T> ChannelErrorExt<T> for Result<T, crossbeam_channel::SendError<T>> {
    fn map_channel_err(self, channel_name: &str) -> ServiceResult<T> {
        self.map_err(|_| ServiceError::ChannelSendError {
            channel: channel_name.to_string(),
        })
    }
}