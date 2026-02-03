use std::sync::Arc;
use super::raft_capnp::{internal_write_batch, internal_log_entry, internal_write_batch_response};

/// Zero-copy write batch wrapper - owns the serialized bytes via Arc.
/// Can be cloned and sent to multiple crossbeam queues without copying the data.
/// The bytes are parsed on-demand using Cap'n Proto's zero-copy deserialization.
#[derive(Clone)]
pub struct OwnedWriteBatch {
    bytes: Arc<[u8]>,
}

impl OwnedWriteBatch {
    /// Create from bytes (takes ownership). Validates the message format.
    pub fn from_bytes(bytes: Vec<u8>) -> capnp::Result<Self> {
        // Validate by parsing once
        let mut slice: &[u8] = &bytes;
        let _ = capnp::serialize::read_message_from_flat_slice(
            &mut slice,
            capnp::message::ReaderOptions::default(),
        )?;
        Ok(Self { bytes: bytes.into() })
    }

    /// Process the message with a closure. This is the zero-copy access pattern.
    /// The reader borrows from the internal bytes and is valid for the closure's scope.
    pub fn with_message<F, R>(&self, f: F) -> capnp::Result<R>
    where
        F: FnOnce(internal_write_batch::Reader<'_>) -> R,
    {
        let mut slice: &[u8] = &self.bytes;
        let reader = capnp::serialize::read_message_from_flat_slice(&mut slice, Default::default())?;
        let msg = reader.get_root::<internal_write_batch::Reader<'_>>()?;
        Ok(f(msg))
    }

    /// Get the raw bytes for storage/transmission (zero-copy)
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Get the batch ID without full deserialization
    pub fn batch_id(&self) -> capnp::Result<String> {
        self.with_message(|r| r.get_batch_id().map(|s| s.to_string().unwrap_or_default()))?.map_err(|e| capnp::Error::failed(format!("{:?}", e)))
    }

    /// Get the number of requests in this batch
    pub fn request_count(&self) -> capnp::Result<usize> {
        self.with_message(|r| r.get_requests().map(|reqs| reqs.len() as usize))?
    }

    /// Get the total size of all payloads in the batch
    pub fn size(&self) -> u64 {
        self.with_message(|r| {
            r.get_requests().map(|reqs| {
                reqs.iter().map(|req| {
                    req.get_payload().map(|p| p.len() as u64).unwrap_or(0)
                }).sum()
            }).unwrap_or(0)
        }).unwrap_or(0)
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.with_message(|r| {
            r.get_requests().map(|reqs| reqs.is_empty()).unwrap_or(true)
        }).unwrap_or(true)
    }

    /// Get the number of requests (convenience wrapper that doesn't return Result)
    pub fn len(&self) -> usize {
        self.request_count().unwrap_or(0)
    }
}

impl std::fmt::Debug for OwnedWriteBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OwnedWriteBatch({} bytes)", self.bytes.len())
    }
}

/// Build an OwnedWriteBatch from a builder closure
pub fn build_owned_write_batch<F>(f: F) -> OwnedWriteBatch
where
    F: FnOnce(internal_write_batch::Builder<'_>),
{
    let mut builder = capnp::message::Builder::new_default();
    {
        let msg = builder.init_root::<internal_write_batch::Builder>();
        f(msg);
    }
    let mut bytes = Vec::new();
    capnp::serialize::write_message(&mut bytes, &builder).expect("write should not fail");
    OwnedWriteBatch::from_bytes(bytes).expect("re-parse should not fail")
}

/// Zero-copy log entry wrapper - owns the serialized bytes via Arc.
/// Used for replication log entries that need to be shared across queues
/// and persisted to disk without copying.
#[derive(Clone)]
pub struct OwnedLogEntry {
    bytes: Arc<[u8]>,
}

impl OwnedLogEntry {
    /// Create from bytes (takes ownership). Validates the message format.
    pub fn from_bytes(bytes: Vec<u8>) -> capnp::Result<Self> {
        // Validate by parsing once
        let mut slice: &[u8] = &bytes;
        let _ = capnp::serialize::read_message_from_flat_slice(
            &mut slice,
            capnp::message::ReaderOptions::default(),
        )?;
        Ok(Self { bytes: bytes.into() })
    }

    /// Create from raw bytes without validation (for recovery from trusted sources)
    pub fn from_bytes_unchecked(bytes: Arc<[u8]>) -> Self {
        Self { bytes }
    }

    /// Iterate commands with zero-copy access (key, payload, node_id)
    pub fn for_each_command<F>(&self, mut f: F) -> capnp::Result<()>
    where
        F: FnMut(&str, &[u8], u32),
    {
        self.with_message(|r| {
            if let Ok(commands) = r.get_commands() {
                for cmd in commands.iter() {
                    let id = cmd.get_id().ok().and_then(|s| s.to_str().ok()).unwrap_or("");
                    let payload = cmd.get_payload().unwrap_or(&[]);
                    let node_id = cmd.get_node_id();
                    f(id, payload, node_id);
                }
            }
        })
    }

    /// Process the message with a closure. This is the zero-copy access pattern.
    pub fn with_message<F, R>(&self, f: F) -> capnp::Result<R>
    where
        F: FnOnce(internal_log_entry::Reader<'_>) -> R,
    {
        let mut slice: &[u8] = &self.bytes;
        let reader = capnp::serialize::read_message_from_flat_slice(&mut slice, Default::default())?;
        let msg = reader.get_root::<internal_log_entry::Reader<'_>>()?;
        Ok(f(msg))
    }

    /// Get the raw bytes for storage/transmission (zero-copy)
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Get the log index
    pub fn index(&self) -> u64 {
        self.with_message(|r| r.get_index()).unwrap_or(0)
    }

    /// Get the term
    pub fn term(&self) -> u64 {
        self.with_message(|r| r.get_term()).unwrap_or(0)
    }

    /// Get the previous log index
    pub fn prev_log_index(&self) -> u64 {
        self.with_message(|r| r.get_prev_log_index()).unwrap_or(0)
    }

    /// Get the previous log term
    pub fn prev_log_term(&self) -> u64 {
        self.with_message(|r| r.get_prev_log_term()).unwrap_or(0)
    }

    /// Get the message ID
    pub fn message_id(&self) -> u64 {
        self.with_message(|r| r.get_message_id()).unwrap_or(0)
    }

    /// Get the timestamp
    pub fn timestamp(&self) -> u64 {
        self.with_message(|r| r.get_timestamp()).unwrap_or(0)
    }

    /// Get the number of commands in this entry
    pub fn command_count(&self) -> usize {
        self.with_message(|r| r.get_commands().map(|c| c.len() as usize).unwrap_or(0)).unwrap_or(0)
    }

    /// Get the data bytes (SBE serialized) for network transmission
    pub fn data(&self) -> Vec<u8> {
        self.with_message(|r| r.get_data().map(|d| d.to_vec()).unwrap_or_default()).unwrap_or_default()
    }
}

impl std::fmt::Debug for OwnedLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OwnedLogEntry(index={}, term={}, {} bytes)",
            self.index(), self.term(), self.bytes.len())
    }
}

/// Build an OwnedLogEntry from a builder closure
pub fn build_owned_log_entry<F>(f: F) -> OwnedLogEntry
where
    F: FnOnce(internal_log_entry::Builder<'_>),
{
    let mut builder = capnp::message::Builder::new_default();
    {
        let msg = builder.init_root::<internal_log_entry::Builder>();
        f(msg);
    }
    let mut bytes = Vec::new();
    capnp::serialize::write_message(&mut bytes, &builder).expect("write should not fail");
    OwnedLogEntry::from_bytes(bytes).expect("re-parse should not fail")
}

/// Zero-copy write batch response wrapper
#[derive(Clone)]
pub struct OwnedWriteBatchResponse {
    bytes: Arc<[u8]>,
}

impl OwnedWriteBatchResponse {
    /// Create from bytes (takes ownership). Validates the message format.
    pub fn from_bytes(bytes: Vec<u8>) -> capnp::Result<Self> {
        let mut slice: &[u8] = &bytes;
        let _ = capnp::serialize::read_message_from_flat_slice(
            &mut slice,
            capnp::message::ReaderOptions::default(),
        )?;
        Ok(Self { bytes: bytes.into() })
    }

    /// Process the message with a closure.
    pub fn with_message<F, R>(&self, f: F) -> capnp::Result<R>
    where
        F: FnOnce(internal_write_batch_response::Reader<'_>) -> R,
    {
        let mut slice: &[u8] = &self.bytes;
        let reader = capnp::serialize::read_message_from_flat_slice(&mut slice, Default::default())?;
        let msg = reader.get_root::<internal_write_batch_response::Reader<'_>>()?;
        Ok(f(msg))
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Get the batch ID
    pub fn batch_id(&self) -> capnp::Result<String> {
        self.with_message(|r| r.get_batch_id().map(|s| s.to_string().unwrap_or_default()))?.map_err(|e| capnp::Error::failed(format!("{:?}", e)))
    }
}

impl std::fmt::Debug for OwnedWriteBatchResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OwnedWriteBatchResponse({} bytes)", self.bytes.len())
    }
}

/// Build an OwnedWriteBatchResponse from a builder closure
pub fn build_owned_write_batch_response<F>(f: F) -> OwnedWriteBatchResponse
where
    F: FnOnce(internal_write_batch_response::Builder<'_>),
{
    let mut builder = capnp::message::Builder::new_default();
    {
        let msg = builder.init_root::<internal_write_batch_response::Builder>();
        f(msg);
    }
    let mut bytes = Vec::new();
    capnp::serialize::write_message(&mut bytes, &builder).expect("write should not fail");
    OwnedWriteBatchResponse::from_bytes(bytes).expect("re-parse should not fail")
}
