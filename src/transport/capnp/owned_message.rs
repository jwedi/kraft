use std::sync::Arc;
use super::raft_capnp::remote_quorum_message;

/// Zero-copy message wrapper - owns the serialized bytes via Arc.
/// Can be cloned and sent to multiple crossbeam queues without copying the data.
/// The bytes are parsed on-demand using Cap'n Proto's zero-copy deserialization.
#[derive(Clone)]
pub struct OwnedQuorumMessage {
    bytes: Arc<[u8]>,
}

impl OwnedQuorumMessage {
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
        F: FnOnce(remote_quorum_message::Reader<'_>) -> R,
    {
        let mut slice: &[u8] = &self.bytes;
        let reader = capnp::serialize::read_message_from_flat_slice(&mut slice, Default::default())?;
        let msg = reader.get_root::<remote_quorum_message::Reader<'_>>()?;
        Ok(f(msg))
    }

    /// Get the raw bytes for network transmission (zero-copy)
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Build an OwnedQuorumMessage from a builder closure
pub fn build_owned_message<F>(f: F) -> OwnedQuorumMessage
where
    F: FnOnce(remote_quorum_message::Builder<'_>),
{
    let mut builder = capnp::message::Builder::new_default();
    {
        let msg = builder.init_root::<remote_quorum_message::Builder>();
        f(msg);
    }
    let mut bytes = Vec::new();
    capnp::serialize::write_message(&mut bytes, &builder).expect("write should not fail");
    OwnedQuorumMessage::from_bytes(bytes).expect("re-parse should not fail")
}
