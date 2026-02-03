use std::sync::Arc;
use log::info;
use super::raft_capnp::remote_quorum_message;
use super::internal_message::{OwnedWriteBatch, OwnedWriteBatchResponse, OwnedLogEntry};

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

// =============================================================================
// Direct message builders - bypass protobuf intermediates
// =============================================================================

/// Build a heartbeat message (AppendEntries with no entry)
pub fn build_heartbeat_message(
    term: u64,
    leader_id: u32,
    prev_log_index: u64,
    prev_log_term: u64,
    commit_index: u64,
    request_id: u64,
) -> OwnedQuorumMessage {
    build_owned_message(|mut msg| {
        let mut req = msg.init_message_payload().init_append_entries_request();
        req.set_term(term);
        req.set_leader_id(leader_id);
        req.set_prev_log_index(prev_log_index);
        req.set_prev_log_term(prev_log_term);
        req.set_commit_index(commit_index);
        req.set_request_id(request_id);
        // No entry - this is a heartbeat
    })
}

/// Build a vote request message
pub fn build_vote_request_message(
    term: u64,
    candidate_id: u32,
    last_log_index: u64,
    last_log_term: u64,
    request_id: u64,
) -> OwnedQuorumMessage {
    build_owned_message(|mut msg| {
        let mut req = msg.init_message_payload().init_vote_request();
        req.set_term(term);
        req.set_candidate_id(candidate_id);
        req.set_last_log_index(last_log_index);
        req.set_last_log_term(last_log_term);
        req.set_request_id(request_id);
    })
}

/// Build a vote response message
pub fn build_vote_response_message(
    vote_granted: bool,
    request_id: u64,
    term: u64,
) -> OwnedQuorumMessage {
    build_owned_message(|mut msg| {
        let mut resp = msg.init_message_payload().init_vote_response();
        resp.set_vote_granted(vote_granted);
        resp.set_request_id(request_id);
        resp.set_term(term);
    })
}

/// Build an append entries message with an optional entry
pub fn build_append_entries_message(
    term: u64,
    leader_id: u32,
    prev_log_index: u64,
    prev_log_term: u64,
    request_id: u64,
    commit_index: u64,
    entry: Option<&Arc<OwnedLogEntry>>,
) -> OwnedQuorumMessage {
    build_owned_message(|mut msg| {
        let mut req = msg.init_message_payload().init_append_entries_request();
        req.set_term(term);
        req.set_leader_id(leader_id);
        req.set_prev_log_index(prev_log_index);
        req.set_prev_log_term(prev_log_term);
        req.set_request_id(request_id);
        req.set_commit_index(commit_index);

        if let Some(log_entry) = entry {
            // Copy from OwnedLogEntry into the RemoteLogEntry
            match log_entry.with_message(|reader| {
                let mut entry_builder = req.init_entry();
                entry_builder.set_index(reader.get_index());
                entry_builder.set_term(reader.get_term());
                entry_builder.set_prev_log_index(reader.get_prev_log_index());
                entry_builder.set_prev_log_term(reader.get_prev_log_term());
                entry_builder.set_message_id(reader.get_message_id());
                entry_builder.set_batch_index(reader.get_index()); // batch_index = index
                // Copy data bytes (SBE serialized) for network transmission
                match reader.get_data() {
                    Ok(data) => {
                        entry_builder.set_data(data);
                    }
                    Err(e) => {
                        log::error!("Failed to read data from OwnedLogEntry: {:?}", e);
                    }
                }
            }) {
                Ok(_) => {}
                Err(e) => {
                    log::error!("Failed to parse OwnedLogEntry message: {:?}", e);
                }
            }
        }
    })
}

/// Build an append entries acknowledge message
pub fn build_append_entries_ack_message(
    last_log_term: u64,
    last_log_index: u64,
    request_id: u64,
    ok: bool,
) -> OwnedQuorumMessage {
    build_owned_message(|mut msg| {
        let mut ack = msg.init_message_payload().init_append_entries_acknowledge();
        ack.set_last_log_term(last_log_term);
        ack.set_last_log_index(last_log_index);
        ack.set_request_id(request_id);
        ack.set_ok(ok);
    })
}

/// Build a put batch request message from an OwnedWriteBatch
pub fn build_put_batch_request_message(batch: &OwnedWriteBatch) -> OwnedQuorumMessage {
    build_owned_message(|mut msg| {
        let _ = batch.with_message(|reader| {
            let mut req = msg.init_message_payload().init_put_batch_request();
            if let Ok(batch_id) = reader.get_batch_id() {
                req.set_batch_id(batch_id);
            }
            if let Ok(requests) = reader.get_requests() {
                let mut put_requests = req.init_put_requests(requests.len());
                for (i, r) in requests.iter().enumerate() {
                    let mut out = put_requests.reborrow().get(i as u32);
                    if let Ok(id) = r.get_id() {
                        out.set_id(id);
                    }
                    // Convert Data (bytes) to Text (string) for the remote payload
                    if let Ok(payload) = r.get_payload() {
                        out.set_payload(&String::from_utf8_lossy(payload));
                    }
                    out.set_node_id(r.get_node_id());
                }
            }
        });
    })
}

/// Build a put batch response message from an OwnedWriteBatchResponse
pub fn build_put_batch_response_message(response: &OwnedWriteBatchResponse, batch_id: &String) -> OwnedQuorumMessage {
    build_owned_message(|mut msg| {
        let _ = response.with_message(|reader| {
            let mut resp = msg.init_message_payload().init_put_batch_response();
            // Responses payload in the response is currently not serialized and we get the batch id from the quorum pending task.
            //if let Ok(batch_id) = reader.get_batch_id() {
            //    resp.set_batch_id(batch_id);
            //}
            resp.set_batch_id(batch_id);
            if let Ok(responses) = reader.get_responses() {
                let mut put_responses = resp.init_responses(responses.len());
                for (i, r) in responses.iter().enumerate() {
                    let mut out = put_responses.reborrow().get(i as u32);
                    if let Ok(id) = r.get_id() {
                        out.set_id(id);
                    }
                    if let Ok(response_type) = r.get_response_type() {
                        out.set_response_type(match response_type {
                            super::raft_capnp::RemoteResponseType::Ok => super::raft_capnp::RemoteResponseType::Ok,
                            super::raft_capnp::RemoteResponseType::Invalid => super::raft_capnp::RemoteResponseType::Invalid,
                            super::raft_capnp::RemoteResponseType::Unspecified => super::raft_capnp::RemoteResponseType::Unspecified,
                        });
                    }
                    if let Ok(message) = r.get_message() {
                        out.set_message(message);
                    }
                    out.set_node_id(r.get_node_id());
                    if let Ok(id) = r.get_batch_id() {
                        out.set_batch_id(id);
                    }
                }
            }
        });
    })
}

/// Build a truncate log request message
pub fn build_truncate_log_request_message(
    prev_term: u64,
    prev_index: u64,
    term: u64,
    leader_id: u32,
    request_id: u64,
) -> OwnedQuorumMessage {
    build_owned_message(|mut msg| {
        let mut req = msg.init_message_payload().init_truncate_log_request();
        req.set_prev_term(prev_term);
        req.set_prev_index(prev_index);
        req.set_term(term);
        req.set_leader_id(leader_id);
        req.set_request_id(request_id);
    })
}

/// Build a truncate log response message
pub fn build_truncate_log_response_message(
    request_id: u64,
    ok: bool,
) -> OwnedQuorumMessage {
    build_owned_message(|mut msg| {
        let mut resp = msg.init_message_payload().init_truncate_log_response();
        resp.set_request_id(request_id);
        resp.set_ok(ok);
    })
}
