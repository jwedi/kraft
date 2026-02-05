pub mod internal_message;
pub mod message_receiver;
pub mod message_sender;
pub mod owned_message;

pub mod raft_capnp {
    include!(concat!(env!("OUT_DIR"), "/src/transport/capnp/raft_capnp.rs"));
}

pub use internal_message::{
    build_owned_log_entry, build_owned_write_batch, build_owned_write_batch_response, OwnedLogEntry, OwnedWriteBatch,
    OwnedWriteBatchResponse,
};
pub use owned_message::{
    build_append_entries_ack_message, build_append_entries_message, build_heartbeat_message, build_owned_message,
    build_put_batch_request_message, build_put_batch_response_message, build_truncate_log_request_message,
    build_truncate_log_response_message, build_vote_request_message, build_vote_response_message, OwnedQuorumMessage,
};
