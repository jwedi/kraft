pub mod owned_message;
pub mod internal_message;
pub mod message_sender;
pub mod message_receiver;

pub mod raft_capnp {
    include!(concat!(env!("OUT_DIR"), "/src/transport/capnp/raft_capnp.rs"));
}

pub use owned_message::{
    OwnedQuorumMessage, build_owned_message,
    build_heartbeat_message, build_vote_request_message, build_vote_response_message,
    build_append_entries_message, build_append_entries_ack_message,
    build_put_batch_request_message, build_put_batch_response_message,
    build_truncate_log_request_message, build_truncate_log_response_message,
};
pub use internal_message::{
    OwnedWriteBatch, build_owned_write_batch,
    OwnedLogEntry, build_owned_log_entry,
    OwnedWriteBatchResponse, build_owned_write_batch_response,
};
