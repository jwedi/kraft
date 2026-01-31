pub mod owned_message;
pub mod message_sender;
pub mod message_receiver;
pub mod conversion;

pub mod raft_capnp {
    include!(concat!(env!("OUT_DIR"), "/src/transport/capnp/raft_capnp.rs"));
}

pub use owned_message::{OwnedQuorumMessage, build_owned_message};
pub use conversion::CapnpProstConverter;
