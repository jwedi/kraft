use std::net::SocketAddr;
use std::sync::Arc;
use crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender};
use dashmap::DashMap;
use log::info;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

use super::owned_message::OwnedQuorumMessage;
use super::message_sender::read_message;
use super::raft_capnp::remote_quorum_message;

/// TCP listener that accepts incoming connections and creates per-peer message queues.
pub struct MessageReceiver {
    listen_addr: SocketAddr,
    pub inbound_queues: Arc<DashMap<u32, CrossbeamSender<OwnedQuorumMessage>>>,
    pub inbound_receivers: Arc<DashMap<u32, CrossbeamReceiver<OwnedQuorumMessage>>>,
}

impl MessageReceiver {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            inbound_queues: Arc::new(DashMap::new()),
            inbound_receivers: Arc::new(DashMap::new()),
        }
    }

    /// Get the inbound queue receiver for a specific node
    pub fn get_queue(&self, node_id: u32) -> Option<CrossbeamReceiver<OwnedQuorumMessage>> {
        self.inbound_receivers.get(&node_id).map(|r| r.value().clone())
    }

    /// Start the TCP listener - accepts connections and spawns handlers
    pub async fn run(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        log::info!("Cap'n Proto listener started on {}", self.listen_addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            stream.set_nodelay(true)?;

            let inbound_queues = self.inbound_queues.clone();
            let inbound_receivers = self.inbound_receivers.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, inbound_queues, inbound_receivers).await {
                    log::warn!("Connection error from {}: {}", peer_addr, e);
                }
            });
        }
    }
}

/// Handle an incoming TCP connection
async fn handle_connection(
    stream: TcpStream,
    inbound_queues: Arc<DashMap<u32, CrossbeamSender<OwnedQuorumMessage>>>,
    inbound_receivers: Arc<DashMap<u32, CrossbeamReceiver<OwnedQuorumMessage>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut reader, _writer) = tokio::io::split(stream);

    // Read ConnectRequest to identify the peer
    info!("handle_connection waiting for connect message");
    let connect_msg = read_message(&mut reader).await?;
    info!("handle_connection received initial message");
    let node_id = connect_msg.with_message(|msg| {
        match msg.get_message_payload().which() {
            Ok(remote_quorum_message::message_payload::Which::ConnectRequest(req)) => {
                req.map(|r| r.get_node_id()).ok()
            }
            _ => None,
        }
    })?.ok_or("Expected ConnectRequest as first message")?;

    log::info!("Accepted Cap'n Proto connection from node {}", node_id);

    // Create bounded crossbeam channel for this peer.
    // Bounded channels provide backpressure to prevent unbounded memory growth
    let (tx, rx) = crossbeam_channel::bounded(10_000);
    inbound_queues.insert(node_id, tx.clone());
    inbound_receivers.insert(node_id, rx);

    loop {
        match read_message(&mut reader).await {
            Ok(msg) => {
                if tx.send(msg).is_err() {
                    log::warn!("Inbound queue closed for node {}", node_id);
                    break;
                }
            }
            Err(e) => {
                log::warn!("Connection closed from node {}: {}", node_id, e);
                break;
            }
        }
    }

    // Clean up queues on disconnect
    inbound_queues.remove(&node_id);
    inbound_receivers.remove(&node_id);

    Ok(())
}
