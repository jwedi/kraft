use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use crossbeam_channel::{Receiver, Sender};
use tokio::sync::{oneshot, RwLock};
use tokio::sync::oneshot::error::RecvError;
use crate::config::config::ClusterNode;
use crate::transport::capnp::owned_message::OwnedQuorumMessage;
use crate::transport::capnp::message_sender::MessageSender;
use crate::transport::capnp::message_receiver::MessageReceiver;

/// Manages Cap'n Proto TCP connections for cluster communication.
/// Provides per-peer outbound send channels and inbound receive queues.
pub struct CapnpStreamManager {
    self_id: u32,
    cluster_nodes: Vec<ClusterNode>,
    listen_addr: SocketAddr,
    outbound_senders: Arc<RwLock<HashMap<u32, Sender<OwnedQuorumMessage>>>>,
    receiver: Arc<MessageReceiver>,
}

impl CapnpStreamManager {
    pub fn new(
        self_id: u32,
        cluster_nodes: Vec<ClusterNode>,
        listen_addr: SocketAddr,
    ) -> Self {
        Self {
            self_id,
            cluster_nodes,
            listen_addr,
            outbound_senders: Arc::new(RwLock::new(HashMap::new())),
            receiver: Arc::new(MessageReceiver::new(listen_addr)),
        }
    }

    /// Start the TCP listener that accepts incoming connections.
    /// Should be spawned as a separate task.
    pub async fn start_listener(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.receiver.clone().run().await
    }

    /// Get or establish an outbound connection to a node.
    /// Returns a crossbeam sender that can be used to send messages.
    pub async fn get_or_connect(&self, node_id: u32) -> Result<Sender<OwnedQuorumMessage>, CapnpConnectError> {
        // Check if already connected
        if let Some(tx) = self.outbound_senders.read().await.get(&node_id) {
            return Ok(tx.clone());
        }

        // Find node's endpoint
        let node = self.cluster_nodes.iter()
            .find(|n| n.node_id == node_id)
            .ok_or_else(|| CapnpConnectError::NoSuchNode(node_id))?;

        let addr: SocketAddr = node.endpoint.parse()
            .map_err(|e| CapnpConnectError::InvalidEndpoint(format!("{}: {}", node.endpoint, e)))?;

        // Create crossbeam channel for this connection
        let (tx, rx) = crossbeam_channel::unbounded();

        // Create and spawn the sender task
        let sender = MessageSender::new(self.self_id, addr, rx);
        let node_id_for_log = node_id;
        let outbound_senders_for_cleanup = Arc::clone(&self.outbound_senders);
        let (connect_callback_writer, connect_callback_receiver) = oneshot::channel();

        tokio::spawn(async move {
            log::info!("Starting MessageSender for node {}", node_id_for_log);
            if let Err(e) = sender.run(connect_callback_writer).await {
                log::error!("MessageSender for node {} failed: {}", node_id_for_log, e);
            }
            log::info!("MessageSender for node {} stopped", node_id_for_log);

            // Clean up: remove from outbound_senders so reconnection will be attempted
            outbound_senders_for_cleanup.write().await.remove(&node_id_for_log);
            log::info!("Removed connection for node {} from outbound_senders", node_id_for_log);
        });
        match connect_callback_receiver.await {
            Ok(res) => {
                if res {
                    self.outbound_senders.write().await.insert(node_id, tx.clone());
                    Ok(tx)
                } else {
                    Err(CapnpConnectError::ConnectionFailed("Err".to_string()))
                }
            }
            Err(e) => {
                Err(CapnpConnectError::ConnectionFailed(e.to_string()))
            }
        }
    }

    /// Get the inbound queue for a node (populated when they connect to us).
    pub fn get_inbound_queue(&self, node_id: u32) -> Option<Receiver<OwnedQuorumMessage>> {
        self.receiver.get_queue(node_id)
    }

    /// Reset connection state for a node, triggering reconnection on next get_or_connect.
    pub async fn reset_connection(&self, node_id: u32) {
        self.outbound_senders.write().await.remove(&node_id);
        log::info!("Reset connection state for node {}", node_id);
    }

    /// Check if we have an outbound connection to a node.
    pub async fn has_outbound_connection(&self, node_id: u32) -> bool {
        self.outbound_senders.read().await.contains_key(&node_id)
    }

    /// Check if we have an inbound connection from a node.
    pub fn has_inbound_connection(&self, node_id: u32) -> bool {
        self.receiver.get_queue(node_id).is_some()
    }
}

#[derive(Debug)]
pub enum CapnpConnectError {
    NoSuchNode(u32),
    InvalidEndpoint(String),
    ConnectionFailed(String),
}

impl std::fmt::Display for CapnpConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CapnpConnectError::NoSuchNode(id) => write!(f, "No such node: {}", id),
            CapnpConnectError::InvalidEndpoint(e) => write!(f, "Invalid endpoint: {}", e),
            CapnpConnectError::ConnectionFailed(e) => write!(f, "Connection failed: {}", e),
        }
    }
}

impl std::error::Error for CapnpConnectError {}
