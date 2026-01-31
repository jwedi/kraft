use std::net::SocketAddr;
use crossbeam_channel::Receiver as CrossbeamReceiver;
use log::{error, info};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use super::owned_message::{OwnedQuorumMessage, build_owned_message};

/// Per-peer TCP sender that reads from a crossbeam channel and sends to a remote node.
pub struct MessageSender {
    node_id: u32,
    remote_addr: SocketAddr,
    rx: CrossbeamReceiver<OwnedQuorumMessage>,
}

impl MessageSender {
    pub fn new(
        node_id: u32,
        remote_addr: SocketAddr,
        rx: CrossbeamReceiver<OwnedQuorumMessage>,
    ) -> Self {
        Self { node_id, remote_addr, rx }
    }

    pub async fn run(self, ok: oneshot::Sender<bool>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Trying to connect to remote port {}", self.remote_addr.port());
        let stream = match TcpStream::connect(self.remote_addr).await {
            Ok(ok) => {
                ok
            }
            Err(e) => {
                ok.send(false).expect("Receiver of MeessageSender.run dropped");
                return Err(Box::new(e))
            }
        };
        info!("Connection to node port {} established", self.remote_addr.port());
        match stream.set_nodelay(true) {
            Ok(_) => {}
            Err(e) => {
                ok.send(false).expect("Receiver of MeessageSender.run dropped");
                return Err(Box::new(e))
            }
        }

        let (mut reader, mut writer) = tokio::io::split(stream);

        // Send ConnectRequest to identify ourselves
        let connect_msg = build_owned_message(|msg| {
            msg.init_message_payload().init_connect_request().set_node_id(self.node_id);
        });
        info!("Writing connect message to node {}", self.node_id);
        match write_message(&mut writer, &connect_msg).await {
            Ok(_) => {}
            Err(e) => {
                ok.send(false).expect("Receiver of MeessageSender.run dropped");
                return Err(Box::new(e))
            }
        }
        info!("Done writing connect message to node {}", self.node_id);
        ok.send(true).expect("Receiver of MeessageSender.run dropped");
        // Spawn reader task for incoming responses
        let reader_handle = tokio::spawn(async move {
            loop {
                match read_message(&mut reader).await {
                    Ok(_msg) => {
                        error!("Received message on the sender receive stream, this should not happen");
                        // Responses are handled by the inbound queue on the receiver side
                        // This task just keeps the connection alive and drains any responses
                    }
                    Err(e) => {
                        log::warn!("Sender receive stream closed remote connection to {}: {}", self.remote_addr.port(), e);
                        break
                    }
                }
            }
        });

        // Main send loop - poll crossbeam queue and send messages
        let rx = self.rx;
        loop {
            let msg = {
                let rx_clone = rx.clone();
                tokio::task::spawn_blocking(move || rx_clone.recv()).await?
            };

            match msg {
                Ok(msg) => {
                    if let Err(e) = write_message(&mut writer, &msg).await {
                        log::warn!("Failed to send message {}", e);
                        break;
                    }
                }
                Err(_) => {
                    log::warn!("Message queue for sender broke");
                    // Channel closed
                    break;
                }
            }
        }

        reader_handle.abort();
        Ok(())
    }
}

/// Write a length-prefixed message over TCP
pub async fn write_message<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &OwnedQuorumMessage,
) -> std::io::Result<()> {
    let bytes = msg.as_bytes();
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(bytes).await?;
    writer.flush().await
}

/// Read a length-prefixed message from TCP
pub async fn read_message<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<OwnedQuorumMessage> {
    let len = reader.read_u32().await? as usize;
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes).await?;
    OwnedQuorumMessage::from_bytes(bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
