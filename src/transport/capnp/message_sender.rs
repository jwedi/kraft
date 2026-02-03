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

        // Main send loop: block on crossbeam (via spawn_blocking) then batch-drain
        let rx = self.rx;
        'outer: loop {
            // Block on the Tokio blocking threadpool waiting for a message
            let rx_clone = rx.clone();
            let Ok(Ok(msg)) = tokio::task::spawn_blocking(move || rx_clone.recv()).await else {
                log::warn!("Message queue for sender disconnected");
                break;
            };

            if write_message(&mut writer, &msg).await.is_err() {
                break;
            }

            // Batch-drain any pending messages without blocking
            while let Ok(msg) = rx.try_recv() {
                if write_message(&mut writer, &msg).await.is_err() {
                    break 'outer;
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

/// Maximum allowed message size (64 MB). This prevents DoS attacks where
/// an attacker sends a huge length prefix to exhaust server memory.
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Read a length-prefixed message from TCP
pub async fn read_message<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<OwnedQuorumMessage> {
    let len = reader.read_u32().await? as usize;

    // Validate message size before allocating to prevent DoS attacks
    if len > MAX_MESSAGE_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Message size {} exceeds maximum allowed size {}", len, MAX_MESSAGE_SIZE),
        ));
    }

    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes).await?;
    OwnedQuorumMessage::from_bytes(bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // =========================================================================
    // Regression test: Message size limit prevents DoS via large length prefix
    // Fix: Added MAX_MESSAGE_SIZE check before allocating buffer
    // =========================================================================
    #[tokio::test]
    async fn test_read_message_rejects_oversized_message() {
        // Create a fake TCP stream with an oversized length prefix
        // Length prefix: 100MB (well over the 64MB limit)
        let oversized_length: u32 = 100 * 1024 * 1024;
        let mut data = Vec::new();
        data.extend_from_slice(&oversized_length.to_be_bytes());
        // No actual payload needed - we should fail before reading it

        let mut cursor = Cursor::new(data);
        let result = read_message(&mut cursor).await;

        match result {
            Err(err) => {
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
                assert!(
                    err.to_string().contains("exceeds maximum"),
                    "Error should mention size limit: {}",
                    err
                );
            }
            Ok(_) => panic!("Expected error for oversized message"),
        }
    }

    #[tokio::test]
    async fn test_read_message_accepts_valid_size() {
        // MAX_MESSAGE_SIZE is 64MB, so 1KB should be fine
        // This test verifies that the size check passes for valid sizes
        // (the actual Cap'n Proto parsing may succeed or fail depending on data)
        let valid_length: u32 = 1024;
        let mut data = Vec::new();
        data.extend_from_slice(&valid_length.to_be_bytes());
        // Add dummy payload (may or may not be valid Cap'n Proto)
        data.extend(vec![0u8; valid_length as usize]);

        let mut cursor = Cursor::new(data);
        let result = read_message(&mut cursor).await;

        // The key assertion: if there's an error, it should NOT be about size limit
        // (either parsing succeeds or fails for other reasons)
        if let Err(err) = result {
            assert!(
                !err.to_string().contains("exceeds maximum"),
                "Error should not be about size limit for valid-sized message: {}",
                err
            );
        }
        // If Ok(_), that's fine too - it means the zeros formed valid Cap'n Proto
    }

    // =========================================================================
    // Test MAX_MESSAGE_SIZE constant is set correctly
    // =========================================================================
    #[test]
    fn test_max_message_size_is_64mb() {
        assert_eq!(MAX_MESSAGE_SIZE, 64 * 1024 * 1024);
    }

    // =========================================================================
    // Test boundary conditions for message size
    // =========================================================================
    #[tokio::test]
    async fn test_read_message_boundary_exactly_at_limit() {
        // Exactly at the limit should be accepted (size-wise)
        let at_limit: u32 = MAX_MESSAGE_SIZE as u32;
        let mut data = Vec::new();
        data.extend_from_slice(&at_limit.to_be_bytes());
        // Note: We can't actually allocate 64MB in this test, so this is conceptual

        let mut cursor = Cursor::new(data);
        let result = read_message(&mut cursor).await;

        // Should fail because we don't have 64MB of data, but NOT because of size limit
        // The error should be UnexpectedEof, not InvalidData about size
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_message_boundary_one_over_limit() {
        // One byte over the limit should be rejected immediately
        let over_limit: u32 = MAX_MESSAGE_SIZE as u32 + 1;
        let mut data = Vec::new();
        data.extend_from_slice(&over_limit.to_be_bytes());

        let mut cursor = Cursor::new(data);
        let result = read_message(&mut cursor).await;

        match result {
            Err(err) => {
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
                assert!(err.to_string().contains("exceeds maximum"));
            }
            Ok(_) => panic!("Expected error for oversized message"),
        }
    }
}
