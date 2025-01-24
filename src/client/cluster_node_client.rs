use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Error};
use crate::server::raftproto::raft_client::RaftClient;

#[derive(Clone, Debug)]
pub struct SharedGrpcChannel {
    channel: Arc<RwLock<Option<Channel>>>,
    endpoint: String,
    pub node_id: u32
}

impl SharedGrpcChannel {
    pub fn new(endpoint: &str, node_id: u32) -> Self {
        Self {
            channel: Arc::new(RwLock::new(None)),
            endpoint: endpoint.to_string(),
            node_id
        }
    }

    async fn get_or_create_channel(&self) -> Result<Channel, String> {
        // Check if the channel is already initialized
        {
            let read_lock = self.channel.read().await;
            if let Some(channel) = &*read_lock {
                return Ok(channel.clone());
            }
        }

        // Channel is not initialized; create it
        let ep = self.endpoint.clone();
        let maybe_new_channel = Channel::from_shared(ep)
            .expect("parsing endpoint url shouldn't fail")
            .connect()
            .await;

        match maybe_new_channel {
            Ok(new_channel) => {
                let mut write_lock = self.channel.write().await;
                *write_lock = Some(new_channel.clone());

                Ok(new_channel)
            }
            Err(e) => {
                Err(e.to_string())
            }
        }
    }

    async fn recreate_channel(&self) -> Result<(), String> {
        let ep = self.endpoint.clone();
        let maybe_new_channel = Channel::from_shared(ep)
            .expect("parsing endpoint url shouldn't fail")
            .connect()
            .await;

        match maybe_new_channel {
            Ok(new_channel) => {
                let mut write_lock = self.channel.write().await;
                *write_lock = Some(new_channel);
                Ok(())
            }
            Err(e) => {
                Err(e.to_string())
            }
        }
    }

    pub async fn get_client(&self) -> Result<RaftClient<Channel>, String> {
        let channel = self.get_or_create_channel().await?;
        Ok(RaftClient::new(channel))
    }
}