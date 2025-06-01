use std::collections::HashMap;
use std::sync::Arc;
use log::info;
use tokio::sync::{mpsc, Mutex};
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tonic::{Request, Status, Streaming};
use crate::client::cluster_node_client::SharedGrpcChannel;
use crate::config::config::ClusterNode;
use crate::server::raftproto::{ConnectRequest, QuorumMessage};
use crate::server::raftproto::quorum_message::MessagePayload;

pub mod raftproto {
    tonic::include_proto!("raftproto"); // The string specified here must match the proto package name
}

pub trait StreamManager {

    async fn accept_stream(&mut self, read_stream: Streaming<QuorumMessage>, node_id: u32);

    async fn get_send_stream(&mut self, node_id: u32) -> Option<Arc<Mutex<mpsc::UnboundedSender<QuorumMessage>>>>;

    async fn get_receive_stream(&mut self, node_id: u32) -> Option<Arc<Mutex<Streaming<QuorumMessage>>>>;
    async fn try_connect(&mut self, node_id: u32) -> Result<Arc<Mutex<mpsc::UnboundedSender<QuorumMessage>>>, ConnectError>;

    async fn reset_send_stream(&mut self, node_id: u32);

    async fn reset_receive_stream(&mut self, node_id: u32);
}

#[derive(Debug)]
pub struct StreamManagerImpl {
    self_id: u32,
    cluster_nodes: Vec<ClusterNode>,
    send_streams: Arc<Mutex<HashMap<u32, Arc<Mutex<mpsc::UnboundedSender<QuorumMessage>>>>>>,
    receiver_streams: Arc<Mutex<HashMap<u32, Arc<Mutex<Streaming<QuorumMessage>>>>>>
}

#[derive(Debug)]
pub enum ConnectError {
    Err(String)
}

impl ConnectError {
    pub fn stringify(&self) -> String {
        match self {
            ConnectError::Err(e) => {
                format!("ConnectError: {}", e)
            }
        }
    }
}

impl StreamManagerImpl {
    pub fn new(self_id: u32, cluster_nodes: Vec<ClusterNode>, send_streams: Arc<Mutex<HashMap<u32, Arc<Mutex<mpsc::UnboundedSender<QuorumMessage>>>>>>, receiver_streams: Arc<Mutex<HashMap<u32, Arc<Mutex<tonic::Streaming<QuorumMessage>>>>>>) -> Self {
        StreamManagerImpl{
            self_id,
            cluster_nodes,
            send_streams,
            receiver_streams
        }
    }

    pub async fn try_connect(&self, node_id: u32) -> Result<Arc<Mutex<mpsc::UnboundedSender<QuorumMessage>>>, ConnectError> {
        let mut write_lock = self.send_streams.lock().await;
        let node = self.cluster_nodes.iter().find(|n| n.node_id == node_id).unwrap();
        let mut client = SharedGrpcChannel::new(node.endpoint.as_str(), node_id);

        let mut raft_client = client.get_client().await.map_err(|e| ConnectError::Err(e))?;

        let (tx, rx) = mpsc::unbounded_channel();

        let request_stream = UnboundedReceiverStream::new(rx);

        let mut req = Request::new(request_stream);
        req.metadata_mut().append("x-node-id", self.self_id.to_string().parse().unwrap());

        let stream_response = raft_client.messages(req).await.map_err(|e| ConnectError::Err(e.message().to_string()))?.into_inner();

        /*
        // TODO handle result
        let connect = tx.send(QuorumMessage{
            message_payload: Some(MessagePayload::ConnectRequestOption(ConnectRequest{node_id: self.self_id}))
        });
        match connect {
            Ok(ok) => {
                log::info!("Sent stream connect request to {}", node_id);
            }
            Err(e) => {
                log::error!("Error {} when sending connect request to {}", e, node_id);
            }
        }*/
        let entry = Arc::new(Mutex::new(tx));
        let resp = Arc::clone(&entry);
        write_lock.insert(node_id, entry);

        Ok(resp)
    }

    pub async fn accept_stream(&self, read_stream: Streaming<QuorumMessage>, node_id: u32) {
        log::info!("Accepting stream from {}", node_id);
        let mut write_lock = self.receiver_streams.lock().await;
        let mut ns = write_lock.get_mut(&node_id);
        match ns {
            None => {
                let entry = Arc::new(Mutex::new(read_stream));
                write_lock.insert(node_id, entry);
            }
            Some(node_stream) => {
                let mut receiver_lock = node_stream.lock().await;
                // TODO close existing stream write_lock.
                *receiver_lock = read_stream;
            }
        }
    }

    pub async fn get_receive_stream(&self, node_id: u32) -> Option<Arc<Mutex<Streaming<QuorumMessage>>>> {
        let mut write_lock = self.receiver_streams.lock().await;
        let entry = write_lock.get(&node_id);
        match entry {
            None => {
                None
            }
            Some(v) => {
                let r = Arc::clone(v);
                Some(r)
            }
        }
    }

    pub async fn get_send_stream(&self, node_id: u32) -> Option<Arc<Mutex<mpsc::UnboundedSender<QuorumMessage>>>> {
        let mut write_lock = self.send_streams.lock().await;
        let entry = write_lock.get(&node_id);
        match entry {
            None => {
                None
            }
            Some(v) => {
                let r = Arc::clone(v);
                Some(r)
            }
        }
    }

    pub async fn reset_send_stream(&self, node_id: u32) {
        info!("Resetting send stream for node: {}", node_id);
        let mut write_lock = self.send_streams.lock().await;
        let entry = write_lock.remove_entry(&node_id);
        match entry {
            None => {
                info!("Got none when trying to reset send stream for: {}", node_id);
            }
            Some(v) => {
                info!("Got send stream for node: {}", node_id);
                let _ = v.1.lock().await.downgrade();
                info!("Reset send stream for node: {}", node_id);
            }
        }
    }

    pub async fn reset_receive_stream(&self, node_id: u32) {
        info!("Resetting receive stream for node: {}", node_id);

        let mut write_lock = self.receiver_streams.lock().await;
        let entry = write_lock.remove_entry(&node_id);
        match entry {
            None => {
                info!("Got none when trying to reset receive stream for: {}", node_id);
            }
            Some(v) => {
                info!("Got receive stream for node: {}", node_id);

                let mut write_l = v.1.lock().await;
                drop(write_l);
                info!("Reset receive stream for node: {}", node_id);
            }
        }
    }
}