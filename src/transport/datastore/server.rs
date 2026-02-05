//! TCP datastore server with per-connection task spawning.

use std::net::SocketAddr;
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use log::{error, info};
use tokio::net::TcpListener;

use crate::query::worker::QueryRequest;
use crate::raft::raft_sm::CommitState;
use crate::transport::datastore::connection;
use crate::transport::write_proxy::WriteBatch;

/// Shared queues for all connections.
#[derive(Clone)]
pub struct Queues {
    pub task_queue: Arc<SegQueue<WriteBatch>>,
    pub query_queue: Arc<SegQueue<QueryRequest>>,
    pub commit_state: Arc<CommitState>,
}

/// TCP-based datastore server using length-prefixed framing and Cap'n Proto serialization.
pub struct DatastoreServer {
    listen_addr: SocketAddr,
    queues: Queues,
}

impl DatastoreServer {
    /// Create a new TCP datastore server.
    pub fn new(
        listen_addr: SocketAddr,
        task_queue: Arc<SegQueue<WriteBatch>>,
        query_queue: Arc<SegQueue<QueryRequest>>,
        commit_state: Arc<CommitState>,
    ) -> Self {
        Self {
            listen_addr,
            queues: Queues {
                task_queue,
                query_queue,
                commit_state,
            },
        }
    }

    /// Run the server, accepting connections and spawning handlers.
    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        info!("TCP datastore server listening on {}", self.listen_addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("TCP datastore: new connection from {}", peer_addr);
                    let queues = self.queues.clone();
                    tokio::spawn(async move {
                        if let Err(e) = connection::handle(stream, queues).await {
                            error!("TCP datastore connection error from {}: {}", peer_addr, e);
                        }
                        info!("TCP datastore: connection closed from {}", peer_addr);
                    });
                }
                Err(e) => {
                    error!("TCP datastore accept error: {}", e);
                }
            }
        }
    }
}
