use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use bus::BusReader;
use crossbeam_queue::SegQueue;
use log::{info, warn};
use tokio::sync::oneshot;
use crate::client::cluster_node_client::SharedGrpcChannel;
use crate::quorum::worker::{LocalQuorumResponse, LocalQuorumWorkerTask};
use crate::raft::raft_sm::{CommitState, LocalRaftMessage};
use crate::service_utils::storage_utils::SerializationData;
use crate::transport::stream_manager::StreamManagerImpl;

pub struct QueryWorker {
    bus: BusReader<Arc<SerializationData>>,
    commit_state: Arc<CommitState>,
    last_applied_commit_index: u64,
    query_structure: HashMap<String, Arc<String>>,
    pending_queries: Arc<SegQueue<QueryRequest>>
}

pub struct QueryResponse {
    pub value: Option<Arc<String>>
}

pub struct QueryRequest {
    pub id: String,
    pub index: u64,
    pub term: u64,
    pub callback: oneshot::Sender<QueryResponse>
}

impl QueryWorker {

    pub fn new(
        bus: BusReader<Arc<SerializationData>>,
        commit_state: Arc<CommitState>,
        pending_queries: Arc<SegQueue<QueryRequest>>
    ) -> Self {
        Self {
            bus,
            commit_state,
            last_applied_commit_index: 0,
            query_structure: HashMap::new(),
            pending_queries
        }
    }

    pub fn run(&mut self) {

        let mut last_version = 0;
        let mut parked: Option<Arc<SerializationData>> = None;
        let mut parked_request: Option<QueryRequest> = None;
        let mut applied_index: u64 = 0;
        let mut applied_term: u64 = 0;
        loop {
            let current_version = self.commit_state.version.load(Ordering::Acquire);
            if current_version != last_version {
                let index = self.commit_state.commit_index.load(Ordering::Acquire);
                let term = self.commit_state.term.load(Ordering::Acquire);
                last_version = current_version;
                applied_index = index;
                applied_term = term;

                match parked.take() {
                    None => {}
                    Some(parked_value) => {
                        for req in parked_value.requests.iter() {
                            self.query_structure.insert(req.id.clone(), Arc::new(req.payload.clone()));
                        }
                    }
                }
                // Apply all new data up until new commit index.
                let mut num_entries_applied = 0;
                loop {
                    match self.bus.try_recv() {
                        Ok(val) => {
                            if (val.term == term && val.index <= index) || (val.term < term) {
                                for req in val.requests.iter() {
                                    self.query_structure.insert(req.id.clone(), Arc::new(req.payload.clone()));
                                }
                            } else {
                                parked = Some(val);
                                break;
                            }
                        }
                        Err(..) => {
                            break;
                        },
                    }
                    num_entries_applied += 1
                }
                info!("Applied transaction log data to commit version: {}, number of updates applied {}", current_version, num_entries_applied)
            }

            // Process read requests
            match parked_request.take() {
                Some(query) => {
                    // Check if query index is ahead of applied index
                    if (query.index <= applied_index && query.term == applied_term) || (query.term < applied_term) {
                        // TODO verify presence of leader for linearizability.
                        self.handle_query(query)
                    } else {
                        parked_request = Some(query);
                        thread::yield_now();
                        continue
                    }
                }
                None => {
                }
            }

            loop {
                let maybe_query = self.pending_queries.pop();

                match maybe_query {
                    Some(query) => {
                        // Check if query index is ahead of applied index
                        if (query.index <= applied_index && query.term == applied_term) || (query.term < applied_term) {
                            // TODO verify presence of leader for linearizability.
                            self.handle_query(query)
                        } else {
                            // If read request has term or index that hasn't been applied park request and restart loop to apply
                            parked_request = Some(query);
                            break;
                        }
                    }
                    None => {
                        thread::yield_now();
                        break;
                    }
                }
            }
        }
    }

    pub fn handle_query(&mut self, query: QueryRequest) {
        let resp = self.query_structure.get(&query.id);
        match resp {
            None => {
                if let Err(_) = query.callback.send(QueryResponse{ value: None}) {
                    warn!("handle_query sending empty response failed, the receiver dropped")
                }
            }
            Some(value) => {
                if let Err(_) = query.callback.send(QueryResponse{ value: Some(Arc::clone(value))}) {
                    warn!("handle_query sending response failed, the receiver dropped")
                }
            }
        }
    }
}