use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use bus::BusReader;
use crossbeam_utils::Backoff;
use crossbeam_queue::SegQueue;
use log::{debug, info, warn};
use tokio::sync::oneshot;
use crate::raft::raft_sm::CommitState;
use crate::service_utils::storage_utils::SerializationData;

pub struct QueryWorker {
    bus: BusReader<Arc<SerializationData>>,
    commit_state: Arc<CommitState>,
    query_structure: HashMap<String, Arc<String>>,
    pending_queries: Arc<SegQueue<QueryRequest>>
}

pub struct QueryResponse {
    pub value: Option<Arc<String>>
}

pub struct QueryRequest {
    pub id: String,
    pub index: u64,
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
            query_structure: HashMap::new(),
            pending_queries
        }
    }

    pub fn run(&mut self) {

        let mut last_version = 0;
        let mut parked: Option<Arc<SerializationData>> = None;
        let mut parked_request: Option<QueryRequest> = None;
        let mut applied_index: u64 = 0;
        let backoff = Backoff::new();
        loop {
            let mut num_entries_applied: u64 = 0;
            let mut queries_fulfilled: u64 = 0;

            let current_version = self.commit_state.version.load(Ordering::Acquire);
            if current_version != last_version {
                let index = self.commit_state.commit_index.load(Ordering::Acquire);
                last_version = current_version;
                match parked.take() {
                    None => {}
                    Some(parked_value) => {
                        if (parked_value.index <= index) {
                            for req in parked_value.requests.iter() {
                                self.query_structure.insert(req.id.clone(), Arc::new(req.payload.clone()));
                            }
                            applied_index = parked_value.index;
                            num_entries_applied += 1;
                        } else {
                            parked = Some(parked_value)
                        }
                    }
                }
                // Apply all new data up until new commit index.
                loop {
                    match self.bus.try_recv() {
                        Ok(val) => {
                            if val.index <= index {
                                for req in val.requests.iter() {
                                    self.query_structure.insert(req.id.clone(), Arc::new(req.payload.clone()));
                                    if val.index < applied_index {
                                        warn!("Received index {} less than previously applied index {}", val.index, applied_index);
                                    }
                                    applied_index = val.index;
                                }
                            } else if val.term == u64::MAX && val.term == u64::MAX {
                                // Truncate signal
                                self.query_structure.clear();
                                last_version = current_version;
                                applied_index = 0;
                                continue;
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
                debug!("Applied transaction log data to commit version: {}, number of updates applied {}", current_version, num_entries_applied)
            }

            // Process read requests
            match parked_request.take() {
                Some(query) => {
                    // Check if query index is ahead of applied index
                    if query.index <= applied_index {
                        // TODO verify presence of leader for linearizability.
                        self.handle_query(query);
                        queries_fulfilled += 1;
                    } else {
                        info!("Parked query with index {}, applied index {}", query.index, applied_index);
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
                        if query.index <= applied_index {
                            // TODO verify presence of leader for linearizability.
                            self.handle_query(query);
                            queries_fulfilled += 1;
                        } else {
                            // If read request has term or index that hasn't been applied park request and restart loop to apply
                            parked_request = Some(query);
                            break;
                        }
                    }
                    None => {
                        break; // Exit to main loop's backoff
                    }
                }
            }

            let work_done = num_entries_applied + queries_fulfilled;
            if work_done > 0 {
                backoff.reset();
            } else {
                if backoff.is_completed() {
                    thread::park_timeout(Duration::from_micros(500));
                    backoff.reset();
                } else {
                    backoff.snooze();
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