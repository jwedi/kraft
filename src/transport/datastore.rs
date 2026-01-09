use std::future::Future;
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::thread;
use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::oneshot;
use uuid::Uuid;


use datastoreproto::datastore_server::{Datastore, DatastoreServer};
use log::{error, info, warn};
use tokio::sync::oneshot::{Receiver, Sender};
use crate::runtime_core::task_buffer::TaskBufferImpl;
use crate::runtime_core::types::{Command, RuntimeTask, RuntimeTaskResponse, CommandType};
use crate::transport::raft::raftproto::{RemotePutRequest, RemotePutResponse};
use crate::service_utils::errors::ServiceError;
use crate::raft::raft_sm::{RaftStateMachineExecutor, StateMachineExecutorImpl, RaftServerState, TermVote, RaftVolatileState, SharedState, LocalRaftMessage, LocalRaftMessagePayload, LocalRequestVoteRequest, LocalRaftResponseMessage, LocalRaftResponsePayload, LocalAppendEntries, LocalRaftWriteBatchRequest, CommitState};
use crossbeam_queue::SegQueue;
use tokio::sync::oneshot::error::RecvError;
use tracing::{field, instrument, Instrument, Level, Metadata, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::query::worker::{QueryRequest, QueryResponse};
use crate::transport::datastore::datastoreproto::{DataStoreRecord, GetDataStoreRecordRequest, GetDataStoreRecordResponse, PutDataStoreRecordRequest, PutDataStoreRecordResponse};
use crate::transport::write_proxy::{WriteBatch, WriteResponse};

pub mod datastoreproto {
    tonic::include_proto!("datastoreproto"); // The string specified here must match the proto package name
}

pub struct DatastoreServerImpl {
    pub task_queue: Arc<SegQueue<WriteBatch>>,
    pub query_queue: Arc<SegQueue<QueryRequest>>,
    pub commit_state: Arc<CommitState>
}

#[tonic::async_trait]
impl Datastore for DatastoreServerImpl {

    async fn get_record(&self, request: Request<GetDataStoreRecordRequest>) -> Result<Response<GetDataStoreRecordResponse>, Status> {
        let _timing_guard = crate::transport::metrics::record_rpc_request("get_record");
        let callback: (Sender<QueryResponse>, Receiver<QueryResponse>) = oneshot::channel();
        let commit_index = self.commit_state.commit_index.load(Ordering::Acquire);
        let term = self.commit_state.term.load(Ordering::Acquire);

        let req_id = request.into_inner().id;
        self.query_queue.push(QueryRequest{
            id: req_id.clone(),
            index: commit_index,
            term,
            callback: callback.0
        });
        let span = tracing::span!(Level::INFO, "awaiting_get_record");
        let receive_resp = callback.1.instrument(span).await;

        match receive_resp {
            Ok(resp) => {
                tracing::debug!("get record returned ok");
                match resp.value {
                    None => {
                        Ok(Response::new(GetDataStoreRecordResponse{
                            record: None
                        }))
                    }
                    Some(value) => {
                        Ok(Response::new((GetDataStoreRecordResponse{
                            record: Some(DataStoreRecord{
                                id: req_id,
                                payload: value.to_string()
                            })
                        })))

                    }
                }
            }
            Err(e) => {
                tracing::info!("get record receive error {}", e);
                Err(Status::internal(e.to_string()))
            }
        }
    }

    #[instrument(skip_all)]
    async fn put_record(&self, request: Request<PutDataStoreRecordRequest>) -> Result<Response<PutDataStoreRecordResponse>, Status> {
        let _timing_guard = crate::transport::metrics::record_rpc_request("put_record");
        tracing::debug!("received put_records request");

        let req = request.into_inner();
        let put_req = RemotePutRequest {
            id: req.key,
            node_id: 0,
            payload: req.value
        };
        let callback: (Sender<WriteResponse>, Receiver<WriteResponse>) = oneshot::channel();
        let span = tracing::span!(Level::INFO, "awaiting_put_record");
        let msg = WriteBatch{
            batch_id: Uuid::new_v4().to_string(),
            requests: vec![put_req],
            callback: callback.0,
            span_parent: Some(span.clone())
        };
        self.task_queue.push(msg);
        let mut receive_resp = callback.1.instrument(span).await;

        match receive_resp {
            Ok(mut resp) => {
                tracing::debug!("put record returned ok");
                match resp.responses.pop() {
                    None => {
                        Ok(Response::new(
                            PutDataStoreRecordResponse {
                                record: None
                            }
                        ))
                    }
                    Some(first) => {
                        Ok(Response::new(PutDataStoreRecordResponse {
                            record: Some(DataStoreRecord{id: first.id, payload: first.message})
                        }))
                    }
                }

            }
            Err(e) => {
                tracing::info!("put record receive error {}", e);
                Err(Status::internal(e.to_string()))
            }
        }
    }
}