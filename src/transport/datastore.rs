use std::future::Future;
use std::sync::{Arc, Mutex};
use std::thread;
use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::oneshot;
use uuid::Uuid;


use datastoreproto::datastore_server::{Datastore, DatastoreServer};
use log::{info, warn};
use tokio::sync::oneshot::{Receiver, Sender};
use crate::runtime_core::task_buffer::TaskBufferImpl;
use crate::runtime_core::types::{Command, RuntimeTask, RuntimeTaskResponse, CommandType};
use crate::server::raftproto::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use crate::service_utils::errors::ServiceError;
use crate::raft::raft_sm::{RaftStateMachineExecutor, StateMachineExecutorImpl, RaftServerState, TermVote, RaftVolatileState, SharedState, RaftMessage, RaftMessagePayload, RequestVoteRequest, RaftResponseMessage, RaftResponsePayload, AppendEntries, RaftWriteBatchRequest};
use crossbeam_queue::SegQueue;
use tokio::sync::oneshot::error::RecvError;
use tracing::{field, instrument, Instrument, Level, Metadata, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::transport::datastore::datastoreproto::{DataStoreRecord, GetDataStoreRecordRequest, GetDataStoreRecordResponse, PutDataStoreRecordRequest, PutDataStoreRecordResponse};
use crate::transport::write_proxy::{WriteBatch, WriteResponse};

pub mod datastoreproto {
    tonic::include_proto!("datastoreproto"); // The string specified here must match the proto package name
}

#[derive(Debug)]
pub struct DatastoreServerImpl {
    pub task_queue: Arc<SegQueue<WriteBatch>> // TODO
}

#[tonic::async_trait]
impl Datastore for DatastoreServerImpl {

    // get_record can be served locally and maybe not even by the main worker thread
    // put_record will most likely be forwarded to the leader.
    // put record need to be smart batched before forwarding to append entries.
    // maybe forwarder queue that batches requests and either forwards remotely or locally?

    // Batchable messages like Raft append entries having a different work queue than control messages might make life easier.
    // Alternative would be to have a local carry-over variable and a local poll that checks carry-over and then queue

    async fn get_record(&self, request: Request<GetDataStoreRecordRequest>) -> Result<Response<GetDataStoreRecordResponse>, Status> {
        todo!()
    }

    #[instrument(skip_all)]
    async fn put_record(&self, request: Request<PutDataStoreRecordRequest>) -> Result<Response<PutDataStoreRecordResponse>, Status> {
        tracing::debug!("received put_records request");

        //let current = Span::current();
        //Span::new_root(current.metadata().unwrap(), &field::ValueSet{});
        let req = request.into_inner();
        let callback: (Sender<WriteResponse>, Receiver<WriteResponse>) = oneshot::channel();
        let span = tracing::span!(Level::INFO, "awaiting_put_record");
        let msg = WriteBatch{
            batch_id: Uuid::new_v4().to_string(),
            requests: vec![],
            callback: callback.0,
            span_parent: Some(span.clone())
        };
        self.task_queue.push(msg);
        let receive_resp = callback.1.instrument(span).await;

        let ok: bool = match receive_resp {
            Ok(resp) => {
                tracing::debug!("put record returned ok");
                resp.status_code.is_success()
            }
            Err(e) => {
                tracing::info!("put record receive error {}", e);
                false
            }
        };
        let response = PutDataStoreRecordResponse {
            record: Some(DataStoreRecord{id: "1234".to_string(), payload: "some_payload".to_string()})
        };
        Ok(Response::new(response))
    }
}