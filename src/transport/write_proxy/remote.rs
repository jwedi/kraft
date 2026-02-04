use std::collections::HashMap;
use std::sync::Arc;
use crossbeam_queue::SegQueue;
use log::warn;
use opentelemetry::trace::TraceContextExt;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{Instrument, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::quorum::types::{LocalQuorumWorkerTask, LocalQuorumWorkerTaskType};
use crate::raft::raft_sm::{LocalRaftResponseMessage, LocalRaftResponsePayload};
use crate::transport::capnp::OwnedWriteBatch;

use super::batch::respond_to_batch_callbacks;
use super::{WriteBatch, WriteResponse};

/// Sends a write batch via quorum worker and handles the response.
pub async fn send_quorum_put_batch_and_handle_response(
    leader_quorum_worker: Arc<SegQueue<LocalQuorumWorkerTask>>,
    batch_id: String,
    batch: OwnedWriteBatch,
    batch_callbacks: HashMap<String, Sender<WriteResponse>>,
    span: Span,
) {
    let s = span.clone();
    let callback: (Sender<WriteResponse>, Receiver<WriteResponse>) = oneshot::channel();
    let wb = LocalQuorumWorkerTaskType::WriteBatch {
        write_batch: WriteBatch {
            batch_id: batch_id.clone(),
            message: batch,
            span_parent: Some(span.clone()),
            callback: callback.0,
        },
    };
    leader_quorum_worker.push(LocalQuorumWorkerTask { task_type: wb });
    let resp = callback.1.instrument(s).await;
    match resp {
        Ok(put_response) => {
            let write_callback_span = tracing::span!(Level::INFO, "write_proxy_remote_success_callbacks");
            write_callback_span.set_parent(span.context());
            write_callback_span.in_scope(|| {
                tracing::event!(Level::INFO, "sending successful callback");
                respond_to_batch_callbacks(put_response.message, batch_callbacks)
            })
        }
        Err(e) => {
            tracing::error!("Remote PutBatch request failed with error: {}", e);
            log::error!("Remote PutBatch request failed with error: {}", e);
            for (_, callback) in batch_callbacks {
                let msg = WriteResponse::error();
                if callback.send(msg).is_err() {
                    warn!("Failed to send write batch response on channel, receiver dropped")
                }
            }
        }
    }
}

/// Handles the local write response callback.
pub async fn handle_local_write_response(
    receiver: Receiver<LocalRaftResponseMessage>,
    parent_span: Span,
    batch_callbacks: HashMap<String, Sender<WriteResponse>>,
) {
    let span = tracing::span!(Level::INFO, "write_proxy_local_batch_forward_response_await");
    span.set_parent(parent_span.context());
    let resp = receiver.instrument(span).await;
    match resp {
        Ok(ok_runtime_resp) => {
            tracing::debug!("raft batch handling completed");
            handle_insert_completed(ok_runtime_resp, batch_callbacks)
        }
        Err(e) => {
            tracing::error!("receiving raft message in proxy failed with error, {}", e);
        }
    }
}

/// Handles a completed insert by dispatching responses to callbacks.
fn handle_insert_completed(
    msg: LocalRaftResponseMessage,
    batch_callbacks: HashMap<String, Sender<WriteResponse>>,
) {
    match msg.payload {
        LocalRaftResponsePayload::WriteBatch(response) => {
            respond_to_batch_callbacks(response.message, batch_callbacks)
        }
        _ => {
            batch_callbacks.into_iter().for_each(|callback| {
                let msg = WriteResponse::error();
                if callback.1.send(msg).is_err() {
                    log::warn!("Sending write response callback failed because the receiver dropped");
                }
            });
        }
    }
}
