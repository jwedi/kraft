//! Per-connection handler with pipelining support.
//!
//! Architecture:
//! - Reader loop spawns a handler task per request (parallel execution)
//! - Responses sent immediately when ready (out-of-order OK)
//! - Client matches responses to requests by request ID

use std::sync::atomic::Ordering;

use bytes::Bytes;
use log::{debug, error};
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::query::worker::{QueryRequest, QueryResponse};
use crate::transport::capnp::raft_capnp;
use crate::transport::capnp::{build_owned_write_batch, build_owned_write_batch_response};
use crate::transport::tcp_datastore::codec::{read_frame, write_frame};
use crate::transport::tcp_datastore::rpc::{RPC_GET_RECORD, RPC_PUT_RECORD};
use crate::transport::tcp_datastore::server::Queues;
use crate::transport::write_proxy::{WriteBatch, WriteResponse};

/// Response message to be written back to the client.
struct ResponseMessage {
    request_id: u64,
    rpc_type: u8,
    payload: Bytes,
}

/// Handle a single TCP connection with pipelining support.
pub async fn handle(stream: TcpStream, queues: Queues) -> std::io::Result<()> {
    let (mut reader, writer) = tokio::io::split(stream);
    let (response_tx, response_rx) = mpsc::unbounded_channel::<ResponseMessage>();

    // Spawn writer task - sends responses as they arrive (unordered by request ID)
    let writer_handle = tokio::spawn(async move {
        write_responses(writer, response_rx).await
    });

    // Reader loop - for each request, spawn handler task
    loop {
        let frame = match read_frame(&mut reader).await {
            Ok(frame) => frame,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Client disconnected gracefully
                debug!("TCP datastore: client disconnected");
                break;
            }
            Err(e) => {
                error!("TCP datastore: read error: {}", e);
                break;
            }
        };

        let (rpc_type, request_id, payload) = frame;
        let tx = response_tx.clone();
        let queues_clone = queues.clone();

        tokio::spawn(async move {
            let response_payload = dispatch_request(rpc_type, payload, &queues_clone).await;
            let _ = tx.send(ResponseMessage {
                request_id,
                rpc_type,
                payload: response_payload,
            });
        });
    }

    // Drop sender to signal writer to finish
    drop(response_tx);

    // Wait for writer to complete
    let _ = writer_handle.await;

    Ok(())
}

/// Writer task: sends responses as they arrive.
async fn write_responses(
    mut writer: WriteHalf<TcpStream>,
    mut rx: mpsc::UnboundedReceiver<ResponseMessage>,
) {
    while let Some(msg) = rx.recv().await {
        if let Err(e) = write_frame(&mut writer, msg.rpc_type, msg.request_id, &msg.payload).await {
            error!("TCP datastore: write error: {}", e);
            break;
        }
    }
    // Flush and shutdown gracefully
    let _ = writer.shutdown().await;
}

/// Dispatch a request to the appropriate handler based on RPC type.
async fn dispatch_request(rpc_type: u8, payload: Bytes, queues: &Queues) -> Bytes {
    match rpc_type {
        RPC_PUT_RECORD => handle_put_record(payload, queues).await,
        RPC_GET_RECORD => handle_get_record(payload, queues).await,
        _ => {
            error!("TCP datastore: unknown RPC type: {}", rpc_type);
            // Return empty response for unknown RPC types
            Bytes::new()
        }
    }
}

/// Handle a PUT record request.
async fn handle_put_record(payload: Bytes, queues: &Queues) -> Bytes {
    // Parse Cap'n Proto request
    let (key, value) = match parse_put_request(&payload) {
        Ok((k, v)) => (k, v),
        Err(e) => {
            error!("TCP datastore: failed to parse PUT request: {}", e);
            return build_error_put_response("parse error");
        }
    };

    let batch_id = Uuid::new_v4().to_string();

    // Build Cap'n Proto write batch
    let write_batch_message = build_owned_write_batch(|mut builder| {
        builder.set_batch_id(&batch_id);
        let mut requests = builder.init_requests(1);
        let mut put_req = requests.get(0);
        put_req.set_id(&key);
        put_req.set_payload(&value);
        put_req.set_node_id(0);
    });

    // Create callback channel
    let (callback_tx, callback_rx) = oneshot::channel::<WriteResponse>();

    let msg = WriteBatch {
        batch_id: batch_id.clone(),
        message: write_batch_message,
        callback: callback_tx,
        span_parent: None,
    };

    // Push to task queue
    queues.task_queue.push(msg);

    // Wait for response
    match callback_rx.await {
        Ok(resp) => build_put_response(&resp),
        Err(_) => {
            error!("TCP datastore: PUT callback channel closed");
            build_error_put_response("internal error")
        }
    }
}

/// Handle a GET record request.
async fn handle_get_record(payload: Bytes, queues: &Queues) -> Bytes {
    // Parse Cap'n Proto request
    let key = match parse_get_request(&payload) {
        Ok(k) => k,
        Err(e) => {
            error!("TCP datastore: failed to parse GET request: {}", e);
            return build_get_response(false, "", &[]);
        }
    };

    let commit_index = queues.commit_state.commit_index.load(Ordering::Acquire);
    let (callback_tx, callback_rx) = oneshot::channel::<QueryResponse>();

    let query = QueryRequest {
        id: key.clone(),
        index: commit_index,
        callback: callback_tx,
    };

    // Push to query queue
    queues.query_queue.push(query);

    // Wait for response
    match callback_rx.await {
        Ok(resp) => match resp.value {
            Some(value) => build_get_response(true, &key, &value),
            None => build_get_response(false, &key, &[]),
        },
        Err(_) => {
            error!("TCP datastore: GET callback channel closed");
            build_get_response(false, &key, &[])
        }
    }
}

/// Parse a PUT request from Cap'n Proto bytes.
fn parse_put_request(payload: &[u8]) -> capnp::Result<(String, Vec<u8>)> {
    let mut slice: &[u8] = payload;
    let reader = capnp::serialize::read_message_from_flat_slice(&mut slice, Default::default())?;
    let msg = reader.get_root::<raft_capnp::internal_put_request::Reader<'_>>()?;
    let key = msg.get_id()?.to_string()?;
    let value = msg.get_payload()?.to_vec();
    Ok((key, value))
}

/// Parse a GET request from Cap'n Proto bytes.
fn parse_get_request(payload: &[u8]) -> capnp::Result<String> {
    let mut slice: &[u8] = payload;
    let reader = capnp::serialize::read_message_from_flat_slice(&mut slice, Default::default())?;
    let msg = reader.get_root::<raft_capnp::datastore_get_request::Reader<'_>>()?;
    let key = msg.get_id()?.to_string()?;
    Ok(key)
}

/// Build a PUT response as Cap'n Proto bytes.
fn build_put_response(resp: &WriteResponse) -> Bytes {
    // Convert the OwnedWriteBatchResponse to bytes for wire transmission
    Bytes::copy_from_slice(resp.message.as_bytes())
}

/// Build an error PUT response.
#[allow(dead_code)]
fn build_error_put_response(message: &str) -> Bytes {
    let response = build_owned_write_batch_response(|mut builder| {
        builder.set_batch_id("");
        let mut responses = builder.init_responses(1);
        let mut resp = responses.get(0);
        resp.set_id("");
        resp.set_response_type(raft_capnp::RemoteResponseType::Invalid);
        resp.set_message(message);
        resp.set_node_id(0);
        resp.set_batch_id("");
    });
    Bytes::copy_from_slice(response.as_bytes())
}

/// Build a GET response as Cap'n Proto bytes.
fn build_get_response(found: bool, id: &str, payload: &[u8]) -> Bytes {
    let mut builder = capnp::message::Builder::new_default();
    {
        let mut msg = builder.init_root::<raft_capnp::datastore_get_response::Builder>();
        msg.set_found(found);
        msg.set_id(id);
        msg.set_payload(payload);
    }
    let mut bytes = Vec::new();
    capnp::serialize::write_message(&mut bytes, &builder).expect("write should not fail");
    Bytes::from(bytes)
}
