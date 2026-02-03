use std::sync::Arc;
use crossbeam_channel::{Receiver, TryRecvError};
use crossbeam_queue::SegQueue;
use log::{error, info, warn};
use tokio::sync::oneshot;
use tracing::{Level, Span};

use crate::quorum::types::{LocalQuorumResponse, LocalQuorumTaskResponseType};
use crate::raft::raft_sm::{
    LocalAppendEntries, LocalAppendEntriesCallbackResponse, LocalRaftMessage,
    LocalRaftMessagePayload, LocalRaftResponseMessage, LocalRaftResponsePayload,
    LocalRaftWriteBatchRequest, LocalRequestVoteRequest,
};
use crate::transport::capnp::owned_message::OwnedQuorumMessage;
use crate::transport::capnp::raft_capnp::remote_quorum_message;
use crate::transport::capnp::{
    build_owned_write_batch, build_owned_write_batch_response,
    build_vote_response_message, build_append_entries_ack_message,
    build_put_batch_response_message, build_truncate_log_response_message,
};
use crate::transport::metrics::{QUORUM_APPEND_ENTRIES_LATENCY, QUORUM_PUT_BATCH_LATENCY};
use crate::transport::raft::raftproto::RemoteLogEntry;
use crate::transport::write_proxy::WriteResponse;

use super::{OngoingBackfill, PendingQuorumTask, PendingQuorumTaskEnum, PendingWriteBatch};

const MAX_MESSAGES_PER_ITERATION: usize = 32;

/// Processes incoming messages from the remote node.
pub fn process_incoming_messages(
    inbound_rx: &mut Option<Receiver<OwnedQuorumMessage>>,
    pending_tasks: &mut Vec<PendingQuorumTask>,
    member_id: u64,
    self_id: u32,
    outbound_tx: &Option<crossbeam_channel::Sender<OwnedQuorumMessage>>,
    response_queue: &Arc<SegQueue<LocalQuorumResponse>>,
    task_queue: &Arc<SegQueue<LocalRaftMessage>>,
    pending_append_entries: &mut std::collections::HashMap<u64, std::time::Instant>,
    pending_write_batches: &mut std::collections::HashMap<String, PendingWriteBatch>,
    ongoing_backfill: &mut Option<OngoingBackfill>,
) {
    // First, collect messages to avoid borrow issues
    let (messages, disconnected) = {
        let rx = match inbound_rx {
            Some(rx) => rx,
            None => return,
        };

        let mut msgs = Vec::new();
        let mut is_disconnected = false;
        for _ in 0..MAX_MESSAGES_PER_ITERATION {
            match rx.try_recv() {
                Ok(msg) => msgs.push(msg),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    log::warn!("Inbound channel disconnected for member {}", member_id);
                    is_disconnected = true;
                    break;
                }
            }
        }
        (msgs, is_disconnected)
    };

    // Clear inbound_rx if channel was disconnected
    if disconnected {
        *inbound_rx = None;
    }

    // Now process collected messages
    for msg in messages {
        if let Err(e) = handle_inbound_message(
            &msg,
            pending_tasks,
            member_id,
            self_id,
            outbound_tx,
            response_queue,
            task_queue,
            pending_append_entries,
            pending_write_batches,
            ongoing_backfill,
        ) {
            log::warn!("Failed to handle inbound message: {:?}", e);
        }
    }
}

/// Routes an inbound message to the appropriate handler.
fn handle_inbound_message(
    msg: &OwnedQuorumMessage,
    pending_tasks: &mut Vec<PendingQuorumTask>,
    member_id: u64,
    self_id: u32,
    outbound_tx: &Option<crossbeam_channel::Sender<OwnedQuorumMessage>>,
    response_queue: &Arc<SegQueue<LocalQuorumResponse>>,
    task_queue: &Arc<SegQueue<LocalRaftMessage>>,
    pending_append_entries: &mut std::collections::HashMap<u64, std::time::Instant>,
    pending_write_batches: &mut std::collections::HashMap<String, PendingWriteBatch>,
    ongoing_backfill: &mut Option<OngoingBackfill>,
) -> capnp::Result<()> {
    use remote_quorum_message::message_payload::Which;

    msg.with_message(|reader| {
        let payload = reader.get_message_payload();
        match payload.which()? {
            Which::VoteResponse(resp) => {
                let resp = resp?;
                on_vote_response(
                    response_queue,
                    resp.get_request_id(),
                    resp.get_term(),
                    resp.get_vote_granted(),
                    member_id,
                );
            }
            Which::AppendEntriesAcknowledge(ack) => {
                let ack = ack?;
                on_append_entries_ack(
                    response_queue,
                    pending_append_entries,
                    ongoing_backfill,
                    member_id,
                    ack.get_request_id(),
                    ack.get_last_log_index(),
                    ack.get_last_log_term(),
                    ack.get_ok(),
                );
            }
            Which::PutBatchResponse(resp) => {
                let resp = resp?;
                on_put_batch_response_capnp(&resp, pending_write_batches, member_id);
            }
            Which::TruncateLogResponse(resp) => {
                let resp = resp?;
                on_truncate_log_response(
                    response_queue,
                    ongoing_backfill,
                    resp.get_request_id(),
                    resp.get_ok(),
                );
            }
            Which::AppendEntriesRequest(req) => {
                let req = req?;
                on_append_entries_request_capnp(&req, pending_tasks, task_queue);
            }
            Which::VoteRequest(req) => {
                let req = req?;
                on_vote_request(
                    pending_tasks,
                    task_queue,
                    req.get_term(),
                    req.get_candidate_id(),
                    req.get_last_log_index(),
                    req.get_last_log_term(),
                    req.get_request_id(),
                );
            }
            Which::PutBatchRequest(req) => {
                let req = req?;
                on_put_batch_request_capnp(&req, pending_tasks, task_queue);
            }
            Which::TruncateLogRequest(req) => {
                let req = req?;
                on_truncate_log_request(
                    response_queue,
                    outbound_tx,
                    member_id,
                    req.get_prev_term(),
                    req.get_prev_index(),
                    req.get_term(),
                    req.get_leader_id(),
                    req.get_request_id(),
                );
            }
            Which::ConnectRequest(_) | Which::ConnectResponse(_) => {
                // Ignore connection handshake messages
            }
        }
        Ok(())
    })?
}

// =========================================================================
// Response Handlers
// =========================================================================

fn on_vote_response(
    response_queue: &Arc<SegQueue<LocalQuorumResponse>>,
    request_id: u64,
    term: u64,
    vote_granted: bool,
    member_id: u64,
) {
    info!(
        "Received vote response for term {} with granted {} from member {}",
        term, vote_granted, member_id
    );
    response_queue.push(LocalQuorumResponse {
        response_type: LocalQuorumTaskResponseType::RequestVoteResponse {
            id: request_id,
            term,
            received_vote: vote_granted,
        },
    });
}

fn on_append_entries_ack(
    response_queue: &Arc<SegQueue<LocalQuorumResponse>>,
    pending_append_entries: &mut std::collections::HashMap<u64, std::time::Instant>,
    ongoing_backfill: &mut Option<OngoingBackfill>,
    member_id: u64,
    request_id: u64,
    last_log_index: u64,
    last_log_term: u64,
    ok: bool,
) {
    // Record latency if we have a pending request
    if let Some(start_time) = pending_append_entries.remove(&request_id) {
        let latency_micros = start_time.elapsed().as_micros() as f64;
        QUORUM_APPEND_ENTRIES_LATENCY
            .with_label_values(&[&member_id.to_string()])
            .observe(latency_micros);
    }

    if !ok {
        // Follower is behind, need backfill
        log::info!(
            "received append entries acknowledge from: {}, but it was not ok, backfilling log from term {} and index {}",
            member_id,
            last_log_term,
            last_log_index
        );
        if ongoing_backfill.is_none() {
            *ongoing_backfill = Some(OngoingBackfill {
                from_term: last_log_term,
                from_index: last_log_index,
            });
            response_queue.push(LocalQuorumResponse {
                response_type: LocalQuorumTaskResponseType::BackfillLog {
                    quorum_node_id: member_id,
                    last_log_term,
                    last_log_index,
                },
            });
        } else {
            log::warn!(
                "Ongoing backfill already in progress, ignoring new backfill request for term {} and index {}",
                last_log_term, last_log_index
            );
        }
    } else if request_id != 0 {
        response_queue.push(LocalQuorumResponse {
            response_type: LocalQuorumTaskResponseType::AppendEntries { id: request_id, ok },
        });
    }
}

fn on_put_batch_response_capnp(
    resp: &crate::transport::capnp::raft_capnp::remote_put_batch_response::Reader<'_>,
    pending_write_batches: &mut std::collections::HashMap<String, PendingWriteBatch>,
    member_id: u64,
) {
    let batch_id = resp
        .get_batch_id()
        .map(|s| s.to_string().unwrap_or_default())
        .unwrap_or_default();

    if let Some(pending) = pending_write_batches.remove(&batch_id) {
        let latency_micros = pending.start_time.elapsed().as_micros() as f64;
        QUORUM_PUT_BATCH_LATENCY
            .with_label_values(&[&member_id.to_string()])
            .observe(latency_micros);

        let message = build_owned_write_batch_response(|mut builder| {
            builder.set_batch_id(&batch_id);
            if let Ok(responses) = resp.get_responses() {
                let mut out_responses = builder.init_responses(responses.len());
                for (i, r) in responses.iter().enumerate() {
                    let mut out = out_responses.reborrow().get(i as u32);
                    if let Ok(id) = r.get_id() {
                        out.set_id(id);
                    }
                    if let Ok(response_type) = r.get_response_type() {
                        out.set_response_type(response_type);
                    }
                    if let Ok(message) = r.get_message() {
                        out.set_message(message);
                    }
                    out.set_node_id(r.get_node_id());
                    if let Ok(batch_id) = r.get_batch_id() {
                        out.set_batch_id(batch_id);
                    }
                }
            }
        });

        if pending.callback.send(WriteResponse::success(message)).is_err() {
            info!("Failed to respond to put batch callback, receiver dropped")
        }
    }
}

fn on_truncate_log_response(
    response_queue: &Arc<SegQueue<LocalQuorumResponse>>,
    ongoing_backfill: &mut Option<OngoingBackfill>,
    request_id: u64,
    ok: bool,
) {
    log::info!(
        "Received truncate log response for request {}, ok: {}",
        request_id,
        ok
    );
    response_queue.push(LocalQuorumResponse {
        response_type: LocalQuorumTaskResponseType::TruncateLogResponse { id: request_id, ok },
    });
    *ongoing_backfill = None;
}

// =========================================================================
// Request Handlers (for when we receive requests from remote nodes)
// =========================================================================

fn on_append_entries_request_capnp(
    req: &crate::transport::capnp::raft_capnp::remote_append_entries_request::Reader<'_>,
    pending_tasks: &mut Vec<PendingQuorumTask>,
    task_queue: &Arc<SegQueue<LocalRaftMessage>>,
) {
    let (callback_tx, callback_rx) = oneshot::channel();
    let span = tracing::span!(Level::INFO, "capnp_append_entries_request");

    let request_id = req.get_request_id();
    let req_term = req.get_term();
    let leader_id = req.get_leader_id();
    let prev_log_index = req.get_prev_log_index();
    let prev_log_term = req.get_prev_log_term();
    let commit_index = req.get_commit_index();

    log::debug!(
        "Received AppendEntries request_id={} term={} leader={} prev_idx={} has_entry={}",
        request_id,
        req_term,
        leader_id,
        prev_log_index,
        req.has_entry()
    );

    let (entry_index, entry_term, entry) = if req.has_entry() {
        if let Ok(e) = req.get_entry() {
            let data = e.get_data().map(|d| d.to_vec()).unwrap_or_default();
            log::debug!(
                "AppendEntries entry: index={} term={} data_size={}",
                e.get_index(),
                e.get_term(),
                data.len()
            );
            let proto_entry = RemoteLogEntry {
                index: e.get_index(),
                data,
                batch_index: e.get_batch_index(),
                term: e.get_term(),
                prev_log_term: e.get_prev_log_term(),
                prev_log_index: e.get_prev_log_index(),
                message_id: e.get_message_id(),
            };
            (e.get_index(), e.get_term(), Some(proto_entry))
        } else {
            log::warn!("AppendEntries has_entry=true but get_entry failed");
            (0, 0, None)
        }
    } else {
        (0, 0, None)
    };

    task_queue.push(LocalRaftMessage {
        payload: LocalRaftMessagePayload::AppendEntries(LocalAppendEntries {
            leader_id,
            term: req_term,
            prev_term: prev_log_term,
            prev_index: prev_log_index,
            request_id,
            entry,
            commit_index,
        }),
        callback: callback_tx,
        parent_span: span,
    });

    pending_tasks.push(PendingQuorumTask {
        callback: callback_rx,
        task: PendingQuorumTaskEnum::AppendEntries {
            request_id,
            log_index: entry_index,
            term: entry_term,
        },
        start_time: std::time::Instant::now(),
    });
}

fn on_truncate_log_request(
    response_queue: &Arc<SegQueue<LocalQuorumResponse>>,
    outbound_tx: &Option<crossbeam_channel::Sender<OwnedQuorumMessage>>,
    member_id: u64,
    prev_term: u64,
    prev_index: u64,
    _term: u64,
    leader_id: u32,
    request_id: u64,
) {
    log::info!(
        "Received truncate log request from leader {} for term {} index {}",
        leader_id,
        prev_term,
        prev_index
    );

    let resp = LocalQuorumTaskResponseType::TruncateLog {
        quorum_node_id: member_id,
        prev_term,
        prev_index,
    };
    response_queue.push(LocalQuorumResponse { response_type: resp });

    let msg = build_truncate_log_response_message(request_id, true);
    if !super::outbound::send_message(outbound_tx, msg, member_id) {
        warn!("Failed to send truncate log response back to leader")
    }
}

fn on_vote_request(
    pending_tasks: &mut Vec<PendingQuorumTask>,
    task_queue: &Arc<SegQueue<LocalRaftMessage>>,
    term: u64,
    candidate_id: u32,
    last_log_index: u64,
    last_log_term: u64,
    request_id: u64,
) {
    info!("Received vote request for term {} from member {}", term, candidate_id);
    let (callback_tx, callback_rx) = oneshot::channel();
    let span = tracing::span!(Level::INFO, "capnp_vote_request");

    task_queue.push(LocalRaftMessage {
        payload: LocalRaftMessagePayload::RequestVote(LocalRequestVoteRequest {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }),
        callback: callback_tx,
        parent_span: span,
    });

    pending_tasks.push(PendingQuorumTask {
        callback: callback_rx,
        task: PendingQuorumTaskEnum::Vote { request_id, term },
        start_time: std::time::Instant::now(),
    });
}

fn on_put_batch_request_capnp(
    req: &crate::transport::capnp::raft_capnp::remote_put_batch_request::Reader<'_>,
    pending_tasks: &mut Vec<PendingQuorumTask>,
    task_queue: &Arc<SegQueue<LocalRaftMessage>>,
) {
    let (callback_tx, callback_rx) = oneshot::channel();
    let span = tracing::span!(Level::INFO, "capnp_put_batch_request");
    let batch_id = req
        .get_batch_id()
        .map(|s| s.to_string().unwrap_or_default())
        .unwrap_or_default();

    let message = build_owned_write_batch(|mut builder| {
        builder.set_batch_id(&batch_id);
        if let Ok(put_requests) = req.get_put_requests() {
            let mut requests = builder.init_requests(put_requests.len());
            for (i, r) in put_requests.iter().enumerate() {
                let mut out = requests.reborrow().get(i as u32);
                if let Ok(id) = r.get_id() {
                    out.set_id(id);
                }
                if let Ok(payload) = r.get_payload() {
                    out.set_payload(payload.as_bytes());
                }
                out.set_node_id(r.get_node_id());
            }
        }
    });

    task_queue.push(LocalRaftMessage {
        payload: LocalRaftMessagePayload::WriteBatch(LocalRaftWriteBatchRequest { message }),
        callback: callback_tx,
        parent_span: span,
    });

    pending_tasks.push(PendingQuorumTask {
        callback: callback_rx,
        task: PendingQuorumTaskEnum::PutBatch { batch_id },
        start_time: std::time::Instant::now(),
    });
}
