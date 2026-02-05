use crossbeam_channel::Sender;
use log::error;
use tokio::sync::oneshot;

use crate::quorum::types::{LocalQuorumResponse, LocalQuorumTaskResponseType};
use crate::raft::raft_sm::{LocalAppendEntriesCallbackResponse, LocalRaftResponseMessage, LocalRaftResponsePayload};
use crate::transport::capnp::owned_message::OwnedQuorumMessage;
use crate::transport::capnp::{
    build_append_entries_ack_message, build_put_batch_response_message, build_vote_response_message,
};

use super::{PendingQuorumTask, PendingQuorumTaskEnum};

/// Processes pending task responses.
pub fn process_pending_task_responses(
    pending_tasks: &mut Vec<PendingQuorumTask>,
    outbound_tx: &Option<Sender<OwnedQuorumMessage>>,
    response_queue: &std::sync::Arc<crossbeam_queue::SegQueue<LocalQuorumResponse>>,
    member_id: u64,
) {
    let mut to_remove = vec![];

    for (i, pt) in pending_tasks.iter_mut().enumerate() {
        match pt.callback.try_recv() {
            Ok(response) => {
                handle_pending_task_response(&pt.task, response, outbound_tx, response_queue, member_id);
                to_remove.push(i);
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                // Still waiting for response
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                log::warn!("Pending task callback channel closed without response");
                to_remove.push(i);
            }
        }
    }

    // Remove completed/closed tasks (reverse order to maintain indices)
    for i in to_remove.into_iter().rev() {
        pending_tasks.remove(i);
    }
}

/// Handles a pending task response by sending the appropriate message back.
fn handle_pending_task_response(
    task: &PendingQuorumTaskEnum,
    response: LocalRaftResponseMessage,
    outbound_tx: &Option<Sender<OwnedQuorumMessage>>,
    response_queue: &std::sync::Arc<crossbeam_queue::SegQueue<LocalQuorumResponse>>,
    member_id: u64,
) {
    match response.payload {
        LocalRaftResponsePayload::RequestVote(granted) => {
            if let PendingQuorumTaskEnum::Vote { request_id, term } = task {
                let msg = build_vote_response_message(granted, *request_id, *term);
                super::outbound::send_message(outbound_tx, msg, member_id);
            }
        }
        LocalRaftResponsePayload::AppendEntries(ae_resp) => {
            if let PendingQuorumTaskEnum::AppendEntries {
                request_id,
                log_index,
                term,
            } = task
            {
                let (ok, last_log_index, last_log_term) = match ae_resp {
                    LocalAppendEntriesCallbackResponse::Ok => (true, *log_index, *term),
                    LocalAppendEntriesCallbackResponse::UnrecognizedLeader => (false, *log_index, *term),
                    LocalAppendEntriesCallbackResponse::WantedPreviousEntry { last_term, last_index } => {
                        (false, last_index, last_term)
                    }
                    LocalAppendEntriesCallbackResponse::TruncateLog { prev_term, prev_index } => {
                        // Also notify local response queue about truncation
                        response_queue.push(LocalQuorumResponse {
                            response_type: LocalQuorumTaskResponseType::TruncateLog {
                                quorum_node_id: member_id,
                                prev_term,
                                prev_index,
                            },
                        });
                        (false, prev_index, prev_term)
                    }
                };

                log::debug!(
                    "Sending AppendEntries ack request_id={} ok={} last_log_index={} last_log_term={}",
                    request_id,
                    ok,
                    last_log_index,
                    last_log_term
                );
                let msg = build_append_entries_ack_message(last_log_term, last_log_index, *request_id, ok);
                super::outbound::send_message(outbound_tx, msg, member_id);
            } else {
                error!("Something bricked with LocalRaftResponsePayload::AppendEntries pending task doesn't match response type")
            }
        }
        LocalRaftResponsePayload::WriteBatch(write_resp) => {
            if let PendingQuorumTaskEnum::PutBatch { batch_id: id } = task {
                let msg = build_put_batch_response_message(&write_resp.message, id);
                super::outbound::send_message(outbound_tx, msg, member_id);
            } else {
                error!("Something bricked with LocalRaftResponsePayload::WriteBatch pending task doesn't match response type")
            }
        }
        _ => {
            log::warn!("Unexpected response payload type for pending task");
        }
    }
}
