use std::sync::Arc;
use crossbeam_channel::Sender;
use log::error;
use tracing::Span;

use crate::service_utils::app_time::now_millis;
use crate::transport::capnp::owned_message::OwnedQuorumMessage;
use crate::transport::capnp::{
    build_heartbeat_message, build_vote_request_message,
    build_truncate_log_request_message,
    build_append_entries_message, OwnedLogEntry,
};
use crate::transport::write_proxy::WriteBatch;
use super::PendingWriteBatch;

const HEARTBEAT_INTERVAL_MS: u128 = 100;

/// Sends a message through the outbound channel.
/// Returns true if the message was sent successfully.
pub fn send_message(
    outbound_tx: &Option<Sender<OwnedQuorumMessage>>,
    msg: OwnedQuorumMessage,
    member_id: u64,
) -> bool {
    if let Some(ref tx) = outbound_tx {
        if tx.send(msg).is_err() {
            log::warn!("Failed to send message to member {}", member_id);
            false
        } else {
            true
        }
    } else {
        log::warn!(
            "failed to send message to member {}, outbound_tx is empty",
            member_id
        );
        false
    }
}

/// Sends a heartbeat if the interval has passed and heartbeats are enabled.
pub fn send_heartbeat_if_needed(
    send_heartbeats: bool,
    last_heartbeat: &mut u128,
    term: u64,
    self_id: u32,
    prev_log_index: u64,
    prev_log_term: u64,
    commit_index: u64,
    outbound_tx: &Option<Sender<OwnedQuorumMessage>>,
    member_id: u64,
) {
    if !send_heartbeats {
        return;
    }

    let now = now_millis();
    if now - *last_heartbeat < HEARTBEAT_INTERVAL_MS {
        return;
    }

    *last_heartbeat = now;

    let msg = build_heartbeat_message(
        term,
        self_id,
        prev_log_index,
        prev_log_term,
        commit_index,
        0, // request_id = 0 for heartbeat
    );
    send_message(outbound_tx, msg, member_id);
}

/// Sends a vote request to the remote member.
pub fn send_vote_request(
    outbound_tx: &Option<Sender<OwnedQuorumMessage>>,
    member_id: u64,
    self_id: u32,
    id: u64,
    term: u64,
    last_index: u64,
    last_term: u64,
    _parent_span: Span,
) {
    log::info!(
        "sending request vote with request id {} for term {} to member {}",
        id,
        term,
        member_id
    );
    let msg = build_vote_request_message(term, self_id, last_index, last_term, id);
    send_message(outbound_tx, msg, member_id);
}

/// Sends an append entries request to the remote member.
pub fn send_append_entries(
    outbound_tx: &Option<Sender<OwnedQuorumMessage>>,
    member_id: u64,
    self_id: u32,
    term: u64,
    prev_log_index: &mut u64,
    prev_log_term: &mut u64,
    commit_index: &mut u64,
    pending_append_entries: &mut std::collections::HashMap<u64, std::time::Instant>,
    last_heartbeat: &mut u128,
    id: u64,
    entry: &Arc<crate::transport::capnp::OwnedLogEntry>,
    new_commit_index: u64,
    _parent_span: Span,
) {
    let request_id = id;
    let prev_idx = entry.prev_log_index();
    if *prev_log_index != prev_idx {
        error!(
            "Trying to send non-consecutive append entries request, prev index {} request prev index {}, id {}",
            *prev_log_index, prev_idx, id
        );
    }

    *prev_log_term = entry.term();
    *prev_log_index = entry.index();
    *commit_index = new_commit_index;
    pending_append_entries.insert(request_id, std::time::Instant::now());

    let msg = build_append_entries_message(
        term,
        self_id,
        prev_idx,
        entry.prev_log_term(),
        request_id,
        new_commit_index,
        Some(entry),
    );

    if !send_message(outbound_tx, msg, member_id) {
        error!("Sending append entries message failed")
    }
    *last_heartbeat = now_millis();
}

/// Sends a write batch to the remote member.
pub fn send_write_batch(
    outbound_tx: &Option<Sender<OwnedQuorumMessage>>,
    member_id: u64,
    pending_write_batches: &mut std::collections::HashMap<String, PendingWriteBatch>,
    write_batch: WriteBatch,
) {
    let batch_id = write_batch.batch_id.clone();

    pending_write_batches.insert(
        batch_id.clone(),
        PendingWriteBatch {
            callback: write_batch.callback,
            start_time: std::time::Instant::now(),
        },
    );

    let msg = crate::transport::capnp::build_put_batch_request_message(&write_batch.message);
    send_message(outbound_tx, msg, member_id);
}

/// Sends a truncate log request to the remote member.
pub fn send_truncate_log(
    outbound_tx: &Option<Sender<OwnedQuorumMessage>>,
    member_id: u64,
    self_id: u32,
    term: u64,
    prev_term: u64,
    prev_index: u64,
    _parent_span: Span,
) {
    let msg = build_truncate_log_request_message(prev_term, prev_index, term, self_id, 0);
    send_message(outbound_tx, msg, member_id);
}

/// Sends backfill entries to the remote member.
/// Now uses OwnedLogEntry directly (already Cap'n Proto format).
pub fn send_backfill_entries(
    outbound_tx: &Option<Sender<OwnedQuorumMessage>>,
    member_id: u64,
    self_id: u32,
    term: u64,
    commit_index: u64,
    last_heartbeat: &mut u128,
    ongoing_backfill: &mut Option<super::OngoingBackfill>,
    _max_message_size_bytes: usize,
    entries: Vec<Arc<OwnedLogEntry>>,
) {
    if entries.is_empty() {
        return;
    }

    let first = entries.first();
    log::info!(
        "received backfill log request with {} entries. First entry: {}",
        entries.len(),
        first.map_or("None".to_string(), |e| format!(
            "prev_term: {}, prev_index: {}",
            e.prev_log_term(), e.prev_log_index()
        ))
    );

    for entry in entries {
        let msg = build_append_entries_message(
            term,
            self_id,
            entry.prev_log_index(),
            entry.prev_log_term(),
            entry.index(),
            commit_index,
            Some(&entry),
        );

        if !send_message(outbound_tx, msg, member_id) {
            break;
        }
    }
    *ongoing_backfill = None;
    *last_heartbeat = now_millis();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;

    #[test]
    fn test_send_heartbeat_when_last_heartbeat_is_zero() {
        // When last_heartbeat is 0, heartbeat should be sent immediately
        let (tx, rx) = unbounded();
        let outbound_tx = Some(tx);
        let mut last_heartbeat: u128 = 0;

        send_heartbeat_if_needed(
            true, // send_heartbeats enabled
            &mut last_heartbeat,
            1,    // term
            1,    // self_id
            0,    // prev_log_index
            0,    // prev_log_term
            5,    // commit_index
            &outbound_tx,
            2,    // member_id
        );

        // Heartbeat should have been sent
        assert!(rx.try_recv().is_ok(), "Expected heartbeat to be sent when last_heartbeat is 0");
        // last_heartbeat should be updated to current time
        assert!(last_heartbeat > 0, "Expected last_heartbeat to be updated");
    }

    #[test]
    fn test_no_heartbeat_when_recently_sent() {
        // When heartbeat was recently sent, no new heartbeat should be sent
        let (tx, rx) = unbounded();
        let outbound_tx = Some(tx);
        let mut last_heartbeat = now_millis(); // Just sent

        send_heartbeat_if_needed(
            true,
            &mut last_heartbeat,
            1, 1, 0, 0, 5,
            &outbound_tx,
            2,
        );

        // No heartbeat should have been sent
        assert!(rx.try_recv().is_err(), "Expected no heartbeat when recently sent");
    }

    #[test]
    fn test_no_heartbeat_when_disabled() {
        // When send_heartbeats is false, no heartbeat should be sent
        let (tx, rx) = unbounded();
        let outbound_tx = Some(tx);
        let mut last_heartbeat: u128 = 0;

        send_heartbeat_if_needed(
            false, // send_heartbeats disabled
            &mut last_heartbeat,
            1, 1, 0, 0, 5,
            &outbound_tx,
            2,
        );

        // No heartbeat should have been sent
        assert!(rx.try_recv().is_err(), "Expected no heartbeat when disabled");
        // last_heartbeat should remain unchanged
        assert_eq!(last_heartbeat, 0);
    }

    #[test]
    fn test_heartbeat_after_interval_elapsed() {
        // When HEARTBEAT_INTERVAL_MS has elapsed, heartbeat should be sent
        let (tx, rx) = unbounded();
        let outbound_tx = Some(tx);
        // Set last_heartbeat to well in the past
        let mut last_heartbeat = now_millis().saturating_sub(HEARTBEAT_INTERVAL_MS + 10);

        send_heartbeat_if_needed(
            true,
            &mut last_heartbeat,
            1, 1, 0, 0, 5,
            &outbound_tx,
            2,
        );

        // Heartbeat should have been sent
        assert!(rx.try_recv().is_ok(), "Expected heartbeat to be sent after interval elapsed");
    }

    #[test]
    fn test_resetting_last_heartbeat_forces_immediate_send() {
        // This simulates the UpdateCommitIndex behavior:
        // 1. First, send a heartbeat (last_heartbeat gets updated)
        // 2. Then reset last_heartbeat to 0 (simulating UpdateCommitIndex)
        // 3. Next heartbeat check should send immediately
        let (tx, rx) = unbounded();
        let outbound_tx = Some(tx);
        let mut last_heartbeat: u128 = 0;

        // First heartbeat
        send_heartbeat_if_needed(true, &mut last_heartbeat, 1, 1, 0, 0, 5, &outbound_tx, 2);
        assert!(rx.try_recv().is_ok(), "First heartbeat should be sent");

        // Verify no immediate second heartbeat
        let saved_heartbeat = last_heartbeat;
        send_heartbeat_if_needed(true, &mut last_heartbeat, 1, 1, 0, 0, 5, &outbound_tx, 2);
        assert!(rx.try_recv().is_err(), "Second heartbeat should not be sent immediately");

        // Reset last_heartbeat to 0 (simulating UpdateCommitIndex)
        last_heartbeat = 0;

        // Now heartbeat should be sent immediately with updated commit_index
        send_heartbeat_if_needed(true, &mut last_heartbeat, 1, 1, 0, 0, 10, &outbound_tx, 2);
        assert!(rx.try_recv().is_ok(), "Heartbeat should be sent after last_heartbeat reset");
    }
}
