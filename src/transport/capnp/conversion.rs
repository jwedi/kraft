use crate::transport::raft::raftproto::{
    RemoteAppendEntriesRequest, RemoteAppendEntriesAcknowledge,
    RemoteVoteRequest, RemoteVoteResponse,
    RemotePutBatchRequest, RemotePutBatchResponse,
    RemotePutRequest, RemotePutResponse,
    RemoteTruncateLogRequest, RemoteTruncateLogResponse,
    RemoteLogEntry, RemoteResponseType,
    TracingContext, TracingContextEntry,
};
use super::raft_capnp::{
    remote_quorum_message, RemoteResponseType as CapnpRemoteResponseType,
};
use super::owned_message::{OwnedQuorumMessage, build_owned_message};

/// Handles conversion between Cap'n Proto and Prost message types
pub struct CapnpProstConverter;

impl CapnpProstConverter {
    pub fn new() -> Self {
        Self
    }

    // =========================================================================
    // Prost -> Cap'n Proto (for outbound messages)
    // =========================================================================

    pub fn vote_request_to_capnp(&self, req: &RemoteVoteRequest) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            let mut v = msg.init_message_payload().init_vote_request();
            v.set_term(req.term);
            v.set_candidate_id(req.candidate_id);
            v.set_last_log_index(req.last_log_index);
            v.set_last_log_term(req.last_log_term);
            v.set_request_id(req.request_id);
        })
    }

    pub fn vote_response_to_capnp(&self, resp: &RemoteVoteResponse) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            let mut v = msg.init_message_payload().init_vote_response();
            v.set_vote_granted(resp.vote_granted);
            v.set_request_id(resp.request_id);
            v.set_term(resp.term);
        })
    }

    pub fn append_entries_to_capnp(&self, req: &RemoteAppendEntriesRequest) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            let mut ae = msg.init_message_payload().init_append_entries_request();
            ae.set_term(req.term);
            ae.set_leader_id(req.leader_id);
            ae.set_prev_log_index(req.prev_log_index);
            ae.set_prev_log_term(req.prev_log_term);
            ae.set_request_id(req.request_id);
            ae.set_commit_index(req.commit_index);

            if let Some(ref entry) = req.entry {
                let mut e = ae.init_entry();
                e.set_index(entry.index);
                e.set_data(&entry.data);
                e.set_batch_index(entry.batch_index);
                e.set_term(entry.term);
                e.set_prev_log_term(entry.prev_log_term);
                e.set_prev_log_index(entry.prev_log_index);
                e.set_message_id(entry.message_id);
            }
        })
    }

    pub fn append_entries_ack_to_capnp(&self, ack: &RemoteAppendEntriesAcknowledge) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            let mut a = msg.init_message_payload().init_append_entries_acknowledge();
            a.set_last_log_term(ack.last_log_term);
            a.set_last_log_index(ack.last_log_index);
            a.set_request_id(ack.request_id);
            a.set_ok(ack.ok);
        })
    }

    pub fn put_batch_request_to_capnp(&self, req: &RemotePutBatchRequest) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            let mut pb = msg.init_message_payload().init_put_batch_request();
            pb.set_batch_id(&req.batch_id);

            let mut requests = pb.init_put_requests(req.put_request.len() as u32);
            for (i, pr) in req.put_request.iter().enumerate() {
                let mut r = requests.reborrow().get(i as u32);
                r.set_id(&pr.id);
                r.set_payload(&pr.payload);
                r.set_node_id(pr.node_id);
            }
        })
    }

    pub fn put_batch_response_to_capnp(&self, resp: &RemotePutBatchResponse) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            let mut pb = msg.init_message_payload().init_put_batch_response();
            pb.set_batch_id(&resp.batch_id);

            let mut responses = pb.init_responses(resp.responses.len() as u32);
            for (i, pr) in resp.responses.iter().enumerate() {
                let mut r = responses.reborrow().get(i as u32);
                r.set_id(&pr.id);
                r.set_response_type(match RemoteResponseType::try_from(pr.response_type) {
                    Ok(RemoteResponseType::Ok) => CapnpRemoteResponseType::Ok,
                    Ok(RemoteResponseType::Invalid) => CapnpRemoteResponseType::Invalid,
                    _ => CapnpRemoteResponseType::Unspecified,
                });
                r.set_message(&pr.message);
                r.set_node_id(pr.node_id);
                r.set_batch_id(&pr.batch_id);
            }
        })
    }

    pub fn truncate_log_request_to_capnp(&self, req: &RemoteTruncateLogRequest) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            let mut t = msg.init_message_payload().init_truncate_log_request();
            t.set_prev_term(req.prev_term);
            t.set_prev_index(req.prev_index);
            t.set_term(req.term);
            t.set_leader_id(req.leader_id);
            t.set_request_id(req.request_id);
        })
    }

    pub fn truncate_log_response_to_capnp(&self, resp: &RemoteTruncateLogResponse) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            let mut t = msg.init_message_payload().init_truncate_log_response();
            t.set_request_id(resp.request_id);
            t.set_ok(resp.ok);
        })
    }

    pub fn connect_request_to_capnp(&self, node_id: u32) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            msg.init_message_payload().init_connect_request().set_node_id(node_id);
        })
    }

    pub fn connect_response_to_capnp(&self, ok: bool) -> OwnedQuorumMessage {
        build_owned_message(|msg| {
            msg.init_message_payload().init_connect_response().set_ok(ok);
        })
    }

    // =========================================================================
    // Cap'n Proto -> Prost (for inbound messages)
    // =========================================================================

    /// Enum representing converted inbound message payloads
    pub fn process_inbound_message(&self, msg: &OwnedQuorumMessage) -> capnp::Result<InboundMessagePayload> {
        msg.with_message(|reader| {
            use remote_quorum_message::message_payload::Which;

            match reader.get_message_payload().which()? {
                Which::ConnectRequest(req) => {
                    let req = req?;
                    Ok(InboundMessagePayload::ConnectRequest {
                        node_id: req.get_node_id()
                    })
                }
                Which::ConnectResponse(resp) => {
                    let resp = resp?;
                    Ok(InboundMessagePayload::ConnectResponse {
                        ok: resp.get_ok()
                    })
                }
                Which::VoteRequest(req) => {
                    let req = req?;
                    Ok(InboundMessagePayload::VoteRequest(RemoteVoteRequest {
                        term: req.get_term(),
                        candidate_id: req.get_candidate_id(),
                        last_log_index: req.get_last_log_index(),
                        last_log_term: req.get_last_log_term(),
                        request_id: req.get_request_id(),
                    }))
                }
                Which::VoteResponse(resp) => {
                    let resp = resp?;
                    Ok(InboundMessagePayload::VoteResponse(RemoteVoteResponse {
                        vote_granted: resp.get_vote_granted(),
                        request_id: resp.get_request_id(),
                        term: resp.get_term(),
                    }))
                }
                Which::AppendEntriesRequest(req) => {
                    let req = req?;
                    let entry = if req.has_entry() {
                        let e = req.get_entry()?;
                        Some(RemoteLogEntry {
                            index: e.get_index(),
                            data: e.get_data()?.to_vec(),
                            batch_index: e.get_batch_index(),
                            term: e.get_term(),
                            prev_log_term: e.get_prev_log_term(),
                            prev_log_index: e.get_prev_log_index(),
                            message_id: e.get_message_id(),
                        })
                    } else {
                        None
                    };

                    Ok(InboundMessagePayload::AppendEntriesRequest(RemoteAppendEntriesRequest {
                        term: req.get_term(),
                        leader_id: req.get_leader_id(),
                        prev_log_index: req.get_prev_log_index(),
                        prev_log_term: req.get_prev_log_term(),
                        request_id: req.get_request_id(),
                        entry,
                        commit_index: req.get_commit_index(),
                    }))
                }
                Which::AppendEntriesAcknowledge(ack) => {
                    let ack = ack?;
                    Ok(InboundMessagePayload::AppendEntriesAcknowledge(RemoteAppendEntriesAcknowledge {
                        last_log_term: ack.get_last_log_term(),
                        last_log_index: ack.get_last_log_index(),
                        request_id: ack.get_request_id(),
                        ok: ack.get_ok(),
                    }))
                }
                Which::PutBatchRequest(req) => {
                    let req = req?;
                    let put_requests = req.get_put_requests()?
                        .iter()
                        .map(|pr| {
                            Ok(RemotePutRequest {
                                id: pr.get_id()?.to_string()?,
                                payload: pr.get_payload()?.to_string()?,
                                node_id: pr.get_node_id(),
                            })
                        })
                        .collect::<capnp::Result<Vec<_>>>()?;

                    Ok(InboundMessagePayload::PutBatchRequest(RemotePutBatchRequest {
                        put_request: put_requests,
                        batch_id: req.get_batch_id()?.to_string()?,
                    }))
                }
                Which::PutBatchResponse(resp) => {
                    let resp = resp?;
                    let responses = resp.get_responses()?
                        .iter()
                        .map(|pr| {
                            let response_type = match pr.get_response_type()? {
                                CapnpRemoteResponseType::Ok => RemoteResponseType::Ok as i32,
                                CapnpRemoteResponseType::Invalid => RemoteResponseType::Invalid as i32,
                                CapnpRemoteResponseType::Unspecified => RemoteResponseType::Unspecified as i32,
                            };
                            Ok(RemotePutResponse {
                                id: pr.get_id()?.to_string()?,
                                response_type,
                                message: pr.get_message()?.to_string()?,
                                node_id: pr.get_node_id(),
                                batch_id: pr.get_batch_id()?.to_string()?,
                            })
                        })
                        .collect::<capnp::Result<Vec<_>>>()?;

                    Ok(InboundMessagePayload::PutBatchResponse(RemotePutBatchResponse {
                        responses,
                        batch_id: resp.get_batch_id()?.to_string()?,
                    }))
                }
                Which::TruncateLogRequest(req) => {
                    let req = req?;
                    Ok(InboundMessagePayload::TruncateLogRequest(RemoteTruncateLogRequest {
                        prev_term: req.get_prev_term(),
                        prev_index: req.get_prev_index(),
                        term: req.get_term(),
                        leader_id: req.get_leader_id(),
                        request_id: req.get_request_id(),
                    }))
                }
                Which::TruncateLogResponse(resp) => {
                    let resp = resp?;
                    Ok(InboundMessagePayload::TruncateLogResponse(RemoteTruncateLogResponse {
                        request_id: resp.get_request_id(),
                        ok: resp.get_ok(),
                    }))
                }
            }
        })?
    }
}

/// Enum representing converted inbound message payloads
#[derive(Debug)]
pub enum InboundMessagePayload {
    ConnectRequest { node_id: u32 },
    ConnectResponse { ok: bool },
    VoteRequest(RemoteVoteRequest),
    VoteResponse(RemoteVoteResponse),
    AppendEntriesRequest(RemoteAppendEntriesRequest),
    AppendEntriesAcknowledge(RemoteAppendEntriesAcknowledge),
    PutBatchRequest(RemotePutBatchRequest),
    PutBatchResponse(RemotePutBatchResponse),
    TruncateLogRequest(RemoteTruncateLogRequest),
    TruncateLogResponse(RemoteTruncateLogResponse),
}
