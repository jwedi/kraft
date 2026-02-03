use std::collections::HashMap;
use tokio::sync::oneshot::Sender;

use crate::transport::capnp::{
    build_owned_write_batch, build_owned_write_batch_response,
    OwnedWriteBatch, OwnedWriteBatchResponse,
};

use super::{WriteBatch, WriteResponse};

/// Combines multiple WriteBatches into a single OwnedWriteBatch for processing.
/// This involves extracting data from each batch and building a new combined message.
/// The callbacks are collected for later response routing.
pub fn prepare_batch_and_callbacks(
    batch_id: &str,
    batches: Vec<WriteBatch>,
) -> (OwnedWriteBatch, HashMap<String, Sender<WriteResponse>>) {
    let mut batch_callbacks: HashMap<String, Sender<WriteResponse>> = HashMap::new();

    // First, collect all request data from all batches
    struct RequestData {
        id: String,
        payload: Vec<u8>,
        node_id: u32,
    }
    let mut all_requests: Vec<RequestData> = vec![];

    for b in batches.into_iter() {
        batch_callbacks.insert(b.batch_id.clone(), b.callback);

        // Extract requests from this batch's OwnedWriteBatch
        let _ = b.message.with_message(|reader| {
            if let Ok(reqs) = reader.get_requests() {
                for req in reqs.iter() {
                    all_requests.push(RequestData {
                        id: req.get_id().map(|s| s.to_string().unwrap_or_default()).unwrap_or_default(),
                        payload: req.get_payload().map(|p| p.to_vec()).unwrap_or_default(),
                        node_id: req.get_node_id(),
                    });
                }
            }
        });
    }

    // Build combined OwnedWriteBatch
    let combined = build_owned_write_batch(|mut builder| {
        builder.set_batch_id(batch_id);
        let mut reqs = builder.init_requests(all_requests.len() as u32);
        for (i, req_data) in all_requests.iter().enumerate() {
            let mut req = reqs.reborrow().get(i as u32);
            req.set_id(&req_data.id);
            req.set_payload(&req_data.payload);
            req.set_node_id(req_data.node_id);
        }
    });

    (combined, batch_callbacks)
}

/// Sends responses to all batch callbacks.
pub fn respond_to_batch_callbacks(
    response: OwnedWriteBatchResponse,
    batch_callbacks: HashMap<String, Sender<WriteResponse>>,
) {
    for (batch_id, callback) in batch_callbacks {
        let batch_response = build_owned_write_batch_response(|mut builder| {
            builder.set_batch_id(&batch_id);
            // Copy relevant responses from the combined response
            let _ = response.with_message(|r| {
                if let Ok(resps) = r.get_responses() {
                    let mut out_resps = builder.init_responses(resps.len() as u32);
                    for (i, resp) in resps.iter().enumerate() {
                        let mut out = out_resps.reborrow().get(i as u32);
                        if let Ok(id) = resp.get_id() {
                            out.set_id(id);
                        }
                        if let Ok(rt) = resp.get_response_type() {
                            out.set_response_type(rt);
                        }
                        if let Ok(msg) = resp.get_message() {
                            out.set_message(msg);
                        }
                        out.set_node_id(resp.get_node_id());
                        if let Ok(bid) = resp.get_batch_id() {
                            out.set_batch_id(bid);
                        }
                    }
                }
            });
        });
        let msg = WriteResponse::success(batch_response);
        if callback.send(msg).is_err() {
            log::warn!("Sending write response callback failed because the receiver dropped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::write_proxy::WriteStatus;
    use tokio::sync::oneshot;

    fn create_test_write_batch(batch_id: &str, requests: Vec<(&str, &[u8], u32)>) -> WriteBatch {
        let (tx, _rx) = oneshot::channel();
        let message = build_owned_write_batch(|mut builder| {
            builder.set_batch_id(batch_id);
            let mut reqs = builder.init_requests(requests.len() as u32);
            for (i, (id, payload, node_id)) in requests.iter().enumerate() {
                let mut req = reqs.reborrow().get(i as u32);
                req.set_id(id);
                req.set_payload(payload);
                req.set_node_id(*node_id);
            }
        });

        WriteBatch {
            batch_id: batch_id.to_string(),
            message,
            span_parent: None,
            callback: tx,
        }
    }

    #[test]
    fn test_prepare_batch_and_callbacks_single_batch() {
        let batch = create_test_write_batch(
            "batch-1",
            vec![("req1", b"payload1", 1), ("req2", b"payload2", 2)],
        );

        let (combined, callbacks) = prepare_batch_and_callbacks("combined-1", vec![batch]);

        // Check callbacks
        assert_eq!(callbacks.len(), 1);
        assert!(callbacks.contains_key("batch-1"));

        // Check combined batch
        let request_count = combined
            .with_message(|r| r.get_requests().map(|reqs| reqs.len()).unwrap_or(0))
            .unwrap_or(0);
        assert_eq!(request_count, 2);
    }

    #[test]
    fn test_prepare_batch_and_callbacks_multiple_batches() {
        let batch1 = create_test_write_batch("batch-1", vec![("req1", b"payload1", 1)]);
        let batch2 = create_test_write_batch("batch-2", vec![("req2", b"payload2", 2)]);
        let batch3 = create_test_write_batch(
            "batch-3",
            vec![("req3", b"payload3", 3), ("req4", b"payload4", 4)],
        );

        let (combined, callbacks) =
            prepare_batch_and_callbacks("combined-all", vec![batch1, batch2, batch3]);

        // Check callbacks
        assert_eq!(callbacks.len(), 3);
        assert!(callbacks.contains_key("batch-1"));
        assert!(callbacks.contains_key("batch-2"));
        assert!(callbacks.contains_key("batch-3"));

        // Check combined batch has all requests
        let request_count = combined
            .with_message(|r| r.get_requests().map(|reqs| reqs.len()).unwrap_or(0))
            .unwrap_or(0);
        assert_eq!(request_count, 4);
    }

    #[test]
    fn test_prepare_batch_and_callbacks_empty_batches() {
        let batch = create_test_write_batch("empty-batch", vec![]);

        let (combined, callbacks) = prepare_batch_and_callbacks("combined-empty", vec![batch]);

        assert_eq!(callbacks.len(), 1);

        let request_count = combined
            .with_message(|r| r.get_requests().map(|reqs| reqs.len()).unwrap_or(0))
            .unwrap_or(0);
        assert_eq!(request_count, 0);
    }

    #[tokio::test]
    async fn test_respond_to_batch_callbacks_sends_responses() {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let mut callbacks = HashMap::new();
        callbacks.insert("batch-1".to_string(), tx1);
        callbacks.insert("batch-2".to_string(), tx2);

        let response = build_owned_write_batch_response(|mut builder| {
            builder.set_batch_id("combined");
            let mut resps = builder.init_responses(1);
            let mut resp = resps.reborrow().get(0);
            resp.set_id("resp1");
            resp.set_response_type(crate::transport::capnp::raft_capnp::RemoteResponseType::Ok);
            resp.set_message("success");
            resp.set_node_id(1);
        });

        respond_to_batch_callbacks(response, callbacks);

        // Both callbacks should receive responses
        let result1 = rx1.await;
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap().status, WriteStatus::Ok);

        let result2 = rx2.await;
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap().status, WriteStatus::Ok);
    }
}
