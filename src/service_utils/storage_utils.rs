use std::sync::Arc;
use crate::transport::raft::raftproto::RemotePutRequest;
use sbe_kraft_replication_schema::log_entry_codec::encoder::{Put_data_commandsEncoder};
use sbe_kraft_replication_schema::log_entry_codec::{LogEntryDecoder, LogEntryEncoder};
use sbe_kraft_replication_schema::message_header_codec::MessageHeaderDecoder;
use sbe_kraft_replication_schema::{message_header_codec, Decoder, ReadBuf, WriteBuf, Encoder};

#[derive(Clone, PartialEq)]
pub struct SerializationData {
    pub index: u64,
    pub term: u64,
    pub timestamp: u64,
    pub prev_index: u64,
    pub prev_term: u64,
    pub message_id: u64,
    pub requests: Vec<RemotePutRequest>,
}

#[derive(Clone)]
pub struct SerializedData {
    pub data: Vec<u8>,
}

pub fn serialize_data(serialization_data: &SerializationData, mut buffer: Vec<u8>, offset: usize) -> (usize, Vec<u8>) {
    let mut entry = LogEntryEncoder::default();
    let mut put_data_commands_encoder = Put_data_commandsEncoder::default();
    entry = entry.wrap(
        WriteBuf::new(buffer.as_mut_slice()),
        message_header_codec::ENCODED_LENGTH + offset,
    );
    entry = entry.header(offset).parent().unwrap(); // TODO

    entry.timestamp(serialization_data.timestamp);
    entry.term(serialization_data.term);
    entry.index(serialization_data.index);
    entry.prev_log_index(serialization_data.prev_index);
    entry.prev_log_term(serialization_data.prev_term);
    entry.message_id(serialization_data.message_id);

    put_data_commands_encoder = entry.put_data_commands_encoder(
        serialization_data.requests.len() as u16,
        put_data_commands_encoder,
    );

    for (request) in serialization_data.requests.iter() {
        put_data_commands_encoder.advance();
        put_data_commands_encoder.key(request.id.as_str());
        put_data_commands_encoder.value(request.payload.as_bytes());
    }
    entry = put_data_commands_encoder.parent().unwrap();

    (entry.get_limit(), buffer)
}

pub fn deserialize_data(data: &[u8], offset: usize) -> Result<(usize, SerializationData), String> {
    let mut entry = LogEntryDecoder::default();
    let buf = ReadBuf::new(data);
    let header = MessageHeaderDecoder::default().wrap(buf, offset);
    entry = entry.header(header, offset);

    let timestamp = entry.timestamp();
    let term = entry.term();
    let index = entry.index();
    let prev_index = entry.prev_log_index();
    let prev_term = entry.prev_log_term();
    let message_id = entry.message_id();

    let mut command_decoder = entry.put_data_commands_decoder();
    let entries = command_decoder.count();
    let mut requests = Vec::with_capacity(entries as usize);
    for _ in 0..entries {
        command_decoder.advance().expect("Expected command to be present when decoding");
        let key_coords = command_decoder.key_decoder();
        let key = command_decoder.key_slice(key_coords);
        let id = String::from_utf8_lossy(key).to_string();
        let value_coords = command_decoder.value_decoder();
        let value = command_decoder.value_slice(value_coords);
        let payload = String::from_utf8_lossy(value).to_string();
        requests.push(RemotePutRequest {
            id,
            payload,
            node_id: 0,
        });
    }
    entry = command_decoder.parent().unwrap();
    return Ok((
        entry.get_limit(),
        SerializationData {
            index,
            term,
            timestamp,
            prev_index,
            prev_term,
            message_id,
            requests,
        },
    ));
}

pub fn deserialize_all_data(data: &[u8]) -> Result<(Vec<SerializationData>), String> {
    let mut offset = 0;
    let mut results = Vec::new();
    while offset < data.len() {
        match deserialize_data(data, offset) {
            Ok((new_offset, serialization_data)) => {
                results.push(serialization_data);
                offset = new_offset;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(results)
}

pub fn deserialize_all_data_as_arc(data: &[u8]) -> Result<(Vec<Arc<SerializationData>>), String> {
    let mut offset = 0;
    let mut results = Vec::new();
    while offset < data.len() {
        match deserialize_data(data, offset) {
            Ok((new_offset, serialization_data)) => {
                results.push(Arc::new(serialization_data));
                offset = new_offset;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(results)
}


#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Read;
    use super::*;

    #[test]
    fn test_serialization() {
        let requests = vec![
            RemotePutRequest {
                id: "1".to_string(),
                payload: "data1".to_string(),
                node_id: 0,
            },
            RemotePutRequest {
                id: "2".to_string(),
                payload: "data2".to_string(),
                node_id: 0,
            },
        ];
        let serialization_data = SerializationData {
            index: 1,
            term: 1,
            timestamp: 1234567890,
            prev_index: 0,
            prev_term: 1,
            message_id: 123,
            requests,
        };

        let mut buffer = vec![0u8; 2048];
        let offset = 0usize;
        let (limit, mut serialized_data) = serialize_data(&serialization_data, buffer, offset);
        serialized_data.truncate(limit);
        let (offset, deserialized_data) = deserialize_data(&serialized_data, 0).unwrap();

        assert_eq!(deserialized_data.index, 1);
        assert_eq!(deserialized_data.term, 1);
        assert_eq!(deserialized_data.timestamp, 1234567890);
        assert_eq!(deserialized_data.prev_index, 0);
        assert_eq!(deserialized_data.prev_term, 1);
        assert_eq!(deserialized_data.message_id, 123);
        assert_eq!(deserialized_data.requests.len(), 2);
        assert_eq!(deserialized_data.requests[0].payload, "data1");
        assert_eq!(deserialized_data.requests[0].id, "1");
        assert_eq!(deserialized_data.requests[1].payload, "data2");
        assert_eq!(deserialized_data.requests[1].id, "2");
        assert_eq!(offset, limit);
    }

    #[test]
    fn test_serialization_buffer_with_multiple_log_entries() {

        let requests_idx1 = vec![
            RemotePutRequest {
                id: "1".to_string(),
                payload: "data1".to_string(),
                node_id: 0,
            },
            RemotePutRequest {
                id: "2".to_string(),
                payload: "data2".to_string(),
                node_id: 0,
            },
        ];
        let serialization_data_idx1 = SerializationData {
            index: 1,
            term: 1,
            timestamp: 1234567890,
            prev_index: 0,
            prev_term: 0,
            message_id: 123,
            requests: requests_idx1,
        };

        let requests_idx2 = vec![
            RemotePutRequest {
                id: "3".to_string(),
                payload: "data3".to_string(),
                node_id: 0,
            },
            RemotePutRequest {
                id: "4".to_string(),
                payload: "data4".to_string(),
                node_id: 0,
            },
        ];
        let serialization_data_idx2 = SerializationData {
            index: 2,
            term: 2,
            timestamp: 1234567891,
            requests: requests_idx2,
            prev_index: 1,
            prev_term: 1,
            message_id: 456,
        };

        let mut buffer = vec![0u8; 2048];
        let (offset1, buffer) = serialize_data(&serialization_data_idx1, buffer, 0usize);
        let (offset2, mut buffer) = serialize_data(&serialization_data_idx2, buffer, offset1);
        buffer.truncate(offset2);

        let (offset3, deserialized_data_idx1) = deserialize_data(&buffer, 0usize).unwrap();
        let (offset4, deserialized_data_idx2) = deserialize_data(&buffer, offset3).unwrap();

        assert_eq!(deserialized_data_idx1.index, 1);
        assert_eq!(deserialized_data_idx1.term, 1);
        assert_eq!(deserialized_data_idx1.timestamp, 1234567890);
        assert_eq!(deserialized_data_idx1.prev_index, 0);
        assert_eq!(deserialized_data_idx1.prev_term, 0);
        assert_eq!(deserialized_data_idx1.message_id, 123);
        assert_eq!(deserialized_data_idx1.requests.len(), 2);
        assert_eq!(deserialized_data_idx1.requests[0].payload, "data1");
        assert_eq!(deserialized_data_idx1.requests[0].id, "1");
        assert_eq!(deserialized_data_idx1.requests[1].id, "2");
        assert_eq!(deserialized_data_idx1.requests[1].payload, "data2");
        assert_eq!(deserialized_data_idx2.index, 2);
        assert_eq!(deserialized_data_idx2.term, 2);
        assert_eq!(deserialized_data_idx2.timestamp, 1234567891);
        assert_eq!(deserialized_data_idx2.prev_index, 1);
        assert_eq!(deserialized_data_idx2.prev_term, 1);
        assert_eq!(deserialized_data_idx2.message_id, 456);
        assert_eq!(deserialized_data_idx2.requests.len(), 2);
        assert_eq!(deserialized_data_idx2.requests[0].id, "3");
        assert_eq!(deserialized_data_idx2.requests[0].payload, "data3");
        assert_eq!(deserialized_data_idx2.requests[1].id, "4");
        assert_eq!(deserialized_data_idx2.requests[1].payload, "data4");
        assert_eq!(offset3, offset1);
        assert_eq!(offset4, offset2);
        assert_eq!(buffer.len(), offset2);
    }

    #[test]
    fn test_deserialize_all() {

        let requests_idx1 = vec![
            RemotePutRequest {
                id: "1".to_string(),
                payload: "data1".to_string(),
                node_id: 0,
            },
            RemotePutRequest {
                id: "2".to_string(),
                payload: "data2".to_string(),
                node_id: 0,
            },
        ];
        let serialization_data_idx1 = SerializationData {
            index: 1,
            term: 1,
            timestamp: 1234567890,
            prev_index: 0,
            prev_term: 0,
            message_id: 123,
            requests: requests_idx1,
        };

        let requests_idx2 = vec![
            RemotePutRequest {
                id: "3".to_string(),
                payload: "data3".to_string(),
                node_id: 0,
            },
            RemotePutRequest {
                id: "4".to_string(),
                payload: "data4".to_string(),
                node_id: 0,
            },
        ];
        let serialization_data_idx2 = SerializationData {
            index: 2,
            term: 2,
            timestamp: 1234567891,
            prev_index: 1,
            prev_term: 1,
            message_id: 456,
            requests: requests_idx2,
        };

        let mut buffer = vec![0u8; 2048];
        let (offset1, buffer) = serialize_data(&serialization_data_idx1, buffer, 0usize);
        let (offset2, mut buffer) = serialize_data(&serialization_data_idx2, buffer, offset1);
        buffer.truncate(offset2);

        let data = deserialize_all_data(&buffer).unwrap();

        assert_eq!(data[0].index, 1);
        assert_eq!(data[0].term, 1);
        assert_eq!(data[0].timestamp, 1234567890);
        assert_eq!(data[0].prev_index, 0);
        assert_eq!(data[0].prev_term, 0);
        assert_eq!(data[0].message_id, 123);
        assert_eq!(data[0].requests.len(), 2);
        assert_eq!(data[0].requests[0].payload, "data1");
        assert_eq!(data[0].requests[0].id, "1");
        assert_eq!(data[0].requests[1].payload, "data2");
        assert_eq!(data[0].requests[1].id, "2");
        assert_eq!(data[1].index, 2);
        assert_eq!(data[1].term, 2);
        assert_eq!(data[1].timestamp, 1234567891);
        assert_eq!(data[1].prev_index, 1);
        assert_eq!(data[1].prev_term, 1);
        assert_eq!(data[1].message_id, 456);
        assert_eq!(data[1].requests.len(), 2);
        assert_eq!(data[1].requests[0].payload, "data3");
        assert_eq!(data[1].requests[0].id, "3");
        assert_eq!(data[1].requests[1].payload, "data4");
        assert_eq!(data[1].requests[1].id, "4");
    }

    // Manual test
    //#[test]
    fn compare_log() {

        let mut file = OpenOptions::new()
            .read(true)
            .open("/Users/jwedi/work/kraft/out/log.sbe");
        let mut buffer = [0u8; 2048];
        let data = file.unwrap().read(&mut buffer).unwrap();
        let (offset, deserialized_data) = deserialize_data(&buffer, 0).unwrap();

        let mut file2 = OpenOptions::new()
            .read(true)
            .open("/Users/jwedi/work/kraft/out3/log.sbe");
        let mut buffer2 = [0u8; 2048];
        let data2 = file2.unwrap().read(&mut buffer2).unwrap();
        let (offset2, deserialized_data2) = deserialize_data(&buffer2, 0).unwrap();
        let req_1 = deserialized_data.requests;
        let req_2 = deserialized_data2.requests;
        assert_eq!(offset, offset2);
        assert_eq!(req_1, req_2);
    }
}
