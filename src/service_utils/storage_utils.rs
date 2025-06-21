use prost::bytes::Bytes;
use crate::server::raftproto::PutRequest;
use sbe_kraft_replication_schema::command_type::CommandType;
use sbe_kraft_replication_schema::log_entry_codec::encoder::CommandsEncoder;
use sbe_kraft_replication_schema::log_entry_codec::{LogEntryDecoder, LogEntryEncoder};
use sbe_kraft_replication_schema::message_header_codec::MessageHeaderDecoder;
use sbe_kraft_replication_schema::put_data_record_codec::PutDataRecordEncoder;
use sbe_kraft_replication_schema::{message_header_codec, Decoder, ReadBuf, WriteBuf, Encoder};

pub struct SerializationData {
    pub index: u64,
    pub term: u64,
    pub timestamp: u64,
    pub requests: Vec<PutRequest>,
}

#[derive(Clone)]
pub struct SerializedData {
    pub data: Vec<u8>,
}

pub fn serialize_data(serialization_data: SerializationData, mut buffer: Vec<u8>, offset: usize) -> (usize, Vec<u8>) {
    let mut entry = LogEntryEncoder::default();
    let mut commands_encoder = CommandsEncoder::default();
    entry = entry.wrap(
        WriteBuf::new(buffer.as_mut_slice()),
        message_header_codec::ENCODED_LENGTH + offset,
    );
    entry = entry.header(offset).parent().unwrap(); // TODO

    entry.timestamp(serialization_data.timestamp);
    entry.term(serialization_data.term);
    entry.index(serialization_data.index);

    commands_encoder =
        entry.commands_encoder(serialization_data.requests.len() as u16, commands_encoder);
    for (request) in serialization_data.requests.iter() {
        commands_encoder.advance();
        commands_encoder.command_type(CommandType::PUT);
        let mut put_data_encoder = PutDataRecordEncoder::default();
        commands_encoder.payload(request.payload.as_bytes()); // TODO
    }
    entry = commands_encoder.parent().unwrap();

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

    let mut command_decoder = entry.commands_decoder();
    let entries = command_decoder.count();
    let mut requests = Vec::with_capacity(entries as usize);
    for _ in 0..entries {
        command_decoder.advance().expect("Expected command to be present when decoding");
        if command_decoder.command_type() == CommandType::PUT {
            let coord = command_decoder.payload_decoder();
            let payload = command_decoder.payload_slice(coord); // TODO parse
            requests.push(PutRequest {
                id: String::new(),
                payload: String::from_utf8_lossy(payload).to_string(), // Assuming payload is UTF-8 encoded
                node_id: 0,
            });
        }
    }
    entry = command_decoder.parent().unwrap();
    return Ok((
        entry.get_limit(),
        SerializationData {
            index,
            term,
            timestamp,
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


#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Read;
    use super::*;

    #[test]
    fn test_serialization() {
        let requests = vec![
            PutRequest {
                id: "1".to_string(),
                payload: "data1".to_string(),
                node_id: 0,
            },
            PutRequest {
                id: "2".to_string(),
                payload: "data2".to_string(),
                node_id: 0,
            },
        ];
        let serialization_data = SerializationData {
            index: 1,
            term: 1,
            timestamp: 1234567890,
            requests,
        };

        let mut buffer = vec![0u8; 2048];
        let offset = 0usize;
        let (limit, mut serialized_data) = serialize_data(serialization_data, buffer, offset);
        serialized_data.truncate(limit);
        let (offset, deserialized_data) = deserialize_data(&serialized_data, 0).unwrap();

        assert_eq!(deserialized_data.index, 1);
        assert_eq!(deserialized_data.term, 1);
        assert_eq!(deserialized_data.timestamp, 1234567890);
        assert_eq!(deserialized_data.requests.len(), 2);
        assert_eq!(deserialized_data.requests[0].payload, "data1");
        assert_eq!(deserialized_data.requests[1].payload, "data2");
        assert_eq!(offset, limit);
    }

    #[test]
    fn test_serialization_buffer_with_multiple_log_entries() {

        let requests_idx1 = vec![
            PutRequest {
                id: "1".to_string(),
                payload: "data1".to_string(),
                node_id: 0,
            },
            PutRequest {
                id: "2".to_string(),
                payload: "data2".to_string(),
                node_id: 0,
            },
        ];
        let serialization_data_idx1 = SerializationData {
            index: 1,
            term: 1,
            timestamp: 1234567890,
            requests: requests_idx1,
        };

        let requests_idx2 = vec![
            PutRequest {
                id: "3".to_string(),
                payload: "data3".to_string(),
                node_id: 0,
            },
            PutRequest {
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
        };

        let mut buffer = vec![0u8; 2048];
        let (offset1, buffer) = serialize_data(serialization_data_idx1, buffer, 0usize);
        let (offset2, mut buffer) = serialize_data(serialization_data_idx2, buffer, offset1);
        buffer.truncate(offset2);

        let (offset3, deserialized_data_idx1) = deserialize_data(&buffer, 0usize).unwrap();
        let (offset4, deserialized_data_idx2) = deserialize_data(&buffer, offset3).unwrap();

        assert_eq!(deserialized_data_idx1.index, 1);
        assert_eq!(deserialized_data_idx1.term, 1);
        assert_eq!(deserialized_data_idx1.timestamp, 1234567890);
        assert_eq!(deserialized_data_idx1.requests.len(), 2);
        assert_eq!(deserialized_data_idx1.requests[0].payload, "data1");
        assert_eq!(deserialized_data_idx1.requests[1].payload, "data2");
        assert_eq!(deserialized_data_idx2.index, 2);
        assert_eq!(deserialized_data_idx2.term, 2);
        assert_eq!(deserialized_data_idx2.timestamp, 1234567891);
        assert_eq!(deserialized_data_idx2.requests.len(), 2);
        assert_eq!(deserialized_data_idx2.requests[0].payload, "data3");
        assert_eq!(deserialized_data_idx2.requests[1].payload, "data4");
        assert_eq!(offset3, offset1);
        assert_eq!(offset4, offset2);
        assert_eq!(buffer.len(), offset2);
    }

    #[test]
    fn test_deserialize_all() {

        let requests_idx1 = vec![
            PutRequest {
                id: "1".to_string(),
                payload: "data1".to_string(),
                node_id: 0,
            },
            PutRequest {
                id: "2".to_string(),
                payload: "data2".to_string(),
                node_id: 0,
            },
        ];
        let serialization_data_idx1 = SerializationData {
            index: 1,
            term: 1,
            timestamp: 1234567890,
            requests: requests_idx1,
        };

        let requests_idx2 = vec![
            PutRequest {
                id: "3".to_string(),
                payload: "data3".to_string(),
                node_id: 0,
            },
            PutRequest {
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
        };

        let mut buffer = vec![0u8; 2048];
        let (offset1, buffer) = serialize_data(serialization_data_idx1, buffer, 0usize);
        let (offset2, mut buffer) = serialize_data(serialization_data_idx2, buffer, offset1);
        buffer.truncate(offset2);

        let data = deserialize_all_data(&buffer).unwrap();

        assert_eq!(data[0].index, 1);
        assert_eq!(data[0].term, 1);
        assert_eq!(data[0].timestamp, 1234567890);
        assert_eq!(data[0].requests.len(), 2);
        assert_eq!(data[0].requests[0].payload, "data1");
        assert_eq!(data[0].requests[1].payload, "data2");
        assert_eq!(data[1].index, 2);
        assert_eq!(data[1].term, 2);
        assert_eq!(data[1].timestamp, 1234567891);
        assert_eq!(data[1].requests.len(), 2);
        assert_eq!(data[1].requests[0].payload, "data3");
        assert_eq!(data[1].requests[1].payload, "data4");
    }

    #[test]
    fn compare_log() {

        let mut file = OpenOptions::new()
            .read(true)
            .open("/Users/jwedi/work/kraft/out/log.sbe");
        let mut buffer = [0u8; 2048];
        let data = file.unwrap().read(&mut buffer).unwrap();
        let (offset, deserialized_data) = deserialize_data(&buffer, 0).unwrap();

        let mut file2 = OpenOptions::new()
            .read(true)
            .open("/Users/jwedi/work/kraft/out2/log.sbe");
        let mut buffer2 = [0u8; 2048];
        let data2 = file2.unwrap().read(&mut buffer2).unwrap();
        let (offset2, deserialized_data2) = deserialize_data(&buffer2, 0).unwrap();
        let req_1 = deserialized_data.requests;
        let req_2 = deserialized_data2.requests;
        assert_eq!(offset, offset2);
        assert_eq!(req_1, req_2);
    }
}
