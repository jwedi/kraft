//! Zero-copy frame encoding/decoding for the TCP datastore protocol.
//!
//! Frame format:
//! ```text
//! +----------------+----------+---------------+---------------------------+
//! | Length (4B LE) | RPC Type | Request ID    | Cap'n Proto Payload       |
//! | u32            | u8       | u64           | Variable                  |
//! +----------------+----------+---------------+---------------------------+
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Header size: length (4) + rpc_type (1) + request_id (8) = 13 bytes
pub const HEADER_SIZE: usize = 4 + 1 + 8;

/// Maximum frame size (16 MB)
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Read a frame from the reader.
///
/// Returns (rpc_type, request_id, payload).
/// The payload is returned as `Bytes` for zero-copy handling.
pub async fn read_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> io::Result<(u8, u64, Bytes)> {
    // Read length (4 bytes, little-endian)
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let frame_len = u32::from_le_bytes(len_buf);

    if frame_len < 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Frame too small: {} bytes (minimum 9)", frame_len),
        ));
    }

    if frame_len > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Frame too large: {} bytes (max {})", frame_len, MAX_FRAME_SIZE),
        ));
    }

    // Read rpc_type (1 byte)
    let rpc_type = reader.read_u8().await?;

    // Read request_id (8 bytes, little-endian)
    let request_id = reader.read_u64_le().await?;

    // Read payload
    let payload_len = frame_len as usize - 9; // 9 = 1 (rpc_type) + 8 (request_id)
    let mut payload = BytesMut::with_capacity(payload_len);
    payload.resize(payload_len, 0);
    reader.read_exact(&mut payload).await?;

    Ok((rpc_type, request_id, payload.freeze()))
}

/// Write a frame to the writer.
///
/// Writes: length (4B) + rpc_type (1B) + request_id (8B) + payload
pub async fn write_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    rpc_type: u8,
    request_id: u64,
    payload: &[u8],
) -> io::Result<()> {
    // Calculate frame length: rpc_type (1) + request_id (8) + payload
    let frame_len = 1 + 8 + payload.len();

    if frame_len > MAX_FRAME_SIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Payload too large: {} bytes", payload.len()),
        ));
    }

    // Build frame in a single buffer to minimize syscalls
    let mut buf = BytesMut::with_capacity(4 + frame_len);
    buf.put_u32_le(frame_len as u32);
    buf.put_u8(rpc_type);
    buf.put_u64_le(request_id);
    buf.put_slice(payload);

    writer.write_all(&buf).await
}

/// Encode a frame into a BytesMut buffer (for batch writing).
pub fn encode_frame(rpc_type: u8, request_id: u64, payload: &[u8]) -> BytesMut {
    let frame_len = 1 + 8 + payload.len();
    let mut buf = BytesMut::with_capacity(4 + frame_len);
    buf.put_u32_le(frame_len as u32);
    buf.put_u8(rpc_type);
    buf.put_u64_le(request_id);
    buf.put_slice(payload);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_read_write_frame() {
        let payload = b"test payload";
        let rpc_type = 1u8;
        let request_id = 12345u64;

        // Write frame
        let mut buf = Vec::new();
        write_frame(&mut buf, rpc_type, request_id, payload)
            .await
            .unwrap();

        // Read frame
        let mut reader = Cursor::new(buf);
        let (read_rpc_type, read_request_id, read_payload) =
            read_frame(&mut reader).await.unwrap();

        assert_eq!(read_rpc_type, rpc_type);
        assert_eq!(read_request_id, request_id);
        assert_eq!(&read_payload[..], payload);
    }

    #[tokio::test]
    async fn test_empty_payload() {
        let rpc_type = 2u8;
        let request_id = 0u64;

        let mut buf = Vec::new();
        write_frame(&mut buf, rpc_type, request_id, &[])
            .await
            .unwrap();

        let mut reader = Cursor::new(buf);
        let (read_rpc_type, read_request_id, read_payload) =
            read_frame(&mut reader).await.unwrap();

        assert_eq!(read_rpc_type, rpc_type);
        assert_eq!(read_request_id, request_id);
        assert!(read_payload.is_empty());
    }

    #[test]
    fn test_encode_frame() {
        let payload = b"hello";
        let buf = encode_frame(1, 100, payload);

        // Length should be: 4 (len) + 1 (rpc) + 8 (id) + 5 (payload) = 18
        assert_eq!(buf.len(), 18);

        // Check length field
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len, 14); // 1 + 8 + 5 = 14
    }
}
