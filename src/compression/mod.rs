//! ClickHouse compression frame format.
//!
//! Each compressed block on the wire is wrapped in a 25-byte header (16 byte
//! checksum + 9 byte size/method header) followed by the compressed body:
//!
//! ```text
//! [16 bytes: CityHash128 checksum over the 9-byte header + compressed body]
//! [1 byte:   method: 0x82 = LZ4, 0x90 = ZSTD, 0x02 = NONE]
//! [4 bytes LE: compressed size (includes the 9-byte header, excludes checksum)]
//! [4 bytes LE: uncompressed size]
//! [N bytes:  compressed body]
//! ```
//!
//! ClickHouse uses CityHash v1.0.2 (the historical variant), NOT modern
//! Google CityHash.

use crate::error::Result;
use clickhouse_rs_cityhash_sys::city_hash_128;
use std::io::Read;

const HEADER_LEN: usize = 9;
const CHECKSUM_LEN: usize = 16;

/// Compression method for the wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionMethod {
    None = 0x02,
    Lz4 = 0x82,
    Zstd = 0x90,
}

impl CompressionMethod {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x02 => Some(Self::None),
            0x82 => Some(Self::Lz4),
            0x90 => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// Encode `data` into a single compression frame (checksum + header + body).
///
/// Panics if an unsupported compression method is requested at runtime
/// (e.g., LZ4 when the `lz4` feature is not enabled — compile-time guarded).
pub fn encode_frame(data: &[u8], method: CompressionMethod) -> Result<Vec<u8>> {
    let body: Vec<u8> = match method {
        CompressionMethod::None => data.to_vec(),
        #[cfg(feature = "lz4")]
        CompressionMethod::Lz4 => lz4_flex::block::compress(data),
        #[cfg(not(feature = "lz4"))]
        CompressionMethod::Lz4 => {
            return Err(crate::error::Error::Compression(
                "LZ4 compression requested but 'lz4' feature not enabled".into(),
            ));
        },
        #[cfg(feature = "zstd")]
        CompressionMethod::Zstd => zstd::stream::encode_all(data, 3)
            .map_err(|e| crate::error::Error::Compression(format!("zstd encode: {e}")))?,
        #[cfg(not(feature = "zstd"))]
        CompressionMethod::Zstd => {
            return Err(crate::error::Error::Compression(
                "ZSTD compression requested but 'zstd' feature not enabled".into(),
            ));
        },
    };

    let compressed_size = (HEADER_LEN + body.len()) as u32;
    let uncompressed_size = data.len() as u32;

    // Build the 9-byte header + compressed body. Checksum covers exactly these 9+N bytes.
    let mut header_and_body = Vec::with_capacity(HEADER_LEN + body.len());
    header_and_body.push(method as u8);
    header_and_body.extend_from_slice(&compressed_size.to_le_bytes());
    header_and_body.extend_from_slice(&uncompressed_size.to_le_bytes());
    header_and_body.extend_from_slice(&body);

    let checksum = city_hash_128(&header_and_body);

    let mut frame = Vec::with_capacity(CHECKSUM_LEN + header_and_body.len());
    frame.extend_from_slice(&checksum.lo.to_le_bytes());
    frame.extend_from_slice(&checksum.hi.to_le_bytes());
    frame.extend_from_slice(&header_and_body);
    Ok(frame)
}

/// Read one compression frame from `r` and return the decompressed body.
///
/// Verifies the CityHash128 checksum. Returns an error on checksum mismatch
/// (corruption indicator — fail loudly).
pub fn decode_frame<R: Read>(r: &mut R) -> Result<Vec<u8>> {
    let mut checksum_bytes = [0u8; CHECKSUM_LEN];
    r.read_exact(&mut checksum_bytes)?;
    let mut expected_lo_bytes = [0u8; 8];
    expected_lo_bytes.copy_from_slice(&checksum_bytes[..8]);
    let mut expected_hi_bytes = [0u8; 8];
    expected_hi_bytes.copy_from_slice(&checksum_bytes[8..]);
    let expected_lo = u64::from_le_bytes(expected_lo_bytes);
    let expected_hi = u64::from_le_bytes(expected_hi_bytes);

    let mut header = [0u8; HEADER_LEN];
    r.read_exact(&mut header)?;
    let method_byte = header[0];
    let method = CompressionMethod::from_byte(method_byte).ok_or_else(|| {
        crate::error::Error::Compression(format!(
            "unknown compression method byte: 0x{method_byte:02x}"
        ))
    })?;
    let compressed_size = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let uncompressed_size =
        u32::from_le_bytes([header[5], header[6], header[7], header[8]]) as usize;

    // Both sizes are attacker-controlled (a corrupt or malicious frame). Cap them
    // so a tiny frame cannot claim a multi-GiB allocation (decompression-bomb/OOM).
    const MAX_FRAME_SIZE: usize = 1 << 30;
    if uncompressed_size > MAX_FRAME_SIZE {
        return Err(crate::error::Error::Compression(format!(
            "uncompressed_size {uncompressed_size} exceeds {MAX_FRAME_SIZE} byte cap"
        )));
    }

    if compressed_size < HEADER_LEN {
        return Err(crate::error::Error::Compression(format!(
            "compressed_size {compressed_size} < header length {HEADER_LEN}"
        )));
    }
    let body_len = compressed_size - HEADER_LEN;
    if body_len > MAX_FRAME_SIZE {
        return Err(crate::error::Error::Compression(format!(
            "compressed body {body_len} exceeds {MAX_FRAME_SIZE} byte cap"
        )));
    }
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body)?;

    // Recompute checksum and compare.
    let mut to_hash = Vec::with_capacity(HEADER_LEN + body_len);
    to_hash.extend_from_slice(&header);
    to_hash.extend_from_slice(&body);
    let actual = city_hash_128(&to_hash);
    if actual.lo != expected_lo || actual.hi != expected_hi {
        return Err(crate::error::Error::Compression(
            "compression frame checksum mismatch (CityHash128) — corruption suspected".into(),
        ));
    }

    let decompressed = match method {
        CompressionMethod::None => body,
        #[cfg(feature = "lz4")]
        CompressionMethod::Lz4 => lz4_flex::block::decompress(&body, uncompressed_size)
            .map_err(|e| crate::error::Error::Compression(format!("lz4 decode: {e}")))?,
        #[cfg(not(feature = "lz4"))]
        CompressionMethod::Lz4 => {
            return Err(crate::error::Error::Compression(
                "LZ4 decompression requested but 'lz4' feature not enabled".into(),
            ));
        },
        #[cfg(feature = "zstd")]
        CompressionMethod::Zstd => zstd::stream::decode_all(&body[..])
            .map_err(|e| crate::error::Error::Compression(format!("zstd decode: {e}")))?,
        #[cfg(not(feature = "zstd"))]
        CompressionMethod::Zstd => {
            return Err(crate::error::Error::Compression(
                "ZSTD decompression requested but 'zstd' feature not enabled".into(),
            ));
        },
    };

    if decompressed.len() != uncompressed_size {
        return Err(crate::error::Error::Compression(format!(
            "decompressed length {} != uncompressed_size {} declared in header",
            decompressed.len(),
            uncompressed_size
        )));
    }
    Ok(decompressed)
}

/// Convenience: decompress a frame from an in-memory byte slice.
pub fn decode_frame_bytes(data: &[u8]) -> Result<Vec<u8>> {
    let mut cursor = std::io::Cursor::new(data);
    decode_frame(&mut cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_frame_rejects_oversized_uncompressed() {
        // 16-byte checksum (the size cap fires before checksum verify) + 9-byte
        // header: method NONE, compressed_size = HEADER_LEN (body_len 0),
        // uncompressed_size = u32::MAX (~4 GiB) — must error, not pre-allocate.
        let mut frame = vec![0u8; 16];
        frame.push(0x02); // NONE
        frame.extend_from_slice(&9u32.to_le_bytes()); // compressed_size
        frame.extend_from_slice(&u32::MAX.to_le_bytes()); // uncompressed_size
        let mut cursor = std::io::Cursor::new(frame);
        assert!(decode_frame(&mut cursor).is_err());
    }

    #[test]
    fn test_none_frame_roundtrip() {
        let payload = b"hello world".to_vec();
        let frame = encode_frame(&payload, CompressionMethod::None).expect("test operation failed");
        assert_eq!(frame.len(), 16 + 9 + payload.len());
        assert_eq!(frame[16], 0x02);
        let decoded = decode_frame_bytes(&frame).expect("test operation failed");
        assert_eq!(decoded, payload);
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_frame_roundtrip() {
        let payload: Vec<u8> = b"abcabcabc".repeat(50);
        let frame = encode_frame(&payload, CompressionMethod::Lz4).expect("test operation failed");
        assert_eq!(frame[16], 0x82);
        let decoded = decode_frame_bytes(&frame).expect("test operation failed");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_checksum_mismatch_detected() {
        let payload = b"abcdefgh".to_vec();
        let mut frame =
            encode_frame(&payload, CompressionMethod::None).expect("test operation failed");
        frame[25] ^= 0xFF; // corrupt body byte
        let err = decode_frame_bytes(&frame).expect_err("expected test operation to fail");
        assert!(
            err.to_string().contains("checksum"),
            "expected checksum error, got: {err}"
        );
    }

    #[test]
    fn test_header_layout() {
        let payload = b"abcdefg".to_vec();
        let frame = encode_frame(&payload, CompressionMethod::None).expect("test operation failed");
        assert_eq!(frame[16], 0x02); // method
        let cs = u32::from_le_bytes([frame[17], frame[18], frame[19], frame[20]]);
        assert_eq!(cs, 16); // compressed_size = 9 (header) + 7 (body)
        let us = u32::from_le_bytes([frame[21], frame[22], frame[23], frame[24]]);
        assert_eq!(us, 7); // uncompressed_size
        assert_eq!(&frame[25..], payload.as_slice());
    }
}
