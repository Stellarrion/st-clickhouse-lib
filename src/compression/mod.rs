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

    // Outbound data is trusted, but the wire size fields are u32: refuse a
    // payload whose sizes do not fit instead of silently truncating the cast
    // (which would emit a corrupt frame). Legitimate streamed writes stay
    // allowed up to the 4 GiB wire limit.
    let compressed_size = u32::try_from(HEADER_LEN + body.len()).map_err(|_| {
        crate::error::Error::Compression(format!(
            "compressed frame of {} bytes exceeds the u32 wire size field",
            HEADER_LEN + body.len()
        ))
    })?;
    let uncompressed_size = u32::try_from(data.len()).map_err(|_| {
        crate::error::Error::Compression(format!(
            "uncompressed payload of {} bytes exceeds the u32 wire size field",
            data.len()
        ))
    })?;

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
///
/// All size fields are server-controlled. `compressed_size` and
/// `uncompressed_size` are validated against
/// [`crate::limits::MAX_FRAME_SIZE`] before any buffer is sized, and
/// decompression output is bounded to the declared `uncompressed_size`, so a
/// hostile or corrupt frame cannot drive an oversized allocation even when its
/// checksum is valid.
pub fn decode_frame<R: Read>(r: &mut R) -> Result<Vec<u8>> {
    // Single buffer holding checksum + header + body: the checksum is
    // recomputed in place over frame[CHECKSUM_LEN..] instead of duplicating
    // the body into a second body-sized allocation.
    let mut frame = Vec::with_capacity(CHECKSUM_LEN + HEADER_LEN);
    let mut checksum_bytes = [0u8; CHECKSUM_LEN];
    r.read_exact(&mut checksum_bytes)?;
    frame.extend_from_slice(&checksum_bytes);
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

    // Both sizes are attacker-controlled (a corrupt or malicious frame). Cap
    // them BEFORE sizing any buffer so a tiny frame cannot claim a huge
    // allocation (decompression-bomb/OOM) — a valid checksum must not bypass
    // these checks either.
    if compressed_size < HEADER_LEN {
        return Err(crate::error::Error::Compression(format!(
            "compressed_size {compressed_size} < header length {HEADER_LEN}"
        )));
    }
    if compressed_size > crate::limits::MAX_FRAME_SIZE {
        return Err(crate::error::Error::Compression(format!(
            "compressed_size {compressed_size} exceeds {} byte frame cap",
            crate::limits::MAX_FRAME_SIZE
        )));
    }
    if uncompressed_size > crate::limits::MAX_FRAME_SIZE {
        return Err(crate::error::Error::Compression(format!(
            "uncompressed_size {uncompressed_size} exceeds {} byte frame cap",
            crate::limits::MAX_FRAME_SIZE
        )));
    }

    frame.extend_from_slice(&header);
    frame.resize(CHECKSUM_LEN + compressed_size, 0);
    r.read_exact(&mut frame[CHECKSUM_LEN + HEADER_LEN..])?;

    // Recompute checksum over header + body and compare.
    let actual = city_hash_128(&frame[CHECKSUM_LEN..]);
    if actual.lo != expected_lo || actual.hi != expected_hi {
        return Err(crate::error::Error::Compression(
            "compression frame checksum mismatch (CityHash128) — corruption suspected".into(),
        ));
    }

    let decompressed = match method {
        CompressionMethod::None => {
            // Reuse the frame allocation: drop the 25-byte frame header.
            frame.drain(..CHECKSUM_LEN + HEADER_LEN);
            frame
        },
        #[cfg(feature = "lz4")]
        CompressionMethod::Lz4 => {
            lz4_flex::block::decompress(&frame[CHECKSUM_LEN + HEADER_LEN..], uncompressed_size)
                .map_err(|e| crate::error::Error::Compression(format!("lz4 decode: {e}")))?
        },
        #[cfg(not(feature = "lz4"))]
        CompressionMethod::Lz4 => {
            return Err(crate::error::Error::Compression(
                "LZ4 decompression requested but 'lz4' feature not enabled".into(),
            ));
        },
        // Bound zstd output to the declared (capped) uncompressed_size: a frame
        // that expands beyond its declaration is rejected during decompression
        // instead of growing past the cap and failing only afterwards.
        #[cfg(feature = "zstd")]
        CompressionMethod::Zstd => {
            zstd::bulk::decompress(&frame[CHECKSUM_LEN + HEADER_LEN..], uncompressed_size).map_err(
                |e| {
                    crate::error::Error::Compression(format!(
                        "zstd decode: {e} (declared uncompressed_size {uncompressed_size})"
                    ))
                },
            )?
        },
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

    /// Build a wire frame with arbitrary (possibly lying) header fields but a
    /// fully valid CityHash128 checksum over header + body.
    fn build_frame(
        method: u8, compressed_size: u32, uncompressed_size: u32, body: &[u8],
    ) -> Vec<u8> {
        let mut header_and_body = Vec::new();
        header_and_body.push(method);
        header_and_body.extend_from_slice(&compressed_size.to_le_bytes());
        header_and_body.extend_from_slice(&uncompressed_size.to_le_bytes());
        header_and_body.extend_from_slice(body);
        let checksum = city_hash_128(&header_and_body);
        let mut frame = Vec::new();
        frame.extend_from_slice(&checksum.lo.to_le_bytes());
        frame.extend_from_slice(&checksum.hi.to_le_bytes());
        frame.extend_from_slice(&header_and_body);
        frame
    }

    #[test]
    fn decode_frame_rejects_oversized_uncompressed() {
        // 16-byte checksum (the size cap fires before checksum verify) + 9-byte
        // header: method NONE, compressed_size = HEADER_LEN (body_len 0),
        // uncompressed_size = u32::MAX (~4 GiB) — must error, not pre-allocate.
        let frame = build_frame(0x02, 9, u32::MAX, b"");
        let mut cursor = std::io::Cursor::new(frame);
        let err =
            decode_frame(&mut cursor).expect_err("oversized uncompressed_size must be rejected");
        assert!(
            err.to_string().contains("uncompressed_size"),
            "expected uncompressed_size cap error, got: {err}"
        );
    }

    #[test]
    fn decode_frame_rejects_oversized_compressed() {
        // compressed_size = u32::MAX (~4 GiB) — must error before the body
        // buffer is sized, regardless of the checksum bytes that follow.
        let frame = build_frame(0x82, u32::MAX, 0, b"");
        let mut cursor = std::io::Cursor::new(frame);
        let err =
            decode_frame(&mut cursor).expect_err("oversized compressed_size must be rejected");
        assert!(
            err.to_string().contains("compressed_size"),
            "expected compressed_size cap error, got: {err}"
        );
    }

    #[test]
    fn decode_frame_rejects_valid_checksum_declared_over_cap() {
        // A fully well-formed frame whose declared uncompressed_size is over
        // the cap: the checksum IS valid for this header+body, so only the
        // pre-allocation cap can stop it. It must fire before any allocation
        // (the compressed body is present and tiny).
        let frame = build_frame(0x02, 9 + 4, u32::MAX, b"abcd");
        let mut cursor = std::io::Cursor::new(frame);
        let err = decode_frame(&mut cursor)
            .expect_err("over-cap declaration with valid checksum must be rejected");
        assert!(
            err.to_string().contains("uncompressed_size"),
            "expected uncompressed_size cap error, got: {err}"
        );
    }

    #[test]
    fn decode_frame_rejects_compressed_below_header_len() {
        // compressed_size < 9 leaves no room for the mandatory header —
        // rejected before any subtraction or allocation.
        let frame = build_frame(0x02, 8, 0, b"");
        let mut cursor = std::io::Cursor::new(frame);
        let err = decode_frame(&mut cursor).expect_err("short compressed_size must be rejected");
        assert!(
            err.to_string().contains("header length"),
            "expected header length error, got: {err}"
        );
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
    #[cfg(feature = "zstd")]
    fn test_zstd_frame_roundtrip() {
        let payload: Vec<u8> = b"xyzxyzxyzxyz".repeat(64);
        let frame = encode_frame(&payload, CompressionMethod::Zstd).expect("test operation failed");
        assert_eq!(frame[16], 0x90);
        let decoded = decode_frame_bytes(&frame).expect("test operation failed");
        assert_eq!(decoded, payload);
    }

    /// A zstd frame that actually expands past its declared uncompressed_size
    /// (with a valid checksum over the lying header + real body) must be
    /// rejected during decompression — the output is bounded by the declared,
    /// capped size instead of decoding everything first.
    #[test]
    #[cfg(feature = "zstd")]
    fn decode_frame_rejects_zstd_expansion_beyond_declared() {
        let payload = vec![0u8; 128 * 1024];
        let body =
            zstd::stream::encode_all(&payload[..], 3).expect("test zstd encode must succeed");
        let declared: u32 = 1024;
        // Header declares 1 KiB; the body really decodes to 128 KiB.
        let frame = build_frame(0x90, 9 + body.len() as u32, declared, &body);
        let err = decode_frame_bytes(&frame)
            .expect_err("zstd expansion beyond declared size must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("zstd"), "expected zstd error, got: {msg}");
        assert!(
            msg.contains(&declared.to_string()),
            "error must name the declared size, got: {msg}"
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
