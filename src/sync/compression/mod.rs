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

use crate::sync::error::Result;
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
            return Err(crate::sync::error::Error::Compression(
                "LZ4 compression requested but 'lz4' feature not enabled".into(),
            ));
        },
        #[cfg(feature = "zstd")]
        CompressionMethod::Zstd => zstd::stream::encode_all(data, 3)
            .map_err(|e| crate::sync::error::Error::Compression(format!("zstd encode: {e}")))?,
        #[cfg(not(feature = "zstd"))]
        CompressionMethod::Zstd => {
            return Err(crate::sync::error::Error::Compression(
                "ZSTD compression requested but 'zstd' feature not enabled".into(),
            ));
        },
    };

    // Outbound data is trusted, but the wire size fields are u32: refuse a
    // payload whose sizes do not fit instead of silently truncating the cast
    // (which would emit a corrupt frame). Legitimate streamed writes stay
    // allowed up to the 4 GiB wire limit.
    let compressed_size = u32::try_from(HEADER_LEN + body.len()).map_err(|_| {
        crate::sync::error::Error::Compression(format!(
            "compressed frame of {} bytes exceeds the u32 wire size field",
            HEADER_LEN + body.len()
        ))
    })?;
    let uncompressed_size = u32::try_from(data.len()).map_err(|_| {
        crate::sync::error::Error::Compression(format!(
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
    let mut lo_bytes = [0u8; 8];
    lo_bytes.copy_from_slice(&checksum_bytes[..8]);
    let mut hi_bytes = [0u8; 8];
    hi_bytes.copy_from_slice(&checksum_bytes[8..]);
    let expected_lo = u64::from_le_bytes(lo_bytes);
    let expected_hi = u64::from_le_bytes(hi_bytes);

    let mut header = [0u8; HEADER_LEN];
    r.read_exact(&mut header)?;
    let method_byte = header[0];
    let method = CompressionMethod::from_byte(method_byte).ok_or_else(|| {
        crate::sync::error::Error::Compression(format!(
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
        return Err(crate::sync::error::Error::Compression(format!(
            "compressed_size {compressed_size} < header length {HEADER_LEN}"
        )));
    }
    if compressed_size > crate::limits::MAX_FRAME_SIZE {
        return Err(crate::sync::error::Error::Compression(format!(
            "compressed_size {compressed_size} exceeds {} byte frame cap",
            crate::limits::MAX_FRAME_SIZE
        )));
    }
    if uncompressed_size > crate::limits::MAX_FRAME_SIZE {
        return Err(crate::sync::error::Error::Compression(format!(
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
        return Err(crate::sync::error::Error::Compression(
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
                .map_err(|e| crate::sync::error::Error::Compression(format!("lz4 decode: {e}")))?
        },
        #[cfg(not(feature = "lz4"))]
        CompressionMethod::Lz4 => {
            return Err(crate::sync::error::Error::Compression(
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
                    crate::sync::error::Error::Compression(format!(
                        "zstd decode: {e} (declared uncompressed_size {uncompressed_size})"
                    ))
                },
            )?
        },
        #[cfg(not(feature = "zstd"))]
        CompressionMethod::Zstd => {
            return Err(crate::sync::error::Error::Compression(
                "ZSTD decompression requested but 'zstd' feature not enabled".into(),
            ));
        },
    };

    if decompressed.len() != uncompressed_size {
        return Err(crate::sync::error::Error::Compression(format!(
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

// ─────────────────────────────────────────────────────────────────────────────
// Multi-frame decompressing reader (response side)
// ─────────────────────────────────────────────────────────────────────────────

/// Wire constants of one compression frame (see the module docs above).
/// `CHECKSUM_LEN` and `HEADER_LEN` are defined at the top of this module.
const COMPRESSED_BODY_HEADER_LEN: usize = HEADER_LEN;
/// Checksum plus the 9-byte size/method header.
const FRAME_HEADER_TOTAL: usize = CHECKSUM_LEN + HEADER_LEN;
/// Offset of the method byte inside a frame (after the 16-byte checksum).
const METHOD_OFFSET: usize = CHECKSUM_LEN;

/// Errors surfaced by [`DecompressingReader`]: compression/protocol failures
/// with enough context to identify the offending frame, and plain I/O from
/// the inner reader.
#[derive(Debug)]
pub enum DecompressError {
    /// Compression frame decode failure (checksum, caps, codec).
    Compression(String),
    /// The block-level decompressed-size budget was exceeded.
    Budget(String),
    /// Underlying transport I/O failure.
    Io(std::io::Error),
}

impl std::fmt::Display for DecompressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compression(msg) => write!(f, "compressed response: {msg}"),
            Self::Budget(msg) => write!(f, "compressed response: {msg}"),
            Self::Io(err) => write!(f, "compressed response I/O: {err}"),
        }
    }
}

impl std::error::Error for DecompressError {}

impl From<DecompressError> for crate::sync::error::Error {
    fn from(err: DecompressError) -> Self {
        match err {
            DecompressError::Io(io) => crate::sync::error::Error::Io(io),
            DecompressError::Compression(msg) => crate::sync::error::Error::Compression(msg),
            DecompressError::Budget(msg) => crate::sync::error::Error::Protocol(msg),
        }
    }
}

impl From<std::io::Error> for DecompressError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

/// A continuously decompressing [`Read`] over one Data-packet body.
///
/// The sync twin of the async `DecompressingStream`
/// (`src/connection/block_reader.rs`) and of clickhouse-cpp's
/// `CompressedReadBuffer`: ClickHouse flushes its ~1 MiB
/// `CompressedWriteBuffer` mid-packet, so a Data packet body larger than the
/// threshold arrives as a *sequence* of frames. This reader serves
/// decompressed bytes from an internal buffer and pulls the next frame from
/// the wrapped reader only when that buffer is drained, so a caller that
/// reads exactly one block consumes exactly the packet body — whether it
/// spans one frame or many — and never reads into the next packet. (Frames
/// never span packets: the server flushes at each packet boundary.)
///
/// Placement: the wrapper sits ABOVE any chunked framing (`ChunkedReader`)
/// — chunk framing wraps the compressed bytes, so decompression must happen
/// after de-chunking.
///
/// Budgets: each frame is bounded by [`crate::limits::MAX_FRAME_SIZE`] (via
/// [`decode_frame`]) and the cumulative decompressed size of the packet body
/// is charged against the block-level [`crate::limits::MAX_BLOCK_BYTES`]
/// budget, so a hostile frame sequence cannot grow the buffer without bound.
pub struct DecompressingReader<R> {
    inner: R,
    /// Decompressed bytes not yet served.
    buf: Vec<u8>,
    pos: usize,
    /// Sniffed as a plain (uncompressed) body: replay `buf`, then delegate.
    plain: bool,
    /// Cumulative decompressed bytes produced for this packet body.
    produced: usize,
}

impl<R: Read> DecompressingReader<R> {
    /// Sniff the payload start and wrap `inner`.
    ///
    /// Heuristic (identical to the async wrapper's): read the first byte and
    /// the following checksum bytes, then look at byte 16 of the payload (the
    /// method byte of a frame header). Unless it is 0x82/0x90/0x02 — and the
    /// compressed-size field covers at least the 9-byte frame header — the
    /// sniffed bytes are replayed verbatim and the reader degrades to a
    /// pass-through, so uncompressed bodies from non-conforming servers stay
    /// parseable. An oversized size claim is rejected outright.
    ///
    /// Unlike the async wrapper there is no sniff timeout: the sync socket
    /// has a blocking read timeout and the 17 sniff bytes of a real frame
    /// arrive together.
    pub fn new(mut inner: R) -> std::result::Result<Self, DecompressError> {
        let mut sniff = [0u8; METHOD_OFFSET + 1];
        inner.read_exact(&mut sniff[..1])?;
        inner.read_exact(&mut sniff[1..])?;

        match sniff[METHOD_OFFSET] {
            0x82 | 0x90 | 0x02 => {},
            _ => {
                return Ok(Self {
                    inner,
                    buf: sniff.to_vec(),
                    pos: 0,
                    plain: true,
                    produced: 0,
                });
            },
        }

        let mut frame = sniff.to_vec();
        let mut rest = [0u8; FRAME_HEADER_TOTAL - METHOD_OFFSET - 1];
        inner.read_exact(&mut rest)?;
        frame.extend_from_slice(&rest);

        let compressed_size =
            u32::from_le_bytes([frame[17], frame[18], frame[19], frame[20]]) as usize;
        // The method byte matched, so this looks like a compressed frame.
        // Sizes below the 9-byte header remain ambiguous with plain payloads
        // and keep the plain fallback; an oversized claim is rejected.
        if compressed_size < COMPRESSED_BODY_HEADER_LEN {
            return Ok(Self {
                inner,
                buf: frame,
                pos: 0,
                plain: true,
                produced: 0,
            });
        }
        if compressed_size > crate::limits::MAX_FRAME_SIZE {
            return Err(DecompressError::Compression(format!(
                "compressed_size {compressed_size} exceeds {} byte frame cap",
                crate::limits::MAX_FRAME_SIZE
            )));
        }

        let mut reader = Self {
            inner,
            buf: Vec::new(),
            pos: 0,
            plain: false,
            produced: 0,
        };
        // Complete + decode the first frame now, so construction fails fast
        // on a corrupt frame and the first read always has bytes to serve.
        frame.resize(CHECKSUM_LEN + compressed_size, 0);
        reader.inner.read_exact(&mut frame[FRAME_HEADER_TOTAL..])?;
        reader.swap_in_decoded(frame)?;
        Ok(reader)
    }

    /// Decode one complete frame, charge the block budget, and stage its
    /// decompressed body as the serving buffer.
    fn swap_in_decoded(&mut self, frame: Vec<u8>) -> std::result::Result<(), DecompressError> {
        let decompressed = decode_frame_bytes(&frame)
            .map_err(|err| DecompressError::Compression(err.to_string()))?;
        self.produced = crate::limits::checked_block_bytes(
            self.produced,
            decompressed.len(),
            "decompressed block",
        )
        .map_err(DecompressError::Budget)?;
        self.buf = decompressed;
        self.pos = 0;
        Ok(())
    }

    /// Pull the next frame to completion from the inner reader.
    ///
    /// A clean end-of-stream between frames is surfaced as `Ok(0)` (EOF) to
    /// the caller: the block parsers stop at the block end, so this only
    /// happens for over-reads after the final block — which are EOF by
    /// definition. A PARTIAL frame header (truncated response) stays an
    /// `UnexpectedEof` error, because at least one byte arrived: the
    /// distinction is made by probing one byte before committing to the
    /// header read.
    fn pull_next_frame(&mut self) -> std::io::Result<bool> {
        let mut first = [0u8; 1];
        match self.inner.read(&mut first) {
            Ok(0) => return Ok(false), // clean EOF at a frame boundary
            Ok(_) => {},
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "interrupted between compression frames",
                ));
            },
            Err(err) => return Err(err),
        }
        let mut frame = vec![0u8; FRAME_HEADER_TOTAL];
        frame[0] = first[0];
        self.inner.read_exact(&mut frame[1..])?;

        let compressed_size =
            u32::from_le_bytes([frame[17], frame[18], frame[19], frame[20]]) as usize;
        if compressed_size < COMPRESSED_BODY_HEADER_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "compressed_size {compressed_size} < header length {COMPRESSED_BODY_HEADER_LEN}"
                ),
            ));
        }
        if compressed_size > crate::limits::MAX_FRAME_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "compressed_size {compressed_size} exceeds {} byte frame cap",
                    crate::limits::MAX_FRAME_SIZE
                ),
            ));
        }
        frame.resize(CHECKSUM_LEN + compressed_size, 0);
        self.inner.read_exact(&mut frame[FRAME_HEADER_TOTAL..])?;
        self.swap_in_decoded(frame)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
        Ok(true)
    }
}

impl<R> DecompressingReader<R> {
    /// Take any decompressed (or replayed plain) bytes the caller has not
    /// consumed. For a conforming server this is empty — the block parsers
    /// consume exactly the packet body — but if a server ever packs bytes
    /// past the block into the same frame sequence, returning them keeps the
    /// caller's parse position correct instead of silently dropping them.
    pub fn into_pending(self) -> Vec<u8> {
        self.buf[self.pos..].to_vec()
    }
}

impl<R: Read> Read for DecompressingReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            if self.pos < self.buf.len() {
                let n = out.len().min(self.buf.len() - self.pos);
                out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            if self.plain {
                return self.inner.read(out);
            }
            match self.pull_next_frame()? {
                true => continue,
                false => return Ok(0),
            }
        }
    }
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
        // uncompressed_size = u32::MAX (~4 GiB) — must error before the body
        // buffer is sized.
        let frame = build_frame(0x02, 9, u32::MAX, b"");
        let err =
            decode_frame_bytes(&frame).expect_err("oversized uncompressed_size must be rejected");
        assert!(
            err.to_string().contains("uncompressed_size"),
            "expected uncompressed_size cap error, got: {err}"
        );
    }

    #[test]
    fn decode_frame_rejects_oversized_compressed() {
        // compressed_size = u32::MAX (~4 GiB) — must error before the body
        // buffer is sized.
        let frame = build_frame(0x82, u32::MAX, 0, b"");
        let err =
            decode_frame_bytes(&frame).expect_err("oversized compressed_size must be rejected");
        assert!(
            err.to_string().contains("compressed_size"),
            "expected compressed_size cap error, got: {err}"
        );
    }

    #[test]
    fn decode_frame_rejects_compressed_below_header_len() {
        // compressed_size < 9 leaves no room for the mandatory header —
        // rejected before any subtraction or allocation.
        let frame = build_frame(0x02, 8, 0, b"");
        let err = decode_frame_bytes(&frame).expect_err("short compressed_size must be rejected");
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

    /// THE multi-frame regression (server-free): a packet body split across
    /// two frames must reassemble into one logical decompressed stream, and
    /// the reader must stop exactly at the end of the frame sequence (the
    /// sentinel stays unread on the inner reader's remaining bytes).
    #[test]
    fn decompressing_reader_concatenates_two_frames() {
        decompressing_reader_case(CompressionMethod::None);
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn decompressing_reader_concatenates_two_frames_lz4() {
        decompressing_reader_case(CompressionMethod::Lz4);
    }

    fn decompressing_reader_case(method: CompressionMethod) {
        let body: Vec<u8> = b"block-body-bytes-".repeat(64); // 1,088 bytes
        let split = body.len() - 371; // split mid-payload like the ~1 MiB flush
        let frame_a = encode_frame(&body[..split], method).expect("encode first frame");
        let frame_b = encode_frame(&body[split..], method).expect("encode second frame");
        let wire = [frame_a, frame_b].concat();

        let mut cursor = std::io::Cursor::new(wire.clone());
        let mut reader = DecompressingReader::new(&mut cursor).expect("sniff as compressed");
        let mut out = Vec::new();
        reader.read_to_end(&mut out).expect("read both frames");
        assert_eq!(out, body, "both frames must decode as one stream");

        // Exactly the two frames were consumed: nothing more, nothing less.
        assert_eq!(
            cursor.position() as usize,
            wire.len(),
            "reader must stop at the end of the frame sequence"
        );
    }

    /// Three frames split inside a payload: every pull boundary must be
    /// transparent to the caller.
    #[test]
    fn decompressing_reader_concatenates_three_frames() {
        let body: Vec<u8> = (0u8..=255).cycle().take(3000).collect();
        let frame_a = encode_frame(&body[..97], CompressionMethod::None).expect("encode");
        let frame_b = encode_frame(&body[97..2001], CompressionMethod::None).expect("encode");
        let frame_c = encode_frame(&body[2001..], CompressionMethod::None).expect("encode");
        let mut cursor = std::io::Cursor::new([frame_a, frame_b, frame_c].concat());
        let mut reader = DecompressingReader::new(&mut cursor).expect("sniff as compressed");
        let mut out = Vec::new();
        reader.read_to_end(&mut out).expect("read all frames");
        assert_eq!(out, body);
    }

    /// The plain-prefix heuristic survives: a body whose 17th byte is not a
    /// compression method byte is served verbatim, and bytes after the
    /// sniffed prefix still come from the inner reader.
    #[test]
    fn decompressing_reader_falls_back_to_plain() {
        let wire = vec![0x01u8; 40];
        let mut cursor = std::io::Cursor::new(wire.clone());
        let mut reader = DecompressingReader::new(&mut cursor).expect("construct");
        let mut out = Vec::new();
        reader.read_to_end(&mut out).expect("plain passthrough");
        assert_eq!(out, wire, "plain bytes must be served verbatim");
    }

    /// A frame header whose compressed_size is u32::MAX must be rejected by
    /// the frame cap before any body buffer is sized.
    #[test]
    fn decompressing_reader_rejects_oversized_frame() {
        let frame = build_frame(0x82, u32::MAX, 0, b"");
        let mut cursor = std::io::Cursor::new(frame);
        let err = match DecompressingReader::new(&mut cursor) {
            Err(err) => err,
            Ok(_) => unreachable!("oversized frame must be rejected"),
        };
        assert!(
            err.to_string().contains("frame cap"),
            "expected frame cap error, got: {err}"
        );
    }

    /// A clean EOF between frames surfaces as EOF, not an error: the block
    /// parsers stop at the block end, so an over-read after the final block
    /// must look like a normal end of stream.
    #[test]
    fn decompressing_reader_clean_eof_between_frames() {
        let payload = b"single-frame-payload".to_vec();
        let frame = encode_frame(&payload, CompressionMethod::None).expect("encode");
        let mut cursor = std::io::Cursor::new(frame);
        let mut reader = DecompressingReader::new(&mut cursor).expect("sniff");
        let mut out = Vec::new();
        reader.read_to_end(&mut out).expect("decode");
        assert_eq!(out, payload);
        // Reading again after everything was consumed: EOF (Ok(0)), not an
        // error — the pull probes one byte and sees clean end of stream.
        let mut byte = [0u8; 1];
        let n = reader.read(&mut byte).expect("over-read must be clean EOF");
        assert_eq!(n, 0);
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
