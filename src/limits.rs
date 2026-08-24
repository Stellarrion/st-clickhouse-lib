//! Shared transport allocation limits (internal).
//!
//! Server-controlled lengths (chunked-transport chunk headers and compression
//! frame size fields) are untrusted 32-bit values. Before any buffer is sized
//! from them, they are validated against the constants below so a small header
//! can never trigger a multi-GiB allocation.
//!
//! The 64 MiB value keeps ample headroom over default ~1 MiB native protocol
//! blocks while bounding a hostile peer's per-frame allocation cost.

/// Maximum accepted length of a single inbound chunked-transport chunk
/// (chunked native protocol framing), in bytes.
pub(crate) const MAX_CHUNK_LEN: usize = 64 * 1024 * 1024;

/// Maximum accepted compressed size (header + body) and uncompressed size of a
/// single compression frame, in bytes.
pub(crate) const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
