//! Shared transport allocation limits (internal).
//!
//! Server-controlled framing lengths and item counts are untrusted. Before any
//! buffer or loop is sized from them, callers validate them against the
//! constants below so a small header cannot trigger a multi-GiB allocation or
//! capacity-overflow panic.
//!
//! The 64 MiB value keeps ample headroom over default ~1 MiB native protocol
//! blocks while bounding a hostile peer's per-frame allocation cost.

/// Maximum accepted length of a single inbound chunked-transport chunk
/// (chunked native protocol framing), in bytes.
pub(crate) const MAX_CHUNK_LEN: usize = 64 * 1024 * 1024;

/// Maximum accepted compressed size (header + body) and uncompressed size of a
/// single compression frame, in bytes.
pub(crate) const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

// ─── Server-controlled item-count caps ─────────────────────────────────────
//
// The native protocol encodes several list lengths as server-controlled
// varints or u64 fields (password complexity rules, ignored PartUUIDs, JSON
// paths, Dynamic subcolumn types, LowCardinality dictionary sizes, and native
// block column/row counts). Before any of these counts sizes a
// `Vec::with_capacity`/`reserve` call or bounds a read loop, it is validated
// against the constants below so a small crafted header can never trigger a
// capacity-overflow panic, a multi-gigabyte allocation, or an unbounded loop.
//
// Every cap is intentionally generous and far above normal ClickHouse output.
// These count caps prevent unbounded count-derived work but are not byte-volume
// budgets: wide columns can still be large. They bound a single list or block
// only — a streamed response may legally deliver many blocks, so these caps
// never limit total response rows.

/// Maximum accepted password complexity rule count in a server Hello packet.
/// Real servers send a handful of rules; 65,536 is orders of magnitude above
/// any legitimate deployment.
pub(crate) const MAX_PASSWORD_COMPLEXITY_RULES: usize = 65_536;

/// Maximum accepted item count for JSON path lists, Dynamic subcolumn type
/// lists, and LowCardinality dictionary keys. Legitimate values are in the
/// tens (LowCardinality dictionaries default to 8,192 entries at most).
pub(crate) const MAX_JSON_DYNAMIC_ITEMS: usize = 65_536;

/// Maximum accepted column count of a single native block.
pub(crate) const MAX_BLOCK_COLUMNS: usize = 65_536;

/// Maximum accepted row count of a single native block. This bounds one block
/// only; a streamed response may legally deliver many blocks each up to this
/// size.
pub(crate) const MAX_BLOCK_ROWS: usize = 10_000_000;

/// Maximum accepted PartUUID count in an ignored PartUUIDs packet.
pub(crate) const MAX_PART_UUIDS: usize = 1_048_576;

/// Validates a server-controlled item count before it sizes any allocation or
/// bounds any loop.
///
/// Returns the count as `usize`, or a message naming the field (`what`), the
/// received value, and the limit. Callers map the message to
/// `Error::Protocol` so a hostile peer produces a deterministic protocol
/// error instead of a panic or an oversized allocation.
pub(crate) fn checked_count(
    value: u64, what: &str, max: usize,
) -> std::result::Result<usize, String> {
    let limit = u64::try_from(max).unwrap_or(u64::MAX);
    if value > limit {
        return Err(format!("{what} count {value} exceeds limit {max}"));
    }
    usize::try_from(value).map_err(|_| format!("{what} count {value} exceeds limit {max}"))
}

#[cfg(test)]
mod count_tests {
    use super::checked_count;

    #[test]
    fn zero_and_small_counts_pass() {
        assert_eq!(checked_count(0, "item", 65_536).expect("0 within cap"), 0);
        assert_eq!(checked_count(1, "item", 65_536).expect("1 within cap"), 1);
    }

    #[test]
    fn boundary_count_passes() {
        assert_eq!(
            checked_count(65_536, "item", 65_536).expect("exact cap passes"),
            65_536
        );
    }

    #[test]
    fn cap_plus_one_is_rejected() {
        let err = checked_count(65_537, "item", 65_536).expect_err("cap + 1 rejected");
        assert_eq!(err, "item count 65537 exceeds limit 65536");
    }

    #[test]
    fn u64_max_is_rejected() {
        let err = checked_count(u64::MAX, "item", 65_536).expect_err("u64::MAX rejected");
        assert_eq!(err, "item count 18446744073709551615 exceeds limit 65536");
    }
}
