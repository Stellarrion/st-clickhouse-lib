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

// ─── Server-controlled byte-length caps ─────────────────────────────────────
//
// Native column payloads are also sized from server-controlled values: every
// String/JSON column value is prefixed by a varint length, fixed-width /
// offset / LowCardinality-index buffers are `rows * width` products, Array and
// Map columns carry an 8-byte little-endian offset whose last value becomes
// the inner element row count, and string values accumulate one column at a
// time. Before any of these lengths feeds a `resize`/`reserve`/`with_capacity`
// or bounds a read loop, they are validated against the constants below so a
// small crafted header cannot trigger a multi-hundred-MiB eager allocation.

/// Maximum accepted byte length of a single inbound string value (String and
/// JSON columns, protocol strings). Retains the historical clickhouse-cpp
/// wire limit (16 MiB - 1) for compatibility.
pub(crate) const MAX_STRING_BYTES: usize = 0x00FF_FFFF;

/// Maximum accepted total byte length of a single inbound column's wire data
/// (accumulated string values, fixed-width / offset / index buffers, and
/// LowCardinality materialized output). Bounds one column only; a streamed
/// response may legally deliver many columns and many blocks.
pub(crate) const MAX_COLUMN_BYTES: usize = 64 * 1024 * 1024;

/// Validates a server-controlled per-value string length (String/JSON column
/// value) against [`MAX_STRING_BYTES`] before it sizes any buffer.
pub(crate) fn checked_string_len(value: u64, what: &str) -> Result<usize, String> {
    let value = usize::try_from(value)
        .map_err(|_| format!("{what} {value} exceeds limit {MAX_STRING_BYTES}"))?;
    if value > MAX_STRING_BYTES {
        return Err(format!("{what} {value} exceeds limit {MAX_STRING_BYTES}"));
    }
    Ok(value)
}

/// Adds `add` claimed bytes to a column's running total with checked addition
/// and the [`MAX_COLUMN_BYTES`] cap. Callers invoke this before the matching
/// `reserve`/`resize`/read so a lying length fails with a deterministic
/// protocol error instead of allocating.
pub(crate) fn checked_column_bytes(acc: usize, add: usize, what: &str) -> Result<usize, String> {
    let total = acc
        .checked_add(add)
        .ok_or_else(|| format!("{what} cumulative byte length overflow"))?;
    if total > MAX_COLUMN_BYTES {
        return Err(format!(
            "{what} cumulative byte length {total} exceeds limit {MAX_COLUMN_BYTES}"
        ));
    }
    Ok(total)
}

/// Validates a `rows * width` fixed-width / offset / index buffer byte length
/// with checked multiplication and the [`MAX_COLUMN_BYTES`] cap, before any
/// `Vec::with_capacity`/`resize` sized from it.
pub(crate) fn checked_column_len(rows: usize, width: usize, what: &str) -> Result<usize, String> {
    let len = rows
        .checked_mul(width)
        .ok_or_else(|| format!("{what} byte length overflow"))?;
    if len > MAX_COLUMN_BYTES {
        return Err(format!(
            "{what} byte length {len} exceeds limit {MAX_COLUMN_BYTES}"
        ));
    }
    Ok(len)
}

/// Validates a nested inner element total (the last Array/Map offset) against
/// [`MAX_BLOCK_ROWS`], before it becomes an inner column row count.
pub(crate) fn checked_nested_total(value: u64, what: &str) -> Result<usize, String> {
    let value = usize::try_from(value)
        .map_err(|_| format!("{what} total {value} exceeds limit {MAX_BLOCK_ROWS}"))?;
    if value > MAX_BLOCK_ROWS {
        return Err(format!(
            "{what} total {value} exceeds limit {MAX_BLOCK_ROWS}"
        ));
    }
    Ok(value)
}

/// Validates one Array/Map offset against the previous offset and the nested
/// element cap. ClickHouse offsets are cumulative prefix sums, so they must be
/// non-decreasing, and the running maximum (the last offset) is the inner
/// element row count bounded by [`MAX_BLOCK_ROWS`].
pub(crate) fn checked_monotonic_offset(
    prev: usize, value: u64, what: &str,
) -> Result<usize, String> {
    let value = checked_nested_total(value, what)?;
    if value < prev {
        return Err(format!("{what} decreased from {prev} to {value}"));
    }
    Ok(value)
}

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

#[cfg(test)]
mod byte_limit_tests {
    use super::{
        MAX_COLUMN_BYTES, MAX_STRING_BYTES, checked_column_bytes, checked_column_len,
        checked_monotonic_offset, checked_nested_total, checked_string_len,
    };

    #[test]
    fn string_len_boundary_passes() {
        assert_eq!(
            checked_string_len(MAX_STRING_BYTES as u64, "string value length").expect("cap passes"),
            MAX_STRING_BYTES
        );
    }

    #[test]
    fn string_len_cap_plus_one_is_rejected() {
        let err = checked_string_len(MAX_STRING_BYTES as u64 + 1, "string value length")
            .expect_err("cap + 1");
        assert_eq!(err, "string value length 16777216 exceeds limit 16777215");
    }

    #[test]
    fn string_len_u64_max_and_2_pow_40_are_rejected() {
        for hostile in [1u64 << 40, u64::MAX] {
            let err = checked_string_len(hostile, "string value length")
                .expect_err("hostile string length must be rejected");
            assert_eq!(
                err,
                format!("string value length {hostile} exceeds limit 16777215")
            );
        }
    }

    #[test]
    fn column_bytes_accumulate_to_cap() {
        let mut acc = 0usize;
        for _ in 0..4 {
            acc = checked_column_bytes(acc, 16 * 1024 * 1024, "string value")
                .expect("four 16 MiB values fit the 64 MiB column cap");
        }
        assert_eq!(acc, MAX_COLUMN_BYTES);
    }

    #[test]
    fn column_bytes_cap_plus_one_is_rejected() {
        let err = checked_column_bytes(MAX_COLUMN_BYTES, 1, "string value")
            .expect_err("cap + 1 cumulative claim must be rejected");
        assert_eq!(
            err,
            "string value cumulative byte length 67108865 exceeds limit 67108864"
        );
    }

    #[test]
    fn column_bytes_overflow_is_rejected() {
        assert!(checked_column_bytes(usize::MAX, 1, "string value").is_err());
    }

    #[test]
    fn column_len_boundary_passes_and_cap_plus_one_is_rejected() {
        let rows = MAX_COLUMN_BYTES / 32;
        assert_eq!(
            checked_column_len(rows, 32, "fixed-width").expect("exact cap passes"),
            MAX_COLUMN_BYTES
        );
        let err = checked_column_len(rows + 1, 32, "fixed-width").expect_err("cap + 1 rejected");
        assert_eq!(
            err,
            "fixed-width byte length 67108896 exceeds limit 67108864"
        );
    }

    #[test]
    fn column_len_multiplication_overflow_is_rejected() {
        assert!(checked_column_len(usize::MAX, usize::MAX, "fixed-width").is_err());
    }

    #[test]
    fn nested_total_boundary_passes_and_cap_plus_one_is_rejected() {
        assert_eq!(
            checked_nested_total(super::MAX_BLOCK_ROWS as u64, "array offset")
                .expect("exact cap passes"),
            super::MAX_BLOCK_ROWS
        );
        let err = checked_nested_total(super::MAX_BLOCK_ROWS as u64 + 1, "array offset")
            .expect_err("cap + 1 rejected");
        assert_eq!(err, "array offset total 10000001 exceeds limit 10000000");
    }

    #[test]
    fn nested_total_u64_max_is_rejected() {
        let err = checked_nested_total(u64::MAX, "array offset").expect_err("u64::MAX rejected");
        assert_eq!(
            err,
            "array offset total 18446744073709551615 exceeds limit 10000000"
        );
    }

    #[test]
    fn monotonic_offset_accepts_non_decreasing_and_rejects_decrease() {
        let mut prev = 0usize;
        for value in [0u64, 2, 5, 5, 9] {
            prev = checked_monotonic_offset(prev, value, "array offset")
                .expect("non-decreasing offsets pass");
        }
        assert_eq!(prev, 9);
        let err =
            checked_monotonic_offset(9, 7, "array offset").expect_err("decreasing offset rejected");
        assert_eq!(err, "array offset decreased from 9 to 7");
    }

    #[test]
    fn monotonic_offset_rejects_huge_value() {
        let err = checked_monotonic_offset(0, 1u64 << 60, "array offset")
            .expect_err("2^60 offset rejected");
        assert_eq!(
            err,
            "array offset total 1152921504606846976 exceeds limit 10000000"
        );
    }
}
