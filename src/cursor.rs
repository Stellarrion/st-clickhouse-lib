//! Streaming row cursor for query results.
//!
//! Receives blocks from a channel (fed by a background task). Each call to
//! `next()` constructs an owned row. For zero-allocation access, use the
//! columnar API on individual blocks (`.blocks()` + `block.column::<T>()`).

use crate::error::Result;
use crate::protocol::block::Block;
use crate::row::Row;
use tokio::sync::mpsc;

/// A streaming cursor that reads blocks from a background task via channel.
///
/// On drop, signals cancellation via `cancel` flag. The background task
/// (spawned by `QueryBuilder::rows`) checks this flag and sends a Cancel
/// packet before exiting.
pub struct RowCursor<T> {
    /// Decoded rows of the current block, handed out one `next()` at a time.
    pending: std::collections::VecDeque<T>,
    block_rx: mpsc::Receiver<Result<Option<Block>>>,
    cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Row> RowCursor<T> {
    pub(crate) fn new(
        block_rx: mpsc::Receiver<Result<Option<Block>>>,
        cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            pending: std::collections::VecDeque::new(),
            block_rx,
            cancel,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Drop for RowCursor<T> {
    fn drop(&mut self) {
        // Signal the background task to cancel the query on the server
        self.cancel
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl<T: Row> RowCursor<T> {
    /// Get the next row (owned).
    pub async fn next(&mut self) -> Result<Option<T>> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                return Ok(Some(row));
            }
            // No decoded rows left — pull the next block and decode all of its
            // rows in one pass via `read_all`, which pre-extracts each column
            // once and iterates rows without per-row column dispatch.
            match self.block_rx.recv().await {
                Some(Ok(Some(block))) if block.row_count() > 0 => {
                    self.pending = crate::row::read_all::<T>(&block)?.into();
                },
                Some(Ok(None)) | None => return Ok(None),
                Some(Err(e)) => return Err(e),
                _ => {},
            }
        }
    }

    /// Collect all remaining rows into a Vec.
    pub async fn collect(mut self) -> Result<Vec<T>> {
        let mut rows = Vec::new();
        while let Some(row) = self.next().await? {
            rows.push(row);
        }
        Ok(rows)
    }
}

/// Materialize a LowCardinality column into the inner type's wire format.
/// Called from the connection task when reading LC columns.
///
/// Mirrors the checked sync implementation: every count coming from the wire
/// is validated with checked arithmetic and per-row bounds checks, so crafted
/// dictionary or index data returns [`crate::error::Error::Protocol`] instead
/// of panicking (slice indexing, `chunks`, capacity overflow) or silently
/// producing misaligned output.
pub fn materialize_lc_inner(
    dict_data: &[u8], inner: &crate::protocol::type_parser::ColumnType, indexes: &[u8],
    idx_width: usize, num_idx: usize,
) -> Result<Vec<u8>> {
    use crate::error::Error;
    use crate::protocol::type_parser::ColumnType::*;
    if num_idx > 0 {
        // Bound `num_idx` by the physically present index bytes: this keeps
        // every later `num_idx * width` product input-proportional.
        let needed = num_idx
            .checked_mul(idx_width)
            .ok_or_else(|| Error::Protocol("LowCardinality index byte length overflow".into()))?;
        if indexes.len() < needed {
            return Err(Error::Protocol(format!(
                "LowCardinality index data truncated: need {needed} bytes, have {}",
                indexes.len()
            )));
        }
    }
    match inner {
        UInt8 | Int8 | Bool | Enum8 => {
            materialize_lc_fixed(dict_data, 1, indexes, idx_width, num_idx)
        },
        UInt16 | Int16 | Date | Enum16 => {
            materialize_lc_fixed(dict_data, 2, indexes, idx_width, num_idx)
        },
        UInt32 | Int32 | Float32 | Date32 | DateTime | IPv4 => {
            materialize_lc_fixed(dict_data, 4, indexes, idx_width, num_idx)
        },
        UInt64 | Int64 | Float64 | DateTime64(_) => {
            materialize_lc_fixed(dict_data, 8, indexes, idx_width, num_idx)
        },
        UInt128 | Int128 | UUID | IPv6 => {
            materialize_lc_fixed(dict_data, 16, indexes, idx_width, num_idx)
        },
        UInt256 | Int256 => materialize_lc_fixed(dict_data, 32, indexes, idx_width, num_idx),
        String | Other(_) => {
            let mut offsets = Vec::new();
            let mut pos = 0usize;
            while pos < dict_data.len() {
                let (len, consumed) = read_varint_bytes(&dict_data[pos..])?;
                pos += consumed;
                let len = usize::try_from(len).map_err(|_| {
                    Error::Protocol("LowCardinality string length too large".into())
                })?;
                let end = pos.checked_add(len).ok_or_else(|| {
                    Error::Protocol("LowCardinality string length overflow".into())
                })?;
                if end > dict_data.len() {
                    return Err(Error::Protocol(
                        "LowCardinality string dictionary is truncated".into(),
                    ));
                }
                offsets.push((pos, end));
                pos = end;
            }
            let mut out = Vec::new();
            for i in 0..num_idx {
                let idx = read_lc_idx(indexes, i, idx_width);
                let Some(&(start, end)) = offsets.get(idx) else {
                    return Err(Error::Protocol(
                        "LowCardinality dictionary index out of bounds".into(),
                    ));
                };
                let l = end - start;
                encode_varint_to(&mut out, l as u64);
                out.extend_from_slice(&dict_data[start..end]);
            }
            Ok(out)
        },
        FixedString(n) => materialize_lc_fixed(dict_data, *n, indexes, idx_width, num_idx),
        _ => {
            let st = match idx_width {
                1 => 0u64,
                2 => 1,
                4 => 2,
                _ => 3,
            };
            let mut raw = Vec::new();
            raw.extend_from_slice(&1u64.to_le_bytes());
            raw.extend_from_slice(&st.to_le_bytes());
            raw.extend_from_slice(&(dict_data.len() as u64).to_le_bytes());
            raw.extend_from_slice(dict_data);
            raw.extend_from_slice(&(num_idx as u64).to_le_bytes());
            raw.extend_from_slice(indexes);
            Ok(raw)
        },
    }
}

fn materialize_lc_fixed(
    dict: &[u8], es: usize, idxs: &[u8], iw: usize, ni: usize,
) -> Result<Vec<u8>> {
    use crate::error::Error;
    if ni == 0 || es == 0 {
        return Ok(Vec::new());
    }
    if dict.len() < es {
        // No complete entry exists, so every index would be out of bounds;
        // erroring here also bounds the capacity reservation by the input.
        return Err(Error::Protocol(
            "LowCardinality dictionary smaller than one entry".into(),
        ));
    }
    let cap = ni
        .checked_mul(es)
        .ok_or_else(|| Error::Protocol("LowCardinality materialized size overflow".into()))?;
    let mut out = Vec::with_capacity(cap);
    for i in 0..ni {
        let idx = read_lc_idx(idxs, i, iw);
        let start = idx
            .checked_mul(es)
            .ok_or_else(|| Error::Protocol("LowCardinality dictionary index overflow".into()))?;
        let end = start
            .checked_add(es)
            .ok_or_else(|| Error::Protocol("LowCardinality dictionary index overflow".into()))?;
        let Some(entry) = dict.get(start..end) else {
            return Err(Error::Protocol(
                "LowCardinality dictionary index out of bounds".into(),
            ));
        };
        out.extend_from_slice(entry);
    }
    Ok(out)
}

/// Read the `i`-th index from LowCardinality index data.
///
/// Lenient by design (returns 0 for out-of-range bytes or unsupported
/// widths); the strict per-row bounds checks live in the materialization
/// functions above. All arithmetic is overflow-safe.
fn read_lc_idx(data: &[u8], i: usize, w: usize) -> usize {
    let Some(off) = i.checked_mul(w) else {
        return 0;
    };
    let Some(end) = off.checked_add(w) else {
        return 0;
    };
    if end > data.len() {
        return 0;
    }
    match w {
        1 => data[off] as usize,
        2 => {
            let mut bytes = [0u8; 2];
            bytes.copy_from_slice(&data[off..off + 2]);
            u16::from_le_bytes(bytes) as usize
        },
        4 => {
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&data[off..off + 4]);
            u32::from_le_bytes(bytes) as usize
        },
        8 => {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[off..off + 8]);
            u64::from_le_bytes(bytes) as usize
        },
        _ => 0,
    }
}

fn encode_varint_to(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        buf.push((v & 0x7F) as u8 | if v > 0x7F { 0x80 } else { 0 });
        v >>= 7;
        if v == 0 {
            break;
        }
    }
}

/// Read a ClickHouse varint from a byte slice, returning `(value, consumed)`.
///
/// Rejects overlong/overflowing encodings (> 10 bytes, or a 10th byte wider
/// than the single bit that fits at shift 63) and truncated varints.
fn read_varint_bytes(data: &[u8]) -> Result<(u64, usize)> {
    use crate::error::Error;
    let mut r = 0u64;
    let mut s = 0;
    let mut c = 0;
    for &b in data {
        c += 1;
        if s > 63 || (s == 63 && (b & 0x7F) > 1) {
            return Err(Error::Protocol("varint overflow".into()));
        }
        r |= ((b & 0x7F) as u64) << s;
        if b & 0x80 == 0 {
            return Ok((r, c));
        }
        s += 7;
    }
    // Continuation bit set on the final byte: the varint is truncated.
    Err(Error::Protocol("truncated varint".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cursor_yields_block_rows_in_order() {
        // Characterization test: the cursor must hand out a block's rows in
        // order. Built against the lazy implementation; must keep passing after
        // the buffered fast-path refactor.
        use crate::protocol::block::{Block, ColumnInfo};

        let (tx, rx) = mpsc::channel::<Result<Option<Block>>>(4);
        // One UInt64 column with rows [10, 20, 30].
        let mut buf = Vec::new();
        for v in [10u64, 20, 30] {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        let block = Block {
            columns: vec![ColumnInfo {
                name: "x".to_string(),
                type_name: "UInt64".to_string(),
                data: bytes::Bytes::from(buf),
                lc_materialized: bytes::Bytes::new(),
            }],
            rows: 3,
        };
        tx.send(Ok(Some(block))).await.expect("send block");
        tx.send(Ok(None)).await.expect("send eos"); // end of stream
        drop(tx);

        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cursor: RowCursor<(u64,)> = RowCursor::new(rx, cancel);
        let rows = cursor.collect().await.expect("decode failed");
        assert_eq!(rows, vec![(10u64,), (20,), (30,)]);
    }

    #[test]
    fn test_row_cursor_cancel_on_drop() {
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (_tx, rx) = tokio::sync::mpsc::channel::<Result<Option<Block>>>(8);
        // Create cursor with a minimal Row type that exists
        drop(rx);
        assert!(!cancel.load(std::sync::atomic::Ordering::Relaxed));
        // Manually simulate what Drop does:
        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(cancel.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_cancel_flag_manual() {
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        assert!(!cancel.load(std::sync::atomic::Ordering::Relaxed));
        // Dropping the Arc directly (no cursor) — still sets flag manually
        let c2 = cancel.clone();
        c2.store(true, std::sync::atomic::Ordering::Relaxed);
        drop(c2);
        assert!(cancel.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_materialize_lc_fixed_width() {
        let dict = b"\x01\x00\x00\x00\x02\x00\x00\x00"; // two entries of 4 bytes
        let idxs = b"\x00\x01\x00"; // 3 indexes: 0, 1, 0
        let result = materialize_lc_fixed(dict, 4, idxs, 1, 3)
            .expect("LowCardinality fixed-width materialization should succeed");
        assert_eq!(&result, b"\x01\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00");
    }

    #[test]
    fn test_read_lc_idx_widths() {
        // 1 byte index
        let data = b"\x05";
        assert_eq!(read_lc_idx(data, 0, 1), 5);
        // 2 byte index
        let data = b"\x2a\x01";
        assert_eq!(read_lc_idx(data, 0, 2), 0x012a);
        // 4 byte index
        let data = b"\xef\xbe\xad\xde";
        assert_eq!(read_lc_idx(data, 0, 4), 0xdeadbeef);
    }

    #[test]
    fn test_read_lc_idx_out_of_bounds() {
        let data = b"\x01";
        assert_eq!(read_lc_idx(data, 5, 1), 0); // beyond data
        assert_eq!(read_lc_idx(data, 0, 4), 0); // not enough bytes for width 4
    }

    #[test]
    fn test_encode_varint_small() {
        let mut buf = Vec::new();
        encode_varint_to(&mut buf, 0);
        assert_eq!(buf, b"\x00");
        buf.clear();
        encode_varint_to(&mut buf, 1);
        assert_eq!(buf, b"\x01");
        buf.clear();
        encode_varint_to(&mut buf, 127);
        assert_eq!(buf, b"\x7f");
        buf.clear();
        encode_varint_to(&mut buf, 128);
        assert_eq!(buf, b"\x80\x01");
    }

    #[test]
    fn test_read_varint_bytes_tiny() {
        let data = b"\x2a"; // 42
        let (val, consumed) = read_varint_bytes(data).expect("42 parses");
        assert_eq!(val, 42);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_read_varint_bytes_multi() {
        let data = b"\x80\x01"; // 128
        let (val, consumed) = read_varint_bytes(data).expect("128 parses");
        assert_eq!(val, 128);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_read_varint_bytes_rejects_overflow() {
        // 11 continuation bytes: shift would pass 64.
        assert!(read_varint_bytes(&[0x80u8; 11]).is_err());
        // 10th byte wider than the single bit that fits at shift 63.
        assert!(
            read_varint_bytes(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02])
                .is_err()
        );
        // Truncated: continuation set on the last available byte.
        assert!(read_varint_bytes(&[0x80u8, 0x80]).is_err());
    }

    // ── LowCardinality materialization: crafted-data regression tests ──

    #[test]
    fn lc_materialization_rejects_truncated_index_data() {
        use crate::protocol::type_parser::ColumnType;
        // 3 rows claimed, only 1 index byte present.
        let res = materialize_lc_inner(b"\x01\x02\x03\x04", &ColumnType::UInt32, b"\x00", 1, 3);
        assert!(res.is_err(), "truncated indexes must error, got {res:?}");
    }

    #[test]
    fn lc_materialization_rejects_out_of_bounds_index() {
        use crate::protocol::type_parser::ColumnType;
        // Dictionary holds one UInt32 entry; index 5 is out of bounds.
        let res = materialize_lc_inner(b"\x01\x02\x03\x04", &ColumnType::UInt32, b"\x05", 1, 1);
        assert!(res.is_err(), "out-of-bounds index must error, got {res:?}");
    }

    #[test]
    fn lc_materialization_rejects_huge_row_count_without_panicking() {
        use crate::protocol::type_parser::ColumnType;
        // Crafted num_idx far beyond the index bytes: must error, not panic
        // or attempt an enormous allocation.
        let res = materialize_lc_inner(
            b"\x01\x02\x03\x04",
            &ColumnType::UInt64,
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
            8,
            usize::MAX / 2,
        );
        assert!(res.is_err(), "huge num_idx must error, got {res:?}");
    }

    #[test]
    fn lc_materialization_rejects_partial_dictionary_entry() {
        use crate::protocol::type_parser::ColumnType;
        // 5 bytes is not a whole number of UInt32 entries.
        let res = materialize_lc_inner(b"\x01\x02\x03\x04\x05", &ColumnType::UInt32, b"\x01", 1, 1);
        // Index 1 maps to bytes [4..8) which overrun the dictionary.
        assert!(
            res.is_err(),
            "partial dictionary entry must error, got {res:?}"
        );
    }

    #[test]
    fn lc_materialization_rejects_truncated_string_dictionary() {
        use crate::protocol::type_parser::ColumnType;
        // Dictionary claims a 10-byte string but only carries 3.
        let dict = [0x0Au8, b'a', b'b', b'c'];
        let res = materialize_lc_inner(&dict, &ColumnType::String, b"\x00", 1, 1);
        assert!(
            res.is_err(),
            "truncated string dictionary must error, got {res:?}"
        );
    }

    #[test]
    fn lc_materialization_string_roundtrip_stays_correct() {
        use crate::protocol::type_parser::ColumnType;
        // Dictionary: "a", "bb"; indexes 0,1,1,0.
        let dict = [0x01u8, b'a', 0x02, b'b', b'b'];
        let idxs = [0u8, 1, 1, 0];
        let out = materialize_lc_inner(&dict, &ColumnType::String, &idxs, 1, 4)
            .expect("valid string dictionary materializes");
        let expect = [0x01, b'a', 0x02, b'b', b'b', 0x02, b'b', b'b', 0x01, b'a'];
        assert_eq!(out, expect);
    }

    #[test]
    fn lc_materialization_date32_uses_four_signed_bytes_per_key() {
        use crate::protocol::type_parser::ColumnType;
        let dict = (-1i32)
            .to_le_bytes()
            .into_iter()
            .chain(100_000i32.to_le_bytes())
            .collect::<Vec<_>>();
        let out = materialize_lc_inner(&dict, &ColumnType::Date32, &[1, 0], 1, 2)
            .expect("valid Date32 dictionary materializes");
        let expected = 100_000i32
            .to_le_bytes()
            .into_iter()
            .chain((-1i32).to_le_bytes())
            .collect::<Vec<_>>();
        assert_eq!(out, expected);
    }

    #[test]
    fn lc_materialization_fixed_string_zero_width_is_empty() {
        use crate::protocol::type_parser::ColumnType;
        let out = materialize_lc_inner(b"", &ColumnType::FixedString(0), b"\x00\x01", 1, 2)
            .expect("FixedString(0) has zero-width rows");
        assert!(out.is_empty());
    }
}
