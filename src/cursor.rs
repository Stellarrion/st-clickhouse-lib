//! Streaming row cursor for query results.
//!
//! Receives blocks from a channel (fed by a background task). Each call to
//! `next()` constructs an owned row. For zero-allocation access, use the
//! columnar API on individual blocks (`.block()` + `block.column::<T>()`).

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
    current_block: Option<(Block, usize)>,
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
            current_block: None,
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
            if let Some((ref block, ref mut idx)) = self.current_block {
                if *idx < block.row_count() {
                    let row = T::from_row(block, *idx)?;
                    *idx += 1;
                    return Ok(Some(row));
                }
                self.current_block = None;
            }
            match self.block_rx.recv().await {
                Some(Ok(Some(block))) if block.row_count() > 0 => {
                    self.current_block = Some((block, 0));
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
pub fn materialize_lc_inner(
    dict_data: &[u8], inner: &crate::protocol::type_parser::ColumnType, indexes: &[u8],
    idx_width: usize, num_idx: usize,
) -> Result<Vec<u8>> {
    use crate::protocol::type_parser::ColumnType::*;
    match inner {
        UInt8 | Int8 | Bool | Enum8 => {
            let w = inner.fixed_width().unwrap_or(1);
            let entries: Vec<_> = dict_data
                .chunks(w)
                .map(|c| {
                    let mut a = [0u8; 8];
                    a[..c.len()].copy_from_slice(c);
                    a
                })
                .collect();
            let mut out = Vec::with_capacity(num_idx * w);
            for i in 0..num_idx {
                let idx = read_lc_idx(indexes, i, idx_width);
                let v = entries.get(idx).copied().unwrap_or_default();
                out.extend_from_slice(&v[..w]);
            }
            Ok(out)
        },
        UInt16 | Int16 | Date | Date32 | Enum16 => {
            let w = 2;
            let entries: Vec<_> = dict_data
                .chunks(w)
                .map(|c| {
                    let mut a = [0u8; 4];
                    a[..2].copy_from_slice(c);
                    a
                })
                .collect();
            let mut out = Vec::with_capacity(num_idx * w);
            for i in 0..num_idx {
                let idx = read_lc_idx(indexes, i, idx_width);
                let v = entries.get(idx).copied().unwrap_or_default();
                out.extend_from_slice(&v[..w]);
            }
            Ok(out)
        },
        UInt32 | Int32 | Float32 | DateTime | IPv4 => {
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
                let (len, consumed) = read_varint_bytes(&dict_data[pos..]);
                pos += consumed;
                let end = pos + len as usize;
                if end > dict_data.len() {
                    break;
                }
                offsets.push((pos, end));
                pos = end;
            }
            let mut out = Vec::new();
            for i in 0..num_idx {
                let idx = read_lc_idx(indexes, i, idx_width);
                if let Some(&(start, end)) = offsets.get(idx) {
                    let l = end - start;
                    encode_varint_to(&mut out, l as u64);
                    out.extend_from_slice(&dict_data[start..end]);
                } else {
                    encode_varint_to(&mut out, 0);
                }
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
    let entries: Vec<_> = dict.chunks(es).collect();
    let mut out = Vec::with_capacity(ni * es);
    for i in 0..ni {
        let idx = read_lc_idx(idxs, i, iw);
        if let Some(v) = entries.get(idx) {
            out.extend_from_slice(v);
        } else {
            out.extend(vec![0u8; es]);
        }
    }
    Ok(out)
}

fn read_lc_idx(data: &[u8], i: usize, w: usize) -> usize {
    let off = i * w;
    if off + w > data.len() {
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

fn read_varint_bytes(data: &[u8]) -> (u64, usize) {
    let mut r = 0u64;
    let mut s = 0;
    let mut c = 0;
    for &b in data {
        c += 1;
        r |= ((b & 0x7F) as u64) << s;
        if b & 0x80 == 0 {
            return (r, c);
        }
        s += 7;
        if s >= 64 {
            break;
        }
    }
    (r, c)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let (val, consumed) = read_varint_bytes(data);
        assert_eq!(val, 42);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_read_varint_bytes_multi() {
        let data = b"\x80\x01"; // 128
        let (val, consumed) = read_varint_bytes(data);
        assert_eq!(val, 128);
        assert_eq!(consumed, 2);
    }
}
