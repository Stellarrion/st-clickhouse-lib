// Shared string column logic. Provided in scope by the including
// module: super::super::error::Result, super::super::protocol::block::ReadColumnContext, super::super::protocol::wire, super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue}.
/// Borrowed String column data — zero copy.
///
/// String columns use **RowBinary format**: each value is `varint(length) + bytes`.
/// Since varints and bodies are interleaved, we keep the raw column bytes by
/// reference (borrowed from the block buffer) and store a `(start, end)` body
/// range per row, computed in a single scan. No string body is ever copied.
#[derive(Debug)]
pub struct StringColumnData<'a> {
    /// Per-row `(start, end)` byte range of each string body within `data`.
    ranges: Vec<(u64, u64)>,
    /// Raw column bytes (varints + bodies), borrowed from the block buffer.
    data: &'a [u8],
}

impl<'a> StringColumnData<'a> {
    pub(crate) fn new(ranges: Vec<(u64, u64)>, data: &'a [u8]) -> Self {
        Self { ranges, data }
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    fn range(&self, index: usize) -> Result<(usize, usize)> {
        let (start, end) = self.ranges.get(index).copied().ok_or_else(|| {
            super::super::error::Error::Protocol("StringColumnData: index out of bounds".into())
        })?;
        Ok((start as usize, end as usize))
    }

    /// Borrowed bytes of the value at `index` — zero alloc, zero copy.
    pub fn get_bytes(&self, index: usize) -> Result<&'a [u8]> {
        let (start, end) = self.range(index)?;
        self.data.get(start..end).ok_or_else(|| {
            super::super::error::Error::Protocol("StringColumnData: invalid range".into())
        })
    }

    pub fn get_string(&self, index: usize) -> Result<String> {
        let bytes = self.get_bytes(index)?;
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }

    /// Get a borrowed string reference — zero alloc.
    pub fn get_str(&self, index: usize) -> Result<&'a str> {
        let bytes = self.get_bytes(index)?;
        std::str::from_utf8(bytes).map_err(|e| {
            super::super::error::Error::Protocol(format!("invalid UTF-8 at row {index}: {e}"))
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<&'a [u8]>> + '_ {
        (0..self.len()).map(|i| self.get_bytes(i))
    }
}

impl<'a> ClickHouseColumnData<'a, String> for StringColumnData<'a> {
    fn len(&self) -> usize {
        self.ranges.len()
    }

    fn get(&self, index: usize) -> Result<String> {
        self.get_string(index)
    }
}

impl ClickHouseValue for String {
    fn ch_type_name() -> &'static str {
        "String"
    }

    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let len = wire::read_varint(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        String::from_utf8(buf).map_err(|e| {
            super::super::error::Error::Protocol(format!("invalid UTF-8 in String: {e}"))
        })
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        wire::write_varint(writer, self.len() as u64)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl ClickHouseColumn for String {
    type ColumnData<'a> = StringColumnData<'a>;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        if rows == 0 {
            return Ok(StringColumnData::new(Vec::new(), &[]));
        }
        // Single scan over the in-buffer bytes: decode each varint to find the
        // body range, recording `(start, end)` without advancing `ctx`. The
        // bodies stay borrowed from the block buffer — no copy, no second pass.
        let mut ranges: Vec<(u64, u64)> = Vec::with_capacity(rows);
        let mut scan_pos = 0usize;
        for _ in 0..rows {
            let (len, consumed) = read_varint_from_slice(&ctx.buf[ctx.pos + scan_pos..])?;
            let len = len as usize;
            let body_start = scan_pos + consumed;
            let body_end = body_start.checked_add(len).ok_or_else(|| {
                super::super::error::Error::Protocol("StringColumnData: body range overflow".into())
            })?;
            ranges.push((body_start as u64, body_end as u64));
            scan_pos = body_end;
        }
        // Consume the whole column in one bounds-checked slice — the borrowed
        // view the ranges index into.
        let data = ctx.read_exact(scan_pos)?;
        Ok(StringColumnData::new(ranges, data))
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for s in data {
            s.write_to(buf)?;
        }
        Ok(())
    }
}

fn read_varint_from_slice(data: &[u8]) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut consumed = 0;
    for &byte in data.iter() {
        consumed += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, consumed));
        }
        shift += 7;
        if shift >= 64 {
            return Err(super::super::error::Error::Protocol(
                "varint overflow".into(),
            ));
        }
    }
    Err(super::super::error::Error::Protocol(
        "unexpected end of data in varint".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_column_data_rowbinary() {
        let ctx_data = vec![
            0u8, 5, b'h', b'e', b'l', b'l', b'o', 6, b'w', b'o', b'r', b'l', b'd', b'!',
        ];
        let mut ctx = ReadColumnContext {
            rows: 3,
            pos: 0,
            buf: &ctx_data,
        };
        let col = String::read_column(&mut ctx).expect("test operation failed");

        assert_eq!(col.len(), 3);
        assert_eq!(col.get_bytes(0).expect("test operation failed"), b"");
        assert_eq!(col.get_bytes(1).expect("test operation failed"), b"hello");
        assert_eq!(col.get_bytes(2).expect("test operation failed"), b"world!");
        assert_eq!(col.get_string(1).expect("test operation failed"), "hello");
    }

    #[test]
    fn string_column_borrows_source_buffer_no_copy() {
        // Zero-copy contract: StringColumnData must reference the block buffer
        // directly, never own a copy of the string bodies. Verified by pointer
        // identity — the returned slice must live inside the original buffer.
        let ctx_data: Vec<u8> = vec![2, b'h', b'i', 5, b'w', b'o', b'r', b'l', b'd'];
        let base = ctx_data.as_ptr() as usize;
        let end = base + ctx_data.len();
        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &ctx_data,
        };
        let col = String::read_column(&mut ctx).expect("read column");
        for i in 0..col.len() {
            let bytes = col.get_bytes(i).expect("get bytes");
            let ptr = bytes.as_ptr() as usize;
            assert!(
                ptr >= base && ptr < end,
                "row {i} bytes must reference the source buffer (zero copy)"
            );
        }
        assert_eq!(col.get_bytes(0).expect("row 0"), b"hi");
        assert_eq!(col.get_bytes(1).expect("row 1"), b"world");
    }
}
