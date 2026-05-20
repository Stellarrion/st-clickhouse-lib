use super::super::error::Result;
use super::super::protocol::block::ReadColumnContext;
use super::super::protocol::wire;
use super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};

/// Owned String column data.
///
/// String columns use **RowBinary format**: each value is `varint(length) + bytes`.
/// Since varints and data are interleaved, we copy strings into a contiguous
/// buffer and build cumulative offset array.
///
/// TODO(zero-copy): Once we have an arena or shared buffer, eliminate the copy
/// by scanning in-place with an index array.
#[derive(Debug)]
pub struct StringColumnData {
    offsets: Vec<u64>,
    data: Vec<u8>,
}

impl StringColumnData {
    pub(crate) fn new(offsets: Vec<u64>, data: Vec<u8>) -> Self {
        Self { offsets, data }
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    fn range(&self, index: usize) -> Result<(usize, usize)> {
        let start = if index == 0 {
            0
        } else {
            self.offsets.get(index - 1).copied().ok_or_else(|| {
                crate::sync::error::Error::Protocol("StringColumnData: index out of bounds".into())
            })? as usize
        };
        let end = self.offsets.get(index).copied().ok_or_else(|| {
            crate::sync::error::Error::Protocol("StringColumnData: index out of bounds".into())
        })? as usize;
        Ok((start, end))
    }

    pub fn get_bytes(&self, index: usize) -> Result<&[u8]> {
        let (start, end) = self.range(index)?;
        self.data.get(start..end).ok_or_else(|| {
            crate::sync::error::Error::Protocol("StringColumnData: invalid range".into())
        })
    }

    pub fn get_string(&self, index: usize) -> Result<String> {
        let bytes = self.get_bytes(index)?;
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }

    /// Get a borrowed string reference — zero alloc.
    pub fn get_str(&self, index: usize) -> Result<&str> {
        let bytes = self.get_bytes(index)?;
        std::str::from_utf8(bytes).map_err(|e| {
            crate::sync::error::Error::Protocol(format!("invalid UTF-8 at row {index}: {e}"))
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<&[u8]>> + '_ {
        (0..self.len()).map(|i| self.get_bytes(i))
    }
}

impl ClickHouseColumnData<'_, String> for StringColumnData {
    fn len(&self) -> usize {
        self.offsets.len()
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
            crate::sync::error::Error::Protocol(format!("invalid UTF-8 in String: {e}"))
        })
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        wire::write_varint(writer, self.len() as u64)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl ClickHouseColumn for String {
    type ColumnData<'a>
        = StringColumnData
    where
        String: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        if rows == 0 {
            return Ok(StringColumnData::new(Vec::new(), Vec::new()));
        }
        // First pass: scan varints to find total bytes needed for `rows` strings.
        // We scan from ctx.pos without advancing it.
        let mut scan_pos = 0usize;
        for _ in 0..rows {
            let avail = ctx.buf.len() - (ctx.pos + scan_pos);
            if avail == 0 {
                return Err(crate::sync::error::Error::Protocol(
                    "StringColumnData: unexpected end of data".into(),
                ));
            }
            let (len, consumed) = read_varint_from_slice(&ctx.buf[ctx.pos + scan_pos..])?;
            scan_pos += consumed + len as usize;
        }
        // Now read exactly `scan_pos` bytes — this is the true extent of this column.
        let raw = ctx.read_exact(scan_pos)?;
        if raw.len() < scan_pos {
            return Err(crate::sync::error::Error::Protocol(
                "StringColumnData: short read".into(),
            ));
        }
        // Second pass: extract string data and build offsets
        let mut offsets = Vec::with_capacity(rows);
        let mut data = Vec::new();
        let mut rpos = 0usize;
        for _ in 0..rows {
            let (len, consumed) = read_varint_from_slice(&raw[rpos..])?;
            rpos += consumed;
            let end = rpos + len as usize;
            data.extend_from_slice(&raw[rpos..end]);
            rpos = end;
            offsets.push(data.len() as u64);
        }
        Ok(StringColumnData::new(offsets, data))
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
            return Err(crate::sync::error::Error::Protocol(
                "varint overflow".into(),
            ));
        }
    }
    Err(crate::sync::error::Error::Protocol(
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
}
