// Shared fixed_string column logic. Provided in scope by the including
// module: super::super::error::Result, super::super::protocol::block::ReadColumnContext, super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue}.
/// A `FixedString(N)` value — fixed-width byte string from ClickHouse.
///
/// The inner `Vec<u8>` always has exactly N bytes (padded with zeros).
/// Strip trailing nulls manually if needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedStringBytes(pub Vec<u8>);

/// Column data for `FixedString(N)` — fixed-width byte arrays.
///
/// Wire format: N bytes per row, no length prefix, no zero-padding stripping.
#[derive(Debug)]
pub struct FixedStringColumnData<'a> {
    pub(crate) data: &'a [u8],
    pub(crate) n: usize,
    pub(crate) count: usize,
}

impl<'a> FixedStringColumnData<'a> {
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the raw bytes of the value at `index` (always `n` bytes).
    pub fn get_bytes(&self, index: usize) -> Result<&'a [u8]> {
        if index >= self.count {
            return Err(super::super::error::Error::Protocol(format!(
                "FixedStringColumnData: index {index} out of bounds (len {})",
                self.count
            )));
        }
        let start = index * self.n;
        let end = start + self.n;
        Ok(&self.data[start..end])
    }
}

impl<'a> ClickHouseColumnData<'a, FixedStringBytes> for FixedStringColumnData<'a> {
    fn len(&self) -> usize {
        self.count
    }
    fn get(&self, index: usize) -> Result<FixedStringBytes> {
        self.get_bytes(index).map(|b| FixedStringBytes(b.to_vec()))
    }
}

impl ClickHouseValue for FixedStringBytes {
    fn ch_type_name() -> &'static str {
        "FixedString"
    }

    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = vec![0u8; 0];
        reader.read_exact(&mut buf)?;
        Ok(FixedStringBytes(buf))
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

impl ClickHouseColumn for FixedStringBytes {
    type ColumnData<'a> = FixedStringColumnData<'a>;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let count = ctx.rows;
        if count == 0 {
            return Ok(FixedStringColumnData {
                data: &[],
                n: 0,
                count: 0,
            });
        }
        // Infer N from remaining buffer length / count
        let n = ctx.buf.len() / count;
        let nbytes = count
            .checked_mul(n)
            .ok_or_else(|| super::super::error::Error::Protocol("FixedString size overflow".into()))?;
        let data = ctx.read_exact(nbytes)?;
        Ok(FixedStringColumnData { data, n, count })
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for fs in data {
            fs.write_to(buf)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_string_read() {
        // 3 rows of FixedString(3): b"abc", b"def", b"ghi"
        let bytes = b"abcdefghi" as &[u8];
        let mut ctx = ReadColumnContext {
            rows: 3,
            pos: 0,
            buf: bytes,
        };
        let col = FixedStringBytes::read_column(&mut ctx).expect("test operation failed");
        assert_eq!(col.len(), 3);
        assert_eq!(col.get_bytes(0).expect("test operation failed"), b"abc");
        assert_eq!(col.get_bytes(1).expect("test operation failed"), b"def");
        assert_eq!(col.get_bytes(2).expect("test operation failed"), b"ghi");
        assert_eq!(
            col.get(0).expect("test operation failed"),
            FixedStringBytes(b"abc".to_vec())
        );
    }

    #[test]
    fn test_fixed_string_empty() {
        let bytes = b"" as &[u8];
        let mut ctx = ReadColumnContext {
            rows: 0,
            pos: 0,
            buf: bytes,
        };
        let col = FixedStringBytes::read_column(&mut ctx).expect("test operation failed");
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_fixed_string_zeros_padded() {
        let bytes = b"ab\0\0cd\0\0" as &[u8];
        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: bytes,
        };
        let col = FixedStringBytes::read_column(&mut ctx).expect("test operation failed");
        assert_eq!(col.get_bytes(0).expect("test operation failed"), b"ab\0\0");
        assert_eq!(col.get_bytes(1).expect("test operation failed"), b"cd\0\0");
    }
}
