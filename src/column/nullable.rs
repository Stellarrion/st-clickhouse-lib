//! Nullable column support: `Nullable(T)`.
//!
//! Wire format (Native columnar):
//! ```text
//! [N bytes null_mask] -- one byte per row, 0 = not null, 1 = null
//! [T column data]     -- values serialized per T, for all N rows
//! ```
//!
//! Even null rows have placeholder values in the T column data (uninitialized
//! or zero). The null mask determines which rows are actually null.

use super::super::error::Result;
use super::super::protocol::block::ReadColumnContext;
use super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};

/// Column data for `Nullable(T)`.
///
/// The null mask is a byte slice (zero-copy into the buffer).
/// The inner column is the full T column (N rows, even for null positions).
pub struct NullableColumnData<'a, T: ClickHouseColumn + 'a> {
    null_mask: &'a [u8],
    inner: T::ColumnData<'a>,
}

impl<'a, T: ClickHouseColumn + 'a> NullableColumnData<'a, T> {
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.null_mask.len()
    }

    pub fn is_empty(&self) -> bool {
        self.null_mask.is_empty()
    }

    /// Check if the value at `index` is null.
    pub fn is_null(&self, index: usize) -> bool {
        self.null_mask.get(index).copied().unwrap_or(0) != 0
    }
}

impl<'a, T: ClickHouseColumn + 'a> ClickHouseColumnData<'a, Option<T>> for NullableColumnData<'a, T>
where
    Option<T>: ClickHouseValue,
{
    fn len(&self) -> usize {
        self.null_mask.len()
    }

    fn get(&self, index: usize) -> Result<Option<T>> {
        if self.is_null(index) {
            Ok(None)
        } else {
            self.inner.get(index).map(Some)
        }
    }
}

// ───────────────────────────────────────────────
// ClickHouseValue for Option<T> (RowBinary format)
// ───────────────────────────────────────────────

impl<T: ClickHouseValue> ClickHouseValue for Option<T> {
    fn ch_type_name() -> &'static str {
        concat!("Nullable(", "T", ")")
    }

    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut flag = [0u8; 1];
        reader.read_exact(&mut flag)?;
        if flag[0] != 0 {
            Ok(None)
        } else {
            Ok(Some(T::read_from(reader)?))
        }
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            None => writer.write_all(&[1])?,
            Some(v) => {
                writer.write_all(&[0])?;
                v.write_to(writer)?;
            },
        }
        Ok(())
    }
}

// ───────────────────────────────────────────────
// ClickHouseColumn for Option<T> (Native columnar)
// ───────────────────────────────────────────────

impl<T: ClickHouseColumn + 'static> ClickHouseColumn for Option<T>
where
    T: ClickHouseValue + Default,
{
    type ColumnData<'a>
        = NullableColumnData<'a, T>
    where
        T: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        let null_mask = ctx.read_exact(rows)?;
        let inner = T::read_column(ctx)?;
        Ok(NullableColumnData { null_mask, inner })
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        // Null mask: one byte per row (0 = not null, 1 = null)
        for val in data {
            buf.push(if val.is_none() { 1 } else { 0 });
        }
        // Values: placeholder (default) for null rows, actual value for non-null
        for val in data {
            match val {
                None => T::default().write_to(buf)?,
                Some(v) => v.write_to(buf)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nullable_uint64_read() {
        let buf = {
            let mut b = Vec::new();
            b.push(1);
            b.push(0);
            b.push(0);
            b.extend_from_slice(&0u64.to_le_bytes());
            b.extend_from_slice(&42u64.to_le_bytes());
            b.extend_from_slice(&0u64.to_le_bytes());
            b
        };
        let mut ctx = ReadColumnContext {
            rows: 3,
            pos: 0,
            buf: &buf,
        };
        let col: NullableColumnData<'_, u64> =
            <Option<u64>>::read_column(&mut ctx).expect("test operation failed");

        assert_eq!(col.len(), 3);
        assert!(col.is_null(0));
        assert!(!col.is_null(1));
        assert!(!col.is_null(2));
        assert_eq!(col.get(0).expect("test operation failed"), None);
        assert_eq!(col.get(1).expect("test operation failed"), Some(42));
        assert_eq!(col.get(2).expect("test operation failed"), Some(0));
    }

    #[test]
    fn test_nullable_all_nulls() {
        let buf = {
            let mut b = Vec::new();
            b.push(1);
            b.push(1);
            b.push(1);
            b.extend_from_slice(&0u64.to_le_bytes());
            b.extend_from_slice(&0u64.to_le_bytes());
            b.extend_from_slice(&0u64.to_le_bytes());
            b
        };
        let mut ctx = ReadColumnContext {
            rows: 3,
            pos: 0,
            buf: &buf,
        };
        let col: NullableColumnData<'_, u64> =
            <Option<u64>>::read_column(&mut ctx).expect("test operation failed");

        assert_eq!(col.get(0).expect("test operation failed"), None);
        assert_eq!(col.get(1).expect("test operation failed"), None);
        assert_eq!(col.get(2).expect("test operation failed"), None);
    }
}
