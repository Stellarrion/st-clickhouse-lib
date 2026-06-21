// Shared array column logic. Provided in scope by the including
// module: super::super::error::Result, super::super::protocol::block::ReadColumnContext, super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue}.
/// Array column data: `Array(T)` for reading via `Block::column::<Vec<T>>()`.
///
/// Wire format (Native):
/// ```text
/// [N * 8 bytes UInt64 offsets]  — cumulative, one per row
/// [T elements]                   — bulk-serialized inner type
/// ```
pub struct ArrayColumnData<'a, T: ClickHouseColumn + 'a> {
    /// Cumulative offsets. Owned Vec — small per array column (one u64 per row).
    /// Previously borrowed directly from the buffer, but that caused UB on
    /// misaligned reads (after Nullable masks, etc.).
    offsets: Vec<u64>,
    inner: T::ColumnData<'a>,
}

impl<'a, T: ClickHouseColumn + 'a> ArrayColumnData<'a, T> {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Get the inner column's slice of elements for array at `index`.
    fn element_range(&self, index: usize) -> Result<(usize, usize)> {
        let start = if index == 0 {
            0
        } else {
            self.offsets.get(index - 1).copied().ok_or_else(|| {
                super::super::error::Error::Protocol("ArrayColumnData: index out of bounds".into())
            })? as usize
        };
        let end = self.offsets.get(index).copied().ok_or_else(|| {
            super::super::error::Error::Protocol("ArrayColumnData: index out of bounds".into())
        })? as usize;
        Ok((start, end))
    }
}

/// `ClickHouseColumnData` produces `Vec<T>` per row.
impl<'a, T: ClickHouseColumn + 'static> ClickHouseColumnData<'a, Vec<T>> for ArrayColumnData<'a, T>
where
    T: 'a,
    T::ColumnData<'a>: ClickHouseColumnData<'a, T>,
{
    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn get(&self, index: usize) -> Result<Vec<T>> {
        let (start, end) = self.element_range(index)?;
        let mut result = Vec::with_capacity(end - start);
        for i in start..end {
            result.push(self.inner.get(i)?);
        }
        Ok(result)
    }
}

// ───────────────────────────────────────────────
// ClickHouseValue for Vec<T>
// ───────────────────────────────────────────────

impl<T: ClickHouseValue + 'static> ClickHouseValue for Vec<T> {
    fn ch_type_name() -> &'static str {
        // Thread-local storage for dynamic type name
        // The name depends on T, which is known at compile time via monomorphization
        // We return the inner type's name — the "Array()" wrapper is handled by Column impl
        T::ch_type_name()
    }

    fn read_from<R: std::io::Read>(_reader: &mut R) -> Result<Self> {
        Err(super::super::error::Error::Protocol(
            "Vec<T> RowBinary read not supported (use Native format)".into(),
        ))
    }

    fn write_to<W: std::io::Write>(&self, _writer: &mut W) -> Result<()> {
        Err(super::super::error::Error::Protocol(
            "Vec<T> RowBinary write not supported".into(),
        ))
    }
}

// ───────────────────────────────────────────────
// ClickHouseColumn for Vec<T>
// ───────────────────────────────────────────────

impl<T: ClickHouseColumn + 'static> ClickHouseColumn for Vec<T>
where
    T: ClickHouseValue,
    Vec<T>: 'static,
{
    type ColumnData<'a>
        = ArrayColumnData<'a, T>
    where
        T: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        if rows == 0 {
            // Empty array column: no offsets, no data
            return Ok(ArrayColumnData {
                offsets: Vec::new(),
                inner: T::read_column(ctx)?,
            });
        }
        let offsets = ctx.read_offsets()?;
        // Total elements = last offset
        let total_elements = offsets[rows - 1] as usize;
        // Temporarily set ctx.rows to total elements for inner read
        let saved_rows = ctx.rows;
        ctx.rows = total_elements;
        let inner = T::read_column(ctx)?;
        ctx.rows = saved_rows;
        Ok(ArrayColumnData { offsets, inner })
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        let mut cumulative = 0u64;
        for arr in data {
            cumulative += arr.len() as u64;
            buf.extend_from_slice(&cumulative.to_le_bytes());
        }
        for elem in data.iter().flatten() {
            elem.write_to(buf)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::protocol::block::ReadColumnContext;
    use super::*;

    #[test]
    fn test_array_column_uint64() {
        // Wire: 2 rows of Array(UInt64)
        // [0, 1, 2] and [3, 4]
        // Offsets: offset[0]=3, offset[1]=5 (cumulative, last = 5)
        // Elements: 0,1,2,3,4 as u64
        let mut buf = Vec::new();
        // Offsets (8 bytes each)
        buf.extend_from_slice(&3u64.to_le_bytes());
        buf.extend_from_slice(&5u64.to_le_bytes());
        // Elements (5 u64 values)
        for v in &[0u64, 1, 2, 3, 4] {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col =
            <Vec<u64> as ClickHouseColumn>::read_column(&mut ctx).expect("test operation failed");

        assert_eq!(col.len(), 2);
        let row0 = col.get(0).expect("test operation failed");
        assert_eq!(row0, vec![0u64, 1, 2]);
        let row1 = col.get(1).expect("test operation failed");
        assert_eq!(row1, vec![3u64, 4]);
    }

    #[test]
    fn test_array_column_empty() {
        // Empty array column: 0 offsets, 0 elements
        let buf = vec![0u8; 0];
        let mut ctx = ReadColumnContext {
            rows: 0,
            pos: 0,
            buf: &buf,
        };
        let col =
            <Vec<u64> as ClickHouseColumn>::read_column(&mut ctx).expect("test operation failed");
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_array_column_single_row() {
        // Wire: 1 row of Array(UInt64), values [42]
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // offset[0]=1
        buf.extend_from_slice(&42u64.to_le_bytes()); // element

        let mut ctx = ReadColumnContext {
            rows: 1,
            pos: 0,
            buf: &buf,
        };
        let col =
            <Vec<u64> as ClickHouseColumn>::read_column(&mut ctx).expect("test operation failed");

        assert_eq!(col.len(), 1);
        assert_eq!(col.get(0).expect("test operation failed"), vec![42u64]);
    }
}
