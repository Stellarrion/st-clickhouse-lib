//! Row trait — deserialize owned rows from ClickHouse blocks.
//!
//! The row API allocates per row (owned Strings, Vecs) for convenience.
//! For zero-allocation access, use the columnar API directly:
//!
//! ```ignore
//! let ages: &[u64] = block.column::<u64>("age")?.as_slice()?;
//! for age in ages { process(age); }
//! ```

use crate::column::{AnyColumnData, ClickHouseColumn, ClickHouseColumnData};
use crate::error::Result;
use crate::protocol::block::Block;

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/shared/row_macros.rs"));

/// Trait for types that can be deserialized from a ClickHouse result row.
/// Always returns owned data — use columnar API for zero-copy access.
pub trait Row: Sized {
    const COLUMN_NAMES: &'static [&'static str];
    const COLUMN_COUNT: usize;

    fn from_row(block: &Block, row_index: usize) -> Result<Self>;

    /// Fast path: construct from pre-extracted column data.
    /// Override for zero per-row column dispatch overhead.
    fn from_columns(_columns: &[&AnyColumnData<'_>], _row_index: usize) -> Result<Self> {
        Err(crate::error::Error::Protocol(
            "from_columns not implemented for this Row type".into(),
        ))
    }

    /// Materialize all `n` rows from pre-extracted columns. The default loops
    /// [`from_columns`](Self::from_columns); tuple impls override it with a
    /// PlainColumn bulk-slice fast path that skips per-row type dispatch.
    fn from_columns_collect(columns: &[&AnyColumnData<'_>], n: usize) -> Result<Vec<Self>> {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(Self::from_columns(columns, i)?);
        }
        Ok(out)
    }
}

define_row_read_all!(crate::error::Error);
impl_tuple_rows!(crate::error::Error);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::block::{Block, ColumnInfo};
    use bytes::Bytes;

    fn u64_col(name: &str, vals: &[u64]) -> ColumnInfo {
        let mut data = Vec::with_capacity(vals.len() * 8);
        for v in vals {
            data.extend_from_slice(&v.to_le_bytes());
        }
        ColumnInfo {
            name: name.to_string(),
            type_name: "UInt64".to_string(),
            data: Bytes::from(data),
            lc_materialized: Bytes::new(),
        }
    }

    /// `(u64, u64)` — all-PlainColumn tuple: exercises the bulk-slice fast path.
    #[test]
    fn read_all_plain_tuple() {
        let block = Block {
            columns: vec![u64_col("a", &[1, 2, 3]), u64_col("b", &[10, 20, 30])],
            rows: 3,
        };
        let rows: Vec<(u64, u64)> = read_all(&block).expect("read");
        assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30)]);
    }

    /// `(u64, String)` — String is not PlainColumn: must fall back to per-row.
    #[test]
    fn read_all_mixed_tuple_falls_back() {
        let mut sdata = Vec::new();
        for s in ["x", "yy", "zzz"] {
            sdata.push(s.len() as u8); // varint (len < 128)
            sdata.extend_from_slice(s.as_bytes());
        }
        let block = Block {
            columns: vec![
                u64_col("a", &[1, 2, 3]),
                ColumnInfo {
                    name: "b".to_string(),
                    type_name: "String".to_string(),
                    data: Bytes::from(sdata),
                    lc_materialized: Bytes::new(),
                },
            ],
            rows: 3,
        };
        let rows: Vec<(u64, String)> = read_all(&block).expect("read");
        assert_eq!(
            rows,
            vec![
                (1, "x".to_string()),
                (2, "yy".to_string()),
                (3, "zzz".to_string())
            ]
        );
    }

    /// Empty block materializes to an empty Vec on every path.
    #[test]
    fn read_all_plain_empty() {
        let block = Block {
            columns: vec![u64_col("a", &[])],
            rows: 0,
        };
        let rows: Vec<(u64,)> = read_all(&block).expect("read");
        assert!(rows.is_empty());
    }
}
