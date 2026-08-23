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

    /// Whether [`from_columns`](Self::from_columns) expects columns in
    /// [`COLUMN_NAMES`](Self::COLUMN_NAMES) order rather than block order.
    ///
    /// The derive opts in because derived structs map fields by name. The
    /// default stays positional for compatibility with existing manual
    /// implementations and tuple rows.
    fn from_columns_by_name() -> bool {
        false
    }

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

    // ── Named-row fast path: column order safety ──
    //
    // Hand-written impl mirroring what `#[derive(Row)]` generates (name-based
    // `from_row`, positional `from_columns`); derive-based coverage lives in
    // tests/row_test.rs where the crate name resolves.

    mod named_order {
        use super::*;

        #[derive(Debug, PartialEq)]
        struct IdValue {
            id: u64,
            value: u64,
        }

        impl Row for IdValue {
            const COLUMN_NAMES: &'static [&'static str] = &["id", "value"];
            const COLUMN_COUNT: usize = 2;

            fn from_columns_by_name() -> bool {
                true
            }

            fn from_row(block: &Block, row_index: usize) -> Result<Self> {
                let id = block.column::<u64>("id")?.get(row_index)?;
                let value = block.column::<u64>("value")?.get(row_index)?;
                Ok(IdValue { id, value })
            }

            fn from_columns(cols: &[&AnyColumnData<'_>], row_index: usize) -> Result<Self> {
                // Mirrors the derive: positional access via `to_typed`.
                // SAFETY: the field requests the concrete Rust type declared
                // on that field, exactly like the derive-generated code.
                let id = unsafe { cols[0].to_typed::<u64>(row_index)? };
                let value = unsafe { cols[1].to_typed::<u64>(row_index)? };
                Ok(IdValue { id, value })
            }
        }

        #[test]
        fn read_all_named_row_matching_order_uses_fast_path() {
            let block = Block {
                columns: vec![u64_col("id", &[1, 2]), u64_col("value", &[10, 20])],
                rows: 2,
            };
            let rows: Vec<IdValue> = read_all(&block).expect("read");
            assert_eq!(
                rows,
                vec![IdValue { id: 1, value: 10 }, IdValue { id: 2, value: 20 }]
            );
        }

        #[test]
        fn read_all_named_row_reordered_columns_are_not_swapped() {
            // SELECT returns (value, id) while the struct declares (id, value).
            // The positional fast path must not silently swap same-typed
            // fields: the columns are reordered once per block.
            let block = Block {
                columns: vec![u64_col("value", &[10, 20]), u64_col("id", &[1, 2])],
                rows: 2,
            };
            let rows: Vec<IdValue> = read_all(&block).expect("read");
            assert_eq!(
                rows,
                vec![IdValue { id: 1, value: 10 }, IdValue { id: 2, value: 20 }]
            );
        }

        #[test]
        fn read_all_named_row_missing_column_falls_back_to_from_row() {
            // "value" is absent: from_row must surface the lookup error
            // instead of the fast path decoding the wrong column.
            let block = Block {
                columns: vec![u64_col("id", &[1, 2])],
                rows: 2,
            };
            let res: Result<Vec<IdValue>> = read_all(&block);
            assert!(res.is_err(), "missing column must error, got {res:?}");
        }

        #[test]
        fn read_all_tuple_stays_positional() {
            // Tuples ignore names: column order is the field order.
            let block = Block {
                columns: vec![u64_col("b", &[10, 20]), u64_col("a", &[1, 2])],
                rows: 2,
            };
            let rows: Vec<(u64, u64)> = read_all(&block).expect("read");
            assert_eq!(rows, vec![(10, 1), (20, 2)]);
        }
    }
}
