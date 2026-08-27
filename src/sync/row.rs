//! Row trait — deserialize owned rows from ClickHouse blocks.
//!
//! The row API allocates per row (owned Strings, Vecs) for convenience.
//! For zero-allocation access, use the columnar API directly:
//!
//! ```rust
//! use st_clickhouse::sync::{Block, ColumnInfo};
//!
//! let mut block = Block::empty();
//! block.rows = 3;
//! block.columns.push(ColumnInfo {
//!     name: "age".to_owned(),
//!     type_name: "UInt64".to_owned(),
//!     data: bytes::Bytes::from(vec![
//!         10, 0, 0, 0, 0, 0, 0, 0, //
//!         20, 0, 0, 0, 0, 0, 0, 0, //
//!         30, 0, 0, 0, 0, 0, 0, 0,
//!     ]),
//!     lc_materialized: Default::default(),
//! });
//! let ages: &[u64] = block
//!     .column::<u64>("age")
//!     .expect("column decodes")
//!     .as_slice()
//!     .expect("fixed-width column yields an aligned slice");
//! assert_eq!(ages, &[10, 20, 30]);
//! ```

use crate::sync::column::{AnyColumnData, ClickHouseColumn, ClickHouseColumnData};
use crate::sync::error::Result;
use crate::sync::protocol::block::Block;

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
        Err(crate::sync::error::Error::Protocol(
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

define_row_read_all!(crate::sync::error::Error);
impl_tuple_rows!(crate::sync::error::Error);
