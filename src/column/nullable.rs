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

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/column/nullable.rs"
));
