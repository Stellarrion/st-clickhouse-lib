//! Buffered column-data skipping for the async engine.
//!
//! The implementation lives in `shared/skip_column.rs` and is shared with the
//! sync engine so both block parsers frame columns identically.

use super::super::error::{Error, Result};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/skip_column.rs"
));
