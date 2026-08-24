//! Buffered column-data skipping for the sync engine.
//!
//! The implementation lives in `shared/skip_column.rs` and is shared with the
//! async engine so both block parsers frame columns identically.
//!
//! The by-type-name entry point is async-only (its unparsable-type string
//! fallback matches the async stream readers), so it is dead code here.
#![allow(dead_code)]

use super::super::error::{Error, Result};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/skip_column.rs"
));
