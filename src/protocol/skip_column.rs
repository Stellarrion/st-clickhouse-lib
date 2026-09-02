//! Buffered column-data skipping for the async engine.
//!
//! The implementation lives in `shared/skip_column.rs` and is shared with the
//! sync engine so both block parsers frame columns identically.
//!
//! The async buffered parser that consumes this module is test-only since
//! production compressed reads stream through the decompressing stream
//! wrapper (`DecompressingStream`), so items here are dead in non-test
//! builds; the unit tests still exercise the framing byte for byte.
#![allow(dead_code)]

use super::super::error::{Error, Result};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/skip_column.rs"
));
