use super::super::error::Result;
use super::super::protocol::block::ReadColumnContext;
use super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/column/fixed_string.rs"
));
