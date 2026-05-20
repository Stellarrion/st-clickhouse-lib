use super::super::column::any::{AnyColumnData, read_column_by_type};
use super::super::column::{ClickHouseColumn, DynamicColumnData, VariantColumnData};
use super::super::error::{Error, Result};
use super::type_parser::parse_type;

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/shared/block.rs"));
