use super::super::error::{Error, Result};
use super::super::protocol::block::ReadColumnContext;
use super::super::protocol::type_parser::{ColumnType, parse_type};
use super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};
use std::fmt;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/column/plain.rs"
));
