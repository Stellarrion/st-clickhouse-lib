use super::super::error::Result;
use super::super::protocol::block::ReadColumnContext;
use super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};
use std::collections::HashMap;
use std::hash::Hash;

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/shared/column/map.rs"));
