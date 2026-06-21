//! Geo types for ClickHouse: Point, Ring, Polygon, MultiPolygon.
//!
//! Wire formats (all delegate to existing Tuple/Array infrastructure):
//! - Point: Tuple(Float64, Float64) — all x values followed by all y values
//! - Ring:  Array(Point) — `Vec<[f64; 2]>`
//! - Polygon: Array(Ring) — `Vec<Vec<[f64; 2]>>`
//! - MultiPolygon: Array(Polygon) — `Vec<Vec<Vec<[f64; 2]>>>`

use super::super::error::Result;
use super::super::protocol::block::ReadColumnContext;
use super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/shared/column/geo.rs"));
