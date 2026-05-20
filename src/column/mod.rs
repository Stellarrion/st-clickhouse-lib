pub(crate) mod any;
mod array;
pub mod fixed_string;
pub mod geo;
pub mod map;
pub mod nullable;
mod plain;
mod string;
mod tuple;

pub use any::{AnyColumnData, read_column_by_type};
pub use array::ArrayColumnData;
pub use fixed_string::{FixedStringBytes, FixedStringColumnData};
pub use geo::{
    MultiPolygon, MultiPolygonColumnData, Point, PointColumnData, Polygon, PolygonColumnData, Ring,
    RingColumnData,
};
pub use nullable::NullableColumnData;
pub use plain::{
    BoolColumnData, Date, DateTime, DateTime64Value, Decimal32, Decimal64, Decimal128, Decimal256,
    DynamicColumnData, DynamicFieldValue, DynamicTypedValue, DynamicValue, Int256, Ipv4, Ipv6,
    Ipv6ColumnData, JsonColumnData, JsonValue, PlainColumn, PlainColumnData, UInt256, Uuid,
    VariantColumnData, VariantValue,
};
pub use string::StringColumnData;
pub use tuple::*;

use super::error::Result;
use super::protocol::block::ReadColumnContext;

/// A single value that can be read from/written to ClickHouse wire format.
///
/// Used for HTTP RowBinary format (row-by-row) and for per-value access
/// from columnar data.
pub trait ClickHouseValue: Sized {
    /// ClickHouse type name, e.g. "UInt64", "Nullable(String)"
    fn ch_type_name() -> &'static str;

    /// Read one value from the wire.
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self>;

    /// Write one value to the wire.
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()>;
}

/// A column of T can be read from the native TCP columnar wire format.
///
/// This is where zero-copy lives. Each type picks the right `ColumnData`
/// variant based on its wire shape (fixed, variable-length, nullable, etc.).
pub trait ClickHouseColumn: ClickHouseValue {
    /// Storage for a column of this type.
    type ColumnData<'a>: ClickHouseColumnData<'a, Self>
    where
        Self: 'a;

    /// Read a column from the wire (columnar format).
    /// Returns zero-copy data where possible.
    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>>;

    /// Write a column to the wire.
    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()>;
}

/// The data returned from reading a column.
///
/// Implementations choose whether to return borrowed or owned data.
/// Fixed-size types return `&[T]` (zero copy). Variable-length types
/// return offset+data views (zero copy). Row types allocate on demand.
pub trait ClickHouseColumnData<'a, T: ClickHouseValue>: Sized {
    /// Number of rows in this column.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get value at index (may or may not allocate depending on implementation).
    fn get(&self, index: usize) -> Result<T>;

    /// Get ALL values as a slice (only works for fixed-size zero-copy types).
    fn as_slice(&self) -> Option<&[T]> {
        None
    }
}
