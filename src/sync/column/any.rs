use super::super::error::Result;
use super::super::protocol::block::ReadColumnContext;
use super::super::protocol::type_parser::ColumnType;
use super::map::RawMapColumnData;
use super::tuple::RawTupleColumnData;
use super::*;
use std::mem::{MaybeUninit, align_of, size_of};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/any_column_macros.rs"
));

// ══════════════════════════════════════════════════════════════════════════
// OwnedColumnData — lifetime-free owned column data for FFI/py binding use
// ══════════════════════════════════════════════════════════════════════════

/// Owned, lifetime-free column data — for crossing FFI boundaries.
///
/// Unsigned integer values are kept as `UInt(u64)` to preserve full `UInt64`
/// precision for FFI/Python callers.
/// Signed integer values are unified into `Int(i64)`.
/// All float types are unified into `Float(f64)`.
/// String, FixedString, UUID, IP, JSON are all unified into `String`.
/// More complex types (Nullable, Array, etc.) are simplified to `Null`.
#[derive(Debug, Clone)]
pub enum OwnedColumnData {
    /// Unsigned integer values.
    UInt(Vec<u64>),
    /// Signed integer values.
    Int(Vec<i64>),
    Float(Vec<f64>),
    String(Vec<String>),
    Bool(Vec<bool>),
    Null(usize),
    Unknown,
}

/// Runtime-dispatched column data — any supported ClickHouse type.
///
/// Allows decoding columns without knowing the type at compile time.
/// Returned by `Block::read_column_by_name()`.
#[derive(Debug)]
pub enum AnyColumnData<'a> {
    UInt8(PlainColumnData<'a, u8>),
    UInt16(PlainColumnData<'a, u16>),
    UInt32(PlainColumnData<'a, u32>),
    UInt64(PlainColumnData<'a, u64>),
    UInt128(PlainColumnData<'a, u128>),
    Int8(PlainColumnData<'a, i8>),
    Int16(PlainColumnData<'a, i16>),
    Int32(PlainColumnData<'a, i32>),
    Int64(PlainColumnData<'a, i64>),
    Int128(PlainColumnData<'a, i128>),
    Float32(PlainColumnData<'a, f32>),
    Float64(PlainColumnData<'a, f64>),
    String(StringColumnData),
    FixedString(FixedStringColumnData<'a>),
    DateTime64(PlainColumnData<'a, DateTime64Value>),
    Decimal32(PlainColumnData<'a, Decimal32>),
    Decimal64(PlainColumnData<'a, Decimal64>),
    Decimal128(PlainColumnData<'a, Decimal128>),
    Decimal256(PlainColumnData<'a, Decimal256>),
    UInt256(PlainColumnData<'a, UInt256>),
    Int256(PlainColumnData<'a, Int256>),
    Bool(BoolColumnData<'a>),
    Date(PlainColumnData<'a, Date>),
    DateTime(PlainColumnData<'a, DateTime>),
    Uuid(PlainColumnData<'a, Uuid>),
    IPv4(PlainColumnData<'a, Ipv4>),
    IPv6(Ipv6ColumnData<'a>),
    JSON(JsonColumnData),
    Variant(VariantColumnData),
    Dynamic(DynamicColumnData),
    AggregateFunction(DynamicColumnData),
    SimpleAggregateFunction(DynamicColumnData),
    Nullable(Box<AnyColumnData<'a>>),
    Array(Box<AnyColumnData<'a>>),
    Map(RawMapColumnData<'a>),
    Tuple(RawTupleColumnData<'a>),
    Unknown,
}

impl<'a> AnyColumnData<'a> {
    /// Convert borrowed column data to owned, erasing type distinctions.
    ///
    /// Unsigned integers use `OwnedColumnData::UInt(u64)`, signed integers
    /// use `OwnedColumnData::Int(i64)`, floats use `Float(f64)`, and
    /// string-like values use `String`.
    pub fn into_owned(&self) -> OwnedColumnData {
        let len = self.len();
        match self {
            Self::UInt8(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(u64::from(col.get(i).unwrap_or(0)));
                }
                OwnedColumnData::UInt(v)
            },
            Self::UInt16(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(u64::from(col.get(i).unwrap_or(0)));
                }
                OwnedColumnData::UInt(v)
            },
            Self::UInt32(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(u64::from(col.get(i).unwrap_or(0)));
                }
                OwnedColumnData::UInt(v)
            },
            Self::UInt64(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).unwrap_or(0));
                }
                OwnedColumnData::UInt(v)
            },
            Self::Int8(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).unwrap_or(0) as i64);
                }
                OwnedColumnData::Int(v)
            },
            Self::Int16(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).unwrap_or(0) as i64);
                }
                OwnedColumnData::Int(v)
            },
            Self::Int32(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).unwrap_or(0) as i64);
                }
                OwnedColumnData::Int(v)
            },
            Self::Int64(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).unwrap_or(0));
                }
                OwnedColumnData::Int(v)
            },
            Self::Float32(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).unwrap_or(0.0) as f64);
                }
                OwnedColumnData::Float(v)
            },
            Self::Float64(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).unwrap_or(0.0));
                }
                OwnedColumnData::Float(v)
            },
            Self::String(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get_string(i).unwrap_or_default());
                }
                OwnedColumnData::String(v)
            },
            Self::FixedString(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    let bytes = col.get_bytes(i).unwrap_or(b"");
                    v.push(std::string::String::from_utf8_lossy(bytes).into_owned());
                }
                OwnedColumnData::String(v)
            },
            Self::Bool(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).unwrap_or(false));
                }
                OwnedColumnData::Bool(v)
            },
            Self::Date(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).map(|d| d.as_days() as i64).unwrap_or(0));
                }
                OwnedColumnData::Int(v)
            },
            Self::DateTime(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).map(|d| d.as_secs() as i64).unwrap_or(0));
                }
                OwnedColumnData::Int(v)
            },
            Self::DateTime64(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).map(|d| d.0).unwrap_or(0));
                }
                OwnedColumnData::Int(v)
            },
            Self::Uuid(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get(i).map(|u| u.to_hyphenated()).unwrap_or_default());
                }
                OwnedColumnData::String(v)
            },
            Self::IPv4(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(
                        col.get(i)
                            .map(|ip| ip.to_std().to_string())
                            .unwrap_or_default(),
                    );
                }
                OwnedColumnData::String(v)
            },
            Self::IPv6(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(
                        col.get(i)
                            .map(|ip| ip.to_std().to_string())
                            .unwrap_or_default(),
                    );
                }
                OwnedColumnData::String(v)
            },
            Self::JSON(col) => {
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    v.push(col.get_string(i).unwrap_or_default());
                }
                OwnedColumnData::String(v)
            },
            Self::Nullable(inner) | Self::Array(inner) => inner.into_owned(),
            Self::Map(_) | Self::Tuple(_) => OwnedColumnData::Null(len),
            Self::UInt128(_)
            | Self::Int128(_)
            | Self::Decimal32(_)
            | Self::Decimal64(_)
            | Self::Decimal128(_)
            | Self::Decimal256(_)
            | Self::UInt256(_)
            | Self::Int256(_)
            | Self::Variant(_)
            | Self::Dynamic(_)
            | Self::AggregateFunction(_)
            | Self::SimpleAggregateFunction(_) => {
                if len == 0 {
                    OwnedColumnData::Unknown
                } else {
                    OwnedColumnData::Null(len)
                }
            },
            Self::Unknown => OwnedColumnData::Unknown,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::UInt8(c) => c.len(),
            Self::UInt16(c) => c.len(),
            Self::UInt32(c) => c.len(),
            Self::UInt64(c) => c.len(),
            Self::UInt128(c) => c.len(),
            Self::Int8(c) => c.len(),
            Self::Int16(c) => c.len(),
            Self::Int32(c) => c.len(),
            Self::Int64(c) => c.len(),
            Self::Int128(c) => c.len(),
            Self::Float32(c) => c.len(),
            Self::Float64(c) => c.len(),
            Self::String(c) => c.len(),
            Self::FixedString(c) => c.len(),
            Self::DateTime64(c) => c.len(),
            Self::Decimal32(c) => c.len(),
            Self::Decimal64(c) => c.len(),
            Self::Decimal128(c) => c.len(),
            Self::Decimal256(c) => c.len(),
            Self::UInt256(c) => c.len(),
            Self::Int256(c) => c.len(),
            Self::Bool(c) => c.len(),
            Self::Date(c) => c.len(),
            Self::DateTime(c) => c.len(),
            Self::Uuid(c) => c.len(),
            Self::IPv4(c) => c.len(),
            Self::IPv6(c) => c.len(),
            Self::JSON(c) => c.len(),
            Self::Variant(c) => c.len(),
            Self::Dynamic(c) => c.len(),
            Self::AggregateFunction(c) => c.len(),
            Self::SimpleAggregateFunction(c) => c.len(),
            Self::Nullable(c) => c.len(),
            Self::Array(c) => c.len(),
            Self::Map(m) => m.offsets.len(),
            Self::Tuple(t) => t.elements.len(),
            Self::Unknown => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Compute wire size for `rows` rows. Returns None for variable-length types (String, etc.).
    pub fn column_data_size(&self, rows: usize) -> Option<usize> {
        use AnyColumnData::*;
        match self {
            UInt8(_) | Int8(_) | Bool(_) => Some(rows),
            UInt16(_) | Int16(_) | Date(_) => rows.checked_mul(2),
            UInt32(_) | Int32(_) | Float32(_) | DateTime(_) | IPv4(_) => rows.checked_mul(4),
            UInt64(_) | Int64(_) | Float64(_) | DateTime64(_) => rows.checked_mul(8),
            UInt128(_) | Int128(_) | Uuid(_) | IPv6(_) => rows.checked_mul(16),
            UInt256(_) | Int256(_) => rows.checked_mul(32),
            Decimal32(_) => rows.checked_mul(4),
            Decimal64(_) => rows.checked_mul(8),
            Decimal128(_) => rows.checked_mul(16),
            Decimal256(_) => rows.checked_mul(32),
            FixedString(_) => None,
            String(_) | JSON(_) => None,
            Nullable(inner) => inner
                .column_data_size(rows)
                .and_then(|s| s.checked_add(rows)),
            Array(inner) => inner.column_data_size(0).map(|_| 0),
            _ => None,
        }
    }

    /// Get the ClickHouse type name for this variant.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::UInt8(_) => "UInt8",
            Self::UInt16(_) => "UInt16",
            Self::UInt32(_) => "UInt32",
            Self::UInt64(_) => "UInt64",
            Self::UInt128(_) => "UInt128",
            Self::Int8(_) => "Int8",
            Self::Int16(_) => "Int16",
            Self::Int32(_) => "Int32",
            Self::Int64(_) => "Int64",
            Self::Int128(_) => "Int128",
            Self::Float32(_) => "Float32",
            Self::Float64(_) => "Float64",
            Self::String(_) => "String",
            Self::FixedString(_) => "FixedString",
            Self::Bool(_) => "Bool",
            Self::Date(_) => "Date",
            Self::DateTime(_) => "DateTime",
            Self::Uuid(_) => "Uuid",
            Self::IPv4(_) => "IPv4",
            Self::IPv6(_) => "IPv6",
            Self::JSON(_) => "JSON",
            Self::Variant(_) => "Variant",
            Self::Dynamic(_) => "Dynamic",
            Self::AggregateFunction(_) => "AggregateFunction",
            Self::SimpleAggregateFunction(_) => "SimpleAggregateFunction",
            Self::DateTime64(_) => "DateTime64",
            Self::Decimal32(_) => "Decimal32",
            Self::Decimal64(_) => "Decimal64",
            Self::Decimal128(_) => "Decimal128",
            Self::Decimal256(_) => "Decimal256",
            Self::UInt256(_) => "UInt256",
            Self::Int256(_) => "Int256",
            Self::Nullable(_) => "Nullable",
            Self::Array(_) => "Array",
            Self::Map(_) => "Map",
            Self::Tuple(_) => "Tuple",
            Self::Unknown => "Unknown",
        }
    }
}

// ───────────────────────────────────────────────
// Runtime typed access to AnyColumnData

impl<'a> AnyColumnData<'a> {
    /// Extract a single typed value at `row_index` from this column.
    ///
    /// Uses `TypeId` to dispatch to the correct variant at runtime, then safely
    /// downcasts the materialized value back to `T`.
    ///
    /// # Safety
    ///
    /// The caller must request the actual ClickHouse/Rust value type stored in
    /// this column. The function validates `TypeId`, size, and alignment before
    /// copying bytes, but it still reinterprets a runtime-selected concrete type
    /// as generic `T`.
    pub unsafe fn to_typed<T: ClickHouseColumn + 'static>(&self, row_index: usize) -> Result<T> {
        use std::any::TypeId;
        let tid = TypeId::of::<T>();

        try_any_typed_columns!(
            self, tid, row_index;
            UInt8 => u8,
            UInt16 => u16,
            UInt32 => u32,
            UInt64 => u64,
            UInt128 => u128,
            Int8 => i8,
            Int16 => i16,
            Int32 => i32,
            Int64 => i64,
            Int128 => i128,
            Float32 => f32,
            Float64 => f64,
            Bool => bool,
            Date => Date,
            DateTime => DateTime,
            Uuid => Uuid,
            IPv4 => Ipv4,
            IPv6 => Ipv6,
            DateTime64 => DateTime64Value,
            Decimal32 => Decimal32,
            Decimal64 => Decimal64,
            Decimal128 => Decimal128,
            Decimal256 => Decimal256,
            UInt256 => UInt256,
            Int256 => Int256,
            String => std::string::String,
            FixedString => FixedStringBytes,
            JSON => JsonValue,
            Variant => VariantValue,
            Dynamic => DynamicValue,
            AggregateFunction => DynamicValue,
            SimpleAggregateFunction => DynamicValue,
        );

        Err(crate::sync::error::Error::Protocol(format!(
            "type mismatch: cannot read {} as {}",
            self.type_name(),
            std::any::type_name::<T>(),
        )))
    }
}

unsafe fn copy_value_checked<T, Inner>(value: Inner) -> Result<T> {
    if size_of::<Inner>() != size_of::<T>() || align_of::<Inner>() != align_of::<T>() {
        return Err(crate::sync::error::Error::Protocol(format!(
            "type layout mismatch in to_typed: {} is size {} align {}, requested {} is size {} align {}",
            std::any::type_name::<Inner>(),
            size_of::<Inner>(),
            align_of::<Inner>(),
            std::any::type_name::<T>(),
            size_of::<T>(),
            align_of::<T>(),
        )));
    }
    let mut out = MaybeUninit::<T>::uninit();
    unsafe {
        std::ptr::copy_nonoverlapping(
            (&value as *const Inner).cast::<u8>(),
            out.as_mut_ptr().cast::<u8>(),
            size_of::<T>(),
        );
        std::mem::forget(value);
        Ok(out.assume_init())
    }
}

/// Read a column from `ctx` using the runtime `ColumnType` for dispatch.
///
/// This is the runtime equivalent of `ClickHouseColumn::read_column::<T>()`.
/// The `ColumnType` is typically obtained from `parse_type()`.
pub fn read_column_by_type<'a>(
    ct: &ColumnType, ctx: &mut ReadColumnContext<'a>,
) -> Result<AnyColumnData<'a>> {
    use ColumnType::*;
    read_any_simple_columns!(
        ct, ctx;
        UInt8 => u8 => UInt8,
        UInt16 => u16 => UInt16,
        UInt32 => u32 => UInt32,
        UInt64 => u64 => UInt64,
        UInt128 => u128 => UInt128,
        Int8 => i8 => Int8,
        Int16 => i16 => Int16,
        Int32 => i32 => Int32,
        Int64 => i64 => Int64,
        Int128 => i128 => Int128,
        Float32 => f32 => Float32,
        Float64 => f64 => Float64,
        UInt256 => crate::sync::column::UInt256 => UInt256,
        Int256 => crate::sync::column::Int256 => Int256,
        Bool => bool => Bool,
        Date => crate::sync::column::Date => Date,
        DateTime => crate::sync::column::DateTime => DateTime,
        DateTime64(_) => DateTime64Value => DateTime64,
        IPv4 => crate::sync::column::Ipv4 => IPv4,
        IPv6 => crate::sync::column::Ipv6 => IPv6,
        UUID => crate::sync::column::Uuid => Uuid,
        Time => crate::sync::column::DateTime => DateTime,
        Time64(_) => DateTime64Value => DateTime64,
        JSON => crate::sync::column::JsonValue => JSON,
        Variant(_) => crate::sync::column::VariantValue => Variant,
        Dynamic => crate::sync::column::DynamicValue => Dynamic,
    );
    match ct {
        String => std::string::String::read_column(ctx).map(AnyColumnData::String),
        FixedString(n) => {
            // Infer size from buffer: data_len / rows
            let n = *n;
            let col = FixedStringColumnData {
                data: ctx.read_rows_bytes(n)?,
                n,
                count: ctx.rows,
            };
            Ok(AnyColumnData::FixedString(col))
        },
        Nullable(inner) => {
            let rows = ctx.rows;
            let _null_mask = ctx.read_exact(rows)?;
            let inner = read_column_by_type(inner, ctx)?;
            // We don't wrap in NullableColumnData; just return inner (skipping nulls)
            // The null mask is already consumed from the buffer.
            Ok(AnyColumnData::Nullable(Box::new(inner)))
        },
        Array(inner) => {
            // Read offsets, recurse for elements
            if ctx.rows == 0 {
                return Ok(AnyColumnData::Array(Box::new(AnyColumnData::Unknown)));
            }
            let offsets = ctx.read_offsets()?;
            let total = offsets[ctx.rows - 1] as usize;
            let saved = ctx.rows;
            ctx.rows = total;
            let inner = read_column_by_type(inner, ctx)?;
            ctx.rows = saved;
            Ok(AnyColumnData::Array(Box::new(inner)))
        },
        Map(k, v) => {
            // Map = Array(Tuple(K,V)): offsets + K elements + V elements
            if ctx.rows == 0 {
                return Ok(AnyColumnData::Map(RawMapColumnData {
                    offsets: Vec::new(),
                    keys_data: &[],
                    values_data: &[],
                }));
            }
            let offsets = ctx.read_offsets()?;
            let total = offsets[ctx.rows - 1] as usize;
            let saved = ctx.rows;
            ctx.rows = total;
            let keys_start = ctx.pos;
            let _ = read_column_by_type(k, ctx)?;
            let keys_end = ctx.pos;
            let vals_start = ctx.pos;
            let _ = read_column_by_type(v, ctx)?;
            let vals_end = ctx.pos;
            ctx.rows = saved;
            Ok(AnyColumnData::Map(RawMapColumnData {
                offsets,
                keys_data: &ctx.buf[keys_start..keys_end],
                values_data: &ctx.buf[vals_start..vals_end],
            }))
        },
        LowCardinality(inner) => read_column_by_type(inner, ctx),
        Date32 => u16::read_column(ctx).map(AnyColumnData::UInt16),
        Nothing => {
            let _ = ctx.read_exact(ctx.rows)?;
            Ok(AnyColumnData::Unknown)
        },
        AggregateFunction | SimpleAggregateFunction => {
            let data = ctx.read_exact(ctx.buf.len() - ctx.pos)?;
            Ok(AnyColumnData::AggregateFunction(DynamicColumnData::new(
                data.to_vec(),
                ctx.rows,
            )))
        },
        Enum8 => i8::read_column(ctx).map(AnyColumnData::Int8),
        Enum16 => i16::read_column(ctx).map(AnyColumnData::Int16),
        Decimal(p, _) => match *p {
            0..=9 => Decimal32::read_column(ctx).map(AnyColumnData::Decimal32),
            10..=18 => Decimal64::read_column(ctx).map(AnyColumnData::Decimal64),
            19..=38 => Decimal128::read_column(ctx).map(AnyColumnData::Decimal128),
            _ => Decimal256::read_column(ctx).map(AnyColumnData::Decimal256),
        },
        Tuple(elems) => {
            // Each sub-column has `rows` rows, same ctx; track byte ranges.
            let mut element_slices = Vec::with_capacity(elems.len());
            for elem in elems {
                let start = ctx.pos;
                let _ = read_column_by_type(elem, ctx)?;
                let end = ctx.pos;
                element_slices.push(&ctx.buf[start..end]);
            }
            Ok(AnyColumnData::Tuple(RawTupleColumnData {
                elements: element_slices,
            }))
        },
        Point => read_column_by_type(&Tuple(vec![Float64, Float64]), ctx),
        Ring => read_column_by_type(&Array(Box::new(Point)), ctx),
        Polygon => read_column_by_type(&Array(Box::new(Ring)), ctx),
        MultiPolygon => read_column_by_type(&Array(Box::new(Polygon)), ctx),
        Other(_) => {
            // Fallback: skip by reading all remaining bytes
            let remaining = ctx.buf.len() - ctx.pos;
            let _ = ctx.read_exact(remaining)?;
            Ok(AnyColumnData::Unknown)
        },
        _ => unreachable!("simple column type should be handled before recursive dispatch"),
    }
}
