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

/// Enum column data — raw integer values with type name for label mappings.
#[derive(Debug)]
pub struct EnumColumnData<'a> {
    pub values: Vec<i64>,
    pub type_name: &'a str,
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
    String(StringColumnData<'a>),
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
    /// Enum column: raw integer values + type_name with label mappings
    Enum(EnumColumnData<'a>),
    /// Map(K, V): offsets + key/val sub-column byte slices
    Map(RawMapColumnData<'a>),
    /// Tuple(elems): borrowed byte slices for each element
    Tuple(RawTupleColumnData<'a>),
    Unknown,
}

impl<'a> AnyColumnData<'a> {
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
            Self::Enum(e) => e.values.len(),
            Self::Map(m) => m.offsets.len(),
            Self::Tuple(t) => t.elements.len(),
            Self::Unknown => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
            Self::Enum(_) => "Enum",
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
        );

        if tid == TypeId::of::<i8>() {
            if let Self::Enum(col) = self {
                let value = *col.values.get(row_index).ok_or_else(|| {
                    crate::error::Error::Protocol(format!(
                        "Enum: index {row_index} out of bounds (len {})",
                        col.values.len()
                    ))
                })?;
                let value = i8::try_from(value).map_err(|_| {
                    crate::error::Error::Protocol(format!("Enum value {value} does not fit i8"))
                })?;
                return unsafe { copy_value_checked::<T, i8>(value) };
            }
        }

        if tid == TypeId::of::<i16>() {
            if let Self::Enum(col) = self {
                let value = *col.values.get(row_index).ok_or_else(|| {
                    crate::error::Error::Protocol(format!(
                        "Enum: index {row_index} out of bounds (len {})",
                        col.values.len()
                    ))
                })?;
                let value = i16::try_from(value).map_err(|_| {
                    crate::error::Error::Protocol(format!("Enum value {value} does not fit i16"))
                })?;
                return unsafe { copy_value_checked::<T, i16>(value) };
            }
        }

        try_any_typed_columns!(
            self, tid, row_index;
            String => std::string::String,
            FixedString => FixedStringBytes,
            JSON => JsonValue,
            Variant => VariantValue,
            Dynamic => DynamicValue,
            AggregateFunction => DynamicValue,
            SimpleAggregateFunction => DynamicValue,
        );

        Err(crate::error::Error::Protocol(format!(
            "type mismatch: cannot read {} as {}",
            self.type_name(),
            std::any::type_name::<T>(),
        )))
    }

    /// Native slice view of this column when it holds PlainColumn values of
    /// type `T` in an aligned buffer — `None` otherwise (non-PlainColumn types
    /// like `String`, or a misaligned buffer). Lets callers materialize
    /// fixed-size columns at memcpy speed instead of per-row `to_typed`.
    pub fn plain_slice<T: 'static>(&self) -> Option<&[T]> {
        let tid = std::any::TypeId::of::<T>();
        try_any_plain_slice!(
            self, tid;
            UInt8 => u8, UInt16 => u16, UInt32 => u32, UInt64 => u64, UInt128 => u128,
            Int8 => i8, Int16 => i16, Int32 => i32, Int64 => i64, Int128 => i128,
            Float32 => f32, Float64 => f64,
            DateTime64 => DateTime64Value, Decimal32 => Decimal32, Decimal64 => Decimal64,
            Decimal128 => Decimal128, Decimal256 => Decimal256, UInt256 => UInt256, Int256 => Int256,
            Date => Date, DateTime => DateTime, Uuid => Uuid, IPv4 => Ipv4,
        );
        None
    }
}

unsafe fn copy_value_checked<T, Inner>(value: Inner) -> Result<T> {
    if size_of::<Inner>() != size_of::<T>() || align_of::<Inner>() != align_of::<T>() {
        return Err(crate::error::Error::Protocol(format!(
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
        UInt256 => crate::column::UInt256 => UInt256,
        Int256 => crate::column::Int256 => Int256,
        Bool => bool => Bool,
        Date => crate::column::Date => Date,
        DateTime => crate::column::DateTime => DateTime,
        DateTime64(_) => DateTime64Value => DateTime64,
        IPv4 => crate::column::Ipv4 => IPv4,
        IPv6 => crate::column::Ipv6 => IPv6,
        UUID => crate::column::Uuid => Uuid,
        Time => crate::column::DateTime => DateTime,
        Time64(_) => DateTime64Value => DateTime64,
        JSON => crate::column::JsonValue => JSON,
        Variant(_) => crate::column::VariantValue => Variant,
        Dynamic => crate::column::DynamicValue => Dynamic,
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
            // Track byte ranges for keys and values sub-columns
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
        Date32 => i32::read_column(ctx).map(AnyColumnData::Int32),
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
        Enum8 | Enum16 => {
            // Read raw integer values (i8 for Enum8, i16 for Enum16)
            // Labels are in the ColumnType — users can access them via column metadata
            let n = ctx.rows;
            let mut values = Vec::with_capacity(n);
            if matches!(ct, Enum8) {
                let col = i8::read_column(ctx)?;
                for i in 0..n {
                    values.push(col.get(i).unwrap_or(0) as i64);
                }
            } else {
                let col = i16::read_column(ctx)?;
                for i in 0..n {
                    values.push(col.get(i).unwrap_or(0) as i64);
                }
            }
            Ok(AnyColumnData::Enum(EnumColumnData {
                values,
                type_name: "", // Filled by block reader
            }))
        },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_any_column_data_len_and_empty() {
        let mut ctx = ReadColumnContext {
            rows: 0,
            pos: 0,
            buf: &[],
        };
        let col = read_column_by_type(&ColumnType::UInt64, &mut ctx)
            .expect("UInt64 column should decode");
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
    }

    #[test]
    fn test_any_column_data_uint64() {
        let buf = 42u64
            .to_le_bytes()
            .into_iter()
            .chain(99u64.to_le_bytes())
            .collect::<Vec<_>>();
        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = read_column_by_type(&ColumnType::UInt64, &mut ctx)
            .expect("UInt64 column should decode");
        assert_eq!(col.len(), 2);
        assert!(!col.is_empty());
        assert_eq!(col.type_name(), "UInt64");
    }

    #[test]
    fn test_any_column_data_date32_uses_four_signed_bytes_per_row() {
        let buf = (-1i32)
            .to_le_bytes()
            .into_iter()
            .chain(100_000i32.to_le_bytes())
            .collect::<Vec<_>>();
        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = read_column_by_type(&ColumnType::Date32, &mut ctx)
            .expect("Date32 column should decode");
        let AnyColumnData::Int32(values) = col else {
            unreachable!("Date32 must decode as signed 32-bit days");
        };
        assert_eq!(values.get(0).expect("row 0"), -1);
        assert_eq!(values.get(1).expect("row 1"), 100_000);
        assert_eq!(ctx.pos, 8, "Date32 must consume four bytes per row");
    }

    #[test]
    fn test_any_column_data_enum() {
        // Enum8 = i8 wire format, 3 rows
        let buf = vec![0x01u8, 0x02, 0x03];
        let mut ctx = ReadColumnContext {
            rows: 3,
            pos: 0,
            buf: &buf,
        };
        let col =
            read_column_by_type(&ColumnType::Enum8, &mut ctx).expect("Enum8 column should decode");
        let AnyColumnData::Enum(e) = &col else {
            unreachable!("checked Enum8 should decode to AnyColumnData::Enum");
        };
        assert_eq!(e.values, vec![1i64, 2, 3]);
        assert_eq!(col.len(), 3);
    }

    #[test]
    fn test_any_column_data_enum16() {
        // 2 rows of Enum16 = two i16s in LE
        let buf: Vec<u8> = (513i16)
            .to_le_bytes()
            .into_iter()
            .chain((42i16).to_le_bytes())
            .collect();
        assert_eq!(buf.len(), 4);
        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = read_column_by_type(&ColumnType::Enum16, &mut ctx)
            .expect("Enum16 column should decode");
        let AnyColumnData::Enum(e) = &col else {
            unreachable!("checked Enum16 should decode to AnyColumnData::Enum");
        };
        assert_eq!(e.values.len(), 2);
        assert_eq!(e.values[0], 513i64);
        assert_eq!(e.values[1], 42i64);
    }

    #[test]
    fn test_any_column_data_map_string_uint64() {
        // Map(String, UInt64): 2 rows
        // Row 0: {"a": 10, "b": 20} → offset = 2
        // Row 1: {"c": 30}          → offset = 3
        let mut buf = Vec::new();
        // Offsets (u64 * 2)
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.extend_from_slice(&3u64.to_le_bytes());
        // Keys: String elements
        buf.push(1);
        buf.push(b'a'); // "a"
        buf.push(1);
        buf.push(b'b'); // "b"
        buf.push(1);
        buf.push(b'c'); // "c"
        // Values: UInt64 elements
        buf.extend_from_slice(&10u64.to_le_bytes());
        buf.extend_from_slice(&20u64.to_le_bytes());
        buf.extend_from_slice(&30u64.to_le_bytes());

        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = read_column_by_type(
            &ColumnType::Map(Box::new(ColumnType::String), Box::new(ColumnType::UInt64)),
            &mut ctx,
        )
        .expect("Map column should decode");
        let AnyColumnData::Map(m) = &col else {
            unreachable!("checked Map should decode to AnyColumnData::Map");
        };
        assert_eq!(m.offsets.len(), 2);
        assert_eq!(m.offsets[0], 2);
        assert_eq!(m.offsets[1], 3);
        assert_eq!(col.len(), 2);
    }

    #[test]
    fn test_any_column_data_tuple_uint64_string() {
        let mut buf = Vec::new();
        // col 0: UInt64 — 2 rows
        buf.extend_from_slice(&42u64.to_le_bytes());
        buf.extend_from_slice(&99u64.to_le_bytes());
        // col 1: String — 2 rows
        buf.push(1);
        buf.push(b'a');
        buf.push(2);
        buf.push(b'b');
        buf.push(b'c');

        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = read_column_by_type(
            &ColumnType::Tuple(vec![ColumnType::UInt64, ColumnType::String]),
            &mut ctx,
        )
        .expect("Tuple column should decode");
        let AnyColumnData::Tuple(t) = &col else {
            unreachable!("checked Tuple should decode to AnyColumnData::Tuple");
        };
        assert_eq!(t.elements.len(), 2);
        assert!(t.elements[0].len() >= 16); // 2 * 8 bytes for u64
        assert_eq!(col.len(), 2);
    }

    #[test]
    fn test_any_column_data_unknown() {
        let buf = vec![0u8; 10];
        let mut ctx = ReadColumnContext {
            rows: 1,
            pos: 0,
            buf: &buf,
        };
        let col = read_column_by_type(&ColumnType::Other("FooBar".into()), &mut ctx)
            .expect("unknown column should be skipped");
        assert_eq!(col.len(), 0);
        assert!(matches!(col, AnyColumnData::Unknown));
    }

    #[test]
    fn test_any_column_data_type_name_strings() {
        let cases: &[(AnyColumnData, &str)] = &[
            (AnyColumnData::UInt8(PlainColumnData::empty()), "UInt8"),
            (AnyColumnData::Int16(PlainColumnData::empty()), "Int16"),
            (AnyColumnData::Float64(PlainColumnData::empty()), "Float64"),
            (
                AnyColumnData::String(StringColumnData::new(vec![], &[])),
                "String",
            ),
            (
                AnyColumnData::Map(RawMapColumnData {
                    offsets: Vec::new(),
                    keys_data: &[],
                    values_data: &[],
                }),
                "Map",
            ),
            (
                AnyColumnData::Tuple(RawTupleColumnData { elements: vec![] }),
                "Tuple",
            ),
            (
                AnyColumnData::Enum(EnumColumnData {
                    values: vec![],
                    type_name: "",
                }),
                "Enum",
            ),
            (AnyColumnData::Unknown, "Unknown"),
        ];
        for (col, expected) in cases {
            assert_eq!(col.type_name(), *expected, "type_name mismatch");
        }
    }
}
