/// Typed scalar extracted from a `Dynamic` or `Variant` column.
#[derive(Debug, Clone, PartialEq)]
pub enum DynamicFieldValue {
    Null,
    Bool(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    UInt256(UInt256),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    Int256(Int256),
    Float32(f32),
    Float64(f64),
    Date(Date),
    Date32(i32),
    DateTime(DateTime),
    DateTime64 {
        value: DateTime64Value,
        scale: u32,
    },
    Time(i32),
    Time64 {
        value: i64,
        scale: u32,
    },
    Decimal32 {
        value: Decimal32,
        scale: u32,
    },
    Decimal64 {
        value: Decimal64,
        scale: u32,
    },
    Decimal128 {
        value: Decimal128,
        scale: u32,
    },
    Decimal256 {
        value: Decimal256,
        scale: u32,
    },
    Uuid(Uuid),
    Ipv4(Ipv4),
    Ipv6(Ipv6),
    Enum8(i8),
    Enum16(i16),
    String(String),
    FixedString(Vec<u8>),
    Json(String),
    Array(Vec<DynamicFieldValue>),
    Map(Vec<(DynamicFieldValue, DynamicFieldValue)>),
    Tuple(Vec<DynamicFieldValue>),
    Raw { type_name: String, bytes: Vec<u8> },
}

impl DynamicFieldValue {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::UInt8(v) => Some(u64::from(*v)),
            Self::UInt16(v) => Some(u64::from(*v)),
            Self::UInt32(v) => Some(u64::from(*v)),
            Self::UInt64(v) => Some(*v),
            Self::Date(v) => Some(u64::from(v.0)),
            Self::DateTime(v) => Some(u64::from(v.0)),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int8(v) => Some(i64::from(*v)),
            Self::Int16(v) => Some(i64::from(*v)),
            Self::Int32(v) => Some(i64::from(*v)),
            Self::Int64(v) => Some(*v),
            Self::Date32(v) | Self::Time(v) => Some(i64::from(*v)),
            Self::DateTime64 { value, .. } => Some(value.0),
            Self::Time64 { value, .. } => Some(*value),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(v) | Self::Json(v) => Some(v),
            _ => None,
        }
    }
}

/// One decoded row from a `Dynamic` or `Variant` column.
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicTypedValue {
    pub type_index: usize,
    pub type_name: String,
    pub value: DynamicFieldValue,
}

/// Column data for Variant — wraps the raw wire bytes.
#[derive(Debug)]
pub struct VariantColumnData {
    data: Vec<u8>,
    count: usize,
    num_variants: usize,
    typed_values: Option<Vec<Option<DynamicTypedValue>>>,
}

impl VariantColumnData {
    pub(crate) fn new(data: Vec<u8>, count: usize, num_variants: usize) -> Self {
        Self {
            data,
            count,
            num_variants,
            typed_values: None,
        }
    }

    pub(crate) fn read_native(type_name: &str, rows: usize, data: &[u8]) -> Result<Self> {
        let types = match parse_type(type_name).map_err(Error::Protocol)? {
            ColumnType::Variant(types) => types,
            other => {
                return Err(Error::Protocol(format!(
                    "expected Variant type, got {other}"
                )));
            }
        };
        let mut pos = 0;
        let typed_values = parse_variant_values(data, &mut pos, rows, &types)?;
        Ok(Self {
            data: data.to_vec(),
            count: rows,
            num_variants: types.len(),
            typed_values: Some(typed_values),
        })
    }
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Get the discriminator for row `index` — which variant type this row belongs to.
    pub fn discriminator(&self, index: usize) -> u8 {
        self.typed_values
            .as_ref()
            .and_then(|values| values.get(index))
            .and_then(|value| value.as_ref())
            .and_then(|value| u8::try_from(value.type_index).ok())
            .unwrap_or_else(|| {
                if index < self.data.len() {
                    self.data[index]
                } else {
                    0
                }
            })
    }
    /// Number of variant types in this column.
    pub fn num_variants(&self) -> usize {
        self.num_variants
    }
    /// Get the raw bytes after discriminators (concatenated sub-columns).
    pub fn raw_bytes(&self) -> &[u8] {
        &self.data[self.count..]
    }
    /// Get a typed decoded value for row `index`, when this column was read with schema context.
    pub fn typed_value(&self, index: usize) -> Option<&DynamicTypedValue> {
        self.typed_values
            .as_ref()
            .and_then(|values| values.get(index))
            .and_then(|value| value.as_ref())
    }
}

impl<'a> ClickHouseColumnData<'a, VariantValue> for VariantColumnData {
    fn len(&self) -> usize {
        self.count
    }
    fn is_empty(&self) -> bool {
        self.count == 0
    }
    fn get(&self, _index: usize) -> Result<VariantValue> {
        Ok(VariantValue(self.data.clone()))
    }
}

/// Dynamic value — stored as raw wire data.
#[derive(Debug, Clone)]
pub struct DynamicValue(pub Vec<u8>);

/// Column data for Dynamic — raw bytes only.
#[derive(Debug)]
pub struct DynamicColumnData {
    data: Vec<u8>,
    count: usize,
    typed_values: Option<Vec<Option<DynamicTypedValue>>>,
}

impl DynamicColumnData {
    pub(crate) fn new(data: Vec<u8>, count: usize) -> Self {
        Self {
            data,
            count,
            typed_values: None,
        }
    }
    pub(crate) fn read_native(rows: usize, data: &[u8]) -> Result<Self> {
        let mut pos = 0;
        let version = parse_u64_le(data, &mut pos)?;
        let typed_values = match version {
            1 => {
                let _max_dynamic_types = parse_varint_checked(data, &mut pos)?;
                let (types, type_names) = parse_dynamic_type_header(data, &mut pos)?;
                let _variant_serialization_version = parse_u64_le(data, &mut pos)?;
                skip_state_prefixes_for_types(data, &mut pos, &types)?;
                parse_deprecated_dynamic_values(data, &mut pos, rows, &types, &type_names)?
            }
            2 | 3 => {
                let (types, type_names) = parse_dynamic_type_header(data, &mut pos)?;
                skip_state_prefixes_for_types(data, &mut pos, &types)?;
                parse_flat_dynamic_values(data, &mut pos, rows, &types, &type_names)?
            }
            other => {
                return Err(Error::Protocol(format!(
                    "unknown Dynamic serialization version {other}"
                )));
            }
        };
        Ok(Self {
            data: data.to_vec(),
            count: rows,
            typed_values: Some(typed_values),
        })
    }
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Get the raw bytes for this column.
    pub fn raw_bytes(&self) -> &[u8] {
        &self.data
    }
    /// Get a typed decoded value for row `index`, when this column was read with schema context.
    pub fn typed_value(&self, index: usize) -> Option<&DynamicTypedValue> {
        self.typed_values
            .as_ref()
            .and_then(|values| values.get(index))
            .and_then(|value| value.as_ref())
    }
}

impl<'a> ClickHouseColumnData<'a, DynamicValue> for DynamicColumnData {
    fn len(&self) -> usize {
        self.count
    }
    fn is_empty(&self) -> bool {
        self.count == 0
    }
    fn get(&self, _index: usize) -> Result<DynamicValue> {
        Ok(DynamicValue(self.data.clone()))
    }
}

// ───────────────────────────────────────────────
fn parse_varint_checked(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    for _ in 0..10 {
        if *pos >= data.len() {
            return Err(Error::Protocol(
                "unexpected eof reading varint".into(),
            ));
        }
        let b = data[*pos];
        *pos += 1;
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(Error::Protocol("varint too long".into()))
}

fn checked_usize_local(value: u64, what: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::Protocol(format!("{what} too large")))
}

fn parse_string_checked(data: &[u8], pos: &mut usize) -> Result<String> {
    let len = checked_usize_local(parse_varint_checked(data, pos)?, "string length")?;
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| Error::Protocol("string length overflow".into()))?;
    if end > data.len() {
        return Err(Error::Protocol(
            "unexpected eof reading string".into(),
        ));
    }
    let out = std::str::from_utf8(&data[*pos..end])
        .map_err(|e| Error::Protocol(format!("invalid utf8 string: {e}")))?
        .to_owned();
    *pos = end;
    Ok(out)
}

fn parse_u64_le(data: &[u8], pos: &mut usize) -> Result<u64> {
    let bytes = parse_exact(data, pos, 8)?;
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(arr))
}

fn parse_dynamic_type_header(
    data: &[u8],
    pos: &mut usize,
) -> Result<(Vec<ColumnType>, Vec<String>)> {
    let type_count = checked_usize_local(parse_varint_checked(data, pos)?, "dynamic types")?;
    let mut types = Vec::with_capacity(type_count);
    let mut type_names = Vec::with_capacity(type_count);
    for _ in 0..type_count {
        let type_name = parse_string_checked(data, pos)?;
        types.push(parse_type(&type_name).map_err(Error::Protocol)?);
        type_names.push(type_name);
    }
    Ok((types, type_names))
}

fn skip_state_prefixes_for_types(
    data: &[u8],
    pos: &mut usize,
    types: &[ColumnType],
) -> Result<()> {
    for typ in types {
        skip_state_prefix_for_type(data, pos, typ)?;
    }
    Ok(())
}

fn skip_state_prefix_for_type(data: &[u8], pos: &mut usize, typ: &ColumnType) -> Result<()> {
    use ColumnType::*;
    match typ {
        Nullable(inner) | Array(inner) | LowCardinality(inner) => {
            skip_state_prefix_for_type(data, pos, inner)
        }
        Map(key, value) => {
            skip_state_prefix_for_type(data, pos, key)?;
            skip_state_prefix_for_type(data, pos, value)
        }
        Tuple(types) | Variant(types) => skip_state_prefixes_for_types(data, pos, types),
        Dynamic => {
            let version = parse_u64_le(data, pos)?;
            match version {
                0 => Ok(()),
                1 => {
                    let _max_dynamic_types = parse_varint_checked(data, pos)?;
                    let (types, _) = parse_dynamic_type_header(data, pos)?;
                    let _variant_serialization_version = parse_u64_le(data, pos)?;
                    skip_state_prefixes_for_types(data, pos, &types)
                }
                2 | 3 => {
                    let (types, _) = parse_dynamic_type_header(data, pos)?;
                    skip_state_prefixes_for_types(data, pos, &types)
                }
                other => Err(Error::Protocol(format!(
                    "unknown nested Dynamic serialization version {other}"
                ))),
            }
        }
        JSON => {
            let version = parse_u64_le(data, pos)?;
            match version {
                1 | 4 => Ok(()),
                0 => {
                    let _max_dynamic_paths = parse_varint_checked(data, pos)?;
                    let paths = checked_usize_local(
                        parse_varint_checked(data, pos)?,
                        "JSON dynamic paths",
                    )?;
                    for _ in 0..paths {
                        let _path = parse_string_checked(data, pos)?;
                    }
                    for _ in 0..paths {
                        skip_state_prefix_for_type(data, pos, &Dynamic)?;
                    }
                    Ok(())
                }
                3 => {
                    let paths = checked_usize_local(
                        parse_varint_checked(data, pos)?,
                        "JSON dynamic paths",
                    )?;
                    for _ in 0..paths {
                        let _path = parse_string_checked(data, pos)?;
                    }
                    for _ in 0..paths {
                        skip_state_prefix_for_type(data, pos, &Dynamic)?;
                    }
                    Ok(())
                }
                other => Err(Error::Protocol(format!(
                    "unknown nested JSON serialization version {other}"
                ))),
            }
        }
        _ => Ok(()),
    }
}

fn parse_exact<'a>(data: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| Error::Protocol("buffer position overflow".into()))?;
    if end > data.len() {
        return Err(Error::Protocol(
            "unexpected eof reading column".into(),
        ));
    }
    let out = &data[*pos..end];
    *pos = end;
    Ok(out)
}

fn parse_variant_values(
    data: &[u8],
    pos: &mut usize,
    rows: usize,
    types: &[ColumnType],
) -> Result<Vec<Option<DynamicTypedValue>>> {
    let mode = parse_u64_le(data, pos)?;
    let type_names = types.iter().map(ToString::to_string).collect::<Vec<_>>();
    match mode {
        0 => parse_variant_basic(data, pos, rows, types, &type_names),
        1 => parse_variant_compact(data, pos, rows, types, &type_names),
        other => Err(Error::Protocol(format!(
            "unknown Variant serialization mode {other}"
        ))),
    }
}

fn parse_variant_basic(
    data: &[u8],
    pos: &mut usize,
    rows: usize,
    types: &[ColumnType],
    type_names: &[String],
) -> Result<Vec<Option<DynamicTypedValue>>> {
    let discriminators = parse_exact(data, pos, rows)?.to_vec();
    let mut counts = vec![0usize; types.len()];
    for &disc in &discriminators {
        if let Some(count) = counts.get_mut(usize::from(disc)) {
            *count += 1;
        }
    }

    let mut decoded = Vec::with_capacity(types.len());
    for (idx, typ) in types.iter().enumerate() {
        decoded.push(decode_column_values(data, pos, typ, counts[idx])?);
    }

    let mut offsets = vec![0usize; types.len()];
    let mut rows_out = Vec::with_capacity(rows);
    for disc in discriminators {
        let type_index = usize::from(disc);
        if type_index >= decoded.len() {
            rows_out.push(None);
            continue;
        }
        let value_index = offsets[type_index];
        offsets[type_index] += 1;
        let value = decoded[type_index]
            .get(value_index)
            .cloned()
            .ok_or_else(|| {
                Error::Protocol("variant row index out of bounds".into())
            })?;
        rows_out.push(Some(DynamicTypedValue {
            type_index,
            type_name: type_names[type_index].clone(),
            value,
        }));
    }
    Ok(rows_out)
}

fn parse_deprecated_dynamic_values(
    data: &[u8],
    pos: &mut usize,
    rows: usize,
    types: &[ColumnType],
    type_names: &[String],
) -> Result<Vec<Option<DynamicTypedValue>>> {
    let discriminators = parse_exact(data, pos, rows)?.to_vec();
    let mut counts = vec![0usize; types.len()];
    for &disc in &discriminators {
        if disc == u8::MAX {
            continue;
        }
        let idx = usize::from(disc);
        if let Some(count) = counts.get_mut(idx) {
            *count += 1;
        } else {
            return Err(Error::Protocol(format!(
                "deprecated Dynamic discriminator {idx} exceeds type count {}",
                types.len()
            )));
        }
    }
    materialize_dynamic_rows(data, pos, rows, types, type_names, &discriminators, &counts)
}

fn parse_flat_dynamic_values(
    data: &[u8],
    pos: &mut usize,
    rows: usize,
    types: &[ColumnType],
    type_names: &[String],
) -> Result<Vec<Option<DynamicTypedValue>>> {
    let width = dynamic_discriminator_width(types.len());
    let bytes = parse_exact(data, pos, checked_len_local(rows, width)?)?;
    let mut discriminators = Vec::with_capacity(rows);
    let mut counts = vec![0usize; types.len()];
    for chunk in bytes.chunks_exact(width) {
        let idx = decode_dynamic_discriminator(chunk)?;
        if idx < types.len() {
            counts[idx] += 1;
        } else if idx != types.len() {
            return Err(Error::Protocol(format!(
                "Dynamic discriminator {idx} exceeds type count {}",
                types.len()
            )));
        }
        discriminators.push(idx);
    }
    materialize_dynamic_rows_usize(data, pos, rows, types, type_names, &discriminators, &counts)
}

fn materialize_dynamic_rows(
    data: &[u8],
    pos: &mut usize,
    rows: usize,
    types: &[ColumnType],
    type_names: &[String],
    discriminators: &[u8],
    counts: &[usize],
) -> Result<Vec<Option<DynamicTypedValue>>> {
    let wide = discriminators
        .iter()
        .map(|&disc| {
            if disc == u8::MAX {
                types.len()
            } else {
                usize::from(disc)
            }
        })
        .collect::<Vec<_>>();
    materialize_dynamic_rows_usize(data, pos, rows, types, type_names, &wide, counts)
}

fn materialize_dynamic_rows_usize(
    data: &[u8],
    pos: &mut usize,
    rows: usize,
    types: &[ColumnType],
    type_names: &[String],
    discriminators: &[usize],
    counts: &[usize],
) -> Result<Vec<Option<DynamicTypedValue>>> {
    let mut decoded = Vec::with_capacity(types.len());
    for (idx, typ) in types.iter().enumerate() {
        decoded.push(decode_column_values(data, pos, typ, counts[idx])?);
    }

    let mut offsets = vec![0usize; types.len()];
    let mut rows_out = Vec::with_capacity(rows);
    for &type_index in discriminators {
        if type_index >= decoded.len() {
            rows_out.push(None);
            continue;
        }
        let value_index = offsets[type_index];
        offsets[type_index] += 1;
        let value = decoded[type_index]
            .get(value_index)
            .cloned()
            .ok_or_else(|| {
                Error::Protocol("dynamic row index out of bounds".into())
            })?;
        rows_out.push(Some(DynamicTypedValue {
            type_index,
            type_name: type_names[type_index].clone(),
            value,
        }));
    }
    Ok(rows_out)
}

fn dynamic_discriminator_width(type_count: usize) -> usize {
    if u8::try_from(type_count).is_ok() {
        1
    } else if u16::try_from(type_count).is_ok() {
        2
    } else if u32::try_from(type_count).is_ok() {
        4
    } else {
        8
    }
}

fn decode_dynamic_discriminator(bytes: &[u8]) -> Result<usize> {
    let value = match bytes.len() {
        1 => u64::from(bytes[0]),
        2 => u64::from(u16::from_le_bytes([bytes[0], bytes[1]])),
        4 => u64::from(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
        8 => u64::from_le_bytes(bytes.try_into().map_err(|_| {
            Error::Protocol("Dynamic discriminator length mismatch".into())
        })?),
        _ => {
            return Err(Error::Protocol(
                "unsupported Dynamic discriminator width".into(),
            ));
        }
    };
    checked_usize_local(value, "Dynamic discriminator")
}

fn parse_variant_compact(
    data: &[u8],
    pos: &mut usize,
    rows: usize,
    types: &[ColumnType],
    type_names: &[String],
) -> Result<Vec<Option<DynamicTypedValue>>> {
    let type_index = checked_usize_local(parse_u64_le(data, pos)?, "variant discriminator")?;
    let compact_rows = checked_usize_local(parse_u64_le(data, pos)?, "variant compact rows")?;
    if type_index >= types.len() {
        return Ok(vec![None; rows]);
    }
    let decoded = decode_column_values(data, pos, &types[type_index], compact_rows)?;
    let mut out = Vec::with_capacity(rows);
    for idx in 0..rows {
        let value = decoded.get(idx).cloned().unwrap_or(DynamicFieldValue::Null);
        out.push(Some(DynamicTypedValue {
            type_index,
            type_name: type_names[type_index].clone(),
            value,
        }));
    }
    Ok(out)
}

fn decode_column_values(
    data: &[u8],
    pos: &mut usize,
    typ: &ColumnType,
    rows: usize,
) -> Result<Vec<DynamicFieldValue>> {
    use ColumnType::*;
    let mut out = Vec::with_capacity(rows);
    match typ {
        Bool | UInt8 => {
            for &v in parse_exact(data, pos, rows)? {
                out.push(if matches!(typ, Bool) {
                    DynamicFieldValue::Bool(v != 0)
                } else {
                    DynamicFieldValue::UInt8(v)
                });
            }
        }
        Int8 => {
            for &v in parse_exact(data, pos, rows)? {
                out.push(DynamicFieldValue::Int8(v as i8));
            }
        }
        UInt16 => decode_fixed(
            data,
            pos,
            rows,
            2,
            |b| DynamicFieldValue::UInt16(u16::from_le_bytes([b[0], b[1]])),
            &mut out,
        )?,
        Int16 => decode_fixed(
            data,
            pos,
            rows,
            2,
            |b| DynamicFieldValue::Int16(i16::from_le_bytes([b[0], b[1]])),
            &mut out,
        )?,
        UInt32 => decode_fixed(
            data,
            pos,
            rows,
            4,
            |b| DynamicFieldValue::UInt32(u32::from_le_bytes([b[0], b[1], b[2], b[3]])),
            &mut out,
        )?,
        Int32 => decode_fixed(
            data,
            pos,
            rows,
            4,
            |b| DynamicFieldValue::Int32(i32::from_le_bytes([b[0], b[1], b[2], b[3]])),
            &mut out,
        )?,
        Float32 => decode_fixed(
            data,
            pos,
            rows,
            4,
            |b| DynamicFieldValue::Float32(f32::from_le_bytes([b[0], b[1], b[2], b[3]])),
            &mut out,
        )?,
        UInt64 => decode_fixed(
            data,
            pos,
            rows,
            8,
            |b| {
                DynamicFieldValue::UInt64(u64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ]))
            },
            &mut out,
        )?,
        Int64 => decode_fixed(
            data,
            pos,
            rows,
            8,
            |b| {
                DynamicFieldValue::Int64(i64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ]))
            },
            &mut out,
        )?,
        Float64 => decode_fixed(
            data,
            pos,
            rows,
            8,
            |b| {
                DynamicFieldValue::Float64(f64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ]))
            },
            &mut out,
        )?,
        UInt128 => decode_fixed(
            data,
            pos,
            rows,
            16,
            |b| DynamicFieldValue::UInt128(u128_from_le(b)),
            &mut out,
        )?,
        Int128 => decode_fixed(
            data,
            pos,
            rows,
            16,
            |b| DynamicFieldValue::Int128(i128_from_le(b)),
            &mut out,
        )?,
        UInt256 => decode_fixed(
            data,
            pos,
            rows,
            32,
            |b| {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(b);
                DynamicFieldValue::UInt256(ch_uint256(arr))
            },
            &mut out,
        )?,
        Int256 => decode_fixed(
            data,
            pos,
            rows,
            32,
            |b| {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(b);
                DynamicFieldValue::Int256(ch_int256(arr))
            },
            &mut out,
        )?,
        Date => decode_fixed(
            data,
            pos,
            rows,
            2,
            |b| DynamicFieldValue::Date(ch_date(u16::from_le_bytes([b[0], b[1]]))),
            &mut out,
        )?,
        Date32 => decode_fixed(
            data,
            pos,
            rows,
            4,
            |b| DynamicFieldValue::Date32(i32::from_le_bytes([b[0], b[1], b[2], b[3]])),
            &mut out,
        )?,
        DateTime => decode_fixed(
            data,
            pos,
            rows,
            4,
            |b| {
                DynamicFieldValue::DateTime(ch_datetime(u32::from_le_bytes([
                    b[0], b[1], b[2], b[3],
                ])))
            },
            &mut out,
        )?,
        DateTime64(scale) => decode_fixed(
            data,
            pos,
            rows,
            8,
            |b| DynamicFieldValue::DateTime64 {
                value: DateTime64Value(i64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ])),
                scale: *scale,
            },
            &mut out,
        )?,
        Time => decode_fixed(
            data,
            pos,
            rows,
            4,
            |b| DynamicFieldValue::Time(i32::from_le_bytes([b[0], b[1], b[2], b[3]])),
            &mut out,
        )?,
        Time64(scale) => decode_fixed(
            data,
            pos,
            rows,
            8,
            |b| DynamicFieldValue::Time64 {
                value: i64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ]),
                scale: *scale,
            },
            &mut out,
        )?,
        Decimal(1..=9, scale) => decode_fixed(
            data,
            pos,
            rows,
            4,
            |b| DynamicFieldValue::Decimal32 {
                value: Decimal32(i32::from_le_bytes([b[0], b[1], b[2], b[3]])),
                scale: *scale,
            },
            &mut out,
        )?,
        Decimal(10..=18, scale) => decode_fixed(
            data,
            pos,
            rows,
            8,
            |b| DynamicFieldValue::Decimal64 {
                value: Decimal64(i64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ])),
                scale: *scale,
            },
            &mut out,
        )?,
        Decimal(19..=38, scale) => decode_fixed(
            data,
            pos,
            rows,
            16,
            |b| DynamicFieldValue::Decimal128 {
                value: Decimal128(i128_from_le(b)),
                scale: *scale,
            },
            &mut out,
        )?,
        Decimal(39..=76, scale) => decode_fixed(
            data,
            pos,
            rows,
            32,
            |b| {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(b);
                DynamicFieldValue::Decimal256 {
                    value: Decimal256(arr),
                    scale: *scale,
                }
            },
            &mut out,
        )?,
        UUID => decode_fixed(
            data,
            pos,
            rows,
            16,
            |b| DynamicFieldValue::Uuid(ch_uuid(u128_from_le(b))),
            &mut out,
        )?,
        IPv4 => decode_fixed(
            data,
            pos,
            rows,
            4,
            |b| DynamicFieldValue::Ipv4(ch_ipv4(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))),
            &mut out,
        )?,
        IPv6 => decode_fixed(
            data,
            pos,
            rows,
            16,
            |b| DynamicFieldValue::Ipv6(ch_ipv6(u128_from_le(b))),
            &mut out,
        )?,
        Enum8 => {
            for &v in parse_exact(data, pos, rows)? {
                out.push(DynamicFieldValue::Enum8(v as i8));
            }
        }
        Enum16 => decode_fixed(
            data,
            pos,
            rows,
            2,
            |b| DynamicFieldValue::Enum16(i16::from_le_bytes([b[0], b[1]])),
            &mut out,
        )?,
        String | JSON => {
            for _ in 0..rows {
                let s = parse_string_checked(data, pos)?;
                out.push(if matches!(typ, JSON) {
                    DynamicFieldValue::Json(s)
                } else {
                    DynamicFieldValue::String(s)
                });
            }
        }
        Nothing => {
            out.resize(rows, DynamicFieldValue::Null);
        }
        FixedString(n) => {
            for _ in 0..rows {
                out.push(DynamicFieldValue::FixedString(
                    parse_exact(data, pos, *n)?.to_vec(),
                ));
            }
        }
        Nullable(inner) => {
            let nulls = parse_exact(data, pos, rows)?.to_vec();
            let inner_values = decode_column_values(data, pos, inner, rows)?;
            for (is_null, value) in nulls.into_iter().zip(inner_values) {
                out.push(if is_null != 0 {
                    DynamicFieldValue::Null
                } else {
                    value
                });
            }
        }
        Array(inner) => {
            let offsets = parse_offsets(data, pos, rows)?;
            let total = offsets.last().copied().unwrap_or(0);
            let values = decode_column_values(data, pos, inner, total)?;
            let mut prev = 0usize;
            for offset in offsets {
                let end = offset.min(values.len());
                out.push(DynamicFieldValue::Array(values[prev..end].to_vec()));
                prev = end;
            }
        }
        Map(key, value) => {
            let offsets = parse_offsets(data, pos, rows)?;
            let total = offsets.last().copied().unwrap_or(0);
            let keys = decode_column_values(data, pos, key, total)?;
            let values = decode_column_values(data, pos, value, total)?;
            let mut prev = 0usize;
            for offset in offsets {
                let end = offset.min(keys.len()).min(values.len());
                out.push(DynamicFieldValue::Map(
                    keys[prev..end]
                        .iter()
                        .cloned()
                        .zip(values[prev..end].iter().cloned())
                        .collect(),
                ));
                prev = end;
            }
        }
        Tuple(types) => {
            let columns = types
                .iter()
                .map(|inner| decode_column_values(data, pos, inner, rows))
                .collect::<Result<Vec<_>>>()?;
            for row in 0..rows {
                out.push(DynamicFieldValue::Tuple(
                    columns
                        .iter()
                        .filter_map(|column| column.get(row).cloned())
                        .collect(),
                ));
            }
        }
        LowCardinality(inner) => {
            out = decode_column_values(data, pos, inner, rows)?;
        }
        Variant(types) => {
            let values = parse_variant_values(data, pos, rows, types)?;
            out.extend(values.into_iter().map(|value| {
                value
                    .map(|typed| typed.value)
                    .unwrap_or(DynamicFieldValue::Null)
            }));
        }
        other => {
            let start = *pos;
            skip_typed_value_column(data, pos, other, rows)?;
            let bytes = data[start..*pos].to_vec();
            for _ in 0..rows {
                out.push(DynamicFieldValue::Raw {
                    type_name: other.to_string(),
                    bytes: bytes.clone(),
                });
            }
        }
    }
    Ok(out)
}

fn decode_fixed<F>(
    data: &[u8],
    pos: &mut usize,
    rows: usize,
    width: usize,
    f: F,
    out: &mut Vec<DynamicFieldValue>,
) -> Result<()>
where
    F: Fn(&[u8]) -> DynamicFieldValue,
{
    let len = rows
        .checked_mul(width)
        .ok_or_else(|| Error::Protocol("fixed column length overflow".into()))?;
    let bytes = parse_exact(data, pos, len)?;
    for chunk in bytes.chunks_exact(width) {
        out.push(f(chunk));
    }
    Ok(())
}

fn u128_from_le(bytes: &[u8]) -> u128 {
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    u128::from_le_bytes(arr)
}

fn i128_from_le(bytes: &[u8]) -> i128 {
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    i128::from_le_bytes(arr)
}

fn checked_len_local(rows: usize, width: usize) -> Result<usize> {
    rows.checked_mul(width)
        .ok_or_else(|| Error::Protocol("column length overflow".into()))
}

fn parse_offsets(data: &[u8], pos: &mut usize, rows: usize) -> Result<Vec<usize>> {
    let mut offsets = Vec::with_capacity(rows);
    for _ in 0..rows {
        offsets.push(checked_usize_local(parse_u64_le(data, pos)?, "offset")?);
    }
    Ok(offsets)
}

fn skip_typed_value_column(
    data: &[u8],
    pos: &mut usize,
    typ: &ColumnType,
    rows: usize,
) -> Result<()> {
    use ColumnType::*;
    match typ {
        UInt8 | Int8 | Bool | Enum8 => {
            parse_exact(data, pos, rows)?;
        }
        UInt16 | Int16 | Date | Enum16 => {
            parse_exact(data, pos, checked_len_local(rows, 2)?)?;
        }
        UInt32 | Int32 | Float32 | Date32 | DateTime | IPv4 | Time => {
            parse_exact(data, pos, checked_len_local(rows, 4)?)?;
        }
        UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => {
            parse_exact(data, pos, checked_len_local(rows, 8)?)?;
        }
        UInt128 | Int128 | UUID => {
            parse_exact(data, pos, checked_len_local(rows, 16)?)?;
        }
        UInt256 | Int256 => {
            parse_exact(data, pos, checked_len_local(rows, 32)?)?;
        }
        IPv6 => {
            parse_exact(data, pos, checked_len_local(rows, 16)?)?;
        }
        Decimal(1..=9, _) => {
            parse_exact(data, pos, checked_len_local(rows, 4)?)?;
        }
        Decimal(10..=18, _) => {
            parse_exact(data, pos, checked_len_local(rows, 8)?)?;
        }
        Decimal(19..=38, _) => {
            parse_exact(data, pos, checked_len_local(rows, 16)?)?;
        }
        Decimal(39..=76, _) => {
            parse_exact(data, pos, checked_len_local(rows, 32)?)?;
        }
        String | JSON => {
            for _ in 0..rows {
                let len = checked_usize_local(parse_varint_checked(data, pos)?, "string length")?;
                parse_exact(data, pos, len)?;
            }
        }
        FixedString(n) => {
            parse_exact(data, pos, checked_len_local(rows, *n)?)?;
        }
        Nullable(inner) => {
            parse_exact(data, pos, rows)?;
            skip_typed_value_column(data, pos, inner, rows)?;
        }
        Array(inner) => {
            let offsets = parse_offsets(data, pos, rows)?;
            skip_typed_value_column(data, pos, inner, offsets.last().copied().unwrap_or(0))?;
        }
        Map(key, value) => {
            let offsets = parse_offsets(data, pos, rows)?;
            let total = offsets.last().copied().unwrap_or(0);
            skip_typed_value_column(data, pos, key, total)?;
            skip_typed_value_column(data, pos, value, total)?;
        }
        Tuple(types) => {
            for inner in types {
                skip_typed_value_column(data, pos, inner, rows)?;
            }
        }
        LowCardinality(inner) => {
            skip_typed_value_column(data, pos, inner, rows)?;
        }
        Nothing => {}
        _ => {
            *pos = data.len();
        }
    }
    Ok(())
}

#[cfg(test)]
mod dynamic_variant_tests {
    use super::*;

    fn write_string_for_test(buf: &mut Vec<u8>, value: &str) {
        let mut len = value.len() as u64;
        loop {
            buf.push((len & 0x7f) as u8 | if len > 0x7f { 0x80 } else { 0 });
            len >>= 7;
            if len == 0 {
                break;
            }
        }
        buf.extend_from_slice(value.as_bytes());
    }

    #[test]
    fn variant_read_native_decodes_extended_types() -> Result<()> {
        let type_name = concat!(
            "Variant(",
            "Bool, UInt128, Int128, UInt256, Int256, Float32, Float64, ",
            "Date, Date32, DateTime, DateTime64(3), Time, Time64(6), ",
            "Decimal(9, 2), Decimal(18, 3), Decimal(38, 4), Decimal(76, 5), ",
            "UUID, IPv4, IPv6, Enum8('a' = 1), Enum16('b' = 2), ",
            "String, JSON, FixedString(2), Nullable(UInt8), Array(UInt8), ",
            "Map(String, UInt8), Tuple(UInt8, String), Nothing)"
        );
        let rows = 30usize;
        let mut data = Vec::with_capacity(8 + rows + 256);
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend(0u8..rows as u8);

        data.push(1);
        data.extend_from_slice(&123u128.to_le_bytes());
        data.extend_from_slice(&(-123i128).to_le_bytes());
        data.extend_from_slice(&[3u8; 32]);
        data.extend_from_slice(&[4u8; 32]);
        data.extend_from_slice(&1.5f32.to_le_bytes());
        data.extend_from_slice(&2.5f64.to_le_bytes());
        data.extend_from_slice(&42u16.to_le_bytes());
        data.extend_from_slice(&(-42i32).to_le_bytes());
        data.extend_from_slice(&1_700_000_000u32.to_le_bytes());
        data.extend_from_slice(&1_700_000_000_123i64.to_le_bytes());
        data.extend_from_slice(&12_345i32.to_le_bytes());
        data.extend_from_slice(&12_345_678i64.to_le_bytes());
        data.extend_from_slice(&12345i32.to_le_bytes());
        data.extend_from_slice(&123456789i64.to_le_bytes());
        data.extend_from_slice(&123456789123456789i128.to_le_bytes());
        data.extend_from_slice(&[17u8; 32]);
        data.extend_from_slice(&0x0011_2233_4455_6677_8899_aabb_ccdd_eeffu128.to_le_bytes());
        data.extend_from_slice(&0x0100_007fu32.to_le_bytes());
        data.extend_from_slice(&0x0000_0000_0000_0000_0000_ffff_c000_0201u128.to_le_bytes());
        data.push(1);
        data.extend_from_slice(&2i16.to_le_bytes());
        write_string_for_test(&mut data, "hello");
        write_string_for_test(&mut data, "{\"x\":1}");
        data.extend_from_slice(b"xy");
        data.push(0);
        data.push(7);
        data.extend_from_slice(&3u64.to_le_bytes());
        data.extend_from_slice(&[1, 2, 3]);
        data.extend_from_slice(&2u64.to_le_bytes());
        write_string_for_test(&mut data, "a");
        write_string_for_test(&mut data, "b");
        data.extend_from_slice(&[8, 9]);
        data.push(11);
        write_string_for_test(&mut data, "tuple");

        let column = VariantColumnData::read_native(type_name, rows, &data)?;
        assert_eq!(column.len(), rows);
        assert_eq!(
            column.typed_value(0).map(|v| &v.value),
            Some(&DynamicFieldValue::Bool(true))
        );
        assert_eq!(
            column.typed_value(3).map(|v| &v.value),
            Some(&DynamicFieldValue::UInt256(UInt256([3u8; 32])))
        );
        assert_eq!(
            column.typed_value(10).map(|v| &v.value),
            Some(&DynamicFieldValue::DateTime64 {
                value: DateTime64Value(1_700_000_000_123),
                scale: 3,
            })
        );
        assert_eq!(
            column.typed_value(12).map(|v| &v.value),
            Some(&DynamicFieldValue::Time64 {
                value: 12_345_678,
                scale: 6,
            })
        );
        assert_eq!(
            column.typed_value(16).map(|v| &v.value),
            Some(&DynamicFieldValue::Decimal256 {
                value: Decimal256([17u8; 32]),
                scale: 5,
            })
        );
        assert_eq!(
            column.typed_value(22).map(|v| &v.value),
            Some(&DynamicFieldValue::String("hello".to_owned()))
        );
        assert_eq!(
            column.typed_value(23).map(|v| &v.value),
            Some(&DynamicFieldValue::Json("{\"x\":1}".to_owned()))
        );
        assert_eq!(
            column.typed_value(25).map(|v| &v.value),
            Some(&DynamicFieldValue::UInt8(7))
        );
        assert_eq!(
            column.typed_value(26).map(|v| &v.value),
            Some(&DynamicFieldValue::Array(vec![
                DynamicFieldValue::UInt8(1),
                DynamicFieldValue::UInt8(2),
                DynamicFieldValue::UInt8(3),
            ]))
        );
        assert_eq!(
            column.typed_value(28).map(|v| &v.value),
            Some(&DynamicFieldValue::Tuple(vec![
                DynamicFieldValue::UInt8(11),
                DynamicFieldValue::String("tuple".to_owned()),
            ]))
        );
        assert_eq!(
            column.typed_value(29).map(|v| &v.value),
            Some(&DynamicFieldValue::Null)
        );
        Ok(())
    }

    #[test]
    fn dynamic_v3_uses_variable_width_discriminators_and_null_slot() -> Result<()> {
        let rows = 3usize;
        let mut data = Vec::new();
        data.extend_from_slice(&3u64.to_le_bytes());
        write_varint_for_test(&mut data, 256);
        for _ in 0..256 {
            write_string_for_test(&mut data, "UInt8");
        }

        data.extend_from_slice(&0u16.to_le_bytes());
        data.extend_from_slice(&255u16.to_le_bytes());
        data.extend_from_slice(&256u16.to_le_bytes());

        data.push(7);
        data.push(9);

        let column = DynamicColumnData::read_native(rows, &data)?;
        assert_eq!(
            column.typed_value(0).map(|v| &v.value),
            Some(&DynamicFieldValue::UInt8(7))
        );
        assert_eq!(
            column.typed_value(1).map(|v| &v.value),
            Some(&DynamicFieldValue::UInt8(9))
        );
        assert_eq!(column.typed_value(2), None);
        Ok(())
    }

    fn write_varint_for_test(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }
}
