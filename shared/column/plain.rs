// Shared plain column logic. Provided in scope by the including
// module: std::fmt, std::marker::PhantomData, std::mem::{align_of, size_of}, super::super::error::{Error, Result}, super::super::protocol::block::ReadColumnContext, super::super::protocol::type_parser::{ColumnType, parse_type}, super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue}.
// ───────────────────────────────────────────────
// UInt256 / Int256 — 32-byte fixed-size types
// ───────────────────────────────────────────────

/// 256-bit unsigned integer, stored as 32 bytes in little-endian order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UInt256(pub [u8; 32]);

/// 256-bit signed integer, stored as 32 bytes in little-endian order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Int256(pub [u8; 32]);

// ───────────────────────────────────────────────
// ClickHouse Type Mapping Types
// ───────────────────────────────────────────────

/// Date stored as days since Unix epoch (UInt16 wire format).
///
/// Use `.as_days()` to get the raw days count.
/// Convert to `chrono::NaiveDate` with:
/// ```ignore
/// let naive_date = chrono::NaiveDate::from_num_days_from_ce(date.as_days() as i32 + 719163);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Date(pub u16);

impl Date {
    /// Create from days since Unix epoch.
    pub fn from_days(days: u16) -> Self {
        Date(days)
    }
    /// Get the days since Unix epoch.
    pub fn as_days(&self) -> u16 {
        self.0
    }
}

impl std::fmt::Display for Date {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} days since epoch", self.0)
    }
}

impl From<u16> for Date {
    fn from(v: u16) -> Self {
        Date(v)
    }
}
impl From<Date> for u16 {
    fn from(v: Date) -> Self {
        v.0
    }
}

/// DateTime stored as Unix timestamp (seconds since epoch, UInt32 wire format).
///
/// Convert to `std::time::SystemTime`:
/// ```ignore
/// let st = std::time::UNIX_EPOCH + std::time::Duration::from_secs(dt.as_secs() as u64);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct DateTime(pub u32);

impl DateTime {
    /// Create from Unix timestamp seconds.
    pub fn from_secs(secs: u32) -> Self {
        DateTime(secs)
    }
    /// Get the Unix timestamp in seconds.
    pub fn as_secs(&self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for DateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for DateTime {
    fn from(v: u32) -> Self {
        DateTime(v)
    }
}
impl From<DateTime> for u32 {
    fn from(v: DateTime) -> Self {
        v.0
    }
}

/// 128-bit UUID (UInt128 wire format).
///
/// Use `.as_bytes()` for byte-level access.
/// Convert to `uuid::Uuid`:
/// ```ignore
/// let u = uuid::Uuid::from_u128(uuid.as_u128());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Uuid(pub u128);

impl Uuid {
    pub fn from_u128(v: u128) -> Self {
        Uuid(v)
    }
    pub fn as_u128(&self) -> u128 {
        self.0
    }
    /// Get the UUID as 16 bytes in standard UUID display order.
    pub fn as_bytes(&self) -> [u8; 16] {
        let wire = self.0.to_be_bytes();
        [
            wire[8], wire[9], wire[10], wire[11], wire[12], wire[13], wire[14], wire[15], wire[0],
            wire[1], wire[2], wire[3], wire[4], wire[5], wire[6], wire[7],
        ]
    }
    /// Create from 16 bytes in standard UUID display order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Uuid(u128::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
    /// Format as standard UUID string: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
    pub fn to_hyphenated(&self) -> String {
        let b = self.as_bytes();
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0],
            b[1],
            b[2],
            b[3],
            b[4],
            b[5],
            b[6],
            b[7],
            b[8],
            b[9],
            b[10],
            b[11],
            b[12],
            b[13],
            b[14],
            b[15]
        )
    }
}

impl std::fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hyphenated())
    }
}

impl From<u128> for Uuid {
    fn from(v: u128) -> Self {
        Uuid(v)
    }
}
impl From<Uuid> for u128 {
    fn from(v: Uuid) -> Self {
        v.0
    }
}

/// IPv4 address stored as a 32-bit integer (UInt32 wire format, LE).
///
/// Convert to `std::net::Ipv4Addr`:
/// ```ignore
/// let addr: std::net::Ipv4Addr = ipv4.into();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Ipv4(pub u32);

impl Ipv4 {
    pub fn from_u32(v: u32) -> Self {
        Ipv4(v)
    }
    pub fn as_u32(&self) -> u32 {
        self.0
    }
    /// Convert to `std::net::Ipv4Addr`.
    pub fn to_std(&self) -> std::net::Ipv4Addr {
        std::net::Ipv4Addr::from(self.0.to_be_bytes())
    }
    /// Create from `std::net::Ipv4Addr`.
    pub fn from_std(addr: std::net::Ipv4Addr) -> Self {
        Ipv4(u32::from_be_bytes(addr.octets()))
    }
}

impl std::fmt::Display for Ipv4 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_std())
    }
}

impl From<std::net::Ipv4Addr> for Ipv4 {
    fn from(v: std::net::Ipv4Addr) -> Self {
        Ipv4::from_std(v)
    }
}
impl From<Ipv4> for std::net::Ipv4Addr {
    fn from(v: Ipv4) -> Self {
        v.to_std()
    }
}
impl From<u32> for Ipv4 {
    fn from(v: u32) -> Self {
        Ipv4(v)
    }
}
impl From<Ipv4> for u32 {
    fn from(v: Ipv4) -> Self {
        v.0
    }
}

/// IPv6 address stored as a 128-bit integer (UInt128 wire format, LE).
///
/// Convert to `std::net::Ipv6Addr`:
/// ```ignore
/// let addr: std::net::Ipv6Addr = ipv6.into();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Ipv6(pub u128);

impl Ipv6 {
    pub fn from_u128(v: u128) -> Self {
        Ipv6(v)
    }
    pub fn as_u128(&self) -> u128 {
        self.0
    }
    /// Convert to `std::net::Ipv6Addr`.
    pub fn to_std(&self) -> std::net::Ipv6Addr {
        std::net::Ipv6Addr::from(self.0.to_be_bytes())
    }
    /// Create from `std::net::Ipv6Addr`.
    pub fn from_std(addr: std::net::Ipv6Addr) -> Self {
        Ipv6(u128::from_be_bytes(addr.octets()))
    }
}

impl std::fmt::Display for Ipv6 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_std())
    }
}

impl From<std::net::Ipv6Addr> for Ipv6 {
    fn from(v: std::net::Ipv6Addr) -> Self {
        Ipv6::from_std(v)
    }
}
impl From<Ipv6> for std::net::Ipv6Addr {
    fn from(v: Ipv6) -> Self {
        v.to_std()
    }
}
impl From<u128> for Ipv6 {
    fn from(v: u128) -> Self {
        Ipv6(v)
    }
}
impl From<Ipv6> for u128 {
    fn from(v: Ipv6) -> Self {
        v.0
    }
}

// ───────────────────────────────────────────────
// JSON, Variant, Dynamic — complex types (CH 24.8+)
// ───────────────────────────────────────────────

/// JSON value — stored as raw JSON text bytes.
/// Access the full text via `get_string()`, or parse with `serde_json`.
#[derive(Debug, Clone)]
pub struct JsonValue(pub String);

impl JsonValue {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Column data for JSON — stored as varint-prefixed strings (same wire as String).
#[derive(Debug)]
pub struct JsonColumnData {
    data: Vec<u8>,
    offsets: Vec<u64>,
}

impl JsonColumnData {
    pub(crate) fn new(offsets: Vec<u64>, data: Vec<u8>) -> Self {
        Self { data, offsets }
    }
    pub fn len(&self) -> usize {
        self.offsets.len()
    }
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
    /// Get the JSON text for a given row.
    pub fn get_string(&self, index: usize) -> Result<String> {
        let start = if index == 0 {
            0
        } else {
            self.offsets[index - 1] as usize
        };
        let end = self.offsets[index] as usize;
        Ok(String::from_utf8_lossy(&self.data[start..end]).into_owned())
    }
}

impl<'a> ClickHouseColumnData<'a, JsonValue> for JsonColumnData {
    fn len(&self) -> usize {
        self.offsets.len()
    }
    fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
    fn get(&self, index: usize) -> Result<JsonValue> {
        self.get_string(index).map(JsonValue)
    }
}

/// Variant value — stored as raw discriminators + sub-columns.
/// The raw data format: [discriminators: u8 * N] [subcol_0] [subcol_1] ...
#[derive(Debug, Clone)]
pub struct VariantValue(pub Vec<u8>);

fn ch_uint256(value: [u8; 32]) -> UInt256 {
    UInt256(value)
}

fn ch_int256(value: [u8; 32]) -> Int256 {
    Int256(value)
}

fn ch_date(value: u16) -> Date {
    Date(value)
}

fn ch_datetime(value: u32) -> DateTime {
    DateTime(value)
}

fn ch_uuid(value: u128) -> Uuid {
    Uuid(value)
}

fn ch_ipv4(value: u32) -> Ipv4 {
    Ipv4(value)
}

fn ch_ipv6(value: u128) -> Ipv6 {
    Ipv6(value)
}

// Shared typed Dynamic/Variant decoder used by both async and sync crates.
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/dynamic_variant.rs"
));

// ───────────────────────────────────────────────
// DateTime64 — 8-byte Int64 wrapper with scale awareness
// ───────────────────────────────────────────────

/// A DateTime64 value stored as the raw Int64 (scaled) timestamp.
/// Scale indicates the number of decimal places (0=seconds, 3=ms, 6=us, 9=ns).
/// Use `DateTime64Value::to_timestamp(scale)` to get seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DateTime64Value(pub i64);

impl DateTime64Value {
    /// Convert to Unix timestamp in seconds (divides by 10^scale).
    pub fn to_timestamp(&self, scale: u32) -> i64 {
        let divisor = 10i64.pow(scale);
        self.0 / divisor
    }

    /// Convert from Unix timestamp seconds to DateTime64 at given scale.
    pub fn from_timestamp(ts: i64, scale: u32) -> Self {
        let multiplier = 10i64.pow(scale);
        DateTime64Value(ts * multiplier)
    }
}

// ───────────────────────────────────────────────
// Decimal types — fixed-size integer-backed types
// ───────────────────────────────────────────────

/// Decimal32 value (4 bytes, precision <= 9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal32(pub i32);

/// Decimal64 value (8 bytes, precision 10-18).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal64(pub i64);

/// Decimal128 value (16 bytes, precision 19-38).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal128(pub i128);

/// Decimal256 value (32 bytes, precision 39-76).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal256(pub [u8; 32]);

// ───────────────────────────────────────────────
// PlainColumn trait — fixed-size types
// ───────────────────────────────────────────────

/// Types that can be safely reinterpreted from wire bytes directly.
///
/// Like `bytemuck::Pod` but scoped to our use case. Implemented for:
/// `u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64`.
///
/// # Safety
/// - No padding bytes: `size_of::<T>()` equals stride between elements.
/// - Any bit pattern is valid: no niche optimizations, no undefined values.
/// - Wire format is little-endian, matching x86/x64/aarch64 native endianness.
pub unsafe trait PlainColumn: ClickHouseValue {}

// ───────────────────────────────────────────────
// Blanket ClickHouseColumn impl for all PlainColumn types
// ───────────────────────────────────────────────

macro_rules! impl_plain_column_column {
    ($($t:ty),+ $(,)?) => {
        $(
            impl ClickHouseColumn for $t {
                type ColumnData<'a> = PlainColumnData<'a, $t>
                where $t: 'a;

                fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
                    let len = ctx.rows;
                    let nbytes = len.checked_mul(size_of::<$t>()).ok_or_else(|| {
                        super::super::error::Error::Protocol("column size overflow".into())
                    })?;
                    let bytes = ctx.read_exact(nbytes)?;
                    Ok(PlainColumnData::<$t> { buf: bytes, count: len, _phantom: PhantomData })
                }

                fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
                    for val in data {
                        val.write_to(buf)?;
                    }
                    Ok(())
                }
            }
        )+
    };
}

impl_plain_column_column!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);
impl_plain_column_column!(
    UInt256,
    Int256,
    DateTime64Value,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256
);
impl_plain_column_column!(Date, DateTime, Uuid, Ipv4);

// ───────────────────────────────────────────────
// PlainColumnData — true zero-copy column data
// ───────────────────────────────────────────────

/// Column data for fixed-size types — a view into the decompression buffer.
///
/// Zero-copy: no bytes are copied. Element access reads directly from the
/// buffer via `ptr::read_unaligned()` which compiles to a single MOV on x86.
/// When the buffer is properly aligned, `as_slice()` returns a `&[T]` view.
pub struct PlainColumnData<'a, T> {
    buf: &'a [u8],
    count: usize,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T: fmt::Debug> fmt::Debug for PlainColumnData<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlainColumnData")
            .field("buf_len", &self.buf.len())
            .field("count", &self.count)
            .field("elem_size", &size_of::<T>())
            .finish()
    }
}

impl<'a, T: PlainColumn + Copy> PlainColumnData<'a, T> {
    /// Create an empty column data.
    pub fn empty() -> Self {
        PlainColumnData {
            buf: &[],
            count: 0,
            _phantom: PhantomData,
        }
    }

    /// Read from a byte slice with a given number of elements.
    pub fn read_from_bytes(bytes: &'a [u8], count: usize) -> Self {
        PlainColumnData {
            buf: bytes,
            count,
            _phantom: PhantomData,
        }
    }

    /// Number of elements.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Access element at index — reads directly from buffer, handles any alignment.
    pub fn get(&self, index: usize) -> Result<T> {
        if index >= self.count {
            return Err(super::super::error::Error::Protocol(format!(
                "PlainColumnData: index {index} out of bounds (len {})",
                self.count
            )));
        }
        let offset = index * size_of::<T>();
        let ptr = self.buf[offset..].as_ptr() as *const T;
        // SAFETY:
        // - T: PlainColumn guarantees any bit pattern is valid (no Undef)
        // - read_unaligned handles any alignment (safe on x86/ARM)
        // - bounds check above ensures offset + size_of<T>() <= buf.len()
        Ok(unsafe { ptr.read_unaligned() })
    }

    /// Get all values as a slice — only when the buffer is properly aligned.
    /// Otherwise returns `None` (callers should use `get()` per element instead).
    pub fn as_slice(&self) -> Option<&'a [T]> {
        if self.count == 0 {
            return Some(&[]);
        }
        let ptr = self.buf.as_ptr() as *const T;
        if (ptr as usize) % align_of::<T>() == 0 {
            // SAFETY: T: PlainColumn guarantees valid bit pattern + no padding.
            // Alignment is verified above. Size is count * size_of::<T>() which
            // matches buf.len() (validated in read_column).
            Some(unsafe { std::slice::from_raw_parts(ptr, self.count) })
        } else {
            None
        }
    }
}

impl<'a, T: PlainColumn + Copy> ClickHouseColumnData<'a, T> for PlainColumnData<'a, T> {
    fn len(&self) -> usize {
        self.count
    }

    fn get(&self, index: usize) -> Result<T> {
        self.get(index)
    }

    fn as_slice(&self) -> Option<&[T]> {
        self.as_slice()
    }
}

// ───────────────────────────────────────────────
// Safe impls for all PlainColumn types
// ───────────────────────────────────────────────

macro_rules! impl_plain_column {
    ($($t:ty),+ $(,)?) => {
        $(
            unsafe impl PlainColumn for $t {}
        )+
    };
}

impl_plain_column!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

unsafe impl PlainColumn for UInt256 {}
unsafe impl PlainColumn for Int256 {}
unsafe impl PlainColumn for DateTime64Value {}
unsafe impl PlainColumn for Decimal32 {}
unsafe impl PlainColumn for Decimal64 {}
unsafe impl PlainColumn for Decimal128 {}
unsafe impl PlainColumn for Decimal256 {}
unsafe impl PlainColumn for Date {}
unsafe impl PlainColumn for DateTime {}
unsafe impl PlainColumn for Uuid {}
unsafe impl PlainColumn for Ipv4 {}

/// Column data for IPv6.
///
/// ClickHouse stores IPv6 in network byte order. That differs from the native
/// little-endian `u128` memory layout used by `PlainColumnData`, so IPv6 needs
/// a small typed wrapper instead of the generic zero-copy fixed-width path.
#[derive(Debug)]
pub struct Ipv6ColumnData<'a> {
    buf: &'a [u8],
    count: usize,
}

impl<'a> Ipv6ColumnData<'a> {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn get(&self, index: usize) -> Result<Ipv6> {
        if index >= self.count {
            return Err(super::super::error::Error::Protocol(format!(
                "Ipv6ColumnData: index {index} out of bounds (len {})",
                self.count
            )));
        }
        let offset = index
            .checked_mul(16)
            .ok_or_else(|| super::super::error::Error::Protocol("IPv6 column offset overflow".into()))?;
        let bytes: [u8; 16] = self.buf[offset..offset + 16]
            .try_into()
            .map_err(|_| super::super::error::Error::Protocol("short IPv6 value".into()))?;
        Ok(Ipv6(u128::from_be_bytes(bytes)))
    }
}

impl<'a> ClickHouseColumnData<'a, Ipv6> for Ipv6ColumnData<'a> {
    fn len(&self) -> usize {
        self.count
    }

    fn get(&self, index: usize) -> Result<Ipv6> {
        self.get(index)
    }

    fn as_slice(&self) -> Option<&[Ipv6]> {
        None
    }
}

// ── bool ────────────────────────────────────────

impl ClickHouseValue for bool {
    fn ch_type_name() -> &'static str {
        "Bool"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0] != 0)
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&[*self as u8])?;
        Ok(())
    }
}

/// Column data for Bool — wraps raw bytes, converts on access.
#[derive(Debug)]
pub struct BoolColumnData<'a>(pub(crate) &'a [u8]);

impl<'a> ClickHouseColumnData<'a, bool> for BoolColumnData<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn get(&self, index: usize) -> Result<bool> {
        if index >= self.0.len() {
            return Err(super::super::error::Error::Protocol(
                "BoolColumnData: index out of bounds".into(),
            ));
        }
        Ok(self.0[index] != 0)
    }
}

impl ClickHouseColumn for bool {
    type ColumnData<'a> = BoolColumnData<'a>;
    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let bytes = ctx.read_exact(ctx.rows)?;
        Ok(BoolColumnData(bytes))
    }
    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for &v in data {
            buf.push(v as u8);
        }
        Ok(())
    }
}

// ── Date, DateTime, Uuid, Ipv4, Ipv6 ClickHouseValue ──

// ── Date, DateTime, Uuid, Ipv4, Ipv6 ClickHouseValue ──

impl ClickHouseValue for Date {
    fn ch_type_name() -> &'static str {
        "Date"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(Date(u16::from_le_bytes(buf)))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for DateTime {
    fn ch_type_name() -> &'static str {
        "DateTime"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(DateTime(u32::from_le_bytes(buf)))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for Uuid {
    fn ch_type_name() -> &'static str {
        "UUID"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;
        Ok(Uuid(u128::from_le_bytes(buf)))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for Ipv4 {
    fn ch_type_name() -> &'static str {
        "IPv4"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(Ipv4(u32::from_le_bytes(buf)))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for Ipv6 {
    fn ch_type_name() -> &'static str {
        "IPv6"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;
        Ok(Ipv6(u128::from_be_bytes(buf)))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_be_bytes())?;
        Ok(())
    }
}

impl ClickHouseColumn for Ipv6 {
    type ColumnData<'a>
        = Ipv6ColumnData<'a>
    where
        Self: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let nbytes = ctx
            .rows
            .checked_mul(16)
            .ok_or_else(|| super::super::error::Error::Protocol("IPv6 column size overflow".into()))?;
        let bytes = ctx.read_exact(nbytes)?;
        Ok(Ipv6ColumnData {
            buf: bytes,
            count: ctx.rows,
        })
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for val in data {
            val.write_to(buf)?;
        }
        Ok(())
    }
}

// ── JsonValue, VariantValue, DynamicValue ClickHouse impls ──

impl std::fmt::Display for JsonValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ClickHouseValue for JsonValue {
    fn ch_type_name() -> &'static str {
        "JSON"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(JsonValue(String::from_utf8_lossy(&buf).into_owned()))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(self.0.as_bytes())?;
        Ok(())
    }
}

impl ClickHouseColumn for JsonValue {
    type ColumnData<'a>
        = JsonColumnData
    where
        Self: 'a;
    fn read_column<'a>(
        ctx: &mut super::super::protocol::block::ReadColumnContext<'a>,
    ) -> Result<Self::ColumnData<'a>> {
        let mut offsets = Vec::with_capacity(ctx.rows);
        let mut data = Vec::new();
        for _ in 0..ctx.rows {
            let (l, consumed) = read_varint_from_slice(&ctx.buf[ctx.pos..]);
            ctx.pos += consumed;
            let _start = data.len();
            data.extend_from_slice(&ctx.buf[ctx.pos..ctx.pos + l]);
            ctx.pos += l;
            offsets.push(data.len() as u64);
        }
        Ok(JsonColumnData::new(offsets, data))
    }
    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for v in data {
            write_string_to_buf(buf, &v.0)?;
        }
        Ok(())
    }
}

/// Read a LEB128 varint from a byte slice, returning (value, bytes_consumed).
fn read_varint_from_slice(data: &[u8]) -> (usize, usize) {
    let mut result = 0u64;
    let mut shift = 0;
    let mut consumed = 0;
    for &b in data {
        consumed += 1;
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return (result as usize, consumed);
        }
        shift += 7;
        if shift >= 64 {
            break;
        }
    }
    (result as usize, consumed)
}

/// Write a string with varint prefix (matching `wire::write_string`).
fn write_string_to_buf(buf: &mut Vec<u8>, s: &str) -> Result<()> {
    let len = s.len() as u64;
    let mut v = len;
    loop {
        buf.push((v & 0x7F) as u8 | if v > 0x7F { 0x80 } else { 0 });
        v >>= 7;
        if v == 0 {
            break;
        }
    }
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

impl std::fmt::Display for VariantValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Variant({} bytes)", self.0.len())
    }
}

impl ClickHouseValue for VariantValue {
    fn ch_type_name() -> &'static str {
        "Variant"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(VariantValue(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

impl ClickHouseColumn for VariantValue {
    type ColumnData<'a>
        = VariantColumnData
    where
        Self: 'a;
    fn read_column<'a>(
        ctx: &mut super::super::protocol::block::ReadColumnContext<'a>,
    ) -> Result<Self::ColumnData<'a>> {
        let count = ctx.rows;
        if ctx.pos + 8 > ctx.buf.len() {
            return Ok(VariantColumnData::new(Vec::new(), count, 0));
        }
        // Read mode (8 bytes)
        let mut mode_bytes = [0u8; 8];
        mode_bytes.copy_from_slice(&ctx.buf[ctx.pos..ctx.pos + 8]);
        let mode = u64::from_le_bytes(mode_bytes);
        ctx.pos += 8;
        let discriminant_offset = if mode == 1
        /* COMPACT */
        {
            // COMPACT: start_offset(8) + num_rows(8)
            if ctx.pos + 16 > ctx.buf.len() {
                return Ok(VariantColumnData::new(Vec::new(), count, 0));
            }
            ctx.pos + 16
        } else {
            // BASIC: discriminators per row
            ctx.pos + count
        };
        let remaining = ctx.buf[discriminant_offset..].to_vec();
        ctx.pos = ctx.buf.len();
        Ok(VariantColumnData::new(remaining, count, 0))
    }
    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for v in data {
            buf.extend_from_slice(&v.0);
        }
        Ok(())
    }
}

impl std::fmt::Display for DynamicValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Dynamic({} bytes)", self.0.len())
    }
}

impl ClickHouseValue for DynamicValue {
    fn ch_type_name() -> &'static str {
        "Dynamic"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(DynamicValue(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

impl ClickHouseColumn for DynamicValue {
    type ColumnData<'a>
        = DynamicColumnData
    where
        Self: 'a;
    fn read_column<'a>(
        ctx: &mut super::super::protocol::block::ReadColumnContext<'a>,
    ) -> Result<Self::ColumnData<'a>> {
        let count = ctx.rows;
        // Read all remaining data as raw bytes
        let data = ctx.buf[ctx.pos..].to_vec();
        ctx.pos = ctx.buf.len();
        Ok(DynamicColumnData::new(data, count))
    }
    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for v in data {
            buf.extend_from_slice(&v.0);
        }
        Ok(())
    }
}

impl ClickHouseValue for u8 {
    fn ch_type_name() -> &'static str {
        "UInt8"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for u16 {
    fn ch_type_name() -> &'static str {
        "UInt16"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for u32 {
    fn ch_type_name() -> &'static str {
        "UInt32"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for u64 {
    fn ch_type_name() -> &'static str {
        "UInt64"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for i8 {
    fn ch_type_name() -> &'static str {
        "Int8"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(i8::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for i16 {
    fn ch_type_name() -> &'static str {
        "Int16"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(i16::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for i32 {
    fn ch_type_name() -> &'static str {
        "Int32"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for i64 {
    fn ch_type_name() -> &'static str {
        "Int64"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for f32 {
    fn ch_type_name() -> &'static str {
        "Float32"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(f32::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for f64 {
    fn ch_type_name() -> &'static str {
        "Float64"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}

// u128 and i128 are also PlainColumn types
impl ClickHouseValue for u128 {
    fn ch_type_name() -> &'static str {
        "UInt128"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;
        Ok(u128::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for i128 {
    fn ch_type_name() -> &'static str {
        "Int128"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;
        Ok(i128::from_le_bytes(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
}
impl ClickHouseValue for UInt256 {
    fn ch_type_name() -> &'static str {
        "UInt256"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;
        Ok(UInt256(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}
impl ClickHouseValue for Int256 {
    fn ch_type_name() -> &'static str {
        "Int256"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;
        Ok(Int256(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

impl ClickHouseValue for DateTime64Value {
    fn ch_type_name() -> &'static str {
        "DateTime64"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(DateTime64Value(i64::from_le_bytes(buf)))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }
}

impl ClickHouseValue for Decimal32 {
    fn ch_type_name() -> &'static str {
        "Decimal32"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(Decimal32(i32::from_le_bytes(buf)))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }
}

impl ClickHouseValue for Decimal64 {
    fn ch_type_name() -> &'static str {
        "Decimal64"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(Decimal64(i64::from_le_bytes(buf)))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }
}

impl ClickHouseValue for Decimal128 {
    fn ch_type_name() -> &'static str {
        "Decimal128"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;
        Ok(Decimal128(i128::from_le_bytes(buf)))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        Ok(())
    }
}

impl ClickHouseValue for Decimal256 {
    fn ch_type_name() -> &'static str {
        "Decimal256"
    }
    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;
        Ok(Decimal256(buf))
    }
    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_column_aligned() {
        // Aligned buffer — as_slice() should work
        let bytes = 1u64
            .to_le_bytes()
            .into_iter()
            .chain(2u64.to_le_bytes())
            .chain(3u64.to_le_bytes())
            .collect::<Vec<_>>();
        let data = PlainColumnData::<u64> {
            buf: &bytes,
            count: 3,
            _phantom: PhantomData,
        };
        assert_eq!(data.len(), 3);
        assert_eq!(data.get(0).expect("test operation failed"), 1);
        assert_eq!(data.get(1).expect("test operation failed"), 2);
        assert_eq!(data.get(2).expect("test operation failed"), 3);
        // Aligned → as_slice works
        let slice = data.as_slice().expect("test operation failed");
        assert_eq!(slice, &[1u64, 2, 3]);
    }

    #[test]
    fn test_plain_column_misaligned() {
        // Misaligned: start at offset 1 (simulates Nullable mask before data)
        let payload = 1u64
            .to_le_bytes()
            .into_iter()
            .chain(2u64.to_le_bytes())
            .collect::<Vec<_>>();
        let bytes = vec![0u8; 1].into_iter().chain(payload).collect::<Vec<_>>();
        let data = PlainColumnData::<u64> {
            buf: &bytes[1..],
            count: 2,
            _phantom: PhantomData,
        };
        assert_eq!(data.len(), 2);
        assert_eq!(data.get(0).expect("test operation failed"), 1);
        assert_eq!(data.get(1).expect("test operation failed"), 2);
        // Misaligned → as_slice returns None
        assert!(data.as_slice().is_none());
    }

    #[test]
    fn test_plain_column_data_oob() {
        let bytes = vec![0u8; 8];
        let data = PlainColumnData::<u64> {
            buf: &bytes,
            count: 1,
            _phantom: PhantomData,
        };
        assert!(data.get(0).is_ok());
        assert!(data.get(1).is_err());
    }

    #[test]
    fn test_clickhouse_value_read_write() {
        let val: u64 = 42;
        let mut buf = Vec::new();
        val.write_to(&mut buf).expect("test operation failed");
        let mut cursor = std::io::Cursor::new(&buf[..]);
        let read = u64::read_from(&mut cursor).expect("test operation failed");
        assert_eq!(read, 42);
    }

    #[test]
    fn test_uint256_roundtrip() {
        let mut arr = [0u8; 32];
        arr[0] = 0x42;
        arr[31] = 0xFF;
        let val = UInt256(arr);
        let mut buf = Vec::new();
        val.write_to(&mut buf).expect("test operation failed");
        assert_eq!(buf.len(), 32);
        let mut cursor = std::io::Cursor::new(&buf[..]);
        let read = UInt256::read_from(&mut cursor).expect("test operation failed");
        assert_eq!(read, val);
    }

    #[test]
    fn test_int256_roundtrip() {
        let mut arr = [0u8; 32];
        arr[0] = 0x80;
        arr[31] = 0x7F;
        let val = Int256(arr);
        let mut buf = Vec::new();
        val.write_to(&mut buf).expect("test operation failed");
        assert_eq!(buf.len(), 32);
        let mut cursor = std::io::Cursor::new(&buf[..]);
        let read = Int256::read_from(&mut cursor).expect("test operation failed");
        assert_eq!(read, val);
    }

    #[test]
    fn test_uint256_plain_column_data() {
        // Two UInt256 values in a contiguous buffer
        let a = [0xAAu8; 32];
        let b = [0xBBu8; 32];
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&a);
        bytes.extend_from_slice(&b);
        let data = PlainColumnData::<UInt256> {
            buf: &bytes,
            count: 2,
            _phantom: PhantomData,
        };
        assert_eq!(data.len(), 2);
        assert_eq!(data.get(0).expect("test operation failed"), UInt256(a));
        assert_eq!(data.get(1).expect("test operation failed"), UInt256(b));
    }
}
