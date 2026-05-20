// Binary wire format helpers for ClickHouse native protocol.
//
// Two sets of functions:
// - `read_varint/read_string/read_bytes` for `std::io::Read` (sync)
// - `parse_varint/parse_string/parse_bytes` for `&[u8]` (zero-copy buffer parsing)

// Shared wire helpers expect Error and Result in the including module scope.

// Matches clickhouse-cpp WireFormat::ReadString.
const MAX_STRING_BYTES: usize = 0x00FF_FFFF;

// ── Sync I/O (std::io::Read/Write) ──

/// Read a ClickHouse varint from a `std::io::Read` source.
#[inline]
pub fn read_varint<R: std::io::Read>(reader: &mut R) -> Result<u64> {
    let mut r = 0u64;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        r |= ((byte[0] & 0x7F) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(r);
        }
        shift += 7;
    }
}

/// Read a ClickHouse string (varint-prefixed) from a `std::io::Read` source.
#[inline]
pub fn read_string<R: std::io::Read>(reader: &mut R) -> Result<String> {
    let len = checked_string_len(read_varint(reader)?)?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf)
        .map_err(|e| Error::Protocol(format!("invalid utf8: {e}")))
}

/// Read a ClickHouse byte string (varint-prefixed).
#[inline]
pub fn read_string_bytes<R: std::io::Read>(reader: &mut R) -> Result<Vec<u8>> {
    let len = checked_string_len(read_varint(reader)?)?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Read a protocol string whose producer is known to emit ASCII/UTF-8.
#[inline]
pub fn read_string_unchecked<R: std::io::Read>(reader: &mut R) -> Result<String> {
    read_string(reader)
}

/// Read `n` bytes from a `std::io::Read` source.
#[inline]
pub fn read_bytes<R: std::io::Read>(reader: &mut R, n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Write a ClickHouse varint.
#[inline]
pub fn write_varint<W: std::io::Write>(writer: &mut W, mut value: u64) -> Result<()> {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            break;
        }
    }
    Ok(())
}

/// Write a ClickHouse string (varint-prefixed).
#[inline]
pub fn write_string<W: std::io::Write>(writer: &mut W, s: &str) -> Result<()> {
    write_varint(writer, s.len() as u64)?;
    writer.write_all(s.as_bytes())?;
    Ok(())
}

/// Write a ClickHouse byte string (varint-prefixed).
#[inline]
pub fn write_string_bytes<W: std::io::Write>(writer: &mut W, value: &[u8]) -> Result<()> {
    write_varint(writer, value.len() as u64)?;
    writer.write_all(value)?;
    Ok(())
}

#[inline]
#[cfg_attr(not(feature = "tokio"), allow(dead_code))]
pub(crate) fn write_varint_to_vec(buf: &mut Vec<u8>, value: u64) {
    let result = write_varint(buf, value);
    debug_assert!(result.is_ok());
}

#[inline]
#[cfg_attr(not(feature = "tokio"), allow(dead_code))]
pub(crate) fn write_string_to_vec(buf: &mut Vec<u8>, value: &str) {
    let result = write_string(buf, value);
    debug_assert!(result.is_ok());
}

#[inline]
#[cfg_attr(not(feature = "tokio"), allow(dead_code))]
pub(crate) fn write_string_bytes_to_vec(buf: &mut Vec<u8>, value: &[u8]) {
    write_varint_to_vec(buf, value.len() as u64);
    buf.extend_from_slice(value);
}

/// Write raw bytes.
#[inline]
pub fn write_bytes<W: std::io::Write>(writer: &mut W, data: &[u8]) -> Result<()> {
    writer.write_all(data)?;
    Ok(())
}

// ── Buffer parsing (&[u8], no I/O) ──

/// Parse a ClickHouse varint from a byte buffer, advancing the position.
#[inline]
pub fn parse_varint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut r = 0u64;
    let mut shift = 0;
    loop {
        if *pos >= buf.len() {
            return Err(Error::Protocol(
                "unexpected end of buffer parsing varint".into(),
            ));
        }
        let byte = buf[*pos];
        *pos += 1;
        r |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(r);
        }
        shift += 7;
    }
}

/// Parse a ClickHouse string from a byte buffer, advancing the position.
#[inline]
pub fn parse_string<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a str> {
    let len = checked_string_len(parse_varint(buf, pos)?)?;
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| Error::Protocol("string length overflow".into()))?;
    if end > buf.len() {
        return Err(Error::Protocol(
            "unexpected end of buffer parsing string".into(),
        ));
    }
    let s = std::str::from_utf8(&buf[*pos..end])
        .map_err(|_| Error::Protocol("invalid utf8 in response".into()))?;
    *pos = end;
    Ok(s)
}

fn checked_string_len(value: u64) -> Result<usize> {
    let value = usize::try_from(value)
        .map_err(|_| Error::Protocol("string length too large".into()))?;
    if value > MAX_STRING_BYTES {
        return Err(Error::Protocol(format!(
            "string length {value} exceeds clickhouse-cpp limit {MAX_STRING_BYTES}"
        )));
    }
    Ok(value)
}

/// Parse a protocol string whose producer is known to emit ASCII/UTF-8.
#[inline]
pub fn parse_string_unchecked<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a str> {
    parse_string(buf, pos)
}

/// Parse raw bytes from a byte buffer, advancing the position.
pub fn parse_bytes<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = (*pos)
        .checked_add(n)
        .ok_or_else(|| Error::Protocol("byte length overflow".into()))?;
    if end > buf.len() {
        return Err(Error::Protocol(
            "unexpected end of buffer parsing bytes".into(),
        ));
    }
    let slice = &buf[*pos..end];
    *pos = end;
    Ok(slice)
}

/// Parse an i32 from a byte buffer (little-endian), advancing the position.
#[inline]
pub fn parse_i32(buf: &[u8], pos: &mut usize) -> Result<i32> {
    let end = (*pos)
        .checked_add(4)
        .ok_or_else(|| Error::Protocol("i32 length overflow".into()))?;
    if end > buf.len() {
        return Err(Error::Protocol(
            "unexpected end of buffer parsing i32".into(),
        ));
    }
    let val = i32::from_le_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos += 4;
    Ok(val)
}

/// Encode a varint into a buffer (for building packets).
#[inline]
pub fn encode_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if v == 0 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip() {
        let vals = [0u64, 1, 127, 128, 16383, 16384, 1 << 28, u64::MAX];
        for v in vals {
            let mut buf = Vec::new();
            write_varint(&mut buf, v).expect("test operation failed");
            let mut pos = 0;
            let parsed = parse_varint(&buf, &mut pos).expect("test operation failed");
            assert_eq!(parsed, v, "roundtrip failed for {v}");
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn test_string_roundtrip() {
        let s = "Hello, ClickHouse! 🎉";
        let mut buf = Vec::new();
        write_string(&mut buf, s).expect("test operation failed");
        let mut pos = 0;
        let parsed = parse_string(&buf, &mut pos).expect("test operation failed");
        assert_eq!(parsed, s);
        assert_eq!(pos, buf.len());
    }
}
