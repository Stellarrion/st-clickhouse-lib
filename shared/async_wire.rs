/// Read a ClickHouse varint (unsigned LEB128) from an async reader.
#[cfg(feature = "tokio")]
#[inline]
pub async fn async_read_varint<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut byte = [0u8; 1];
    loop {
        reader.read_exact(&mut byte).await?;
        let b = byte[0];
        if shift == 63 && (b & 0x7F) > 1 {
            return Err(Error::Protocol("varint overflow".into()));
        }
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(Error::Protocol("varint overflow".into()));
        }
    }
}

/// Read a length-prefixed string from an async reader.
#[cfg(feature = "tokio")]
#[inline]
pub async fn async_read_string<R: AsyncRead + Unpin>(reader: &mut R) -> Result<String> {
    let len = checked_string_len(async_read_varint(reader).await?)?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    String::from_utf8(buf).map_err(|e| Error::Protocol(format!("invalid UTF-8: {e}")))
}

/// Read a length-prefixed protocol byte string from an async reader.
#[cfg(feature = "tokio")]
#[inline]
pub async fn async_read_string_bytes<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let len = checked_string_len(async_read_varint(reader).await?)?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Read exactly `n` bytes from an async reader.
#[cfg(feature = "tokio")]
#[inline]
pub async fn async_read_exact<R: AsyncRead + Unpin>(reader: &mut R, n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Read an i32 (4 bytes LE) from an async reader.
#[cfg(feature = "tokio")]
#[inline]
pub async fn async_read_i32<R: AsyncRead + Unpin>(reader: &mut R) -> Result<i32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await?;
    Ok(i32::from_le_bytes(buf))
}
