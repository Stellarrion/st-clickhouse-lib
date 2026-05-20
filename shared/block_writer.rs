#[must_use]
pub fn block_capacity(block: &Block) -> usize {
    let header = varint_len(1)
        + 1
        + varint_len(2)
        + std::mem::size_of::<i32>()
        + varint_len(0)
        + varint_len(block.columns.len() as u64)
        + varint_len(block.rows as u64);
    header
        + block
            .columns
            .iter()
            .map(|col| {
                varint_len(col.name.len() as u64)
                    + col.name.len()
                    + varint_len(col.type_name.len() as u64)
                    + col.type_name.len()
                    + 1
                    + col.data.len()
            })
            .sum::<usize>()
}

#[must_use]
pub fn data_packet_capacity(table_name: &str, block: &Block) -> usize {
    varint_len(2) + varint_len(table_name.len() as u64) + table_name.len() + block_capacity(block)
}

/// Write a Native block to the output buffer.
///
/// Format:
///   [BlockInfo]        — dim=1, is_overflows, dim=2, bucket_num, dim=0
///   varint(num_columns)
///   varint(num_rows)
///   For each column:
///     string(name)
///     string(type_name)
///     uint8(custom_serialization) — always 0 for now
///     column_data (raw bytes from ColumnInfo.data)
pub fn write_block(buf: &mut Vec<u8>, block: &Block) -> Result<()> {
    // BlockInfo: dim=1 (is_overflows=0)
    wire::write_varint(buf, 1)?;
    buf.push(0); // is_overflows = false
                 // dim=2 (bucket_num = -1)
    wire::write_varint(buf, 2)?;
    buf.extend_from_slice(&(-1i32).to_le_bytes());
    // terminator
    wire::write_varint(buf, 0)?;

    wire::write_varint(buf, block.columns.len() as u64)?;
    wire::write_varint(buf, block.rows as u64)?;

    for col in &block.columns {
        wire::write_string(buf, &col.name)?;
        wire::write_string(buf, &col.type_name)?;
        buf.push(0); // custom serialization = 0
        buf.extend_from_slice(&col.data);
    }

    Ok(())
}

/// Write a Data packet (ClientCode 2) for sending blocks to the server.
/// Used for INSERT and external tables.
pub fn write_data_packet(buf: &mut Vec<u8>, table_name: &str, block: &Block) -> Result<()> {
    wire::write_varint(buf, 2)?; // ClientCode::Data
    wire::write_string(buf, table_name)?;
    write_block(buf, block)
}

/// Write a Data packet with the block compressed using the given method.
///
/// Compresses the entire block (BlockInfo + columns) into a single compression
/// frame before wrapping in the Data packet envelope.
pub fn write_data_packet_compressed(
    buf: &mut Vec<u8>, table_name: &str, block: &Block, method: CompressionMethod,
) -> Result<()> {
    wire::write_varint(buf, 2)?; // ClientCode::Data
    wire::write_string(buf, table_name)?;
    // Write block to temp buffer, compress it
    let mut block_buf = Vec::with_capacity(block_capacity(block));
    write_block(&mut block_buf, block)?;
    let compressed = encode_frame(&block_buf, method)?;
    buf.extend_from_slice(&compressed);
    Ok(())
}

#[inline]
fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}
