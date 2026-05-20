#![no_main]

use libfuzzer_sys::fuzz_target;
use st_clickhouse::sync::protocol::response::parse_response_with_revision;
use st_clickhouse::sync::protocol::wire;

fuzz_target!(|data: &[u8]| {
    let mut pos = 0usize;
    let mut normalized = Vec::with_capacity(data.len());

    while pos < data.len() {
        let len = usize::from(data[pos]);
        pos += 1;
        if len == 0 {
            break;
        }
        let end = pos.saturating_add(len).min(data.len());
        normalized.extend_from_slice(&data[pos..end]);
        pos = end;
    }

    let _ = parse_response_with_revision(normalized, 54483);

    let mut chunk_cursor = std::io::Cursor::new(data);
    while let Ok(len) = wire::read_varint(&mut chunk_cursor) {
        if len == 0 || len > 1_000_000 {
            break;
        }
        let mut buf = vec![0u8; len as usize];
        if std::io::Read::read_exact(&mut chunk_cursor, &mut buf).is_err() {
            break;
        }
    }
});
