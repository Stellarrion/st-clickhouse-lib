#![no_main]

use libfuzzer_sys::fuzz_target;
use st_clickhouse::sync::protocol::response::parse_response_with_revision;
use st_clickhouse::sync::protocol::wire;

fuzz_target!(|data: &[u8]| {
    let mut response = Vec::with_capacity(data.len() + 16);

    if data.first().copied().unwrap_or(0) & 1 == 0 {
        wire::write_varint(&mut response, 13).ok();
    } else {
        wire::write_varint(&mut response, 16).ok();
    }
    response.extend_from_slice(data);

    let _ = parse_response_with_revision(response, 54483);

    let mut announcement = Vec::with_capacity(data.len() + 1);
    wire::write_varint(&mut announcement, 15).ok();
    announcement.extend_from_slice(data);
    let _ = parse_response_with_revision(announcement, 54483);
});
