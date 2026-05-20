#![no_main]

use libfuzzer_sys::fuzz_target;
use st_clickhouse::protocol::wire;

fuzz_target!(|data: &[u8]| {
    // Block header: compression_method(1) + compressed_size(varint) + decompressed_size(varint)
    let mut c = std::io::Cursor::new(data);
    let _compression_method = c.get_ref().first().copied().unwrap_or(0);
    let _ = wire::read_varint(&mut c);
    let _ = wire::read_varint(&mut c);

    // Block info: num_columns(varint) + num_rows(varint) + ...
    let mut c = std::io::Cursor::new(data);
    let _ = wire::read_varint(&mut c);
    let _ = wire::read_varint(&mut c);
    let _: Vec<u8> = data.iter().copied().collect();
});
