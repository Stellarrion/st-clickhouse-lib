#![no_main]

use libfuzzer_sys::fuzz_target;
use st_clickhouse::protocol::wire;

fuzz_target!(|data: &[u8]| {
    // Varint decoding — should never panic or infinite-loop
    let _ = wire::read_varint(&mut std::io::Cursor::new(data));
    // String decoding — should never panic
    let _ = wire::read_string(&mut std::io::Cursor::new(data));
    // Varint + string
    let mut c = std::io::Cursor::new(data);
    let _ = wire::read_varint(&mut c);
    let _ = wire::read_string(&mut c);
});
