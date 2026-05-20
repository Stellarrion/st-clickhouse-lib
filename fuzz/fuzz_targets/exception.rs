#![no_main]

use libfuzzer_sys::fuzz_target;
use st_clickhouse::protocol::wire;

fuzz_target!(|data: &[u8]| {
    // Exception chain: code(varint) + name(string) + message(string) + stack_trace(string)
    // + has_nested(varint) + nested_exception...
    let mut c = std::io::Cursor::new(data);
    for _ in 0..5 {
        // code
        if wire::read_varint(&mut c).is_err() { break; }
        // name
        if wire::read_string(&mut c).is_err() { break; }
        // message
        if wire::read_string(&mut c).is_err() { break; }
        // stack_trace
        if wire::read_string(&mut c).is_err() { break; }
        // has_nested
        if wire::read_varint(&mut c).is_err() { break; }
    }
});
