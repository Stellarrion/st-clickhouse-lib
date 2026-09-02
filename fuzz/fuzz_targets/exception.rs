#![no_main]

use libfuzzer_sys::fuzz_target;
use st_clickhouse::fuzz_hooks::parse_exception_chain;

fuzz_target!(|data: &[u8]| {
    // Drive the real sync exception-chain parser: per level an i32 LE code,
    // name/message/stack_trace length-prefixed strings, and a 1-byte
    // has_nested flag that chains the next level. Any input — truncated,
    // corrupt, or deeper than the MAX_EXCEPTION_CHAIN_DEPTH cap — must
    // terminate with a Result, never panic, hang, or over-allocate.
    let mut pos = 0usize;
    let _ = parse_exception_chain(data, &mut pos);
});
