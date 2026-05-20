use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn next_query_id_with_prefix(
    buf: &mut [u8; 22], prefix: &[u8], process_prefix: &AtomicU64, counter: &AtomicU64,
) -> usize {
    buf[..prefix.len()].copy_from_slice(prefix);
    let process_prefix_value = match process_prefix.load(Ordering::Relaxed) {
        0 => {
            let value = (u64::from(std::process::id()) & 0xffff_ffff) << 32;
            process_prefix.store(value, Ordering::Relaxed);
            value
        },
        value => value,
    };
    let n = process_prefix_value | (counter.fetch_add(1, Ordering::Relaxed) & 0xffff_ffff);
    let mut started = false;
    let mut pos = prefix.len();
    for shift in (0..64).step_by(4).rev() {
        let digit = ((n >> shift) & 0x0f) as u8;
        if digit != 0 || started || shift == 0 {
            started = true;
            buf[pos] = if digit < 10 {
                b'0' + digit
            } else {
                b'a' + (digit - 10)
            };
            pos += 1;
        }
    }
    pos
}
