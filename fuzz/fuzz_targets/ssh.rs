#![no_main]

use libfuzzer_sys::fuzz_target;
use st_clickhouse::protocol::handshake::{ssh_auth_user, ssh_signature_message};
use st_clickhouse::protocol::wire;

fuzz_target!(|data: &[u8]| {
    let split_a = data.len().min(data.first().copied().unwrap_or(0) as usize);
    let split_b = split_a
        + data[split_a..]
            .len()
            .min(data.get(1).copied().unwrap_or(0) as usize);

    let database = String::from_utf8_lossy(&data[..split_a]);
    let user = String::from_utf8_lossy(&data[split_a..split_b]);
    let challenge = &data[split_b..];

    let marked = ssh_auth_user(&user);
    let payload = ssh_signature_message(54483, &database, &user, challenge);

    let mut frame = Vec::new();
    let _ = wire::write_string(&mut frame, &marked);
    let _ = wire::write_string_bytes(&mut frame, &payload);

    let mut cursor = std::io::Cursor::new(&frame);
    let _ = wire::read_string(&mut cursor);
    let _ = wire::read_varint(&mut cursor);
});
