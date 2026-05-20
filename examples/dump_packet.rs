use st_clickhouse::client_info;
use st_clickhouse::protocol::wire;
fn main() {
    // Replicate exactly what build_query_packet_core + write_empty_data_marker does
    const REV: u64 = 54483;
    let mut b = Vec::new();
    wire::write_varint(&mut b, 1).expect("test operation failed"); // Query
    wire::write_string(&mut b, "").expect("test operation failed"); // query_id
    wire::write_varint(&mut b, 1).expect("test operation failed"); // query_kind
    client_info::write_client_info(&mut b, REV, None);
    wire::write_string(&mut b, "").expect("test operation failed"); // settings terminator
    wire::write_string(&mut b, "").expect("test operation failed"); // inter-server secret
    wire::write_varint(&mut b, 2).expect("test operation failed"); // stage
    wire::write_varint(&mut b, 0).expect("test operation failed"); // compression
    wire::write_string(&mut b, "SELECT 1").expect("test operation failed"); // query
    wire::write_string(&mut b, "").expect("test operation failed"); // params
    // Empty data marker
    wire::write_varint(&mut b, 2).expect("test operation failed");
    wire::write_string(&mut b, "").expect("test operation failed");
    wire::write_varint(&mut b, 1).expect("test operation failed");
    b.push(0);
    wire::write_varint(&mut b, 2).expect("test operation failed");
    b.extend_from_slice(&(-1i32).to_le_bytes());
    wire::write_varint(&mut b, 0).expect("test operation failed");
    // Print bytes
    for byte in &b {
        print!("{:02x} ", byte);
    }
    println!();
    eprintln!("Total bytes: {}", b.len());
}
