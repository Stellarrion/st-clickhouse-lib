use super::packet::ClientPacket;
use super::wire;

/// Build ClickHouse Client::IgnoredPartUUIDs (8).
///
/// The server accepts this packet before a Query packet on the same connection
/// and applies the UUID set to the following query context.
pub(crate) fn build_ignored_part_uuids_packet(uuids: &[[u8; 16]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + uuids.len() * 16);
    wire::write_varint_to_vec(&mut buf, ClientPacket::IgnoredPartUUIDs as u64);
    wire::write_varint_to_vec(&mut buf, uuids.len() as u64);
    for uuid in uuids {
        buf.extend_from_slice(uuid);
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_part_uuids_packet_matches_clickhouse_layout() {
        let uuid = [7u8; 16];
        let packet = build_ignored_part_uuids_packet(&[uuid]);
        let mut cursor = &packet[..];
        assert_eq!(wire::read_varint(&mut cursor).expect("packet type"), 8);
        assert_eq!(wire::read_varint(&mut cursor).expect("uuid count"), 1);
        let mut decoded = [0u8; 16];
        std::io::Read::read_exact(&mut cursor, &mut decoded).expect("uuid bytes");
        assert_eq!(decoded, uuid);
        assert!(cursor.is_empty());
    }
}
