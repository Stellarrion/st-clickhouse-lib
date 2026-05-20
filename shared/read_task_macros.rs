macro_rules! define_read_task_packet_builders {
    ($vis:vis) => {
        $vis fn build_empty_cluster_function_read_task_response() -> Vec<u8> {
            let mut pkt = Vec::with_capacity(4);
            wire::write_varint_to_vec(&mut pkt, 9); // Client::ReadTaskResponse
            wire::write_varint_to_vec(&mut pkt, 1); // initial cluster processing protocol version
            wire::write_string_to_vec(&mut pkt, ""); // empty path = no task
            pkt
        }

        $vis fn build_finished_merge_tree_read_task_response(stream_id: &str) -> Vec<u8> {
            let mut pkt = Vec::with_capacity(32 + stream_id.len());
            wire::write_varint_to_vec(&mut pkt, 10); // Client::MergeTreeReadTaskResponse
            pkt.extend_from_slice(&revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION.to_le_bytes());
            pkt.push(b'1'); // writeBoolText(true)
            wire::write_varint_to_vec(&mut pkt, 0); // empty RangesInDataPartsDescription
            if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 7 {
                wire::write_string_to_vec(&mut pkt, stream_id);
            }
            pkt
        }
    };
}
