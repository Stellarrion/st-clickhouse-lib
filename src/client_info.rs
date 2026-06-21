//! ClientInfo serialization for the ClickHouse native protocol.
//!
//! Encodes the client information block sent after Query packets.

use crate::protocol::revision as protocol_revision;
use crate::protocol::wire;

/// OpenTelemetry tracing context (see protocol spec §3.1).
#[derive(Clone)]
pub struct TracingContext {
    /// 16-byte trace ID.
    pub trace_id: [u8; 16],
    /// Span ID.
    pub span_id: u64,
    /// Tracestate header value.
    pub tracestate: String,
    /// Trace flags byte.
    pub trace_flags: u8,
}

#[derive(Clone, Debug)]
pub(crate) struct ClientInfoTemplate {
    pub(crate) before_initial_query_id: Vec<u8>,
    pub(crate) after_initial_query_id: Vec<u8>,
}

pub(crate) fn build_client_info_template(revision: u64, quota_key: &str) -> ClientInfoTemplate {
    let mut before_initial_query_id = Vec::with_capacity(16);
    let mut after_initial_query_id = Vec::with_capacity(96);

    wire::write_string_to_vec(&mut before_initial_query_id, ""); // initial_user
    wire::write_string_to_vec(&mut after_initial_query_id, "0.0.0.0:0"); // initial_address
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME {
        after_initial_query_id.extend_from_slice(&0i64.to_le_bytes());
    }
    after_initial_query_id.push(1); // interface = TCP
    wire::write_string_to_vec(&mut after_initial_query_id, ""); // os_user
    wire::write_string_to_vec(&mut after_initial_query_id, "rust-client");
    wire::write_string_to_vec(&mut after_initial_query_id, "st-clickhouse");
    wire::write_varint_to_vec(&mut after_initial_query_id, 26);
    wire::write_varint_to_vec(&mut after_initial_query_id, 4);
    wire::write_varint_to_vec(&mut after_initial_query_id, revision);
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
        wire::write_string_to_vec(&mut after_initial_query_id, quota_key);
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH {
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_VERSION_PATCH {
        wire::write_varint_to_vec(&mut after_initial_query_id, 2);
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
        after_initial_query_id.push(0);
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS {
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER {
        after_initial_query_id.push(0);
    }

    ClientInfoTemplate {
        before_initial_query_id,
        after_initial_query_id,
    }
}

pub(crate) fn write_client_info_from_template(
    buf: &mut Vec<u8>, template: &ClientInfoTemplate, query_id: &[u8],
) {
    buf.extend_from_slice(&template.before_initial_query_id);
    wire::write_string_bytes_to_vec(buf, query_id);
    buf.extend_from_slice(&template.after_initial_query_id);
}

/// Write the ClientInfo block into `buf` at the given protocol revision.
pub fn write_client_info(buf: &mut Vec<u8>, revision: u64, tracing: Option<&TracingContext>) {
    write_client_info_with_query_id(buf, revision, tracing, b"")
}

pub(crate) fn write_client_info_with_query_id(
    buf: &mut Vec<u8>, revision: u64, tracing: Option<&TracingContext>, query_id: &[u8],
) {
    wire::write_string_to_vec(buf, ""); // initial_user
    wire::write_string_bytes_to_vec(buf, query_id); // initial_query_id
    wire::write_string_to_vec(buf, "0.0.0.0:0"); // initial_address
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME {
        buf.extend_from_slice(&0i64.to_le_bytes()); // initial_query_start_time
    }
    buf.push(1); // interface = TCP
    wire::write_string_to_vec(buf, ""); // os_user
    wire::write_string_to_vec(buf, "rust-client"); // client_hostname
    wire::write_string_to_vec(buf, "st-clickhouse"); // client_name
    wire::write_varint_to_vec(buf, 26); // version_major
    wire::write_varint_to_vec(buf, 4); // version_minor
    wire::write_varint_to_vec(buf, revision); // protocol_version
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
        wire::write_string_to_vec(buf, ""); // quota_key
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH {
        wire::write_varint_to_vec(buf, 0); // distributed_depth
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_VERSION_PATCH {
        wire::write_varint_to_vec(buf, 2); // version_patch
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
        if let Some(tc) = tracing {
            buf.push(1); // has_trace_context = true
            buf.extend_from_slice(&tc.trace_id); // uuid (16 bytes)
            buf.extend_from_slice(&tc.span_id.to_le_bytes()); // uint64 span_id
            wire::write_string_to_vec(buf, &tc.tracestate);
            buf.push(tc.trace_flags); // trace_flags uint8
        } else {
            buf.push(0); // has_trace_context = false
        }
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
        wire::write_varint_to_vec(buf, 0); // collaborate_with_initiator
        wire::write_varint_to_vec(buf, 0); // count_participating_replicas
        wire::write_varint_to_vec(buf, 0); // number_of_current_replicas
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS {
        wire::write_varint_to_vec(buf, 0); // script_query_number
        wire::write_varint_to_vec(buf, 0); // script_line_number
    }
    if revision >= protocol_revision::DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER {
        buf.push(0); // has_jwt = 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REV: u64 = protocol_revision::DEFAULT_PROTOCOL_REVISION;

    #[test]
    fn quota_key_encoded_in_client_info_template() {
        // The default revision always carries the quota_key field
        // (DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO).
        let with_key = build_client_info_template(REV, "tenant-42");
        let empty = build_client_info_template(REV, "");

        // The key (9 bytes) must be present in the template that carries it,
        // and absent when the key is empty.
        assert!(
            with_key
                .after_initial_query_id
                .windows(9)
                .any(|w| w == b"tenant-42"),
            "quota_key not encoded in client info template"
        );
        assert!(
            !empty
                .after_initial_query_id
                .windows(9)
                .any(|w| w == b"tenant-42"),
            "empty quota_key must not encode the key"
        );
    }
}
