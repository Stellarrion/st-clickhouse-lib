//! ClientInfo serialization for the ClickHouse native protocol.
//!
//! Encodes the client information block sent after Query packets.
//!
//! ## Wire format (C++ `Client::Impl::SendQuery`)
//!
//! ```text
//! query_kind (1 byte, not varint)
//! initial_user (string)
//! initial_query_id (string)
//! initial_address (string)
//! initial_query_start_time (int64)     ← rev >= 54449
//! interface (1 byte)
//! os_user (string)
//! client_hostname (string)
//! client_name (string)
//! client_version_major (varint)
//! client_version_minor (varint)
//! client_revision (varint)
//! quota_key (string)                    ← rev >= 54458
//! distributed_depth (varint = 0)        ← rev >= 54448
//! client_version_patch (varint = 0)     ← rev >= 54401
//! opentelemetry (1 byte + 16+8+str+1)  ← rev >= 54442
//! parallel_replicas (3 varints = 0)     ← rev >= 54453
//! replicate_work (1 byte = 0)           ← rev >= 54480?
//! ```

use crate::sync::config::ClientConfig;
use crate::sync::protocol::revision;
use crate::sync::protocol::wire;

#[derive(Clone, Debug)]
pub(crate) struct ClientInfoTemplate {
    pub(crate) before_initial_query_id: Vec<u8>,
    pub(crate) after_initial_query_id: Vec<u8>,
}

pub(crate) fn build_client_info_template(
    config: &ClientConfig, protocol_revision: u64,
) -> ClientInfoTemplate {
    let rev = protocol_revision;
    let mut before_initial_query_id = Vec::with_capacity(64);
    let mut after_initial_query_id = Vec::with_capacity(128);

    // query_kind = INITIAL_QUERY (1 byte, not varint)
    before_initial_query_id.push(1);

    // initial_user (string)
    wire::write_string_to_vec(&mut before_initial_query_id, &config.initial_user);

    // initial_address (string)
    wire::write_string_to_vec(&mut after_initial_query_id, &config.initial_address);

    if rev >= revision::DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME {
        after_initial_query_id.extend_from_slice(&0i64.to_le_bytes());
    }

    // interface = TCP (1 byte)
    after_initial_query_id.push(1);

    wire::write_string_to_vec(&mut after_initial_query_id, &config.os_user);
    wire::write_string_to_vec(&mut after_initial_query_id, &config.client_hostname);
    wire::write_string_to_vec(&mut after_initial_query_id, &config.client_name);
    wire::write_varint_to_vec(&mut after_initial_query_id, config.client_version_major);
    wire::write_varint_to_vec(&mut after_initial_query_id, config.client_version_minor);
    wire::write_varint_to_vec(&mut after_initial_query_id, protocol_revision);

    if rev >= revision::DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
        wire::write_string_to_vec(&mut after_initial_query_id, &config.quota_key);
    }

    if rev >= revision::DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH {
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
    }

    if rev >= revision::DBMS_MIN_REVISION_WITH_VERSION_PATCH {
        wire::write_varint_to_vec(&mut after_initial_query_id, config.client_version_patch);
    }

    if rev >= revision::DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
        after_initial_query_id.push(0);
    }

    if rev >= revision::DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS {
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
    }

    if rev >= revision::DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS {
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
        wire::write_varint_to_vec(&mut after_initial_query_id, 0);
    }

    if rev >= revision::DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER {
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

/// Write the ClientInfo block into `buf` using the given configuration.
///
/// The `query_id` is the query identifier (passed separately from config
/// because it's generated fresh per query).
pub fn write_client_info(
    buf: &mut Vec<u8>, config: &ClientConfig, query_id: &str, protocol_revision: u64,
) {
    let template = build_client_info_template(config, protocol_revision);
    write_client_info_from_template(buf, &template, query_id.as_bytes());
}
