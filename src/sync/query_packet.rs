use crate::query_id::next_query_id_with_prefix;
use crate::sync::client_info::ClientInfoTemplate;
use crate::sync::config::ClientConfig;
use crate::sync::protocol::revision;
use crate::sync::protocol::wire::{self, encode_varint};
use std::sync::atomic::AtomicU64;

static QUERY_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
static QUERY_ID_PROCESS_PREFIX: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub(super) struct QueryPacketTemplate {
    pub(super) prefix: Vec<u8>,
    pub(super) client_info: Option<ClientInfoTemplate>,
    pub(super) before_query: Vec<u8>,
    pub(super) select_suffix: Vec<u8>,
    pub(super) insert_suffix: Vec<u8>,
    pub(super) select_capacity: usize,
    pub(super) insert_capacity: usize,
}

pub(super) fn next_query_id(buf: &mut [u8; 22]) -> usize {
    next_query_id_with_prefix(buf, b"st-ch-", &QUERY_ID_PROCESS_PREFIX, &QUERY_ID_COUNTER)
}

pub(super) fn build_query_packet_template(config: &ClientConfig, rev: u64) -> QueryPacketTemplate {
    let mut prefix = Vec::with_capacity(1);
    encode_varint(&mut prefix, 1); // ClientCode::Query

    let client_info = (rev >= revision::DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        .then(|| crate::sync::client_info::build_client_info_template(config, rev));

    let mut before_query = Vec::with_capacity(256);
    write_serialized_settings(config, rev, &mut before_query);

    if rev >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES {
        wire::write_string_to_vec(&mut before_query, "");
    }
    if rev >= revision::DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
        wire::write_string_to_vec(&mut before_query, "");
    }

    encode_varint(&mut before_query, 2); // stage = Complete
    encode_varint(
        &mut before_query,
        if config.compression.is_some() { 1 } else { 0 },
    );

    let mut insert_suffix = Vec::with_capacity(1);
    if rev >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
        wire::write_string_to_vec(&mut insert_suffix, "");
    }

    let mut select_suffix = insert_suffix.clone();
    write_empty_data_block_to(&mut select_suffix);

    let client_info_len = client_info
        .as_ref()
        .map(|t| t.before_initial_query_id.len() + t.after_initial_query_id.len())
        .unwrap_or(0);
    let fixed_capacity = prefix.len() + client_info_len + before_query.len();
    QueryPacketTemplate {
        prefix,
        client_info,
        before_query,
        select_capacity: fixed_capacity + select_suffix.len(),
        insert_capacity: fixed_capacity + insert_suffix.len(),
        select_suffix,
        insert_suffix,
    }
}

fn write_serialized_settings(config: &ClientConfig, rev: u64, buf: &mut Vec<u8>) {
    if rev >= revision::DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION
        && !config.settings.contains_key(
            crate::sync::protocol::settings::RATIO_OF_DEFAULTS_FOR_SPARSE_SERIALIZATION,
        )
    {
        wire::write_string_to_vec(
            buf,
            crate::sync::protocol::settings::RATIO_OF_DEFAULTS_FOR_SPARSE_SERIALIZATION,
        );
        encode_varint(buf, 0);
        wire::write_string_to_vec(buf, "1");
    }
    if !config
        .settings
        .contains_key(crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING)
    {
        wire::write_string_to_vec(
            buf,
            crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
        );
        encode_varint(buf, 0);
        wire::write_string_to_vec(buf, "1");
    }
    for (name, value) in &config.settings {
        wire::write_string_to_vec(buf, name);
        encode_varint(buf, 0);
        wire::write_string_to_vec(buf, value);
    }
    wire::write_string_to_vec(buf, "");
}

pub(super) fn write_empty_data_block_to(buf: &mut Vec<u8>) {
    encode_varint(buf, 2); // Data packet
    wire::write_string_to_vec(buf, ""); // table name
    write_empty_block_body_to(buf);
}

fn write_empty_block_body_to(buf: &mut Vec<u8>) {
    encode_varint(buf, 1); // BlockInfo dim=1 (is_overflows)
    buf.push(0);
    encode_varint(buf, 2); // BlockInfo dim=2 (bucket_num)
    buf.extend_from_slice(&(-1i32).to_le_bytes());
    encode_varint(buf, 0); // BlockInfo dim=0 (terminator)
    encode_varint(buf, 0); // num_columns=0
    encode_varint(buf, 0); // num_rows=0
}
