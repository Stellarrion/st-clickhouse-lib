use crate::compression::CompressionMethod;
use crate::connection::io::{
    QueryPacketCommonTemplate, build_query_packet_common_template, merge_settings,
    query_packet_common_fixed_capacity, write_empty_data_for,
    write_query_packet_common_from_template,
};
use crate::protocol::block::Block;
use crate::protocol::parameters::{
    QueryParameter, query_parameters_capacity, write_query_parameters_to_vec,
};
use crate::protocol::revision;
use crate::protocol::wire;
use crate::query_id::next_query_id_with_prefix;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

static QUERY_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
static QUERY_ID_PROCESS_PREFIX: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub(crate) struct QueryPacketTemplate {
    pub(crate) revision: u64,
    compression: Option<CompressionMethod>,
    /// Per-client quota key, retained so the template can be rebuilt if the
    /// server negotiates a different revision (mirrors `compression`).
    quota_key: String,
    common: QueryPacketCommonTemplate,
    select_suffix: Vec<u8>,
    insert_suffix: Vec<u8>,
    select_capacity: usize,
    insert_capacity: usize,
}

pub(crate) fn query_id_bytes<'a>(custom: Option<&'a str>, buf: &'a mut [u8; 22]) -> &'a [u8] {
    if let Some(id) = custom {
        id.as_bytes()
    } else {
        let len = next_query_id(buf);
        &buf[..len]
    }
}

pub(crate) fn next_query_id(buf: &mut [u8; 22]) -> usize {
    next_query_id_with_prefix(buf, b"st-ch-", &QUERY_ID_PROCESS_PREFIX, &QUERY_ID_COUNTER)
}

pub(crate) fn build_query_packet_template(
    settings: &HashMap<String, String>, compression: Option<CompressionMethod>, rev: u64,
    quota_key: &str,
) -> QueryPacketTemplate {
    let common = build_query_packet_common_template(settings, compression, rev, quota_key);

    let mut insert_suffix = Vec::with_capacity(1);
    if rev >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
        wire::write_string_to_vec(&mut insert_suffix, "");
    }
    let mut select_suffix = insert_suffix.clone();
    write_empty_data_for(&mut select_suffix, compression);

    let fixed_capacity = query_packet_common_fixed_capacity(&common);

    QueryPacketTemplate {
        revision: rev,
        compression,
        quota_key: quota_key.to_owned(),
        common,
        select_capacity: fixed_capacity + select_suffix.len(),
        insert_capacity: fixed_capacity + insert_suffix.len(),
        select_suffix,
        insert_suffix,
    }
}

pub(crate) fn build_query_packet_from_template(
    template: &QueryPacketTemplate, query: &str, query_id: &[u8], include_empty_block: bool,
    params: &[QueryParameter],
) -> Vec<u8> {
    let capacity = if include_empty_block {
        template.select_capacity
    } else {
        template.insert_capacity
    };
    let mut b = Vec::with_capacity(
        capacity + query.len() + query_id.len() * 2 + query_parameters_capacity(params) + 16,
    );
    write_query_packet_common_from_template(&mut b, &template.common, query_id);
    wire::write_string_to_vec(&mut b, query);
    if params.is_empty() && include_empty_block {
        b.extend_from_slice(&template.select_suffix);
    } else if params.is_empty() {
        b.extend_from_slice(&template.insert_suffix);
    } else {
        if template.revision >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
            write_query_parameters_to_vec(&mut b, params);
        }
        if include_empty_block {
            write_empty_data_for(&mut b, template.compression);
        }
    }
    b
}

pub(crate) fn build_query_packet(
    template: &QueryPacketTemplate, query: &str, external_tables: &[(String, Block)],
    query_id: &[u8], params: &[QueryParameter],
) -> Vec<u8> {
    let mut b = build_query_packet_from_template(template, query, query_id, false, params);
    for (name, block) in external_tables {
        if block.row_count() > 0 {
            crate::protocol::block_writer::write_data_packet(&mut b, name, block).ok();
        }
    }
    write_empty_data_for(&mut b, template.compression);
    b
}

pub(crate) fn build_query_packet_from_cached_or_revision(
    cached: &QueryPacketTemplate, settings: &HashMap<String, String>, rev: u64, query: &str,
    query_id: &[u8], include_empty_block: bool, params: &[QueryParameter],
) -> Vec<u8> {
    if cached.revision == rev {
        return build_query_packet_from_template(
            cached,
            query,
            query_id,
            include_empty_block,
            params,
        );
    }

    let template =
        build_query_packet_template(settings, cached.compression, rev, &cached.quota_key);
    build_query_packet_from_template(&template, query, query_id, include_empty_block, params)
}

pub(crate) fn merge_materialized_settings(
    base: &HashMap<String, String>, overrides: &HashMap<String, String>,
) -> HashMap<String, String> {
    merge_settings(base, overrides)
}
