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
use crate::query_id::next_query_id;
use std::collections::HashMap;

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

/// Resolve the query ID bytes for a Query packet: the caller-supplied custom
/// ID when present, otherwise the next ID from the process-wide standard
/// generator shared with the sync engine (see `crate::query_id`).
pub(crate) fn query_id_bytes<'a>(custom: Option<&'a str>, buf: &'a mut [u8; 22]) -> &'a [u8] {
    if let Some(id) = custom {
        id.as_bytes()
    } else {
        let len = next_query_id(buf);
        &buf[..len]
    }
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

/// Whether the per-client cached query template can be reused for a query
/// instead of cloning the settings map and re-serializing the whole template.
///
/// Reuse is safe exactly when there are no per-query overrides (settings or
/// compression) and the cached template was built for the server's negotiated
/// revision. Callers must *additionally* exclude modes that inject settings of
/// their own (e.g. `RawCapture`), since those diverge from the client-level
/// template regardless of per-query overrides.
pub(crate) fn cached_template_reusable(
    per_query_settings: &HashMap<String, String>, per_query_compression: Option<CompressionMethod>,
    cached_rev: u64, negotiated_rev: u64,
) -> bool {
    per_query_settings.is_empty() && per_query_compression.is_none() && cached_rev == negotiated_rev
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_template_reusable_conditions() {
        let rev = revision::DEFAULT_PROTOCOL_REVISION;
        let empty = HashMap::<String, String>::new();
        let mut overrides = HashMap::new();
        overrides.insert("max_threads".to_string(), "4".to_string());

        // Common case: no overrides, matching revision → reusable.
        assert!(cached_template_reusable(&empty, None, rev, rev));
        // Per-query settings override → must rebuild.
        assert!(!cached_template_reusable(&overrides, None, rev, rev));
        // Per-query compression override → must rebuild.
        assert!(!cached_template_reusable(
            &empty,
            Some(CompressionMethod::Lz4),
            rev,
            rev
        ));
        // Different negotiated revision → must rebuild.
        assert!(!cached_template_reusable(&empty, None, rev, rev + 1));
    }

    #[test]
    fn cached_template_packet_matches_freshly_built() {
        // Reusing the cached template must yield a byte-identical query packet
        // to rebuilding from the same inputs — this is the correctness contract
        // that makes the fast path safe.
        let rev = revision::DEFAULT_PROTOCOL_REVISION;
        let settings = HashMap::from([("max_threads".to_string(), "4".to_string())]);
        let cached =
            build_query_packet_template(&settings, Some(CompressionMethod::Lz4), rev, "quota");
        let fresh =
            build_query_packet_template(&settings, Some(CompressionMethod::Lz4), rev, "quota");
        let query_id = b"st-ch-1";
        let a = build_query_packet(&cached, "SELECT 1", &[], query_id, &[]);
        let b = build_query_packet(&fresh, "SELECT 1", &[], query_id, &[]);
        assert_eq!(
            a, b,
            "cached-template packet must equal freshly-built packet"
        );
    }
}
