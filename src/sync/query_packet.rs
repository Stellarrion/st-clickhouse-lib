use crate::sync::client_info::ClientInfoTemplate;
use crate::sync::config::ClientConfig;
use crate::sync::protocol::revision;
use crate::sync::protocol::wire::{self, encode_varint};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub(super) struct QueryPacketTemplate {
    pub(super) prefix: Vec<u8>,
    pub(super) client_info: Option<ClientInfoTemplate>,
    pub(super) before_query: Vec<u8>,
    /// Byte length of the serialized settings block at the start of
    /// `before_query`. The remaining bytes are revision-framed tail
    /// (interserver fields, stage, compression) reusable by overlay packets.
    pub(super) settings_len: usize,
    pub(super) select_suffix: Vec<u8>,
    pub(super) insert_suffix: Vec<u8>,
    pub(super) select_capacity: usize,
    pub(super) insert_capacity: usize,
}

pub(super) fn build_query_packet_template(config: &ClientConfig, rev: u64) -> QueryPacketTemplate {
    let mut prefix = Vec::with_capacity(1);
    encode_varint(&mut prefix, 1); // ClientCode::Query

    let client_info = (rev >= revision::DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        .then(|| crate::sync::client_info::build_client_info_template(config, rev));

    let mut before_query = Vec::with_capacity(256);
    let settings_len = write_serialized_settings_overlay(
        &config.settings,
        &HashMap::new(),
        rev,
        &mut before_query,
    );

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
    // The empty-block encode only fails when the matching codec feature is
    // not compiled in (an invalid configuration); mirror the async path's
    // debug_assert so release builds still surface a deterministic protocol
    // error instead of silently wedging the connection.
    if let Err(err) = write_empty_data_block_for(&mut select_suffix, config.compression) {
        debug_assert!(false, "failed to encode empty compressed block: {err}");
    }

    let client_info_len = client_info
        .as_ref()
        .map(|t| t.before_initial_query_id.len() + t.after_initial_query_id.len())
        .unwrap_or(0);
    let fixed_capacity = prefix.len() + client_info_len + before_query.len();
    QueryPacketTemplate {
        prefix,
        client_info,
        before_query,
        settings_len,
        select_capacity: fixed_capacity + select_suffix.len(),
        insert_capacity: fixed_capacity + insert_suffix.len(),
        select_suffix,
        insert_suffix,
    }
}

/// Serialize the settings block: automatic client defaults, the connection's
/// session settings, then the per-query overlay, then the empty-name terminator.
///
/// `overlay` wins on duplicate keys: a base entry shadowed by the overlay is
/// emitted once, with the overlay value. Neither map is mutated.
///
/// Returns the number of bytes written, so packet builders can splice the
/// post-settings tail of a cached template after an overlay block.
pub(super) fn write_serialized_settings_overlay(
    base: &HashMap<String, String>, overlay: &HashMap<String, String>, rev: u64, buf: &mut Vec<u8>,
) -> usize {
    let start = buf.len();
    if rev >= revision::DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION
        && !base.contains_key(
            crate::sync::protocol::settings::RATIO_OF_DEFAULTS_FOR_SPARSE_SERIALIZATION,
        )
        && !overlay.contains_key(
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
    if !base
        .contains_key(crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING)
        && !overlay.contains_key(
            crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
        )
    {
        wire::write_string_to_vec(
            buf,
            crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING,
        );
        encode_varint(buf, 0);
        wire::write_string_to_vec(buf, "1");
    }
    for (name, value) in base {
        if !overlay.contains_key(name) {
            wire::write_string_to_vec(buf, name);
            encode_varint(buf, 0);
            wire::write_string_to_vec(buf, value);
        }
    }
    for (name, value) in overlay {
        wire::write_string_to_vec(buf, name);
        encode_varint(buf, 0);
        wire::write_string_to_vec(buf, value);
    }
    wire::write_string_to_vec(buf, "");
    buf.len() - start
}

/// Serialize the client's trailing empty Data block.
///
/// When the query packet's compression flag is set, the server expects this
/// block's body in compressed form exactly like every other client Data
/// packet — sending it plain makes the server try to parse the plain bytes as
/// a compression frame and stall until its read timeout (the sync-side
/// symptom of the P0 compression defect). Mirrors the async
/// `write_empty_data_for`.
pub(super) fn write_empty_data_block_for(
    buf: &mut Vec<u8>, compression: Option<crate::sync::compression::CompressionMethod>,
) -> crate::sync::error::Result<()> {
    encode_varint(buf, 2); // Data packet
    wire::write_string_to_vec(buf, ""); // table name (never compressed)
    let mut block = Vec::with_capacity(16);
    write_empty_block_body_to(&mut block);
    match compression {
        Some(
            method @ (crate::sync::compression::CompressionMethod::Lz4
            | crate::sync::compression::CompressionMethod::Zstd),
        ) => {
            let frame = crate::sync::compression::encode_frame(&block, method)?;
            buf.extend_from_slice(&frame);
        },
        Some(crate::sync::compression::CompressionMethod::None) | None => {
            buf.extend_from_slice(&block);
        },
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::protocol::wire;
    use std::collections::HashMap;

    fn settings_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    /// Parse a serialized settings block: `name`, flags varint, `value`
    /// entries until the empty-name terminator.
    fn parse_settings(bytes: &[u8]) -> Vec<(String, String)> {
        let mut reader = std::io::Cursor::new(bytes);
        let mut out = Vec::new();
        loop {
            let name = wire::read_string(&mut reader).expect("setting name");
            if name.is_empty() {
                return out;
            }
            let _flags = wire::read_varint(&mut reader).expect("setting flags");
            let value = wire::read_string(&mut reader).expect("setting value");
            out.push((name, value));
        }
    }

    fn overlay_settings(
        base: &[(&str, &str)], overlay: &[(&str, &str)], rev: u64,
    ) -> Vec<(String, String)> {
        let base = settings_map(base);
        let overlay = settings_map(overlay);
        let mut buf = Vec::new();
        write_serialized_settings_overlay(&base, &overlay, rev, &mut buf);
        parse_settings(&buf)
    }

    #[test]
    fn overlay_merges_base_and_overlay_with_precedence() {
        let entries = overlay_settings(
            &[("max_threads", "4"), ("max_block_size", "1000")],
            &[("max_threads", "9"), ("max_insert_block_size", "500")],
            revision::DEFAULT_PROTOCOL_REVISION,
        );
        let by_name: HashMap<_, _> = entries.iter().cloned().collect();
        assert_eq!(
            by_name.get("max_threads").map(String::as_str),
            Some("9"),
            "overlay must win on duplicate keys"
        );
        assert_eq!(
            by_name.get("max_block_size").map(String::as_str),
            Some("1000"),
            "unshadowed base entries must survive"
        );
        assert_eq!(
            by_name.get("max_insert_block_size").map(String::as_str),
            Some("500"),
            "overlay-only entries must be added"
        );
        assert_eq!(
            entries.iter().filter(|(n, _)| n == "max_threads").count(),
            1,
            "duplicate keys must be emitted exactly once"
        );
    }

    #[test]
    fn empty_overlay_matches_template_serialization() {
        for rev in [
            revision::MIN_SUPPORTED_PROTOCOL_REVISION,
            revision::DEFAULT_PROTOCOL_REVISION,
        ] {
            let mut config = ClientConfig::default();
            config.settings = settings_map(&[("max_threads", "4"), ("a", "1")]);
            let template = build_query_packet_template(&config, rev);
            let mut overlay_buf = Vec::new();
            write_serialized_settings_overlay(
                &config.settings,
                &HashMap::new(),
                rev,
                &mut overlay_buf,
            );
            assert_eq!(
                &template.before_query[..template.settings_len],
                overlay_buf.as_slice(),
                "empty overlay must reproduce the cached template settings at rev {rev}"
            );
        }
    }

    #[test]
    fn automatic_defaults_suppressed_only_when_overridden() {
        let json_key = crate::sync::protocol::settings::OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING;
        let sparse_key =
            crate::sync::protocol::settings::RATIO_OF_DEFAULTS_FOR_SPARSE_SERIALIZATION;

        // Nothing overrides: both automatic defaults are serialized.
        let entries = overlay_settings(&[("x", "1")], &[], revision::DEFAULT_PROTOCOL_REVISION);
        assert!(entries.iter().any(|(n, v)| n == json_key && v == "1"));
        assert!(entries.iter().any(|(n, v)| n == sparse_key && v == "1"));

        // Base overrides the sparse default, overlay overrides the JSON default.
        let entries = overlay_settings(
            &[(sparse_key, "0.5")],
            &[(json_key, "0")],
            revision::DEFAULT_PROTOCOL_REVISION,
        );
        assert_eq!(
            entries.iter().filter(|(n, _)| n == sparse_key).count(),
            1,
            "sparse default must not be duplicated"
        );
        assert!(entries.iter().any(|(n, v)| n == sparse_key && v == "0.5"));
        assert_eq!(
            entries.iter().filter(|(n, _)| n == json_key).count(),
            1,
            "JSON default must not be duplicated"
        );
        assert!(entries.iter().any(|(n, v)| n == json_key && v == "0"));

        // Old revisions never send the sparse default.
        let entries = overlay_settings(&[], &[], revision::MIN_SUPPORTED_PROTOCOL_REVISION);
        assert!(!entries.iter().any(|(n, _)| n == sparse_key));
        assert!(entries.iter().any(|(n, v)| n == json_key && v == "1"));
    }

    #[test]
    fn overlay_serialization_does_not_mutate_inputs() {
        let base = settings_map(&[("max_threads", "4")]);
        let overlay = settings_map(&[("max_threads", "9")]);
        let mut buf = Vec::new();
        write_serialized_settings_overlay(
            &base,
            &overlay,
            revision::DEFAULT_PROTOCOL_REVISION,
            &mut buf,
        );
        assert_eq!(base, settings_map(&[("max_threads", "4")]));
        assert_eq!(overlay, settings_map(&[("max_threads", "9")]));
    }
}
