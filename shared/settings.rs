// ClickHouse settings used by the native protocol client.

/// Ask ClickHouse to serialize JSON/Object columns as JSON strings in Native format.
///
/// This matches clickhouse-cpp's supported JSON path. The client sends it by
/// default for materialized block reads, unless the user explicitly overrides it.
pub const OUTPUT_FORMAT_NATIVE_WRITE_JSON_AS_STRING: &str =
    "output_format_native_write_json_as_string";

/// Ask ClickHouse to use flattened Native serialization for Dynamic and JSON.
///
/// `query_raw` enables this by default so it can preserve Dynamic/Variant/JSON
/// payloads without materializing them. Users can override it explicitly.
pub const OUTPUT_FORMAT_NATIVE_USE_FLATTENED_DYNAMIC_AND_JSON_SERIALIZATION: &str =
    "output_format_native_use_flattened_dynamic_and_json_serialization";

/// Sparse serialization default required by newer Native protocol revisions.
pub const RATIO_OF_DEFAULTS_FOR_SPARSE_SERIALIZATION: &str =
    "ratio_of_defaults_for_sparse_serialization";
