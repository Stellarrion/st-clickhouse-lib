use crate::sync::error::{Error, Result};
use crate::sync::protocol::response::{parse_block_body_shared, parse_block_shared};
use crate::sync::protocol::revision;
use crate::sync::protocol::wire::{
    parse_bytes, parse_i32, parse_string, parse_string_bytes, parse_varint,
};

/// Parse a full response buffer into blocks.
///
/// `arena` is the response `Vec<u8>`. Column data is stored as `Bytes` slices
/// into an `Arc<Vec<u8>>` wrapping `arena` — zero-copy, freed when the last
/// `Block` is dropped.
pub fn parse_response(
    arena: Vec<u8>, protocol_revision: u64,
) -> Result<Vec<crate::sync::protocol::block::Block>> {
    parse_response_with_revision(arena, protocol_revision)
}

pub fn parse_response_with_revision(
    arena: Vec<u8>, protocol_revision: u64,
) -> Result<Vec<crate::sync::protocol::block::Block>> {
    let shared = bytes::Bytes::from(arena);
    let buf: &[u8] = &shared;
    let mut blocks = Vec::new();
    let mut pos = 0;

    while pos < buf.len() {
        let packet_type = parse_varint(buf, &mut pos)?;
        match packet_type {
            1 => {
                let block = parse_block_shared(&shared, &mut pos)?;
                blocks.push(block);
            },
            2 => {
                let (code, name, message) = parse_exception_chain(buf, &mut pos)?;
                return Err(Error::ServerError {
                    code,
                    name,
                    message,
                });
            },
            3 => skip_progress(buf, &mut pos, protocol_revision)?,
            4 => {},
            5 => break,
            6 => skip_profile_info(buf, &mut pos, protocol_revision)?,
            7 | 8 => {
                let _ = parse_block_shared(&shared, &mut pos)?;
            },
            10 | 14 => {
                let _tag = parse_string(buf, &mut pos)?;
                let _ = parse_block_body_shared(&shared, &mut pos)?;
            },
            11 => {
                let _table_name = parse_string(buf, &mut pos)?;
                let _columns = parse_string(buf, &mut pos)?;
            },
            12 => skip_part_uuids(buf, &mut pos)?,
            17 => {
                let _timezone = parse_string(buf, &mut pos)?;
            },
            15 => skip_parallel_read_announcement(buf, &mut pos)?,
            13 | 16 => {
                return Err(Error::Protocol(format!(
                    "server requested distributed read task packet {packet_type}; use streaming query APIs so the client can respond"
                )));
            },
            18 => {
                return Err(Error::Protocol(
                    "unexpected SSHChallenge packet after handshake".into(),
                ));
            },
            other => return Err(Error::Protocol(format!("unknown packet type: {other}"))),
        }
    }

    Ok(blocks)
}

/// Parse an Exception packet body.
///
/// Returns the root exception's `(code, name)` plus the whole nested chain
/// joined into one message. A truncated or otherwise unparsable body returns
/// `Err(Error::Protocol(_))` so a malformed packet is never mistaken for a
/// terminal server exception. The chain depth is capped at 1,000 levels (the
/// internal `MAX_EXCEPTION_CHAIN_DEPTH` limit); a deeper chain is a protocol
/// error naming the cap.
pub fn parse_exception_chain(buf: &[u8], pos: &mut usize) -> Result<(i32, String, String)> {
    let mut parts = Vec::new();
    let mut root: Option<(i32, String)> = None;
    let mut depth = 0usize;
    loop {
        if depth >= crate::limits::MAX_EXCEPTION_CHAIN_DEPTH {
            return Err(Error::Protocol(format!(
                "exception nesting too deep: more than {} levels",
                crate::limits::MAX_EXCEPTION_CHAIN_DEPTH
            )));
        }
        let code = parse_i32(buf, pos)?;
        let name = String::from_utf8_lossy(parse_string_bytes(buf, pos)?).into_owned();
        let message = String::from_utf8_lossy(parse_string_bytes(buf, pos)?).into_owned();
        let _stack = parse_string_bytes(buf, pos)?;
        parts.push(format!("{name} (code {code}): {message}"));
        if root.is_none() {
            root = Some((code, name));
        }
        depth += 1;
        let flag = parse_bytes(buf, pos, 1)?;
        if flag.first().copied().unwrap_or(0) == 0 {
            break;
        }
    }
    let (code, name) = root.unwrap_or((0, "unknown".to_string()));
    Ok((code, name, parts.join(" | nested: ")))
}

fn skip_progress(buf: &[u8], pos: &mut usize, protocol_revision: u64) -> Result<()> {
    parse_varint(buf, pos)?;
    parse_varint(buf, pos)?;
    parse_varint(buf, pos)?;
    if protocol_revision >= revision::DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
        parse_varint(buf, pos)?;
        parse_varint(buf, pos)?;
    }
    if protocol_revision >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
        parse_varint(buf, pos)?;
    }
    if protocol_revision >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS {
        parse_varint(buf, pos)?;
    }
    Ok(())
}

fn skip_profile_info(buf: &[u8], pos: &mut usize, protocol_revision: u64) -> Result<()> {
    parse_varint(buf, pos)?;
    parse_varint(buf, pos)?;
    parse_varint(buf, pos)?;
    parse_bytes(buf, pos, 1)?;
    parse_varint(buf, pos)?;
    parse_bytes(buf, pos, 1)?;
    if protocol_revision >= revision::DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION {
        parse_bytes(buf, pos, 1)?;
        parse_varint(buf, pos)?;
    }
    Ok(())
}

fn skip_part_uuids(buf: &[u8], pos: &mut usize) -> Result<()> {
    let count = crate::limits::checked_count(
        parse_varint(buf, pos)?,
        "PartUUID",
        crate::limits::MAX_PART_UUIDS,
    )
    .map_err(Error::Protocol)?;
    advance(buf, pos, checked_len(count, 16)?)
}

fn skip_parallel_read_announcement(buf: &[u8], pos: &mut usize) -> Result<()> {
    let _version = parse_u64_le(buf, pos)?;
    let _mode = parse_u8(buf, pos)?;
    skip_ranges_in_data_parts_description(
        buf,
        pos,
        revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION,
    )?;
    let _replica_num = parse_u64_le(buf, pos)?;
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 4 {
        let _mark_segment_size = parse_u64_le(buf, pos)?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 5 {
        let _initial_participating_replicas = parse_varint(buf, pos)?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 6 {
        let _min_marks_per_request = parse_varint(buf, pos)?;
    }
    if revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION >= 7 {
        let _stream_id = parse_string(buf, pos)?;
    }
    Ok(())
}

fn skip_ranges_in_data_parts_description(
    buf: &[u8], pos: &mut usize, parallel_replicas_protocol_version: u64,
) -> Result<()> {
    let count = checked_usize(parse_varint(buf, pos)?, "parallel replica part ranges")?;
    for _ in 0..count {
        skip_merge_tree_part_info(buf, pos)?;
        skip_mark_ranges(buf, pos)?;
        let _rows = parse_varint(buf, pos)?;
        if parallel_replicas_protocol_version >= 5 {
            let _projection_name = parse_string(buf, pos)?;
        }
        if parallel_replicas_protocol_version >= 6 {
            let _min_marks_per_task = parse_varint(buf, pos)?;
        }
    }
    Ok(())
}

fn skip_merge_tree_part_info(buf: &[u8], pos: &mut usize) -> Result<()> {
    let _version = parse_u64_le(buf, pos)?;
    let _partition_id = parse_string(buf, pos)?;
    let _min_block = parse_u64_le(buf, pos)?;
    let _max_block = parse_u64_le(buf, pos)?;
    let _level = parse_u64_le(buf, pos)?;
    let _mutation = parse_u64_le(buf, pos)?;
    let _use_legacy_max_level = parse_u8(buf, pos)?;
    Ok(())
}

fn skip_mark_ranges(buf: &[u8], pos: &mut usize) -> Result<()> {
    let count = checked_usize(parse_u64_le(buf, pos)?, "parallel replica mark ranges")?;
    advance(buf, pos, checked_len(count, 16)?)
}

fn parse_u64_le(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let bytes = parse_bytes(buf, pos, 8)?;
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(out))
}

fn parse_u8(buf: &[u8], pos: &mut usize) -> Result<u8> {
    let bytes = parse_bytes(buf, pos, 1)?;
    Ok(bytes[0])
}

fn checked_len(rows: usize, width: usize) -> Result<usize> {
    rows.checked_mul(width)
        .ok_or_else(|| Error::Protocol("size overflow".into()))
}

fn checked_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::Protocol(format!("{name} too large")))
}

fn advance(buf: &[u8], pos: &mut usize, len: usize) -> Result<()> {
    let end = (*pos)
        .checked_add(len)
        .ok_or_else(|| Error::Protocol("buffer position overflow".into()))?;
    if end > buf.len() {
        return Err(Error::Protocol(
            "unexpected end of buffer skipping packet data".into(),
        ));
    }
    *pos = end;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::protocol::wire;

    fn put_string(buf: &mut Vec<u8>, s: &str) {
        wire::write_varint(buf, s.len() as u64).expect("test operation failed");
        buf.extend_from_slice(s.as_bytes());
    }

    /// Wire bytes for an Exception packet (type 2) and optional nested chain.
    fn exception_packet(entries: &[(i32, &str, &str, bool)]) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 2).expect("test operation failed");
        for (code, name, msg, nested) in entries {
            buf.extend_from_slice(&code.to_le_bytes());
            put_string(&mut buf, name);
            put_string(&mut buf, msg);
            put_string(&mut buf, ""); // stack trace
            buf.push(u8::from(*nested));
        }
        buf
    }

    // ── Server-controlled count cap tests ────────────────────────────────

    fn part_uuids_packet(count: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 12).expect("test write"); // PartUUIDs
        wire::write_varint(&mut buf, count).expect("test write");
        buf
    }

    #[test]
    fn part_uuid_count_u64_max_is_protocol_error() {
        let err = parse_response(
            part_uuids_packet(u64::MAX),
            revision::DEFAULT_PROTOCOL_REVISION,
        )
        .err()
        .expect("u64::MAX PartUUID count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "PartUUID count 18446744073709551615 exceeds limit 1048576"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn part_uuid_count_cap_plus_one_is_protocol_error() {
        let err = parse_response(
            part_uuids_packet(1_048_577),
            revision::DEFAULT_PROTOCOL_REVISION,
        )
        .err()
        .expect("cap + 1 PartUUID count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn exception_packet_yields_structured_server_error() {
        let arena = exception_packet(&[(60, "DB::Exception", "unknown function xyz", false)]);
        let err = parse_response(arena, revision::DEFAULT_PROTOCOL_REVISION)
            .err()
            .expect("server exception must be Err");
        match &err {
            Error::ServerError {
                code,
                name,
                message,
            } => {
                assert_eq!(*code, 60);
                assert_eq!(name, "DB::Exception");
                assert_eq!(message, "DB::Exception (code 60): unknown function xyz");
            },
            _other => unreachable!("expected ServerError, got {err:?}"),
        }
    }

    #[test]
    fn exception_packet_lossily_decodes_non_utf8_text() {
        let mut arena = Vec::new();
        wire::write_varint(&mut arena, 2).expect("test operation failed");
        arena.extend_from_slice(&46i32.to_le_bytes());
        wire::write_varint(&mut arena, 3).expect("test operation failed");
        arena.extend_from_slice(b"DB\xff");
        wire::write_varint(&mut arena, 4).expect("test operation failed");
        arena.extend_from_slice(b"bad\xfe");
        put_string(&mut arena, "");
        arena.push(0);

        let err = parse_response(arena, revision::DEFAULT_PROTOCOL_REVISION)
            .err()
            .expect("server exception must be Err");
        let Error::ServerError { name, message, .. } = err else {
            unreachable!("expected ServerError");
        };
        assert!(name.contains('�'));
        assert!(message.contains('�'));
    }

    // ── Exception-chain nesting-depth cap tests ──────────────────────────

    fn nested_chain_entries(levels: usize) -> Vec<(i32, &'static str, &'static str, bool)> {
        (0..levels)
            .map(|i| (60, "DB::Exception", "nested frame", i + 1 < levels))
            .collect()
    }

    #[test]
    fn exception_chain_exactly_cap_depth_parses() {
        let arena = exception_packet(&nested_chain_entries(
            crate::limits::MAX_EXCEPTION_CHAIN_DEPTH,
        ));
        let err = parse_response(arena, revision::DEFAULT_PROTOCOL_REVISION)
            .err()
            .expect("chain exactly at the cap must still parse");
        let Error::ServerError { code, message, .. } = err else {
            unreachable!("expected ServerError, got {err:?}");
        };
        assert_eq!(code, 60);
        assert_eq!(
            message.matches(" | nested: ").count(),
            crate::limits::MAX_EXCEPTION_CHAIN_DEPTH - 1,
            "every level up to the cap must be joined into the message"
        );
    }

    #[test]
    fn exception_chain_cap_plus_one_is_protocol_error() {
        let arena = exception_packet(&nested_chain_entries(
            crate::limits::MAX_EXCEPTION_CHAIN_DEPTH + 1,
        ));
        let err = parse_response(arena, revision::DEFAULT_PROTOCOL_REVISION)
            .err()
            .expect("chain deeper than the cap must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                &format!(
                    "exception nesting too deep: more than {} levels",
                    crate::limits::MAX_EXCEPTION_CHAIN_DEPTH
                )
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    /// Raw chain body (no packet-type prefix) as consumed directly by
    /// [`parse_exception_chain`] — the same entry point the fuzz target
    /// exercises via `st_clickhouse::fuzz_hooks`.
    fn chain_body(entries: &[(i32, &'static str, &'static str, bool)]) -> Vec<u8> {
        let mut body = Vec::new();
        for (code, name, msg, nested) in entries {
            body.extend_from_slice(&code.to_le_bytes());
            put_string(&mut body, name);
            put_string(&mut body, msg);
            put_string(&mut body, "");
            body.push(u8::from(*nested));
        }
        body
    }

    #[test]
    fn parse_exception_chain_direct_cap_boundary() {
        let cap = crate::limits::MAX_EXCEPTION_CHAIN_DEPTH;

        // Exactly-cap chain parses and keeps the joined message.
        let mut pos = 0;
        let (code, _, message) =
            parse_exception_chain(&chain_body(&nested_chain_entries(cap)), &mut pos)
                .expect("exactly-cap chain must parse");
        assert_eq!(code, 60);
        assert_eq!(message.matches(" | nested: ").count(), cap - 1);

        // One extra nested level (cap + 1) is rejected with the cap named.
        let mut pos = 0;
        let err = parse_exception_chain(&chain_body(&nested_chain_entries(cap + 1)), &mut pos)
            .expect_err("cap + 1 chain must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                &format!("exception nesting too deep: more than {cap} levels")
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn nested_exception_chain_reports_root_and_full_chain() {
        let arena = exception_packet(&[
            (1000, "DB::Exception", "outer failure", true),
            (48, "DB::Exception", "inner cause", false),
        ]);
        let err = parse_response(arena, revision::DEFAULT_PROTOCOL_REVISION)
            .err()
            .expect("nested exception must be Err");
        match &err {
            Error::ServerError {
                code,
                name,
                message,
            } => {
                assert_eq!(*code, 1000, "root code must be reported");
                assert_eq!(name, "DB::Exception");
                assert!(
                    message.contains("outer failure") && message.contains("inner cause"),
                    "message must carry the whole chain: {message}"
                );
            },
            _other => unreachable!("expected ServerError, got {err:?}"),
        }
    }

    #[test]
    fn truncated_exception_packet_is_a_protocol_error() {
        let mut arena = exception_packet(&[(60, "DB::Exception", "unknown function xyz", false)]);
        arena.truncate(arena.len() - 3); // cut into the has_nested flag
        let err = parse_response(arena, revision::DEFAULT_PROTOCOL_REVISION)
            .err()
            .expect("truncated packet must be Err");
        assert!(
            matches!(err, Error::Protocol(_)),
            "malformed packet must stay a protocol error, got {err:?}"
        );
    }
}
