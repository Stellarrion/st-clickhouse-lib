//! ClickHouse native protocol handshake.
//!
//! Sync version using `std::io::Read + Write`. No tokio dependency.
//!
//! ## Server hello format
//!
//! ```text
//! packet_type (varint = 0)
//! name (string)
//! major (varint)
//! minor (varint)
//! revision (varint)                                      ← always
//! timezone (string)                                       ← client rev >= 54058
//! display_name (string)                                   ← client rev >= 54372
//! version_patch (varint)                                  ← client rev >= 54401
//! chunked protocol strings                                ← client rev >= 54470
//! password complexity rules                               ← client rev >= 54461
//! interserver nonce (i64 LE)                              ← client rev >= 54462
//! server settings                                         ← client rev >= 54474
//! ```
//!
//! The server tailors optional fields to the protocol revision advertised by
//! the client, capped by the server revision.

use crate::sync::config::ClientConfig;
use crate::sync::error::{Error, Result};
use crate::sync::protocol::packet::{ClientPacket, ServerPacket};
use crate::sync::protocol::revision;
use crate::sync::protocol::wire;

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/shared/ssh_auth.rs"));

/// Server information received during handshake.
#[derive(Clone, Debug)]
pub struct ServerInfo {
    pub name: String,
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    pub revision: u64,
    pub negotiated_revision: u64,
    pub timezone: Option<String>,
    pub display_name: Option<String>,
    pub server_parallel_replicas_protocol_version: u64,
    pub proto_send_chunked_srv: String,
    pub proto_recv_chunked_srv: String,
    pub use_chunked_send: bool,
    pub use_chunked_recv: bool,
    pub password_complexity_rules: Vec<(String, String)>,
    pub interserver_secret_nonce: Option<i64>,
    pub server_query_plan_serialization_version: Option<u64>,
    pub worker_cluster_function_protocol_version: u64,
}

/// Perform a ClickHouse native protocol handshake over a sync stream.
pub fn handshake(
    stream: &mut (impl std::io::Read + std::io::Write), config: &ClientConfig,
) -> Result<ServerInfo> {
    revision::validate_supported_revision(config.client_revision)
        .map_err(crate::sync::error::Error::Protocol)?;
    if config.ssh_signer.is_some()
        && config.client_revision < revision::DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION
    {
        return Err(crate::sync::error::Error::Protocol(format!(
            "SSH-key authentication requires protocol revision >= {}",
            revision::DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION
        )));
    }

    // ── Client hello ──
    let mut buf = Vec::new();
    wire::encode_varint(&mut buf, ClientPacket::Hello as u64);
    wire::write_string(&mut buf, &config.client_name)?;
    wire::write_varint(&mut buf, config.client_version_major)?;
    wire::write_varint(&mut buf, config.client_version_minor)?;
    wire::write_varint(&mut buf, config.client_revision)?;
    wire::write_string(&mut buf, &config.database)?;
    if config.ssh_signer.is_some() {
        wire::write_string(&mut buf, &ssh_auth_user(&config.user))?;
        wire::write_string(&mut buf, "")?;
    } else {
        wire::write_string(&mut buf, &config.user)?;
        wire::write_string(&mut buf, &config.password)?;
    }
    stream.write_all(&buf)?;
    stream.flush()?;

    if let Some(signer) = config.ssh_signer.as_ref() {
        perform_ssh_auth(stream, config, signer)?;
    }

    // ── Server hello ──
    let packet_type = wire::read_varint(stream)?;
    if packet_type == ServerPacket::Exception as u64 {
        return Err(crate::sync::error::Error::Authentication(
            read_exception_chain(stream)?,
        ));
    }
    if packet_type != ServerPacket::Hello as u64 {
        return Err(crate::sync::error::Error::Protocol(format!(
            "expected hello (0), got {packet_type}",
        )));
    }
    let name = wire::read_string(stream)?;
    let major = wire::read_varint(stream)?;
    let minor = wire::read_varint(stream)?;
    let server_revision = wire::read_varint(stream)?;
    let negotiated_revision = revision::effective_revision(config.client_revision, server_revision);

    let server_parallel_replicas_protocol_version = if negotiated_revision
        >= revision::DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
    {
        wire::read_varint(stream)?
    } else {
        revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION
    };

    let timezone = if negotiated_revision >= revision::DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
        Some(wire::read_string(stream)?)
    } else {
        None
    };

    let display_name =
        if negotiated_revision >= revision::DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
            Some(wire::read_string(stream)?)
        } else {
            None
        };

    let patch = if negotiated_revision >= revision::DBMS_MIN_REVISION_WITH_VERSION_PATCH {
        wire::read_varint(stream)?
    } else {
        0
    };

    let (proto_send_chunked_srv, proto_recv_chunked_srv) =
        if negotiated_revision >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
            (wire::read_string(stream)?, wire::read_string(stream)?)
        } else {
            (String::new(), String::new())
        };

    let password_complexity_rules = if negotiated_revision
        >= revision::DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES
    {
        let count = crate::limits::checked_count(
            wire::read_varint(stream)?,
            "password complexity rule",
            crate::limits::MAX_PASSWORD_COMPLEXITY_RULES,
        )
        .map_err(crate::sync::error::Error::Protocol)?;
        let mut rules = Vec::with_capacity(count);
        for _ in 0..count {
            rules.push((wire::read_string(stream)?, wire::read_string(stream)?));
        }
        rules
    } else {
        Vec::new()
    };

    let interserver_secret_nonce =
        if negotiated_revision >= revision::DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 {
            let mut buf = [0u8; 8];
            stream.read_exact(&mut buf)?;
            Some(i64::from_le_bytes(buf))
        } else {
            None
        };

    if negotiated_revision >= revision::DBMS_MIN_REVISION_WITH_SERVER_SETTINGS {
        skip_settings_strings_with_flags(stream)?;
    }

    let server_query_plan_serialization_version =
        if negotiated_revision >= revision::DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION {
            Some(wire::read_varint(stream)?)
        } else {
            None
        };

    let worker_cluster_function_protocol_version = if negotiated_revision
        >= revision::DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL
    {
        wire::read_varint(stream)?
    } else {
        revision::DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION
    };

    Ok(ServerInfo {
        name,
        major,
        minor,
        patch,
        revision: server_revision,
        negotiated_revision,
        timezone,
        display_name,
        server_parallel_replicas_protocol_version,
        proto_send_chunked_srv,
        proto_recv_chunked_srv,
        use_chunked_send: false,
        use_chunked_recv: false,
        password_complexity_rules,
        interserver_secret_nonce,
        server_query_plan_serialization_version,
        worker_cluster_function_protocol_version,
    })
}

fn perform_ssh_auth(
    stream: &mut (impl std::io::Read + std::io::Write), config: &ClientConfig, signer: &SshSigner,
) -> Result<()> {
    let mut request = Vec::with_capacity(1);
    wire::write_varint(&mut request, ClientPacket::SSHChallengeRequest as u64)?;
    stream.write_all(&request)?;
    stream.flush()?;

    let packet_type = wire::read_varint(stream)?;
    let challenge = if packet_type == ServerPacket::SSHChallenge as u64 {
        wire::read_string_bytes(stream)?
    } else if packet_type == ServerPacket::Exception as u64 {
        return Err(crate::sync::error::Error::Authentication(
            read_exception_chain(stream)?,
        ));
    } else {
        return Err(crate::sync::error::Error::Protocol(format!(
            "expected SSHChallenge or Exception packet, got {packet_type}"
        )));
    };

    let to_sign = ssh_signature_message(
        config.client_revision,
        &config.database,
        &config.user,
        &challenge,
    );
    let signature = signer(&to_sign).map_err(crate::sync::error::Error::Authentication)?;

    let mut response = Vec::with_capacity(1 + signature.len());
    wire::write_varint(&mut response, ClientPacket::SSHChallengeResponse as u64)?;
    wire::write_string(&mut response, &signature)?;
    stream.write_all(&response)?;
    stream.flush()?;
    Ok(())
}

fn read_exception_chain(stream: &mut impl std::io::Read) -> Result<String> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    loop {
        if depth >= crate::limits::MAX_EXCEPTION_CHAIN_DEPTH {
            return Err(Error::Protocol(format!(
                "exception nesting too deep: more than {} levels",
                crate::limits::MAX_EXCEPTION_CHAIN_DEPTH
            )));
        }
        let mut code_buf = [0u8; 4];
        stream.read_exact(&mut code_buf)?;
        let code = i32::from_le_bytes(code_buf);
        let name = wire::read_string(stream)?;
        let msg = wire::read_string(stream)?;
        let _stack = wire::read_string(stream)?;
        parts.push(format!("{name} (code {code}): {msg}"));
        depth += 1;
        let mut has_nested = [0u8; 1];
        stream.read_exact(&mut has_nested)?;
        if has_nested[0] == 0 {
            break;
        }
    }
    Ok(parts.join(" | nested: "))
}

fn skip_settings_strings_with_flags(
    stream: &mut (impl std::io::Read + std::io::Write),
) -> Result<()> {
    loop {
        let name = wire::read_string(stream)?;
        if name.is_empty() {
            return Ok(());
        }
        let _flags = wire::read_varint(stream)?;
        let _value = wire::read_string(stream)?;
    }
}

#[cfg(test)]
mod password_rule_limit_tests {
    use super::{ServerInfo, handshake};
    use crate::sync::config::ClientConfig;
    use crate::sync::error::{Error, Result};
    use crate::sync::protocol::wire;
    use std::io::{Read, Write};

    /// Reads a fixed payload; writes are discarded. A `Cursor` cannot be used
    /// because client-hello writes would overwrite the preloaded server bytes.
    struct ServerHelloStream<'a> {
        data: &'a [u8],
        pos: usize,
    }

    impl Read for ServerHelloStream<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.data.len() - self.pos;
            let take = buf.len().min(n);
            buf[..take].copy_from_slice(&self.data[self.pos..self.pos + take]);
            self.pos += take;
            Ok(take)
        }
    }

    impl Write for ServerHelloStream<'_> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Server Hello bytes at negotiated revision 54461 — the first revision
    /// that carries password complexity rules and the last that omits every
    /// later field, so the rules block terminates the packet.
    fn hello_with_rule_count(rule_count: u64, rules: &[(&str, &str)]) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 0).expect("test write"); // ServerPacket::Hello
        wire::write_string(&mut buf, "srv").expect("test write"); // server name
        wire::write_varint(&mut buf, 26).expect("test write"); // version major
        wire::write_varint(&mut buf, 4).expect("test write"); // version minor
        wire::write_varint(&mut buf, 54461).expect("test write"); // server revision
        wire::write_string(&mut buf, "UTC").expect("test write"); // timezone (rev >= 54058)
        wire::write_string(&mut buf, "srv").expect("test write"); // display name (rev >= 54372)
        wire::write_varint(&mut buf, 0).expect("test write"); // version patch (rev >= 54401)
        wire::write_varint(&mut buf, rule_count).expect("test write"); // rules (rev >= 54461)
        for (pattern, message) in rules {
            wire::write_string(&mut buf, pattern).expect("test write");
            wire::write_string(&mut buf, message).expect("test write");
        }
        buf
    }

    fn run_handshake(server_bytes: &[u8]) -> Result<ServerInfo> {
        let mut config = ClientConfig::default();
        config.client_revision = 54461;
        let mut stream = ServerHelloStream {
            data: server_bytes,
            pos: 0,
        };
        handshake(&mut stream, &config)
    }

    #[test]
    fn hello_rule_count_u64_max_is_protocol_error() {
        let err = run_handshake(&hello_with_rule_count(u64::MAX, &[]))
            .expect_err("u64::MAX rule count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "password complexity rule count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn hello_rule_count_cap_plus_one_is_protocol_error() {
        let err = run_handshake(&hello_with_rule_count(65_537, &[]))
            .expect_err("cap + 1 rule count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn hello_rules_within_cap_parse() {
        let info = run_handshake(&hello_with_rule_count(1, &[("min_len", "too short")]))
            .expect("rule count within cap parses");
        assert_eq!(info.password_complexity_rules.len(), 1);
        assert_eq!(info.password_complexity_rules[0].0, "min_len");
        assert_eq!(info.password_complexity_rules[0].1, "too short");
        assert_eq!(info.negotiated_revision, 54461);
    }

    /// Server bytes for an Exception packet (type 2) carrying a chain
    /// `levels` deep: per level, i32 LE code plus three length-prefixed
    /// strings and the 1-byte has_nested flag.
    fn exception_hello(levels: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        wire::write_varint(&mut buf, 2).expect("test write"); // ServerPacket::Exception
        for i in 0..levels {
            buf.extend_from_slice(&60i32.to_le_bytes());
            wire::write_string(&mut buf, "DB::Exception").expect("test write");
            wire::write_string(&mut buf, "auth failed").expect("test write");
            wire::write_string(&mut buf, "").expect("test write");
            buf.push(u8::from(i + 1 < levels));
        }
        buf
    }

    #[test]
    fn hello_exception_chain_exactly_cap_is_authentication_error() {
        let cap = crate::limits::MAX_EXCEPTION_CHAIN_DEPTH;
        let err = run_handshake(&exception_hello(cap))
            .expect_err("exception hello must fail the handshake");
        match &err {
            Error::Authentication(msg) => assert_eq!(
                msg.matches(" | nested: ").count(),
                cap - 1,
                "chain at exactly the cap must be fully reported"
            ),
            other => unreachable!("expected Authentication error, got {other:?}"),
        }
    }

    #[test]
    fn hello_exception_chain_cap_plus_one_is_protocol_error() {
        let err = run_handshake(&exception_hello(
            crate::limits::MAX_EXCEPTION_CHAIN_DEPTH + 1,
        ))
        .expect_err("chain deeper than the cap must be rejected");
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
}
