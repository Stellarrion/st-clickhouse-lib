use crate::error::Result;
use crate::protocol::packet::{ClientPacket, ServerPacket};
use crate::protocol::revision as protocol_revision;
use crate::protocol::wire;
use crate::runtime::io::{AsyncRead, AsyncReadExt, AsyncWrite};

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/shared/ssh_auth.rs"));

/// Information about the connected ClickHouse server.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub name: String,
    pub version_major: u64,
    pub version_minor: u64,
    pub version_patch: u64,
    pub revision: u64,
    pub negotiated_revision: u64,
    pub timezone: String,
    pub display_name: String,
    pub server_parallel_replicas_protocol_version: u64,
    pub proto_send_chunked_srv: String,
    pub proto_recv_chunked_srv: String,
    pub password_complexity_rules: Vec<(String, String)>,
    pub interserver_secret_nonce: Option<i64>,
    pub server_query_plan_serialization_version: Option<u64>,
    pub worker_cluster_function_protocol_version: u64,
    /// Server's chunked send mode preference (unsupported until rev >= 54470).
    pub chunked_send: String,
    /// Server's chunked recv mode preference (unsupported until rev >= 54470).
    pub chunked_recv: String,
}

/// Perform the client-server handshake.
///
/// Client sends:
///   varint(0) — ClientCode::Hello
///   string(client_name)
///   varint(version_major)
///   varint(version_minor)
///   varint(revision) — our protocol version; server uses this to decide response fields
///   string(default_database)
///   string(user)
///   string(password)
///
/// Server responds with:
///   varint(0) — ServerCode::Hello  OR  varint(2) — Exception
///   string(server_name)
///   varint(version_major)
///   varint(version_minor)
///   varint(server_revision)
///   [fields depend on min(client_revision, server_revision)]
///   [string(timezone) if client_revision >= 54058]
///   [string(display_name) if client_revision >= 54372]
///   [varint(version_patch) if client_revision >= 54401]
///   [string(chunked send/recv caps) if client_revision >= 54470]
///   [password_rules if client_revision >= 54461]
///   [i64(nonce) if client_revision >= 54462]
///   [server settings if client_revision >= 54474]
pub async fn handshake<
    S: crate::runtime::io::AsyncWrite + crate::runtime::io::AsyncRead + Unpin,
>(
    stream: &mut S, client_name: &str, revision: u64, database: &str, user: &str, password: &str,
    ssh_signer: Option<&SshSigner>,
) -> Result<ServerInfo> {
    use crate::runtime::io::AsyncWriteExt;

    protocol_revision::validate_supported_revision(revision)
        .map_err(crate::error::Error::Config)?;
    if ssh_signer.is_some()
        && revision < protocol_revision::DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION
    {
        return Err(crate::error::Error::Config(format!(
            "SSH-key authentication requires protocol revision >= {}",
            protocol_revision::DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION
        )));
    }

    // Send client hello
    let mut buf = Vec::new();
    wire::write_varint(&mut buf, ClientPacket::Hello as u64)?;
    wire::write_string(&mut buf, client_name)?;
    wire::write_varint(&mut buf, 26)?; // version_major (match server)
    wire::write_varint(&mut buf, 4)?; // version_minor
    wire::write_varint(&mut buf, revision)?;
    wire::write_string(&mut buf, database)?;
    if ssh_signer.is_some() {
        wire::write_string(&mut buf, &ssh_auth_user(user))?;
        wire::write_string(&mut buf, "")?;
    } else {
        wire::write_string(&mut buf, user)?;
        wire::write_string(&mut buf, password)?;
    }
    stream.write_all(&buf).await?;
    stream.flush().await?;

    if let Some(signer) = ssh_signer {
        perform_ssh_auth(stream, revision, database, user, signer).await?;
    }

    // Receive server hello

    let packet_type = wire::async_read_varint(stream).await?;

    if packet_type == ServerPacket::Exception as u64 {
        // Exception during handshake
        return Err(crate::error::Error::Authentication(
            read_exception_chain(stream).await?,
        ));
    }

    if packet_type != ServerPacket::Hello as u64 {
        return Err(crate::error::Error::Protocol(format!(
            "expected Hello packet, got {packet_type}"
        )));
    }

    let name = wire::async_read_string(stream).await?;
    let version_major = wire::async_read_varint(stream).await?;
    let version_minor = wire::async_read_varint(stream).await?;
    let server_revision = wire::async_read_varint(stream).await?;

    let negotiated_revision = protocol_revision::effective_revision(revision, server_revision);

    let server_parallel_replicas_protocol_version = if negotiated_revision
        >= protocol_revision::DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
    {
        wire::async_read_varint(stream).await?
    } else {
        protocol_revision::DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION
    };

    // rev >= 54058: server timezone (present at 54460)
    let timezone =
        if negotiated_revision >= protocol_revision::DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
            wire::async_read_string(stream).await?
        } else {
            String::new()
        };

    // rev >= 54372: server display name (present at 54460)
    let display_name =
        if negotiated_revision >= protocol_revision::DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
            wire::async_read_string(stream).await?
        } else {
            String::new()
        };

    // rev >= 54401: version patch (present at 54460)
    let version_patch =
        if negotiated_revision >= protocol_revision::DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            wire::async_read_varint(stream).await?
        } else {
            0
        };

    let (proto_send_chunked_srv, proto_recv_chunked_srv) = if negotiated_revision
        >= protocol_revision::DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
    {
        (
            wire::async_read_string(stream).await?,
            wire::async_read_string(stream).await?,
        )
    } else {
        (String::new(), String::new())
    };

    // rev >= 54461: password complexity rules (absent at 54460)
    let password_complexity_rules = if negotiated_revision
        >= protocol_revision::DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES
    {
        let count = crate::limits::checked_count(
            wire::async_read_varint(stream).await?,
            "password complexity rule",
            crate::limits::MAX_PASSWORD_COMPLEXITY_RULES,
        )
        .map_err(crate::error::Error::Protocol)?;
        let mut rules = Vec::with_capacity(count);
        for _ in 0..count {
            let pattern = wire::async_read_string(stream).await?;
            let message = wire::async_read_string(stream).await?;
            rules.push((pattern, message));
        }
        rules
    } else {
        Vec::new()
    };

    // rev >= 54462: inter-server secret NONCE (absent at 54460)
    let interserver_secret_nonce =
        if negotiated_revision >= protocol_revision::DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 {
            let mut buf = [0u8; 8];
            stream.read_exact(&mut buf).await?;
            Some(i64::from_le_bytes(buf))
        } else {
            None
        };

    if negotiated_revision >= protocol_revision::DBMS_MIN_REVISION_WITH_SERVER_SETTINGS {
        skip_settings_strings_with_flags(stream).await?;
    }

    let server_query_plan_serialization_version = if negotiated_revision
        >= protocol_revision::DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION
    {
        Some(wire::async_read_varint(stream).await?)
    } else {
        None
    };

    let worker_cluster_function_protocol_version = if negotiated_revision
        >= protocol_revision::DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL
    {
        wire::async_read_varint(stream).await?
    } else {
        protocol_revision::DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION
    };

    Ok(ServerInfo {
        name,
        version_major,
        version_minor,
        version_patch,
        revision: server_revision,
        negotiated_revision,
        timezone,
        display_name,
        server_parallel_replicas_protocol_version,
        proto_send_chunked_srv,
        proto_recv_chunked_srv,
        password_complexity_rules,
        interserver_secret_nonce,
        server_query_plan_serialization_version,
        worker_cluster_function_protocol_version,
        chunked_send: String::new(),
        chunked_recv: String::new(),
    })
}

async fn perform_ssh_auth<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S, revision: u64, database: &str, user: &str, signer: &SshSigner,
) -> Result<()> {
    use crate::runtime::io::AsyncWriteExt;

    let mut request = Vec::with_capacity(1);
    wire::write_varint(&mut request, ClientPacket::SSHChallengeRequest as u64)?;
    stream.write_all(&request).await?;
    stream.flush().await?;

    let packet_type = wire::async_read_varint(stream).await?;
    let challenge = if packet_type == ServerPacket::SSHChallenge as u64 {
        wire::async_read_string_bytes(stream).await?
    } else if packet_type == ServerPacket::Exception as u64 {
        return Err(crate::error::Error::Authentication(
            read_exception_chain(stream).await?,
        ));
    } else {
        return Err(crate::error::Error::Protocol(format!(
            "expected SSHChallenge or Exception packet, got {packet_type}"
        )));
    };

    let to_sign = ssh_signature_message(revision, database, user, &challenge);
    let signature = signer(&to_sign).map_err(crate::error::Error::Authentication)?;

    let mut response = Vec::with_capacity(1 + signature.len());
    wire::write_varint(&mut response, ClientPacket::SSHChallengeResponse as u64)?;
    wire::write_string(&mut response, &signature)?;
    stream.write_all(&response).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_exception_chain<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> Result<String> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    loop {
        if depth >= crate::limits::MAX_EXCEPTION_CHAIN_DEPTH {
            return Err(crate::error::Error::Protocol(format!(
                "exception nesting too deep: more than {} levels",
                crate::limits::MAX_EXCEPTION_CHAIN_DEPTH
            )));
        }
        let code = wire::async_read_i32(stream).await?;
        let name = wire::async_read_string(stream).await?;
        let msg = wire::async_read_string(stream).await?;
        let _stack = wire::async_read_string(stream).await?;
        parts.push(format!("{name} (code {code}): {msg}"));
        depth += 1;
        let mut has_nested = [0u8; 1];
        stream.read_exact(&mut has_nested).await?;
        if has_nested[0] == 0 {
            break;
        }
    }
    Ok(parts.join(" | nested: "))
}

async fn skip_settings_strings_with_flags<S: crate::runtime::io::AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<()> {
    loop {
        let name = wire::async_read_string(stream).await?;
        if name.is_empty() {
            return Ok(());
        }
        let _flags = wire::async_read_varint(stream).await?;
        let _value = wire::async_read_string(stream).await?;
    }
}

// Re-exports used by tcp.rs connection module
#[allow(dead_code)]
pub(crate) async fn read_packet_type_handshake<S: crate::runtime::io::AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<u64> {
    wire::async_read_varint(stream).await
}

#[cfg(test)]
mod password_rule_limit_tests {
    use super::{ServerInfo, handshake};
    use crate::error::{Error, Result};
    use crate::protocol::wire;
    use crate::runtime::io::AsyncWriteExt as _;

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

    async fn run_handshake(server_bytes: Vec<u8>) -> Result<ServerInfo> {
        let (mut server_end, mut client_end) = tokio::io::duplex(4096);
        server_end
            .write_all(&server_bytes)
            .await
            .expect("seed server hello");
        handshake(
            &mut client_end,
            "client",
            54461,
            "default",
            "user",
            "pass",
            None,
        )
        .await
    }

    #[tokio::test]
    async fn hello_rule_count_u64_max_is_protocol_error() {
        let err = run_handshake(hello_with_rule_count(u64::MAX, &[]))
            .await
            .expect_err("u64::MAX rule count must be rejected");
        match &err {
            Error::Protocol(msg) => assert_eq!(
                msg,
                "password complexity rule count 18446744073709551615 exceeds limit 65536"
            ),
            other => unreachable!("expected Protocol error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hello_rule_count_cap_plus_one_is_protocol_error() {
        let err = run_handshake(hello_with_rule_count(65_537, &[]))
            .await
            .expect_err("cap + 1 rule count must be rejected");
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn hello_rules_within_cap_parse() {
        let info = run_handshake(hello_with_rule_count(1, &[("min_len", "too short")]))
            .await
            .expect("rule count within cap parses");
        assert_eq!(info.password_complexity_rules.len(), 1);
        assert_eq!(info.password_complexity_rules[0].0, "min_len");
        assert_eq!(info.password_complexity_rules[0].1, "too short");
        assert_eq!(info.negotiated_revision, 54461);
    }

    /// Like [`run_handshake`] but the server payload is written by a spawned
    /// task: deep exception chains exceed the duplex buffer, so seeding must
    /// overlap with the handshake's reads instead of blocking on them.
    async fn run_handshake_spawned(server_bytes: Vec<u8>) -> Result<ServerInfo> {
        let (mut server_end, mut client_end) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let _ = server_end.write_all(&server_bytes).await;
        });
        handshake(
            &mut client_end,
            "client",
            54461,
            "default",
            "user",
            "pass",
            None,
        )
        .await
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
            wire::write_string(&mut buf, "").expect("test write"); // stack trace
            buf.push(u8::from(i + 1 < levels));
        }
        buf
    }

    #[tokio::test]
    async fn hello_exception_chain_exactly_cap_is_authentication_error() {
        let cap = crate::limits::MAX_EXCEPTION_CHAIN_DEPTH;
        let err = run_handshake_spawned(exception_hello(cap))
            .await
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

    #[tokio::test]
    async fn hello_exception_chain_cap_plus_one_is_protocol_error() {
        let err = run_handshake_spawned(exception_hello(
            crate::limits::MAX_EXCEPTION_CHAIN_DEPTH + 1,
        ))
        .await
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
