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
        let count = wire::async_read_varint(stream).await?;
        let mut rules = Vec::with_capacity(count as usize);
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
    loop {
        let code = wire::async_read_i32(stream).await?;
        let name = wire::async_read_string(stream).await?;
        let msg = wire::async_read_string(stream).await?;
        let _stack = wire::async_read_string(stream).await?;
        parts.push(format!("{name} (code {code}): {msg}"));
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
