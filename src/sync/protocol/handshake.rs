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
use crate::sync::error::Result;
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
        let count = wire::read_varint(stream)? as usize;
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
    loop {
        let mut code_buf = [0u8; 4];
        stream.read_exact(&mut code_buf)?;
        let code = i32::from_le_bytes(code_buf);
        let name = wire::read_string(stream)?;
        let msg = wire::read_string(stream)?;
        let _stack = wire::read_string(stream)?;
        parts.push(format!("{name} (code {code}): {msg}"));
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
