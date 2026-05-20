/// Marker ClickHouse expects before the username for SSH-key authentication.
pub const SSH_KEY_AUTHENTICATION_MARKER: &str = " SSH KEY AUTHENTICATION ";

/// Signs the exact SSH-auth challenge payload and returns the wire signature.
pub type SshSigner =
    std::sync::Arc<dyn Fn(&[u8]) -> std::result::Result<String, String> + Send + Sync + 'static>;

pub fn ssh_auth_user(user: &str) -> String {
    let mut out = String::with_capacity(SSH_KEY_AUTHENTICATION_MARKER.len() + user.len());
    out.push_str(SSH_KEY_AUTHENTICATION_MARKER);
    out.push_str(user);
    out
}

pub fn ssh_signature_message(
    revision: u64, database: &str, user: &str, challenge: &[u8],
) -> Vec<u8> {
    let mut message = Vec::with_capacity(20 + database.len() + user.len() + challenge.len());
    message.extend_from_slice(revision.to_string().as_bytes());
    message.extend_from_slice(database.as_bytes());
    message.extend_from_slice(user.as_bytes());
    message.extend_from_slice(challenge);
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_auth_user_adds_clickhouse_marker() {
        assert_eq!(ssh_auth_user("alice"), " SSH KEY AUTHENTICATION alice");
    }

    #[test]
    fn ssh_signature_message_matches_clickhouse_order() {
        assert_eq!(
            ssh_signature_message(54483, "default", "alice", b"challenge"),
            b"54483defaultalicechallenge"
        );
    }
}
