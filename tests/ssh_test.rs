//! SSH-key authentication integration tests.
//!
//! Requires a ClickHouse server with SSH-key authentication configured:
//!   <users>
//!     <ssh_user>
//!       <password></password>
//!       <ssh_authentication_keys>
//!         <key>ssh-ed25519 AAAAC3... user@host</key>
//!       </ssh_authentication_keys>
//!     </ssh_user>
//!   </users>
//!
//! Set env vars:
//!   CLICKHOUSE_HOST=127.0.0.1:9000
//!   CLICKHOUSE_SSH_USER=ssh_user
//!   CLICKHOUSE_SSH_KEY_PATH=/path/to/private_key  (Ed25519 or RSA)

use st_clickhouse::Client;
use std::process::Command;

fn ssh_host() -> String {
    std::env::var("CLICKHOUSE_SSH_HOST")
        .or_else(|_| std::env::var("CLICKHOUSE_HOST"))
        .unwrap_or_else(|_| "127.0.0.1:9000".to_string())
}

fn ssh_user() -> Option<String> {
    std::env::var("CLICKHOUSE_SSH_USER").ok()
}

fn ssh_key_path() -> Option<String> {
    std::env::var("CLICKHOUSE_SSH_KEY_PATH").ok()
}

fn ssh_available() -> bool {
    ssh_user().is_some() && ssh_key_path().is_some()
}

/// Sign the challenge using `ssh-keygen -Y sign` (OpenSSH).
/// The challenge message format is: `{revision}{database}{user}{challenge}`.
fn sign_with_ssh_key(message: &[u8], key_path: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Write the message to a temp file
    let msg_path = std::env::temp_dir().join("clickhouse_ssh_challenge.bin");
    std::fs::write(&msg_path, message)?;

    // Use ssh-keygen to produce a detached signature
    let output = Command::new("ssh-keygen")
        .args([
            "-Y",
            "sign",
            "-f",
            key_path,
            "-n",
            "clickhouse",
            &msg_path.to_string_lossy(),
        ])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("ssh-keygen failed: {stderr}").into());
    }

    // The signature is written to a .sig file
    let sig_path = format!("{}.sig", msg_path.to_string_lossy());
    let sig = std::fs::read_to_string(&sig_path)?;

    // Clean up
    let _ = std::fs::remove_file(&msg_path);
    let _ = std::fs::remove_file(&sig_path);

    Ok(sig.trim().to_string())
}

/// Connect using SSH public-key authentication with an Ed25519 key.
#[tokio::test]
async fn test_ssh_auth_connect() {
    if !ssh_available() {
        eprintln!("Skipping SSH test: set CLICKHOUSE_SSH_USER and CLICKHOUSE_SSH_KEY_PATH");
        return;
    }

    let host = ssh_host();
    let user = ssh_user().expect("CLICKHOUSE_SSH_USER must be set");
    let key_path = ssh_key_path().expect("CLICKHOUSE_SSH_KEY_PATH must be set");

    let client = Client::connect_with_ssh_signer(&host, &user, move |msg: &[u8]| {
        sign_with_ssh_key(msg, &key_path).map_err(|e| format!("ssh signing failed: {e}"))
    })
    .await
    .expect("SSH auth connect should succeed");

    let rows: Vec<(u8,)> = client
        .query("SELECT 1")
        .all()
        .await
        .expect("SELECT 1 over SSH auth should succeed");
    assert_eq!(rows, vec![(1,)]);
}

/// SSH connection should fail with wrong key.
#[tokio::test]
async fn test_ssh_auth_wrong_key_fails() {
    if !ssh_available() {
        eprintln!("Skipping SSH test: set CLICKHOUSE_SSH_USER and CLICKHOUSE_SSH_KEY_PATH");
        return;
    }

    let host = ssh_host();
    let user = ssh_user().expect("CLICKHOUSE_SSH_USER must be set");

    // Use a dummy signer that returns garbage
    let result = Client::connect_with_ssh_signer(&host, &user, |_: &[u8]| {
        Ok("invalid_signature".to_string())
    })
    .await;

    assert!(result.is_err(), "SSH auth with wrong signature should fail");
}
