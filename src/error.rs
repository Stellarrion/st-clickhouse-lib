use std::fmt;

/// Result alias for the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for all crate operations.
#[derive(Debug)]
pub enum Error {
    /// Protocol violation (unexpected packet, invalid data).
    Protocol(String),
    /// Network or I/O error.
    Io(std::io::Error),
    /// Compression/decompression failure.
    Compression(String),
    /// Authentication failure (invalid credentials).
    Authentication(String),
    /// Server returned an exception.
    ServerError {
        code: i32,
        name: String,
        message: String,
    },
    /// Operation timed out (connect, query, receive).
    Timeout(String),
    /// Connection was closed or lost.
    ConnectionClosed(String),
    /// Configuration error (invalid address, missing feature).
    Config(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Compression(msg) => write!(f, "compression error: {msg}"),
            Error::Authentication(msg) => write!(f, "authentication error: {msg}"),
            Error::ServerError {
                code,
                name,
                message,
            } => {
                write!(f, "server error (code={code}, name={name}): {message}")
            },
            Error::Timeout(msg) => write!(f, "timeout: {msg}"),
            Error::ConnectionClosed(msg) => write!(f, "connection closed: {msg}"),
            Error::Config(msg) => write!(f, "configuration error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl Error {
    /// Returns `true` if this is a timeout error.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Error::Timeout(_))
    }

    /// Returns `true` if this is a server exception.
    pub fn is_server_error(&self) -> bool {
        matches!(self, Error::ServerError { .. })
    }

    /// Returns `true` if this is an auth failure.
    pub fn is_authentication_error(&self) -> bool {
        matches!(self, Error::Authentication(_))
    }

    /// Returns `true` if this is a connection-related error.
    pub fn is_connection_error(&self) -> bool {
        matches!(
            self,
            Error::ConnectionClosed(_) | Error::Timeout(_) | Error::Io(_)
        )
    }

    /// Returns `true` if the error is retryable (connection issues, timeouts).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Io(_) | Error::Timeout(_) | Error::ConnectionClosed(_) | Error::Protocol(_)
        )
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
