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
    /// Authentication failure.
    Authentication(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Compression(msg) => write!(f, "compression error: {msg}"),
            Error::Authentication(msg) => write!(f, "authentication error: {msg}"),
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

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
