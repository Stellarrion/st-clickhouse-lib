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
    /// Server returned an exception for the query.
    ServerError {
        /// ClickHouse error code (e.g. 46 for `UNKNOWN_FUNCTION`).
        code: i32,
        /// Exception name (e.g. `DB::Exception`).
        name: String,
        /// Root message plus any nested exception chain.
        message: String,
    },
    /// Operation timed out (TCP connect or connection setup deadline).
    Timeout(String),
    /// Invalid client configuration (e.g. a zero `connect_timeout`).
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
            } => write!(f, "server error (code={code}, name={name}): {message}"),
            Error::Timeout(msg) => write!(f, "timeout: {msg}"),
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

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl Error {
    /// Returns `true` if the server returned an exception for the query.
    pub fn is_server_error(&self) -> bool {
        matches!(self, Error::ServerError { .. })
    }

    /// Returns `true` if a configured deadline expired (connect/setup).
    pub fn is_timeout(&self) -> bool {
        matches!(self, Error::Timeout(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_display_and_predicate() {
        let err = Error::Timeout("connect to 127.0.0.1:9000 timed out".into());
        assert!(err.is_timeout());
        assert!(!err.is_server_error());
        assert_eq!(
            err.to_string(),
            "timeout: connect to 127.0.0.1:9000 timed out"
        );
    }

    #[test]
    fn config_display_is_distinct_from_protocol() {
        let err = Error::Config("connect_timeout must be greater than zero".into());
        assert!(!err.is_timeout());
        assert_eq!(
            err.to_string(),
            "configuration error: connect_timeout must be greater than zero"
        );
    }

    #[test]
    fn server_error_display_includes_code_name_and_message() {
        let err = Error::ServerError {
            code: 60,
            name: "DB::Exception".into(),
            message: "unknown function xyz".into(),
        };
        assert!(err.is_server_error());
        assert_eq!(
            err.to_string(),
            "server error (code=60, name=DB::Exception): unknown function xyz"
        );
    }
}
