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
    /// An accumulating query result exceeded the configured response-size
    /// budget (`ClientConfig::max_response_size`).
    ///
    /// The cumulative decoded payload bytes of the result blocks passed the
    /// limit. The client sends `Cancel` and drains the remaining response so
    /// the single connection stays usable; if that bounded drain fails the
    /// socket is shut down instead (the next query then fails fast rather
    /// than reading the aborted response's bytes). Raise the limit, or use a
    /// streaming API (`SyncClient::start_stream` /
    /// `SyncClient::query_with_block_view`), which is not size-budgeted.
    ResponseTooLarge {
        /// The configured budget in bytes.
        limit: usize,
        /// Decoded payload bytes accumulated when the limit was exceeded.
        received: usize,
    },
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
            Error::ResponseTooLarge { limit, received } => write!(
                f,
                "response too large: decoded {received} bytes of result blocks exceeds \
                 max_response_size {limit}; raise ClientConfig::max_response_size \
                 (with_max_response_size), or use a streaming API \
                 (start_stream/query_with_block_view) which is not size-budgeted"
            ),
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

    /// Returns `true` if this is a response-size budget breach.
    pub fn is_response_too_large(&self) -> bool {
        matches!(self, Error::ResponseTooLarge { .. })
    }
}

impl Error {
    /// Build the budget-breach error from an internal
    /// [`crate::limits::ResponseBudget`] after a failed `charge`, reporting
    /// the configured limit and the decoded total at breach.
    pub(crate) fn response_budget_exceeded(budget: &crate::limits::ResponseBudget) -> Self {
        Error::ResponseTooLarge {
            limit: budget.limit(),
            received: budget.used(),
        }
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
    fn response_too_large_display_names_limit_and_remedies() {
        let err = Error::ResponseTooLarge {
            limit: 1024,
            received: 2048,
        };
        assert!(err.is_response_too_large());
        assert!(!err.is_timeout());
        let text = err.to_string();
        assert!(
            text.contains("max_response_size 1024"),
            "error must name the limit: {text}"
        );
        assert!(
            text.contains("with_max_response_size"),
            "error must say how to raise the limit: {text}"
        );
        assert!(
            text.contains("streaming API"),
            "error must point at unbudgeted streaming APIs: {text}"
        );
    }

    #[test]
    fn response_budget_exceeded_reports_limit_and_decoded_total() {
        let mut budget = crate::limits::ResponseBudget::new(16);
        budget.charge(16).expect("at cap");
        budget.charge(1).expect_err("breach");
        match Error::response_budget_exceeded(&budget) {
            Error::ResponseTooLarge { limit, received } => {
                assert_eq!(limit, 16);
                assert_eq!(received, 17);
            },
            other => unreachable!("expected ResponseTooLarge, got {other:?}"),
        }
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
