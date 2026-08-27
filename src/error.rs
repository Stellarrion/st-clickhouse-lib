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
    /// A pool slot could not be acquired within `acquire_timeout`.
    PoolTimeout(String),
    /// An accumulating query result exceeded the configured response-size
    /// budget (`max_response_size`).
    ///
    /// The cumulative decoded payload bytes of the result blocks passed the
    /// limit set by `Client::with_max_response_size` (async engine; the sync
    /// engine reads it from `ClientConfig::max_response_size`). The read stops
    /// at a block boundary and the mid-response socket is discarded; the next
    /// query on the pool reconnects. Raise the limit, or switch to a
    /// streaming API (`Client::query(..).rows()` / `BlockStream`), which is
    /// not size-budgeted.
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
            } => {
                write!(f, "server error (code={code}, name={name}): {message}")
            },
            Error::Timeout(msg) => write!(f, "timeout: {msg}"),
            Error::ConnectionClosed(msg) => write!(f, "connection closed: {msg}"),
            Error::Config(msg) => write!(f, "configuration error: {msg}"),
            Error::PoolTimeout(msg) => write!(f, "pool acquire timeout: {msg}"),
            Error::ResponseTooLarge { limit, received } => write!(
                f,
                "response too large: decoded {received} bytes of result blocks exceeds \
                 max_response_size {limit}; raise Client::with_max_response_size, or use a \
                 streaming API (rows()/BlockStream) which is not size-budgeted"
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

impl Error {
    /// Returns `true` if this is a timeout error.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Error::Timeout(_))
    }

    /// Returns `true` if this is a pool-acquire timeout.
    pub fn is_pool_timeout(&self) -> bool {
        matches!(self, Error::PoolTimeout(_))
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

    /// Returns `true` if the socket must not be reused.
    ///
    /// Query timeouts are connection-fatal too: cancellation is bounded and a
    /// partially drained response must never return to the pool. A
    /// response-too-large breach aborts the read mid-response, so that socket
    /// is discarded as well.
    pub fn is_broken_connection(&self) -> bool {
        matches!(
            self,
            Error::ConnectionClosed(_)
                | Error::Io(_)
                | Error::Timeout(_)
                | Error::ResponseTooLarge { .. }
        )
    }

    /// Returns `true` if the error is retryable (connection issues, timeouts).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Io(_) | Error::Timeout(_) | Error::ConnectionClosed(_) | Error::PoolTimeout(_)
        )
    }
}

impl Error {
    /// Build the budget-breach error from an internal
    /// [`crate::limits::ResponseBudget`] after a failed `charge`, reporting
    /// the configured limit and the decoded total at breach.
    #[cfg_attr(not(feature = "tokio"), expect(dead_code))]
    pub(crate) fn response_budget_exceeded(budget: &crate::limits::ResponseBudget) -> Self {
        Error::ResponseTooLarge {
            limit: budget.limit(),
            received: budget.used(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_timeout_is_pool_timeout_only() {
        assert!(Error::PoolTimeout("no slot".into()).is_pool_timeout());
        assert!(!Error::Timeout("query".into()).is_pool_timeout());
        assert!(!Error::ConnectionClosed("x".into()).is_pool_timeout());
    }

    #[test]
    fn broken_connection_includes_timeouts() {
        assert!(
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset"
            ))
            .is_broken_connection()
        );
        assert!(Error::ConnectionClosed("server closed".into()).is_broken_connection());
        assert!(Error::Timeout("query exceeded deadline".into()).is_broken_connection());
        assert!(
            Error::ResponseTooLarge {
                limit: 16,
                received: 17
            }
            .is_broken_connection(),
            "a mid-response budget breach must discard the socket"
        );
    }

    #[test]
    fn response_too_large_is_not_retried_and_names_the_limit() {
        let e = Error::ResponseTooLarge {
            limit: 1024,
            received: 2048,
        };
        assert!(
            !e.is_retryable(),
            "a deterministic budget breach must not be retried"
        );
        assert!(!e.is_timeout());
        assert!(
            e.to_string().contains("max_response_size 1024"),
            "error must name the limit: {e}"
        );
        assert!(
            e.to_string().contains("with_max_response_size"),
            "error must say how to raise the limit: {e}"
        );
    }

    #[test]
    fn response_budget_exceeded_reports_limit_and_decoded_total() {
        let mut budget = crate::limits::ResponseBudget::new(1024);
        budget.charge(600).expect("within budget");
        budget.charge(600).expect_err("breach");
        let e = Error::response_budget_exceeded(&budget);
        match e {
            Error::ResponseTooLarge { limit, received } => {
                assert_eq!(limit, 1024);
                assert_eq!(received, 1200);
            },
            other => unreachable!("expected ResponseTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn pool_timeout_is_retryable_but_not_timeout() {
        let e = Error::PoolTimeout("no slot".into());
        assert!(e.is_retryable(), "PoolTimeout must stay retryable");
        assert!(!e.is_timeout(), "PoolTimeout must NOT match is_timeout");
    }

    #[test]
    fn protocol_errors_are_not_retried() {
        assert!(!Error::Protocol("deterministic decode failure".into()).is_retryable());
    }

    #[test]
    fn pool_timeout_display() {
        assert_eq!(
            Error::PoolTimeout("no slot".to_owned()).to_string(),
            "pool acquire timeout: no slot"
        );
    }
}
