//! Error type mapping: st-clickhouse errors → Python exceptions.

use pyo3::PyErr;
use pyo3::exceptions::{PyConnectionError, PyValueError};

/// Map a Rust Error to the appropriate Python PyErr.
pub fn to_py_err(err: st_clickhouse::sync::error::Error) -> PyErr {
    match &err {
        st_clickhouse::sync::error::Error::Protocol(msg) => {
            // Protocol violations → ValueError (invalid data format)
            PyValueError::new_err(format!("ClickHouse protocol error: {msg}"))
        },
        st_clickhouse::sync::error::Error::Io(e) => {
            // I/O errors → ConnectionError
            PyConnectionError::new_err(format!("I/O error: {e}"))
        },
        st_clickhouse::sync::error::Error::Compression(msg) => {
            // Compression errors → ValueError
            PyValueError::new_err(format!("ClickHouse compression error: {msg}"))
        },
        st_clickhouse::sync::error::Error::Authentication(msg) => {
            // Auth errors → ConnectionError with clear message
            PyConnectionError::new_err(format!("ClickHouse authentication error: {msg}"))
        },
        st_clickhouse::sync::error::Error::ServerError {
            code,
            name,
            message,
        } => {
            // Server exceptions → ValueError whose text `_errors.map_error`
            // recognises as a query failure (QueryError on the Python side).
            PyValueError::new_err(format!(
                "ClickHouse server error (code={code}, name={name}): {message}"
            ))
        },
    }
}
