//! st-clickhouse-py bindings — native ClickHouse protocol client.
//!
//! Architecture:
//! - Rust native extension (`st_clickhouse._native`) provides low-level bindings
//! - Python package (`st_clickhouse/`) adds high-level Client, AsyncClient
//! - Core is 100% sync (st-clickhouse), async via Python asyncio.to_thread()
//!
//! Module contents:
//! - `_Client` — sync client (connects via native protocol)
//! - `_Block` — column-oriented result block
//! - `_Column` — typed column data with Python conversion
//! - `_RowIterator` — lazy row iterator
//! - `convert_blocks_to_dicts()` — utility to convert blocks
//!
//! Usage:
//! ```python
//! from st_clickhouse import Client
//!
//! # Sync
//! client = Client("127.0.0.1:9000")
//! rows = client.query("SELECT number FROM system.numbers LIMIT 5")
//!
//! # Async
//! async with AsyncClient("127.0.0.1:9000") as client:
//!     rows = await client.query("SELECT 1")
//! ```

mod block;
mod client;
mod conversion;
mod errors;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyList, PyTuple};

use block::{PyBlock, PyColumn, PyRowIterator};
use client::{PyClient, PyQueryStream};
use st_clickhouse::sync::protocol::block::Block;

// ══════════════════════════════════════════════════════════════════════════
// Python module definition
// ══════════════════════════════════════════════════════════════════════════

/// ClickHouse native protocol client — Python bindings.
///
/// Low-level Rust bindings. Usage via ``st_clickhouse.Client`` and
/// ``st_clickhouse.AsyncClient`` (Python wrappers).
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register classes
    m.add_class::<PyClient>()?;
    m.add_class::<PyBlock>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PyRowIterator>()?;
    m.add_class::<PyQueryStream>()?;

    // Register utility functions
    m.add_function(wrap_pyfunction!(blocks_to_dicts, m)?)?;
    m.add_function(wrap_pyfunction!(dicts_to_block, m)?)?;

    // Export constants
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}

/// Convert a list of Block objects to a list of row dicts.
#[pyfunction]
fn blocks_to_dicts(blocks: &Bound<'_, PyList>, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
    // Collect owned Block copies to avoid lifetime issues
    let owned: Vec<Block> = blocks
        .iter()
        .map(|item| -> PyResult<Block> {
            let py_block: PyRef<'_, PyBlock> = item.extract()?;
            Ok(py_block.inner.as_ref().clone())
        })
        .collect::<PyResult<Vec<_>>>()?;
    conversion::blocks_to_py_dicts(&owned, py)
}

/// Convert a list of dicts to a Block for INSERT.
///
/// Args:
///     rows: list[dict] — row dicts
///     columns: list[(name, type)] — column definitions
///
/// Returns:
///     _Block — ready for insert_blocks()
#[pyfunction]
fn dicts_to_block(
    rows: &Bound<'_, PyList>, columns: &Bound<'_, PyList>, py: Python<'_>,
) -> PyResult<PyBlock> {
    // Extract column info
    let mut col_info: Vec<(String, String)> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let col_def = item.cast::<PyTuple>()?;
        let name: String = col_def.get_item(0)?.extract()?;
        let typ: String = col_def.get_item(1)?.extract()?;
        col_info.push((name, typ));
    }

    // Convert Python objects to owned Rust values
    let py_rows: Vec<Py<PyAny>> = rows.iter().map(Bound::unbind).collect();

    let block = conversion::py_dicts_to_block(&py_rows, &col_info, py)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    Ok(PyBlock {
        inner: Box::new(block),
    })
}
