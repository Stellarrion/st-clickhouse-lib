//! Python `_Client` and `_QueryStream` classes.
//!
//! Architecture:
//! - All blocking I/O methods release the GIL
//!   during network reads/writes.
//! - `_QueryStream` uses a dedicated Rust reader thread + bounded channel
//!   (`std::sync::mpsc::sync_channel`). The reader thread holds NO Python
//!   objects, so block delivery is fully GIL-free.
//! - Python `AsyncClient.query_stream` uses ONE forwarder thread + asyncio.Queue
//!   (NOT `asyncio.to_thread` per block - that would waste thread pool slots).
//!
//! Data flow:
//! ```text
//! TCP Socket ──► Rust Reader Thread ──► mpsc Channel ──► Python Forwarder ──► asyncio.Queue ──► async for
//!   (I/O, no GIL)   (one thread)         (bounded 32)    (one thread)         (event loop)     (0 threads blocked)
//! ```
//!
//! The ONLY thread that blocks on I/O is the Rust reader thread.
//! The Python forwarder thread blocks on `rx.recv()` (GIL released during wait).
//! The async generator NEVER blocks a thread pool thread.
//!
//! Wraps `st_clickhouse::sync::client::{SyncClient, QueryStream}`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyTuple};
use rustls::pki_types::pem::PemObject;

use st_clickhouse::sync::client::{QueryStream, SyncClient};
use st_clickhouse::sync::compression::CompressionMethod;
use st_clickhouse::sync::config::ClientConfig;
use st_clickhouse::sync::protocol::block::Block;
use st_clickhouse::sync::protocol::parameters::QueryParameter;
use st_clickhouse::sync::protocol::table_status::QualifiedTableName;

use crate::block::PyBlock;
use crate::conversion;
use crate::errors::to_py_err;

// ══════════════════════════════════════════════════════════════════════════
// _Client
// ══════════════════════════════════════════════════════════════════════════

/// Synchronous ClickHouse client exposed to Python.
///
/// All I/O-bound methods release the GIL during blocking operations.
/// Use `Client` (sync) or `AsyncClient` (async, one forwarder thread) from Python.
#[pyclass(name = "_Client", module = "st_clickhouse._native")]
pub struct PyClient {
    inner: SyncClient,
    addr: String,
    user: String,
    database: String,
    query_timeout: Duration,
}

#[pymethods]
impl PyClient {
    #[new]
    #[pyo3(signature = (
        addr,
        user = "default".to_string(),
        password = "".to_string(),
        database = "".to_string(),
        settings = None,
        compression = None,
        connect_timeout = 10.0,
        query_timeout = 300.0,
        max_response_size = 268435456,
        tls = false,
        tls_domain = None,
        tls_ca_file = None,
        tls_client_cert = None,
        tls_client_key = None,
        ssh_signer = None,
        validate_schema = false,
    ))]
    #[expect(
        clippy::too_many_arguments,
        reason = "PyO3 constructor mirrors the public Python keyword API"
    )]
    fn new(
        py: Python<'_>, addr: &str, user: String, password: String, database: String,
        settings: Option<&Bound<'_, PyDict>>, compression: Option<String>, connect_timeout: f64,
        query_timeout: f64, max_response_size: usize, tls: bool, tls_domain: Option<String>,
        tls_ca_file: Option<String>, tls_client_cert: Option<String>,
        tls_client_key: Option<String>, ssh_signer: Option<Py<PyAny>>, validate_schema: bool,
    ) -> PyResult<Self> {
        // Build ClientConfig from Python constructor args
        let mut config = ClientConfig::default();
        config.max_response_size = max_response_size;
        config.user = user;
        config.password = password;
        config.database = database;
        config.connect_timeout = Duration::from_secs_f64(connect_timeout);
        config.query_timeout = Duration::from_secs_f64(query_timeout);
        config.validate_schema = validate_schema;

        // Parse addr for host:port
        if let Some((host, port_str)) = addr.rsplit_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                config.host = host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_string();
                config.port = port;
            }
        } else {
            config.host = addr.to_string();
        }

        // Apply compression
        if let Some(ref method) = compression {
            config.compression = Some(match method.to_lowercase().as_str() {
                "lz4" => CompressionMethod::Lz4,
                "zstd" => CompressionMethod::Zstd,
                other => {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "unknown compression method: '{other}'. Use 'lz4' or 'zstd'."
                    )));
                },
            });
        }

        // Apply settings
        if let Some(s) = settings {
            for item in s.iter() {
                let key: String = item.0.extract().map_err(|e| {
                    pyo3::exceptions::PyTypeError::new_err(format!("setting key: {e}"))
                })?;
                let val: String = item.1.extract().map_err(|e| {
                    pyo3::exceptions::PyTypeError::new_err(format!("setting value: {e}"))
                })?;
                config.settings.insert(key, val);
            }
        }

        // Apply TLS configuration
        if tls {
            let tls_domain = tls_domain.unwrap_or_else(|| config.host.clone());

            // Build root certificate store
            let root_store = if let Some(ca_file) = &tls_ca_file {
                let ca_pem = std::fs::read(ca_file).map_err(|e| {
                    pyo3::exceptions::PyIOError::new_err(format!(
                        "cannot read CA file {ca_file}: {e}"
                    ))
                })?;
                let mut store = rustls::RootCertStore::empty();
                for cert in rustls::pki_types::CertificateDer::pem_slice_iter(&ca_pem) {
                    let cert = cert.map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "invalid CA certificate: {e}"
                        ))
                    })?;
                    store.add(cert).map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "invalid CA certificate: {e}"
                        ))
                    })?;
                }
                store
            } else {
                rustls::RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
                }
            };
            let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

            let tls_config = if let (Some(cert_file), Some(key_file)) =
                (&tls_client_cert, &tls_client_key)
            {
                let cert_pem = std::fs::read(cert_file).map_err(|e| {
                    pyo3::exceptions::PyIOError::new_err(format!(
                        "cannot read cert file {cert_file}: {e}"
                    ))
                })?;
                let key_pem = std::fs::read(key_file).map_err(|e| {
                    pyo3::exceptions::PyIOError::new_err(format!(
                        "cannot read key file {key_file}: {e}"
                    ))
                })?;
                let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
                    rustls::pki_types::CertificateDer::pem_slice_iter(&cert_pem)
                        .collect::<Result<_, _>>()
                        .map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "invalid client cert: {e}"
                            ))
                        })?;
                let key =
                    rustls::pki_types::PrivateKeyDer::from_pem_slice(&key_pem).map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!("invalid client key: {e}"))
                    })?;
                builder.with_client_auth_cert(certs, key).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("mTLS config error: {e}"))
                })?
            } else {
                builder.with_no_client_auth()
            };
            config = config.with_tls(tls_config, &tls_domain);
        }

        if let Some(signer) = ssh_signer {
            config = config.with_ssh_signer(move |payload| {
                Python::attach(|py| {
                    let payload = PyBytes::new(py, payload);
                    let signature = signer
                        .call1(py, (payload,))
                        .map_err(|e| e.to_string())?
                        .extract::<String>(py)
                        .map_err(|e| e.to_string())?;
                    Ok(signature)
                })
            });
        }

        // Connect — release GIL during TCP handshake
        let client = py
            .detach(|| SyncClient::connect_with_config(config))
            .map_err(to_py_err)?;

        let addr_str = addr.to_string();
        Ok(PyClient {
            inner: client,
            addr: addr_str,
            user: String::new(),
            database: String::new(),
            query_timeout: Duration::from_secs(300),
        })
    }

    /// Execute a DDL/DML query (no result rows).
    /// GIL is released during the blocking network call.
    ///
    /// `settings` is a per-query overlay on the connection's session settings:
    /// it is merged into this query's packet only and never persists on the
    /// connection.
    #[pyo3(signature = (query, params = None, ignored_part_uuids = None, *, settings = None))]
    fn execute(
        &mut self, query: &str, params: Option<&Bound<'_, PyDict>>,
        ignored_part_uuids: Option<&Bound<'_, PyAny>>, settings: Option<&Bound<'_, PyDict>>,
        py: Python<'_>,
    ) -> PyResult<()> {
        let params = py_params_to_query_parameters(params)?;
        let ignored_part_uuids = py_ignored_part_uuids(ignored_part_uuids)?;
        let settings = py_settings_to_map(settings)?;
        py.detach(|| {
            self.inner
                .execute_with_params_settings_and_ignored_part_uuids(
                    query,
                    &params,
                    &settings,
                    &ignored_part_uuids,
                )
        })
        .map_err(to_py_err)
    }

    /// Execute a SELECT query. Returns list of row dicts.
    /// GIL is released during network I/O, re-acquired for Python conversion.
    ///
    /// `settings` is a per-query overlay on the connection's session settings:
    /// it is merged into this query's packet only and never persists on the
    /// connection.
    #[pyo3(signature = (query, params = None, ignored_part_uuids = None, *, settings = None))]
    fn query(
        &mut self, query: &str, params: Option<&Bound<'_, PyDict>>,
        ignored_part_uuids: Option<&Bound<'_, PyAny>>, settings: Option<&Bound<'_, PyDict>>,
        py: Python<'_>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let params = py_params_to_query_parameters(params)?;
        let ignored_part_uuids = py_ignored_part_uuids(ignored_part_uuids)?;
        let settings = py_settings_to_map(settings)?;
        let blocks = py
            .detach(|| {
                self.inner
                    .query_with_params_settings_and_ignored_part_uuids(
                        query,
                        &params,
                        &settings,
                        &ignored_part_uuids,
                    )
            })
            .map_err(to_py_err)?;
        conversion::blocks_to_py_dicts(&blocks, py)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Execute a SELECT query. Returns list of row tuples.
    /// This avoids per-row dict allocation and is faster for large row sets.
    ///
    /// `settings` is a per-query overlay on the connection's session settings:
    /// it is merged into this query's packet only and never persists on the
    /// connection.
    #[pyo3(signature = (query, params = None, ignored_part_uuids = None, *, settings = None))]
    fn query_tuples(
        &mut self, query: &str, params: Option<&Bound<'_, PyDict>>,
        ignored_part_uuids: Option<&Bound<'_, PyAny>>, settings: Option<&Bound<'_, PyDict>>,
        py: Python<'_>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let params = py_params_to_query_parameters(params)?;
        let ignored_part_uuids = py_ignored_part_uuids(ignored_part_uuids)?;
        let settings = py_settings_to_map(settings)?;
        let blocks = py
            .detach(|| {
                self.inner
                    .query_with_params_settings_and_ignored_part_uuids(
                        query,
                        &params,
                        &settings,
                        &ignored_part_uuids,
                    )
            })
            .map_err(to_py_err)?;
        conversion::blocks_to_py_tuples(&blocks, py)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Execute a SELECT query. Returns `{column_name: list[values]}`.
    /// This is the fastest fully materialized Python representation.
    ///
    /// `settings` is a per-query overlay on the connection's session settings:
    /// it is merged into this query's packet only and never persists on the
    /// connection.
    #[pyo3(signature = (query, params = None, ignored_part_uuids = None, *, settings = None))]
    fn query_columns(
        &mut self, query: &str, params: Option<&Bound<'_, PyDict>>,
        ignored_part_uuids: Option<&Bound<'_, PyAny>>, settings: Option<&Bound<'_, PyDict>>,
        py: Python<'_>,
    ) -> PyResult<Py<PyAny>> {
        let params = py_params_to_query_parameters(params)?;
        let ignored_part_uuids = py_ignored_part_uuids(ignored_part_uuids)?;
        let settings = py_settings_to_map(settings)?;
        let blocks = py
            .detach(|| {
                self.inner
                    .query_with_params_settings_and_ignored_part_uuids(
                        query,
                        &params,
                        &settings,
                        &ignored_part_uuids,
                    )
            })
            .map_err(to_py_err)?;
        conversion::blocks_to_py_column_map(&blocks, py)
    }

    /// Execute a SELECT query. Returns list of Block objects (column-oriented).
    /// GIL is released during network I/O.
    ///
    /// `settings` is a per-query overlay on the connection's session settings:
    /// it is merged into this query's packet only and never persists on the
    /// connection.
    #[pyo3(signature = (query, params = None, ignored_part_uuids = None, *, settings = None))]
    fn query_blocks(
        &mut self, query: &str, params: Option<&Bound<'_, PyDict>>,
        ignored_part_uuids: Option<&Bound<'_, PyAny>>, settings: Option<&Bound<'_, PyDict>>,
        py: Python<'_>,
    ) -> PyResult<Vec<PyBlock>> {
        let params = py_params_to_query_parameters(params)?;
        let ignored_part_uuids = py_ignored_part_uuids(ignored_part_uuids)?;
        let settings = py_settings_to_map(settings)?;
        let blocks = py
            .detach(|| {
                self.inner
                    .query_with_params_settings_and_ignored_part_uuids(
                        query,
                        &params,
                        &settings,
                        &ignored_part_uuids,
                    )
            })
            .map_err(to_py_err)?;
        Ok(blocks
            .into_iter()
            .filter(|b| b.row_count() > 0)
            .map(|b| PyBlock { inner: Box::new(b) })
            .collect())
    }

    /// Start a streaming SELECT query. Returns a `_QueryStream` iterator.
    ///
    /// Internally spawns a Rust reader thread + bounded channel.
    /// The reader thread holds NO Python objects and releases the GIL.
    /// Blocks pushed to the channel are pure Rust `Block` types.
    fn query_stream(&mut self, query: &str, py: Python<'_>) -> PyResult<PyQueryStream> {
        // Send the query packet — GIL released during TCP write
        let qs = py
            .detach(|| self.inner.start_stream(query))
            .map_err(to_py_err)?;
        PyQueryStream::start(qs)
    }

    /// Insert blocks into a table using the native protocol.
    fn insert(
        &mut self, query: &str, table_name: &str, blocks: &Bound<'_, PyList>, py: Python<'_>,
    ) -> PyResult<()> {
        // Extract blocks to owned Vec<Block> first (GIL held for Python access)
        let inner_blocks: Vec<Block> = blocks
            .iter()
            .map(|item| -> PyResult<Block> {
                let py_block: PyRef<'_, PyBlock> = item.extract()?;
                Ok(py_block.inner.as_ref().clone())
            })
            .collect::<PyResult<Vec<_>>>()?;

        // Send blocks — GIL released during I/O
        py.detach(|| -> PyResult<()> {
            self.inner.begin_insert(query).map_err(to_py_err)?;
            for block in &inner_blocks {
                self.inner.send_data(table_name, block).map_err(to_py_err)?;
            }
            self.inner.end_insert().map_err(to_py_err)
        })
    }

    /// Begin an INSERT stream.
    fn begin_insert_stream(&mut self, query: &str, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.begin_insert(query))
            .map_err(to_py_err)
    }

    /// Send a data block in an active INSERT stream.
    fn send_data(
        &mut self, table_name: &str, block: &Bound<'_, PyBlock>, py: Python<'_>,
    ) -> PyResult<()> {
        let inner = block.borrow().inner.as_ref().clone();
        py.detach(|| self.inner.send_data(table_name, &inner))
            .map_err(to_py_err)
    }

    /// End an INSERT stream.
    fn end_insert_stream(&mut self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.end_insert()).map_err(to_py_err)
    }

    /// Ping the server. Returns True on success.
    fn ping(&mut self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| self.inner.ping().map(|_| true))
            .map_err(to_py_err)
    }

    /// Cancel the currently running query.
    fn cancel(&mut self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.cancel()).map_err(to_py_err)
    }

    /// Request replication/read-only status for tables.
    fn tables_status(&mut self, tables: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let tables = py_tables_to_qualified_names(tables)?;
        let response = py
            .detach(|| self.inner.tables_status(&tables))
            .map_err(to_py_err)?;
        let out = PyDict::new(py);
        for (name, status) in response.table_states_by_id {
            let key = PyTuple::new(py, [&name.database, &name.table])?;
            let value = PyDict::new(py);
            value.set_item("is_replicated", status.is_replicated)?;
            value.set_item("absolute_delay", status.absolute_delay)?;
            value.set_item("is_readonly", status.is_readonly)?;
            out.set_item(key, value)?;
        }
        Ok(out.into())
    }

    /// Request status for one table. Returns None for missing tables.
    fn table_status(&mut self, database: &str, table: &str, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let status = py
            .detach(|| self.inner.table_status(database, table))
            .map_err(to_py_err)?;
        let Some(status) = status else {
            return Ok(py.None());
        };
        let value = PyDict::new(py);
        value.set_item("is_replicated", status.is_replicated)?;
        value.set_item("absolute_delay", status.absolute_delay)?;
        value.set_item("is_readonly", status.is_readonly)?;
        Ok(value.into())
    }

    /// Return cached `DESCRIBE TABLE` metadata.
    fn schema_for_table(&mut self, table: &str, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = py
            .detach(|| self.inner.schema_for_table(table))
            .map_err(to_py_err)?;
        py_table_schema(py, &schema)
    }

    /// Refresh cached `DESCRIBE TABLE` metadata.
    fn refresh_schema_for_table(&mut self, table: &str, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = py
            .detach(|| self.inner.refresh_schema_for_table(table))
            .map_err(to_py_err)?;
        py_table_schema(py, &schema)
    }

    fn clear_schema_cache(&mut self) {
        self.inner.clear_schema_cache();
    }

    /// Get server info as a dict. No I/O — reads cached handshake data.
    fn server_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let info = self.inner.get_server_info();
        let dict = PyDict::new(py);
        dict.set_item("name", &info.name)?;
        dict.set_item("version_major", info.major)?;
        dict.set_item("version_minor", info.minor)?;
        dict.set_item("revision", info.revision)?;
        if let Some(tz) = &info.timezone {
            dict.set_item("timezone", tz)?;
        }
        if let Some(dn) = &info.display_name {
            dict.set_item("display_name", dn)?;
        }
        Ok(dict.into())
    }

    /// Set a ClickHouse session setting at runtime.
    fn set_setting(&mut self, name: &str, value: &str) {
        self.inner.set_setting(name, value);
    }

    fn address(&self) -> &str {
        &self.addr
    }

    fn __repr__(&self) -> String {
        format!("<_Client server={} user={}>", self.addr, self.user)
    }
}

fn py_table_schema(
    py: Python<'_>, schema: &st_clickhouse::sync::schema::TableSchema,
) -> PyResult<Py<PyAny>> {
    let columns = PyList::empty(py);
    for col in &schema.columns {
        let item = PyDict::new(py);
        item.set_item("name", &col.name)?;
        item.set_item("type", &col.type_name)?;
        columns.append(item)?;
    }
    let out = PyDict::new(py);
    out.set_item("columns", columns)?;
    Ok(out.into())
}

/// Extract a per-query settings dict into owned strings.
///
/// Runs with the GIL held, before `py.detach`, so the overlay map is fully
/// owned Rust data by the time the query packet is built.
fn py_settings_to_map(settings: Option<&Bound<'_, PyDict>>) -> PyResult<HashMap<String, String>> {
    let Some(settings) = settings else {
        return Ok(HashMap::new());
    };
    let mut out = HashMap::with_capacity(settings.len());
    for item in settings.iter() {
        let key: String = item
            .0
            .extract()
            .map_err(|e| pyo3::exceptions::PyTypeError::new_err(format!("setting key: {e}")))?;
        // Values coerce like query-parameter values (bool → "0"/"1", numbers
        // → decimal text, anything else → `str()`), so the per-query dict
        // stays as permissive as the historical Python helper that
        // stringified values with `str(v)`.
        let value = if let Ok(value) = item.1.extract::<String>() {
            value
        } else if let Ok(value) = item.1.extract::<bool>() {
            if value {
                "1".to_string()
            } else {
                "0".to_string()
            }
        } else if let Ok(value) = item.1.extract::<i64>() {
            value.to_string()
        } else if let Ok(value) = item.1.extract::<u64>() {
            value.to_string()
        } else if let Ok(value) = item.1.extract::<f64>() {
            value.to_string()
        } else {
            item.1.str()?.to_str()?.to_owned()
        };
        out.insert(key, value);
    }
    Ok(out)
}

fn py_params_to_query_parameters(
    params: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<QueryParameter>> {
    let Some(params) = params else {
        return Ok(Vec::new());
    };

    params
        .iter()
        .map(|item| {
            let name: String = item.0.extract().map_err(|e| {
                pyo3::exceptions::PyTypeError::new_err(format!("parameter name: {e}"))
            })?;
            if item.1.is_none() {
                return Ok(QueryParameter::null(name));
            }
            let value = if let Ok(value) = item.1.extract::<String>() {
                value
            } else if let Ok(value) = item.1.extract::<bool>() {
                if value {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            } else if let Ok(value) = item.1.extract::<i64>() {
                value.to_string()
            } else if let Ok(value) = item.1.extract::<u64>() {
                value.to_string()
            } else if let Ok(value) = item.1.extract::<f64>() {
                value.to_string()
            } else {
                item.1.str()?.to_str()?.to_owned()
            };
            Ok(QueryParameter::new(name, value))
        })
        .collect()
}

fn py_ignored_part_uuids(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<[u8; 16]>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_none() {
        return Ok(Vec::new());
    }
    let iter = value.try_iter().map_err(|e| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "ignored_part_uuids must be an iterable of UUID strings or 16-byte values: {e}"
        ))
    })?;
    iter.map(|item| py_uuid_to_bytes(&item?)).collect()
}

fn py_tables_to_qualified_names(value: &Bound<'_, PyAny>) -> PyResult<Vec<QualifiedTableName>> {
    let iter = value.try_iter().map_err(|e| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "tables must be an iterable of (database, table) pairs: {e}"
        ))
    })?;
    iter.map(|item| {
        let item = item?;
        let (database, table): (String, String) = item.extract().map_err(|e| {
            pyo3::exceptions::PyTypeError::new_err(format!(
                "table entry must be a (database, table) pair: {e}"
            ))
        })?;
        Ok(QualifiedTableName::new(database, table))
    })
    .collect()
}

fn py_uuid_to_bytes(value: &Bound<'_, PyAny>) -> PyResult<[u8; 16]> {
    if let Ok(bytes) = value.downcast::<PyBytes>() {
        let bytes = bytes.as_bytes();
        if bytes.len() != 16 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "UUID bytes must be exactly 16 bytes, got {}",
                bytes.len()
            )));
        }
        let mut out = [0u8; 16];
        out.copy_from_slice(bytes);
        return Ok(out);
    }

    let s = value.str()?.to_str()?.to_owned();
    parse_uuid_string(&s)
}

fn parse_uuid_string(value: &str) -> PyResult<[u8; 16]> {
    let mut hex = [0u8; 32];
    let mut len = 0usize;
    for b in value.bytes() {
        if b == b'-' {
            continue;
        }
        if len >= hex.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "invalid UUID '{value}'"
            )));
        }
        hex[len] = b;
        len += 1;
    }
    if len != hex.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "invalid UUID '{value}'"
        )));
    }

    let mut out = [0u8; 16];
    for (idx, chunk) in hex.chunks_exact(2).enumerate() {
        let hi = hex_nibble(chunk[0]).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid UUID '{value}'"))
        })?;
        let lo = hex_nibble(chunk[1]).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid UUID '{value}'"))
        })?;
        out[idx] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

// ══════════════════════════════════════════════════════════════════════════
// _QueryStream — channel-based streaming iterator
// ══════════════════════════════════════════════════════════════════════════

/// Streaming query result — yields blocks one at a time via a background
/// Rust reader thread + bounded channel.
///
/// Architecture:
/// ```text
/// Python __next__ ──► rx.recv() ──► mpsc Channel (32) ──► Reader Thread ──► TCP Socket
///   (GIL released)     (blocks wait)                        (no GIL)
/// ```
///
/// Thread safety:
/// - `Mutex<Receiver<T>>` is `Send + Sync` (required by `#[pyclass]`)
/// - The reader thread owns the `SyncSender<T>` (moved in via `thread::spawn`)
/// - `Arc<AtomicBool>` is `Send + Sync` for cancellation
/// - The mutex is only locked briefly to call `recv()` on the receiver
///   (no contention — the receiver is single-consumer)
#[pyclass(name = "_QueryStream", module = "st_clickhouse._native")]
pub struct PyQueryStream {
    /// Channel receiver in `Arc<Mutex<>>` — `Arc` enables cloning for
    /// closure capture inside `py.detach()`, `Mutex` for `Sync`.
    /// Ok(None) = end-of-stream.
    rx: Arc<Mutex<Receiver<Result<Option<Block>, String>>>>,
    /// Atomic cancellation flag — set from any thread, checked by reader.
    cancel: Arc<AtomicBool>,
}

impl PyQueryStream {
    /// Create a new channel-based stream.
    ///
    /// Spawns a reader thread that reads from `qs` and sends blocks
    /// through a bounded channel (capacity 32 for backpressure).
    /// The thread exits when the stream ends, an error occurs, or `cancel()`.
    fn start(qs: QueryStream) -> PyResult<Self> {
        let (tx, rx): (
            SyncSender<Result<Option<Block>, String>>,
            Receiver<Result<Option<Block>, String>>,
        ) = sync_channel(32);
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_clone = cancel.clone();

        // Reader thread — owns QueryStream, holds NO Python objects
        thread::Builder::new()
            .name("ch-query-stream".into())
            .spawn(move || {
                let mut stream = qs;
                loop {
                    // Check cancellation before each blocking TCP read
                    if cancel_clone.load(Ordering::Relaxed) {
                        break;
                    }

                    let item = match stream.read_next_block() {
                        Ok(Some(block)) if block.row_count() == 0 => continue,
                        Ok(Some(block)) => Ok(Some(block)),
                        Ok(None) => {
                            // End of stream — signal and exit
                            let _ = tx.send(Ok(None));
                            break;
                        },
                        Err(e) => Err(e.to_string()),
                    };

                    // Send to channel (bounded — blocks on full for backpressure)
                    // If Python consumer is slow, this blocks the reader thread
                    // which stops reading from TCP, which tells the server to slow down.
                    if tx.send(item).is_err() {
                        // Receiver dropped (cancelled or GC'd) — exit
                        break;
                    }
                }
            })
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to spawn query stream reader thread: {e}"
                ))
            })?;

        Ok(PyQueryStream {
            rx: Arc::new(Mutex::new(rx)),
            cancel,
        })
    }
}

#[pymethods]
impl PyQueryStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Get next block, blocking until one is available.
    ///
    /// GIL is RELEASED during `Mutex::lock` + `Receiver::recv` — allows
    /// other Python threads to run while we wait for the reader thread.
    ///
    /// We clone the `Arc<Mutex<Receiver>>` and move the clone into the
    /// `py.detach` closure. Arc<T> is `Send` when T: Send + Sync.
    fn __next__(slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyBlock>> {
        // Clone the Arc — owned, Send, can be moved into the closure
        let rx = slf.rx.clone();

        let result = py.detach(move || -> Result<Option<Block>, String> {
            let guard = rx
                .lock()
                .map_err(|_| "query stream receiver lock poisoned".to_string())?;
            guard
                .recv()
                .map_err(|_| "query stream reader stopped".to_string())?
        });

        match result {
            Ok(Some(block)) => Ok(Some(PyBlock {
                inner: Box::new(block),
            })),
            Ok(None) => Ok(None), // PyO3 converts Ok(None) → StopIteration
            Err(e) if e == "query stream reader stopped" => Ok(None),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }

    /// Cancel the stream from Python.
    /// Sets the atomic flag — reader thread exits on next check.
    fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }

    fn __repr__(&self) -> String {
        "<_QueryStream>".to_string()
    }
}
