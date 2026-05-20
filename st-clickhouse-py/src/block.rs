//! Python `_Block` and `_Column` classes — lazy column-oriented access.
//!
//! `_Block` holds the raw column data (zero-copy until accessed).
//! `_Column` provides type-aware lists on demand.
//!
//! Usage (Python):
//! ```python
//! blocks = client.query_blocks("SELECT id, name FROM users")
//! for block in blocks:
//!     ids = block["id"].to_list()       # → [1, 2, ...]
//!     names = block["name"].to_list()   # → ["Alice", ...]
//!     for row in block.rows():
//!         print(row["id"], row["name"])
//! ```

use crate::conversion;
use crate::errors::to_py_err;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use st_clickhouse::sync::column::OwnedColumnData;
use st_clickhouse::sync::protocol::block::Block;

// ══════════════════════════════════════════════════════════════════════════
// _Block
// ══════════════════════════════════════════════════════════════════════════

#[pyclass(name = "_Block", module = "st_clickhouse._native")]
pub struct PyBlock {
    pub(crate) inner: Box<Block>,
}

#[pymethods]
impl PyBlock {
    fn __len__(&self) -> usize {
        self.inner.row_count()
    }

    fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    fn column_count(&self) -> usize {
        self.inner.column_count()
    }

    fn column_names(&self) -> Vec<&str> {
        self.inner.columns.iter().map(|c| c.name.as_str()).collect()
    }

    fn column_types(&self) -> Vec<&str> {
        self.inner
            .columns
            .iter()
            .map(|c| c.type_name.as_str())
            .collect()
    }

    fn column_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for col in &self.inner.columns {
            dict.set_item(&col.name, &col.type_name)?;
        }
        Ok(dict.into())
    }

    fn __getitem__(&self, name: &str, _py: Python<'_>) -> PyResult<PyColumn> {
        let idx = self
            .inner
            .columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("column '{name}' not found"))
            })?;

        let info = &self.inner.columns[idx];
        let any = self.inner.read_column_by_index(idx).map_err(to_py_err)?;

        Ok(PyColumn {
            name: info.name.clone(),
            type_name: info.type_name.clone(),
            any: any.into_owned(),
            count: self.inner.row_count(),
        })
    }

    fn column_by_index(&self, index: usize, _py: Python<'_>) -> PyResult<PyColumn> {
        let info = self.inner.columns.get(index).ok_or_else(|| {
            pyo3::exceptions::PyIndexError::new_err(format!(
                "column index {index} out of range ({} columns)",
                self.inner.column_count()
            ))
        })?;

        let any = self.inner.read_column_by_index(index).map_err(to_py_err)?;

        Ok(PyColumn {
            name: info.name.clone(),
            type_name: info.type_name.clone(),
            any: any.into_owned(),
            count: self.inner.row_count(),
        })
    }

    fn to_dicts(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        conversion::blocks_to_py_dicts(std::slice::from_ref(self.inner.as_ref()), py)
    }

    fn to_columns(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        conversion::blocks_to_py_columns(std::slice::from_ref(self.inner.as_ref()), py)
    }

    fn rows(&self, py: Python<'_>) -> PyResult<PyRowIterator> {
        let col_count = self.inner.column_count();
        let mut col_names = Vec::with_capacity(col_count);
        let mut col_values: Vec<Vec<Py<PyAny>>> = Vec::with_capacity(col_count);

        for i in 0..col_count {
            col_names.push(self.inner.columns[i].name.clone());
            let info = &self.inner.columns[i];
            let values =
                conversion::column_to_py_list_typed(&self.inner, i, info, py).map_err(to_py_err)?;
            col_values.push(values);
        }

        Ok(PyRowIterator {
            col_names,
            col_values,
            current: 0,
            total: self.inner.row_count(),
        })
    }

    fn __repr__(&self) -> String {
        let cols: Vec<&str> = self.inner.columns.iter().map(|c| c.name.as_str()).collect();
        format!(
            "<Block rows={} cols=[{}]>",
            self.inner.row_count(),
            cols.join(", ")
        )
    }
}

// ══════════════════════════════════════════════════════════════════════════
// _Column
// ══════════════════════════════════════════════════════════════════════════

#[pyclass(name = "_Column", module = "st_clickhouse._native")]
pub struct PyColumn {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) any: OwnedColumnData,
    pub(crate) count: usize,
}

#[pymethods]
impl PyColumn {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn type_name(&self) -> &str {
        &self.type_name
    }

    fn __len__(&self) -> usize {
        self.count
    }

    fn to_list(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.any {
            OwnedColumnData::UInt(values) => Ok(PyList::new(py, values.iter().copied())?.into()),
            OwnedColumnData::Int(values) => {
                if self.type_name == "Date" {
                    Ok(PyList::new(
                        py,
                        values
                            .iter()
                            .map(|&days| conversion::date_from_days(days, py)),
                    )?
                    .into())
                } else if self.type_name.starts_with("DateTime") {
                    let scale = extract_datetime64_scale(&self.type_name).unwrap_or(0);
                    Ok(PyList::new(
                        py,
                        values
                            .iter()
                            .map(|&raw| conversion::datetime_from_timestamp(raw, scale, py)),
                    )?
                    .into())
                } else {
                    Ok(PyList::new(py, values.iter().copied())?.into())
                }
            },
            OwnedColumnData::Float(values) => Ok(PyList::new(py, values.iter().copied())?.into()),
            OwnedColumnData::String(values) => {
                Ok(PyList::new(py, values.iter().map(|s| s.as_str()))?.into())
            },
            OwnedColumnData::Bool(values) => Ok(PyList::new(py, values.iter().copied())?.into()),
            OwnedColumnData::Null(n) => Ok(PyList::new(py, (0..*n).map(|_| py.None()))?.into()),
            OwnedColumnData::Unknown => {
                Ok(PyList::new(py, (0..self.count).map(|_| py.None()))?.into())
            },
        }
    }

    fn __getitem__(&self, index: usize, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.any {
            OwnedColumnData::UInt(values) => values.get(index).copied().into_py_any(py),
            OwnedColumnData::Int(values) => values.get(index).copied().into_py_any(py),
            OwnedColumnData::Float(values) => values.get(index).copied().into_py_any(py),
            OwnedColumnData::String(values) => match values.get(index) {
                Some(value) => value.as_str().into_py_any(py),
                None => Ok(py.None()),
            },
            OwnedColumnData::Bool(values) => values.get(index).copied().into_py_any(py),
            _ => Ok(py.None()),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "<Column name={} type={} len={}>",
            self.name, self.type_name, self.count
        )
    }
}

fn extract_datetime64_scale(type_name: &str) -> Option<u32> {
    let rest = type_name.strip_prefix("DateTime64(")?;
    let scale_str = rest.strip_suffix(')')?;
    scale_str.parse::<u32>().ok()
}

// ══════════════════════════════════════════════════════════════════════════
// PyRowIterator
// ══════════════════════════════════════════════════════════════════════════

#[pyclass(name = "_RowIterator", module = "st_clickhouse._native")]
pub struct PyRowIterator {
    col_names: Vec<String>,
    col_values: Vec<Vec<Py<PyAny>>>,
    current: usize,
    total: usize,
}

#[pymethods]
impl PyRowIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if slf.current >= slf.total {
            return Ok(None);
        }
        let row = PyDict::new(py);
        for (i, name) in slf.col_names.iter().enumerate() {
            row.set_item(name, &slf.col_values[i][slf.current])?;
        }
        slf.current += 1;
        Ok(Some(row.into()))
    }
}
