// Shared block helpers expect Error, Result, column dispatch, and parse_type in scope.

/// Raw native block payload returned by `query_raw`.
///
/// `data` contains the native block body after the Data packet table name:
/// BlockInfo, column and row counts, column names/types, custom serialization
/// flags, and column bytes exactly as read from the server stream.
#[derive(Debug, Clone)]
pub struct RawBlock {
    pub table_name: String,
    pub columns: usize,
    pub rows: usize,
    pub data: bytes::Bytes,
}

impl RawBlock {
    /// Decoded payload bytes of this raw block (the native block body
    /// length). The unit of the cumulative response budget for raw-capture
    /// APIs (`query_raw` / `fetch::<RawBlocks>()`), mirroring
    /// [`Block::payload_bytes`].
    pub fn payload_bytes(&self) -> usize {
        self.data.len()
    }
}

/// Minimal decoded block structure.
///
/// The actual column data is stored in `columns` and accessed
/// via `Block::column::<T>()`.
///
/// Column data uses `bytes::Bytes` (reference-counted shared buffer) for
/// zero-copy slicing from the decompression buffer. When decompressed data
/// is stored as a single `Bytes` allocation, each column is a `Bytes::slice()`
/// that shares the same allocation without copying.
#[derive(Clone)]
#[repr(align(64))]
pub struct Block {
    pub columns: Vec<ColumnInfo>,
    pub rows: usize,
}

#[derive(Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    /// Raw data buffer for this column, sliced from the decompression buffer.
    /// Uses `bytes::Bytes` for zero-copy sharing of the underlying allocation.
    pub data: bytes::Bytes,
    /// Pre-materialized data for LowCardinality columns. Empty for non-LC columns
    /// or if materialization failed. When non-empty, this contains the inner type's
    /// wire-format bytes and should be used instead of `data` for reading.
    pub lc_materialized: bytes::Bytes,
}

/// Borrowed view of one column in a native block.
///
/// This is used by streaming visitor APIs that want to inspect or count block
/// data without constructing owned [`Block`] and [`ColumnInfo`] values.
#[derive(Debug, Clone, Copy)]
pub struct ColumnView<'a> {
    pub name: &'a str,
    pub type_name: &'a str,
    pub data: &'a [u8],
}

/// Borrowed view of a native block.
#[derive(Debug, Clone, Copy)]
pub struct BlockView<'a> {
    pub columns: &'a [ColumnView<'a>],
    pub rows: usize,
}

impl BlockView<'_> {
    pub fn row_count(&self) -> usize {
        self.rows
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

impl Block {
    pub fn empty() -> Self {
        Block {
            columns: Vec::new(),
            rows: 0,
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get a column by index (0-based).
    pub fn column_by_index<T: ClickHouseColumn>(&self, index: usize) -> Result<T::ColumnData<'_>> {
        let info = self.columns.get(index).ok_or_else(|| {
            Error::Protocol(format!("column at index {index} not found"))
        })?;
        let buf = self.column_buf(info);
        let mut ctx = ReadColumnContext {
            rows: self.rows,
            pos: 0,
            buf,
        };
        T::read_column(&mut ctx)
    }

    /// Get a column by name and attempt to decode it as the requested type.
    ///
    /// For fixed-size types (`u64`, `i32`, etc.), this returns a zero-copy
    /// view into the block's data buffer. No allocation.
    pub fn column<T: ClickHouseColumn>(&self, name: &str) -> Result<T::ColumnData<'_>> {
        let info = self
            .columns
            .iter()
            .find(|c| c.name == name)
            .ok_or_else(|| {
                Error::Protocol(format!("column '{name}' not found"))
            })?;

        let buf = self.column_buf(info);
        let mut ctx = ReadColumnContext {
            rows: self.rows,
            pos: 0,
            buf,
        };

        T::read_column(&mut ctx)
    }

    /// Decode a `Variant(...)` column with row-level type information.
    pub fn variant_column(&self, name: &str) -> Result<VariantColumnData> {
        let info = self
            .columns
            .iter()
            .find(|c| c.name == name)
            .ok_or_else(|| {
                Error::Protocol(format!("column '{name}' not found"))
            })?;
        VariantColumnData::read_native(
            &info.type_name,
            self.rows,
            self.column_buf(info),
        )
    }

    /// Decode a `Dynamic` column with row-level type information.
    pub fn dynamic_column(&self, name: &str) -> Result<DynamicColumnData> {
        let info = self
            .columns
            .iter()
            .find(|c| c.name == name)
            .ok_or_else(|| {
                Error::Protocol(format!("column '{name}' not found"))
            })?;
        if info.type_name != "Dynamic" {
            return Err(Error::Protocol(format!(
                "expected Dynamic column '{name}', got {}",
                info.type_name
            )));
        }
        DynamicColumnData::read_native(self.rows, self.column_buf(info))
    }

    /// Read a column by name with runtime type dispatch.
    ///
    /// The column type is parsed from the server's type string, and the
    /// appropriate decoding is chosen at runtime. This is the dynamic
    /// counterpart to `column::<T>()`.
    /// Read column data by index, returning an `AnyColumnData` for runtime dispatch.
    pub fn read_column_by_index(&self, index: usize) -> Result<AnyColumnData<'_>> {
        let info = self.columns.get(index).ok_or_else(|| {
            Error::Protocol(format!("column at index {index} not found"))
        })?;
        let buf = self.column_buf(info);
        let mut ctx = ReadColumnContext {
            rows: self.rows,
            pos: 0,
            buf,
        };
        read_column_by_type(
            &parse_type(&info.type_name).map_err(|e| {
                Error::Protocol(format!("bad type '{}': {e}", info.type_name))
            })?,
            &mut ctx,
        )
    }

    /// Read a column by name, returning an `AnyColumnData` for runtime dispatch.
    pub fn read_column_by_name(&self, name: &str) -> Result<AnyColumnData<'_>> {
        let info = self
            .columns
            .iter()
            .find(|c| c.name == name)
            .ok_or_else(|| {
                Error::Protocol(format!("column '{name}' not found"))
            })?;

        let ct = parse_type(&info.type_name).map_err(|e| {
            Error::Protocol(format!(
                "cannot parse type '{}': {e}",
                info.type_name
            ))
        })?;

        let buf = self.column_buf(info);
        let mut ctx = ReadColumnContext {
            rows: self.rows,
            pos: 0,
            buf,
        };

        read_column_by_type(&ct, &mut ctx)
    }

    /// Return the buffer to use for reading a column. For LowCardinality
    /// columns with pre-materialized data, returns the materialized buffer;
    /// otherwise returns the raw data.
    fn column_buf<'a>(&'a self, info: &'a ColumnInfo) -> &'a [u8] {
        if info.type_name.starts_with("LowCardinality(") && !info.lc_materialized.is_empty() {
            &info.lc_materialized
        } else {
            &info.data
        }
    }

    /// Decoded payload bytes of this block: the sum of every column's data
    /// buffer length plus any pre-materialized LowCardinality buffer.
    ///
    /// This is the unit of the client's cumulative response budget
    /// (`max_response_size`): accumulating query APIs add each retained
    /// block's `payload_bytes()` to the budget and fail with a
    /// response-too-large error once it passes the configured limit.
    /// Streaming APIs do not budget blocks.
    pub fn payload_bytes(&self) -> usize {
        self.columns
            .iter()
            .map(|c| c.data.len() + c.lc_materialized.len())
            .sum()
    }

    /// Convert the block to fully owned data (copies column buffers).
    /// Use this when the block needs to outlive the decompression buffer.
    pub fn to_owned(&self) -> Self {
        Block {
            columns: self
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    type_name: c.type_name.clone(),
                    data: bytes::Bytes::copy_from_slice(&c.data),
                    lc_materialized: if !c.lc_materialized.is_empty() {
                        bytes::Bytes::copy_from_slice(&c.lc_materialized)
                    } else {
                        bytes::Bytes::new()
                    },
                })
                .collect(),
            rows: self.rows,
        }
    }
}

// ───────────────────────────────────────────────
// ReadColumnContext — zero-copy buffer reader
// ───────────────────────────────────────────────

/// A read context into a decompressed block buffer.
///
/// Tracks position and provides zero-copy `read_exact()` that returns
/// slices into the underlying buffer.
pub struct ReadColumnContext<'a> {
    pub rows: usize,
    pub(crate) pos: usize,
    pub(crate) buf: &'a [u8],
}

impl<'a> ReadColumnContext<'a> {
    pub fn new(rows: usize, buf: &'a [u8]) -> Self {
        Self { rows, pos: 0, buf }
    }

    /// Read exactly `n` bytes from the buffer. Returns a view — zero copy.
    pub fn read_exact(&mut self, n: usize) -> Result<&'a [u8]> {
        let start = self.pos;
        let end = start.checked_add(n).ok_or_else(|| {
            Error::Protocol(format!(
                "ReadColumnContext: requested {n} bytes at offset {start}, length overflow"
            ))
        })?;
        if end > self.buf.len() {
            return Err(Error::Protocol(format!(
                "ReadColumnContext: requested {n} bytes at offset {start}, buffer len {}",
                self.buf.len()
            )));
        }
        self.pos = end;
        Ok(&self.buf[start..end])
    }

    /// Read `rows * width` bytes with checked length arithmetic.
    pub fn read_rows_bytes(&mut self, width: usize) -> Result<&'a [u8]> {
        let n = self.rows.checked_mul(width).ok_or_else(|| {
            Error::Protocol(
                "ReadColumnContext: row byte length overflow".into(),
            )
        })?;
        self.read_exact(n)
    }

    /// Read ClickHouse Array/Map offsets as aligned owned integers.
    pub fn read_offsets(&mut self) -> Result<Vec<u64>> {
        let rows = self.rows;
        if rows == 0 {
            return Ok(Vec::new());
        }
        let bytes = self.read_rows_bytes(8)?;
        Ok(bytes
            .chunks_exact(8)
            .map(|c| {
                let mut offset = [0u8; 8];
                offset.copy_from_slice(c);
                u64::from_le_bytes(offset)
            })
            .collect())
    }
}
