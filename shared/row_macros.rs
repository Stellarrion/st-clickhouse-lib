macro_rules! define_row_read_all {
    ($error:path) => {
        /// Read all rows from a block.
        ///
        /// **Fast path** (tuples): pre-extracts all columns once via `AnyColumnData`,
        /// then iterates rows using `from_columns`. Column dispatch + buffer parse
        /// happens once per column, not once per row. ~50% faster for large results.
        pub fn read_all<T: Row>(block: &Block) -> Result<Vec<T>> {
            if T::COLUMN_COUNT > 0 && T::COLUMN_COUNT <= 8 {
                let n = block.row_count();
                let mut columns: Vec<AnyColumnData<'_>> = Vec::with_capacity(T::COLUMN_COUNT);
                for i in 0..T::COLUMN_COUNT {
                    let Ok(column) = block.read_column_by_index(i) else {
                        let mut rows = Vec::with_capacity(n);
                        for row in 0..n {
                            rows.push(T::from_row(block, row).map_err(|e| {
                                <$error>::Protocol(format!("decode row {row}: {e}"))
                            })?);
                        }
                        return Ok(rows);
                    };
                    columns.push(column);
                }
                let col_refs: Vec<&AnyColumnData<'_>> = columns.iter().collect();
                // Materialize via the PlainColumn bulk fast path where possible
                // (overridden per-tuple); fall back to per-row on any failure.
                match T::from_columns_collect(&col_refs, n) {
                    Ok(rows) => Ok(rows),
                    Err(_) => {
                        let mut rows = Vec::with_capacity(n);
                        for row in 0..n {
                            rows.push(T::from_row(block, row).map_err(|e| {
                                <$error>::Protocol(format!("decode row {row}: {e}"))
                            })?);
                        }
                        Ok(rows)
                    },
                }
            } else {
                let mut rows = Vec::with_capacity(block.row_count());
                for i in 0..block.row_count() {
                    rows.push(
                        T::from_row(block, i)
                            .map_err(|e| <$error>::Protocol(format!("decode row {i}: {e}")))?,
                    );
                }
                Ok(rows)
            }
        }
    };
}

macro_rules! impl_tuple_row {
    ($error:path, $n:expr, ($($T:ident),+)) => {
        impl<$($T),+> Row for ($($T,)+)
        where
            $($T: ClickHouseColumn + 'static,)+
        {
            const COLUMN_NAMES: &'static [&'static str] = &[$(stringify!($T)),+];
            const COLUMN_COUNT: usize = $n;

            #[allow(non_snake_case, unused_assignments)]
            fn from_row(block: &Block, row_index: usize) -> Result<Self> {
                let mut idx = 0usize;
                $(
                    let col: <$T as ClickHouseColumn>::ColumnData<'_> = block.column_by_index::<$T>(idx)?;
                    let $T = col.get(row_index)?;
                    idx += 1;
                )+
                Ok(($($T,)+))
            }

            #[allow(non_snake_case, unused_assignments)]
            fn from_columns(cols: &[&AnyColumnData<'_>], row_index: usize) -> Result<Self> {
                let mut idx = 0usize;
                $(
                    let col = cols.get(idx)
                        .ok_or_else(|| <$error>::Protocol(format!("col {idx} not found")))?;
                    // SAFETY: tuple Row impls request the concrete Rust type
                    // declared by the tuple field. `to_typed` validates the
                    // runtime variant, size, and alignment before copying.
                    let $T: $T = unsafe { col.to_typed::<$T>(row_index)? };
                    idx += 1;
                )+
                Ok(($($T,)+))
            }

            /// Bulk materialization: when every field is a PlainColumn over an
            /// aligned buffer, index native slices directly (no per-row TypeId
            /// dispatch). Otherwise fall back to per-row `from_columns`.
            #[allow(non_snake_case, unused_assignments)]
            fn from_columns_collect(
                cols: &[&AnyColumnData<'_>], n: usize,
            ) -> Result<Vec<Self>> {
                let mut idx = 0usize;
                $(
                    let $T: Option<&[$T]> =
                        cols.get(idx).and_then(|c| c.plain_slice::<$T>());
                    idx += 1;
                )+
                // Fast path only when every field has a native slice covering
                // all n rows. plain_slice yields these solely for PlainColumn
                // (Copy) types, so the reads below are sound bitwise copies.
                if $( $T.as_ref().map_or(false, |s| s.len() >= n) )&&+ {
                    $(
                        let $T = $T.expect("plain_slice reported Some");
                    )+
                    let mut out = Vec::with_capacity(n);
                    for i in 0..n {
                        out.push(($(
                            // SAFETY: plain_slice verified alignment + valid bit
                            // patterns; the guard above proved i < n <= len, and
                            // PlainColumn types are Copy so this is a bitwise copy.
                            unsafe { std::ptr::read($T.as_ptr().add(i)) },
                        )+));
                    }
                    Ok(out)
                } else {
                    let mut out = Vec::with_capacity(n);
                    for i in 0..n {
                        out.push(Self::from_columns(cols, i)?);
                    }
                    Ok(out)
                }
            }
        }
    };
}

macro_rules! impl_tuple_rows {
    ($error:path) => {
        impl_tuple_row!($error, 1, (T1));
        impl_tuple_row!($error, 2, (T1, T2));
        impl_tuple_row!($error, 3, (T1, T2, T3));
        impl_tuple_row!($error, 4, (T1, T2, T3, T4));
        impl_tuple_row!($error, 5, (T1, T2, T3, T4, T5));
        impl_tuple_row!($error, 6, (T1, T2, T3, T4, T5, T6));
        impl_tuple_row!($error, 7, (T1, T2, T3, T4, T5, T6, T7));
        impl_tuple_row!($error, 8, (T1, T2, T3, T4, T5, T6, T7, T8));
    };
}
