macro_rules! try_any_typed_columns {
    ($self:expr, $tid:expr, $row_index:expr; $( $variant:ident => $ty:ty ),+ $(,)?) => {
        $(
            if $tid == std::any::TypeId::of::<$ty>() {
                if let Self::$variant(col) = $self {
                    return col
                        .get($row_index)
                        .and_then(|v| unsafe { copy_value_checked::<T, $ty>(v) });
                }
            }
        )+
    };
}

/// Mirror of `try_any_typed_columns!` that yields the whole native `&[T]`
/// slice when the variant holds PlainColumn values of the requested type in
/// an aligned buffer. Returns `None` for non-PlainColumn / misaligned columns,
/// letting the caller fall back to per-row extraction.
macro_rules! try_any_plain_slice {
    ($self:expr, $tid:expr; $( $variant:ident => $ty:ty ),+ $(,)?) => {
        $(
            if $tid == std::any::TypeId::of::<$ty>() {
                if let Self::$variant(col) = $self {
                    if let Some(slice) = col.as_slice() {
                        // SAFETY: the TypeId check proves T == $ty; `as_slice`
                        // verified alignment and `PlainColumn` guarantees every
                        // bit pattern is a valid `$ty`, so the buffer is a
                        // sound `&[$ty]` reinterpreted as `&[T]`.
                        return Some(unsafe {
                            std::slice::from_raw_parts(
                                slice.as_ptr() as *const T,
                                slice.len(),
                            )
                        });
                    }
                }
            }
        )+
    };
}

macro_rules! read_any_simple_columns {
    ($ct:expr, $ctx:expr; $( $variant:pat => $ty:ty => $any:ident ),+ $(,)?) => {
        $(
            if matches!($ct, $variant) {
                return <$ty>::read_column($ctx).map(AnyColumnData::$any);
            }
        )+
    };
}
