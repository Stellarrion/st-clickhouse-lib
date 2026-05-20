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

macro_rules! read_any_simple_columns {
    ($ct:expr, $ctx:expr; $( $variant:pat => $ty:ty => $any:ident ),+ $(,)?) => {
        $(
            if matches!($ct, $variant) {
                return <$ty>::read_column($ctx).map(AnyColumnData::$any);
            }
        )+
    };
}

