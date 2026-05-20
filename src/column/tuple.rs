use super::super::error::Result;
use super::super::protocol::block::ReadColumnContext;
use super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};

/// Untyped Tuple column data — raw byte slices for each element.
/// Used by [`super::AnyColumnData::Tuple`] for runtime-dispatched access.
#[derive(Debug, Clone)]
pub struct RawTupleColumnData<'a> {
    pub elements: Vec<&'a [u8]>,
}

// ───────────────────────────────────────────────
// Tuple(T1, T2) — wire format: [T1 data][T2 data] with NO offsets
// ───────────────────────────────────────────────

pub struct Tuple2Data<'a, T1: ClickHouseColumn + 'a, T2: ClickHouseColumn + 'a> {
    col1: T1::ColumnData<'a>,
    col2: T2::ColumnData<'a>,
    rows: usize,
}

impl<'a, T1, T2> ClickHouseColumnData<'a, (T1, T2)> for Tuple2Data<'a, T1, T2>
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T1::ColumnData<'a>: ClickHouseColumnData<'a, T1>,
    T2::ColumnData<'a>: ClickHouseColumnData<'a, T2>,
{
    fn len(&self) -> usize {
        self.rows
    }
    fn get(&self, index: usize) -> Result<(T1, T2)> {
        Ok((self.col1.get(index)?, self.col2.get(index)?))
    }
}

impl<T1: ClickHouseValue, T2: ClickHouseValue> ClickHouseValue for (T1, T2) {
    fn ch_type_name() -> &'static str {
        "Tuple"
    }
    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::error::Error::Protocol(
            "tuple single read not supported".into(),
        ))
    }
    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Ok(())
    }
}

impl<T1, T2> ClickHouseColumn for (T1, T2)
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T1: ClickHouseValue,
    T2: ClickHouseValue,
{
    type ColumnData<'a>
        = Tuple2Data<'a, T1, T2>
    where
        T1: 'a,
        T2: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        let col1 = T1::read_column(ctx)?;
        let col2 = T2::read_column(ctx)?;
        Ok(Tuple2Data { col1, col2, rows })
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for (t1, _) in data {
            t1.write_to(buf)?;
        }
        for (_, t2) in data {
            t2.write_to(buf)?;
        }
        Ok(())
    }
}

// ───────────────────────────────────────────────
// Tuple(T1, T2, T3)
// ───────────────────────────────────────────────

pub struct Tuple3Data<
    'a,
    T1: ClickHouseColumn + 'a,
    T2: ClickHouseColumn + 'a,
    T3: ClickHouseColumn + 'a,
> {
    col1: T1::ColumnData<'a>,
    col2: T2::ColumnData<'a>,
    col3: T3::ColumnData<'a>,
    rows: usize,
}

impl<'a, T1, T2, T3> ClickHouseColumnData<'a, (T1, T2, T3)> for Tuple3Data<'a, T1, T2, T3>
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T1::ColumnData<'a>: ClickHouseColumnData<'a, T1>,
    T2::ColumnData<'a>: ClickHouseColumnData<'a, T2>,
    T3::ColumnData<'a>: ClickHouseColumnData<'a, T3>,
{
    fn len(&self) -> usize {
        self.rows
    }
    fn get(&self, index: usize) -> Result<(T1, T2, T3)> {
        Ok((
            self.col1.get(index)?,
            self.col2.get(index)?,
            self.col3.get(index)?,
        ))
    }
}

impl<T1: ClickHouseValue, T2: ClickHouseValue, T3: ClickHouseValue> ClickHouseValue
    for (T1, T2, T3)
{
    fn ch_type_name() -> &'static str {
        "Tuple"
    }
    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::error::Error::Protocol(
            "tuple single read not supported".into(),
        ))
    }
    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Ok(())
    }
}

impl<T1: ClickHouseValue, T2: ClickHouseValue, T3: ClickHouseValue, T4: ClickHouseValue>
    ClickHouseValue for (T1, T2, T3, T4)
{
    fn ch_type_name() -> &'static str {
        "Tuple"
    }
    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::error::Error::Protocol(
            "tuple single read not supported".into(),
        ))
    }
    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Ok(())
    }
}

impl<T1, T2, T3> ClickHouseColumn for (T1, T2, T3)
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
{
    type ColumnData<'a>
        = Tuple3Data<'a, T1, T2, T3>
    where
        T1: 'a,
        T2: 'a,
        T3: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        let col1 = T1::read_column(ctx)?;
        let col2 = T2::read_column(ctx)?;
        let col3 = T3::read_column(ctx)?;
        Ok(Tuple3Data {
            col1,
            col2,
            col3,
            rows,
        })
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for (t1, _, _) in data {
            t1.write_to(buf)?;
        }
        for (_, t2, _) in data {
            t2.write_to(buf)?;
        }
        for (_, _, t3) in data {
            t3.write_to(buf)?;
        }
        Ok(())
    }
}

impl<T1, T2, T3, T4> ClickHouseColumn for (T1, T2, T3, T4)
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
    T4: ClickHouseValue,
{
    type ColumnData<'a>
        = Tuple4Data<'a, T1, T2, T3, T4>
    where
        T1: 'a,
        T2: 'a,
        T3: 'a,
        T4: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        let col1 = T1::read_column(ctx)?;
        let col2 = T2::read_column(ctx)?;
        let col3 = T3::read_column(ctx)?;
        let col4 = T4::read_column(ctx)?;
        Ok(Tuple4Data {
            col1,
            col2,
            col3,
            col4,
            rows,
        })
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for (t1, _, _, _) in data {
            t1.write_to(buf)?;
        }
        for (_, t2, _, _) in data {
            t2.write_to(buf)?;
        }
        for (_, _, t3, _) in data {
            t3.write_to(buf)?;
        }
        for (_, _, _, t4) in data {
            t4.write_to(buf)?;
        }
        Ok(())
    }
}

// ───────────────────────────────────────────────
// Tuple(T1, T2, T3, T4)
// ───────────────────────────────────────────────

pub struct Tuple4Data<
    'a,
    T1: ClickHouseColumn + 'a,
    T2: ClickHouseColumn + 'a,
    T3: ClickHouseColumn + 'a,
    T4: ClickHouseColumn + 'a,
> {
    col1: T1::ColumnData<'a>,
    col2: T2::ColumnData<'a>,
    col3: T3::ColumnData<'a>,
    col4: T4::ColumnData<'a>,
    rows: usize,
}

impl<'a, T1, T2, T3, T4> ClickHouseColumnData<'a, (T1, T2, T3, T4)>
    for Tuple4Data<'a, T1, T2, T3, T4>
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T1::ColumnData<'a>: ClickHouseColumnData<'a, T1>,
    T2::ColumnData<'a>: ClickHouseColumnData<'a, T2>,
    T3::ColumnData<'a>: ClickHouseColumnData<'a, T3>,
    T4::ColumnData<'a>: ClickHouseColumnData<'a, T4>,
{
    fn len(&self) -> usize {
        self.rows
    }
    fn get(&self, index: usize) -> Result<(T1, T2, T3, T4)> {
        Ok((
            self.col1.get(index)?,
            self.col2.get(index)?,
            self.col3.get(index)?,
            self.col4.get(index)?,
        ))
    }
}

// ───────────────────────────────────────────────
// Tuple(T1, T2, T3, T4, T5)
// ───────────────────────────────────────────────

pub struct Tuple5Data<
    'a,
    T1: ClickHouseColumn + 'a,
    T2: ClickHouseColumn + 'a,
    T3: ClickHouseColumn + 'a,
    T4: ClickHouseColumn + 'a,
    T5: ClickHouseColumn + 'a,
> {
    col1: T1::ColumnData<'a>,
    col2: T2::ColumnData<'a>,
    col3: T3::ColumnData<'a>,
    col4: T4::ColumnData<'a>,
    col5: T5::ColumnData<'a>,
    rows: usize,
}

impl<'a, T1, T2, T3, T4, T5> ClickHouseColumnData<'a, (T1, T2, T3, T4, T5)>
    for Tuple5Data<'a, T1, T2, T3, T4, T5>
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T5: ClickHouseColumn,
    T1::ColumnData<'a>: ClickHouseColumnData<'a, T1>,
    T2::ColumnData<'a>: ClickHouseColumnData<'a, T2>,
    T3::ColumnData<'a>: ClickHouseColumnData<'a, T3>,
    T4::ColumnData<'a>: ClickHouseColumnData<'a, T4>,
    T5::ColumnData<'a>: ClickHouseColumnData<'a, T5>,
{
    fn len(&self) -> usize {
        self.rows
    }
    fn get(&self, index: usize) -> Result<(T1, T2, T3, T4, T5)> {
        Ok((
            self.col1.get(index)?,
            self.col2.get(index)?,
            self.col3.get(index)?,
            self.col4.get(index)?,
            self.col5.get(index)?,
        ))
    }
}

impl<
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
    T4: ClickHouseValue,
    T5: ClickHouseValue,
> ClickHouseValue for (T1, T2, T3, T4, T5)
{
    fn ch_type_name() -> &'static str {
        "Tuple"
    }
    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::error::Error::Protocol(
            "tuple single read not supported".into(),
        ))
    }
    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Ok(())
    }
}

impl<T1, T2, T3, T4, T5> ClickHouseColumn for (T1, T2, T3, T4, T5)
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T5: ClickHouseColumn,
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
    T4: ClickHouseValue,
    T5: ClickHouseValue,
{
    type ColumnData<'a>
        = Tuple5Data<'a, T1, T2, T3, T4, T5>
    where
        T1: 'a,
        T2: 'a,
        T3: 'a,
        T4: 'a,
        T5: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        let col1 = T1::read_column(ctx)?;
        let col2 = T2::read_column(ctx)?;
        let col3 = T3::read_column(ctx)?;
        let col4 = T4::read_column(ctx)?;
        let col5 = T5::read_column(ctx)?;
        Ok(Tuple5Data {
            col1,
            col2,
            col3,
            col4,
            col5,
            rows,
        })
    }

    fn write_column(_data: &[Self], _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

// ───────────────────────────────────────────────
// Tuple(T1, T2, T3, T4, T5, T6)
// ───────────────────────────────────────────────

pub struct Tuple6Data<
    'a,
    T1: ClickHouseColumn + 'a,
    T2: ClickHouseColumn + 'a,
    T3: ClickHouseColumn + 'a,
    T4: ClickHouseColumn + 'a,
    T5: ClickHouseColumn + 'a,
    T6: ClickHouseColumn + 'a,
> {
    col1: T1::ColumnData<'a>,
    col2: T2::ColumnData<'a>,
    col3: T3::ColumnData<'a>,
    col4: T4::ColumnData<'a>,
    col5: T5::ColumnData<'a>,
    col6: T6::ColumnData<'a>,
    rows: usize,
}

impl<'a, T1, T2, T3, T4, T5, T6> ClickHouseColumnData<'a, (T1, T2, T3, T4, T5, T6)>
    for Tuple6Data<'a, T1, T2, T3, T4, T5, T6>
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T5: ClickHouseColumn,
    T6: ClickHouseColumn,
    T1::ColumnData<'a>: ClickHouseColumnData<'a, T1>,
    T2::ColumnData<'a>: ClickHouseColumnData<'a, T2>,
    T3::ColumnData<'a>: ClickHouseColumnData<'a, T3>,
    T4::ColumnData<'a>: ClickHouseColumnData<'a, T4>,
    T5::ColumnData<'a>: ClickHouseColumnData<'a, T5>,
    T6::ColumnData<'a>: ClickHouseColumnData<'a, T6>,
{
    fn len(&self) -> usize {
        self.rows
    }
    fn get(&self, index: usize) -> Result<(T1, T2, T3, T4, T5, T6)> {
        Ok((
            self.col1.get(index)?,
            self.col2.get(index)?,
            self.col3.get(index)?,
            self.col4.get(index)?,
            self.col5.get(index)?,
            self.col6.get(index)?,
        ))
    }
}

impl<
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
    T4: ClickHouseValue,
    T5: ClickHouseValue,
    T6: ClickHouseValue,
> ClickHouseValue for (T1, T2, T3, T4, T5, T6)
{
    fn ch_type_name() -> &'static str {
        "Tuple"
    }
    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::error::Error::Protocol(
            "tuple single read not supported".into(),
        ))
    }
    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Ok(())
    }
}

impl<T1, T2, T3, T4, T5, T6> ClickHouseColumn for (T1, T2, T3, T4, T5, T6)
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T5: ClickHouseColumn,
    T6: ClickHouseColumn,
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
    T4: ClickHouseValue,
    T5: ClickHouseValue,
    T6: ClickHouseValue,
{
    type ColumnData<'a>
        = Tuple6Data<'a, T1, T2, T3, T4, T5, T6>
    where
        T1: 'a,
        T2: 'a,
        T3: 'a,
        T4: 'a,
        T5: 'a,
        T6: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        let col1 = T1::read_column(ctx)?;
        let col2 = T2::read_column(ctx)?;
        let col3 = T3::read_column(ctx)?;
        let col4 = T4::read_column(ctx)?;
        let col5 = T5::read_column(ctx)?;
        let col6 = T6::read_column(ctx)?;
        Ok(Tuple6Data {
            col1,
            col2,
            col3,
            col4,
            col5,
            col6,
            rows,
        })
    }

    fn write_column(_data: &[Self], _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

// ───────────────────────────────────────────────
// Tuple(T1, T2, T3, T4, T5, T6, T7)
// ───────────────────────────────────────────────

pub struct Tuple7Data<
    'a,
    T1: ClickHouseColumn + 'a,
    T2: ClickHouseColumn + 'a,
    T3: ClickHouseColumn + 'a,
    T4: ClickHouseColumn + 'a,
    T5: ClickHouseColumn + 'a,
    T6: ClickHouseColumn + 'a,
    T7: ClickHouseColumn + 'a,
> {
    col1: T1::ColumnData<'a>,
    col2: T2::ColumnData<'a>,
    col3: T3::ColumnData<'a>,
    col4: T4::ColumnData<'a>,
    col5: T5::ColumnData<'a>,
    col6: T6::ColumnData<'a>,
    col7: T7::ColumnData<'a>,
    rows: usize,
}

impl<'a, T1, T2, T3, T4, T5, T6, T7> ClickHouseColumnData<'a, (T1, T2, T3, T4, T5, T6, T7)>
    for Tuple7Data<'a, T1, T2, T3, T4, T5, T6, T7>
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T5: ClickHouseColumn,
    T6: ClickHouseColumn,
    T7: ClickHouseColumn,
    T1::ColumnData<'a>: ClickHouseColumnData<'a, T1>,
    T2::ColumnData<'a>: ClickHouseColumnData<'a, T2>,
    T3::ColumnData<'a>: ClickHouseColumnData<'a, T3>,
    T4::ColumnData<'a>: ClickHouseColumnData<'a, T4>,
    T5::ColumnData<'a>: ClickHouseColumnData<'a, T5>,
    T6::ColumnData<'a>: ClickHouseColumnData<'a, T6>,
    T7::ColumnData<'a>: ClickHouseColumnData<'a, T7>,
{
    fn len(&self) -> usize {
        self.rows
    }
    fn get(&self, index: usize) -> Result<(T1, T2, T3, T4, T5, T6, T7)> {
        Ok((
            self.col1.get(index)?,
            self.col2.get(index)?,
            self.col3.get(index)?,
            self.col4.get(index)?,
            self.col5.get(index)?,
            self.col6.get(index)?,
            self.col7.get(index)?,
        ))
    }
}

impl<
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
    T4: ClickHouseValue,
    T5: ClickHouseValue,
    T6: ClickHouseValue,
    T7: ClickHouseValue,
> ClickHouseValue for (T1, T2, T3, T4, T5, T6, T7)
{
    fn ch_type_name() -> &'static str {
        "Tuple"
    }
    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::error::Error::Protocol(
            "tuple single read not supported".into(),
        ))
    }
    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Ok(())
    }
}

impl<T1, T2, T3, T4, T5, T6, T7> ClickHouseColumn for (T1, T2, T3, T4, T5, T6, T7)
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T5: ClickHouseColumn,
    T6: ClickHouseColumn,
    T7: ClickHouseColumn,
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
    T4: ClickHouseValue,
    T5: ClickHouseValue,
    T6: ClickHouseValue,
    T7: ClickHouseValue,
{
    type ColumnData<'a>
        = Tuple7Data<'a, T1, T2, T3, T4, T5, T6, T7>
    where
        T1: 'a,
        T2: 'a,
        T3: 'a,
        T4: 'a,
        T5: 'a,
        T6: 'a,
        T7: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        let col1 = T1::read_column(ctx)?;
        let col2 = T2::read_column(ctx)?;
        let col3 = T3::read_column(ctx)?;
        let col4 = T4::read_column(ctx)?;
        let col5 = T5::read_column(ctx)?;
        let col6 = T6::read_column(ctx)?;
        let col7 = T7::read_column(ctx)?;
        Ok(Tuple7Data {
            col1,
            col2,
            col3,
            col4,
            col5,
            col6,
            col7,
            rows,
        })
    }

    fn write_column(_data: &[Self], _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

// ───────────────────────────────────────────────
// Tuple(T1, T2, T3, T4, T5, T6, T7, T8)
// ───────────────────────────────────────────────

pub struct Tuple8Data<
    'a,
    T1: ClickHouseColumn + 'a,
    T2: ClickHouseColumn + 'a,
    T3: ClickHouseColumn + 'a,
    T4: ClickHouseColumn + 'a,
    T5: ClickHouseColumn + 'a,
    T6: ClickHouseColumn + 'a,
    T7: ClickHouseColumn + 'a,
    T8: ClickHouseColumn + 'a,
> {
    col1: T1::ColumnData<'a>,
    col2: T2::ColumnData<'a>,
    col3: T3::ColumnData<'a>,
    col4: T4::ColumnData<'a>,
    col5: T5::ColumnData<'a>,
    col6: T6::ColumnData<'a>,
    col7: T7::ColumnData<'a>,
    col8: T8::ColumnData<'a>,
    rows: usize,
}

impl<'a, T1, T2, T3, T4, T5, T6, T7, T8> ClickHouseColumnData<'a, (T1, T2, T3, T4, T5, T6, T7, T8)>
    for Tuple8Data<'a, T1, T2, T3, T4, T5, T6, T7, T8>
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T5: ClickHouseColumn,
    T6: ClickHouseColumn,
    T7: ClickHouseColumn,
    T8: ClickHouseColumn,
    T1::ColumnData<'a>: ClickHouseColumnData<'a, T1>,
    T2::ColumnData<'a>: ClickHouseColumnData<'a, T2>,
    T3::ColumnData<'a>: ClickHouseColumnData<'a, T3>,
    T4::ColumnData<'a>: ClickHouseColumnData<'a, T4>,
    T5::ColumnData<'a>: ClickHouseColumnData<'a, T5>,
    T6::ColumnData<'a>: ClickHouseColumnData<'a, T6>,
    T7::ColumnData<'a>: ClickHouseColumnData<'a, T7>,
    T8::ColumnData<'a>: ClickHouseColumnData<'a, T8>,
{
    fn len(&self) -> usize {
        self.rows
    }
    fn get(&self, index: usize) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8)> {
        Ok((
            self.col1.get(index)?,
            self.col2.get(index)?,
            self.col3.get(index)?,
            self.col4.get(index)?,
            self.col5.get(index)?,
            self.col6.get(index)?,
            self.col7.get(index)?,
            self.col8.get(index)?,
        ))
    }
}

impl<
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
    T4: ClickHouseValue,
    T5: ClickHouseValue,
    T6: ClickHouseValue,
    T7: ClickHouseValue,
    T8: ClickHouseValue,
> ClickHouseValue for (T1, T2, T3, T4, T5, T6, T7, T8)
{
    fn ch_type_name() -> &'static str {
        "Tuple"
    }
    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::error::Error::Protocol(
            "tuple single read not supported".into(),
        ))
    }
    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Ok(())
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8> ClickHouseColumn for (T1, T2, T3, T4, T5, T6, T7, T8)
where
    T1: ClickHouseColumn,
    T2: ClickHouseColumn,
    T3: ClickHouseColumn,
    T4: ClickHouseColumn,
    T5: ClickHouseColumn,
    T6: ClickHouseColumn,
    T7: ClickHouseColumn,
    T8: ClickHouseColumn,
    T1: ClickHouseValue,
    T2: ClickHouseValue,
    T3: ClickHouseValue,
    T4: ClickHouseValue,
    T5: ClickHouseValue,
    T6: ClickHouseValue,
    T7: ClickHouseValue,
    T8: ClickHouseValue,
{
    type ColumnData<'a>
        = Tuple8Data<'a, T1, T2, T3, T4, T5, T6, T7, T8>
    where
        T1: 'a,
        T2: 'a,
        T3: 'a,
        T4: 'a,
        T5: 'a,
        T6: 'a,
        T7: 'a,
        T8: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        let col1 = T1::read_column(ctx)?;
        let col2 = T2::read_column(ctx)?;
        let col3 = T3::read_column(ctx)?;
        let col4 = T4::read_column(ctx)?;
        let col5 = T5::read_column(ctx)?;
        let col6 = T6::read_column(ctx)?;
        let col7 = T7::read_column(ctx)?;
        let col8 = T8::read_column(ctx)?;
        Ok(Tuple8Data {
            col1,
            col2,
            col3,
            col4,
            col5,
            col6,
            col7,
            col8,
            rows,
        })
    }

    fn write_column(_data: &[Self], _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

// ───────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple2_uint64_string() {
        // Tuple(UInt64, String): 2 rows — no offsets, just sequential columns
        let mut buf = Vec::new();
        buf.extend_from_slice(&42u64.to_le_bytes());
        buf.extend_from_slice(&99u64.to_le_bytes());
        buf.push(1);
        buf.push(b'a');
        buf.push(2);
        buf.push(b'b');
        buf.push(b'c');

        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = <(u64, String) as ClickHouseColumn>::read_column(&mut ctx)
            .expect("test operation failed");
        assert_eq!(col.len(), 2);
        assert_eq!(
            col.get(0).expect("test operation failed"),
            (42, "a".to_string())
        );
        assert_eq!(
            col.get(1).expect("test operation failed"),
            (99, "bc".to_string())
        );
    }

    #[test]
    fn test_tuple3_uint64_string_u16() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.push(1);
        buf.push(b'x');
        buf.push(1);
        buf.push(b'y');
        buf.extend_from_slice(&100u16.to_le_bytes());
        buf.extend_from_slice(&200u16.to_le_bytes());

        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = <(u64, String, u16) as ClickHouseColumn>::read_column(&mut ctx)
            .expect("test operation failed");
        assert_eq!(col.len(), 2);
        assert_eq!(
            col.get(0).expect("test operation failed"),
            (1, "x".to_string(), 100u16)
        );
        assert_eq!(
            col.get(1).expect("test operation failed"),
            (2, "y".to_string(), 200u16)
        );
    }

    #[test]
    fn test_tuple5_uint64_string_u16_u8_f64() {
        let mut buf = Vec::new();
        // row 0: u64=1, String="x", u16=100, u8=42, f64=1.5
        // row 1: u64=2, String="yz", u16=200, u8=99, f64=2.5
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.push(1);
        buf.push(b'x');
        buf.push(2);
        buf.push(b'y');
        buf.push(b'z');
        buf.extend_from_slice(&100u16.to_le_bytes());
        buf.extend_from_slice(&200u16.to_le_bytes());
        buf.extend_from_slice(&42u8.to_le_bytes());
        buf.extend_from_slice(&99u8.to_le_bytes());
        buf.extend_from_slice(&1.5f64.to_le_bytes());
        buf.extend_from_slice(&2.5f64.to_le_bytes());

        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = <(u64, String, u16, u8, f64) as ClickHouseColumn>::read_column(&mut ctx)
            .expect("test operation failed");
        assert_eq!(col.len(), 2);
        assert_eq!(
            col.get(0).expect("test operation failed"),
            (1, "x".to_string(), 100u16, 42u8, 1.5f64)
        );
        assert_eq!(
            col.get(1).expect("test operation failed"),
            (2, "yz".to_string(), 200u16, 99u8, 2.5f64)
        );
    }
}
