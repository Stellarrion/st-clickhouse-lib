//! Geo types for ClickHouse: Point, Ring, Polygon, MultiPolygon.
//!
//! Wire formats (all delegate to existing Tuple/Array infrastructure):
//! - Point: Tuple(Float64, Float64) — all x values followed by all y values
//! - Ring:  Array(Point) — `Vec<[f64; 2]>`
//! - Polygon: Array(Ring) — `Vec<Vec<[f64; 2]>>`
//! - MultiPolygon: Array(Polygon) — `Vec<Vec<Vec<[f64; 2]>>>`

use super::super::error::Result;
use super::super::protocol::block::ReadColumnContext;
use super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};

// ───────────────────────────────────────────────
// Point = Tuple(Float64, Float64)
// ───────────────────────────────────────────────

/// A 2D point: (x, y) as two Float64 values.
///
/// Wire format: Tuple(Float64, Float64) — ClickHouse Native serializes tuple
/// elements column-major: all x values, then all y values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point(pub f64, pub f64);

/// Column data for Point: reads tuple elements in ClickHouse's column-major
/// Native layout.
pub struct PointColumnData<'a> {
    data: crate::sync::column::plain::PlainColumnData<'a, f64>,
    count: usize,
}

impl<'a> PointColumnData<'a> {
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the point at `index` as a (x, y) pair.
    pub fn get_point(&self, index: usize) -> Result<Point> {
        let x = self.data.get(index)?;
        let y = self.data.get(self.count + index)?;
        Ok(Point(x, y))
    }
}

impl<'a> ClickHouseColumnData<'a, Point> for PointColumnData<'a> {
    fn len(&self) -> usize {
        self.count
    }

    fn get(&self, index: usize) -> Result<Point> {
        self.get_point(index)
    }
}

impl ClickHouseValue for Point {
    fn ch_type_name() -> &'static str {
        "Tuple(Float64, Float64)"
    }

    fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let mut buf_x = [0u8; 8];
        let mut buf_y = [0u8; 8];
        reader.read_exact(&mut buf_x)?;
        reader.read_exact(&mut buf_y)?;
        Ok(Point(f64::from_le_bytes(buf_x), f64::from_le_bytes(buf_y)))
    }

    fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.0.to_le_bytes())?;
        writer.write_all(&self.1.to_le_bytes())?;
        Ok(())
    }
}

impl ClickHouseColumn for Point {
    type ColumnData<'a> = PointColumnData<'a>;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let count = ctx.rows;
        let nbytes = count.checked_mul(16).ok_or_else(|| {
            crate::sync::error::Error::Protocol("Point column size overflow".into())
        })?;
        let bytes = ctx.read_exact(nbytes)?;
        let inner_data =
            crate::sync::column::plain::PlainColumnData::<f64>::read_from_bytes(bytes, count * 2);
        Ok(PointColumnData {
            data: inner_data,
            count,
        })
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        for p in data {
            buf.extend_from_slice(&p.0.to_le_bytes());
        }
        for p in data {
            buf.extend_from_slice(&p.1.to_le_bytes());
        }
        Ok(())
    }
}

// ───────────────────────────────────────────────
// Ring = Array(Point) — Vec<Point>
// ───────────────────────────────────────────────

/// A ring (closed polygon ring) as a sequence of Points.
/// Wire format: Array(Point) — UInt64 cumulative offsets + Point data.
#[derive(Debug, Clone, PartialEq)]
pub struct Ring(pub Vec<Point>);

impl ClickHouseValue for Ring {
    fn ch_type_name() -> &'static str {
        "Array(Tuple(Float64, Float64))"
    }

    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::sync::error::Error::Protocol(
            "Ring RowBinary read not supported".into(),
        ))
    }

    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Err(crate::sync::error::Error::Protocol(
            "Ring RowBinary write not supported".into(),
        ))
    }
}

/// Column data for Ring = Array(Point).
pub struct RingColumnData<'a> {
    offsets: Vec<u64>,
    points: PointColumnData<'a>,
}

impl<'a> RingColumnData<'a> {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    fn point_range(&self, index: usize) -> Result<(usize, usize)> {
        let start = if index == 0 {
            0
        } else {
            self.offsets[index - 1] as usize
        };
        let end = self.offsets[index] as usize;
        Ok((start, end))
    }

    pub fn get_ring(&self, index: usize) -> Result<Ring> {
        let (start, end) = self.point_range(index)?;
        let mut points = Vec::with_capacity(end - start);
        for i in start..end {
            points.push(self.points.get_point(i)?);
        }
        Ok(Ring(points))
    }
}

impl<'a> ClickHouseColumnData<'a, Ring> for RingColumnData<'a> {
    fn len(&self) -> usize {
        self.offsets.len()
    }
    fn get(&self, index: usize) -> Result<Ring> {
        self.get_ring(index)
    }
}

impl ClickHouseColumn for Ring {
    type ColumnData<'a> = RingColumnData<'a>;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        if rows == 0 {
            return Ok(RingColumnData {
                offsets: Vec::new(),
                points: PointColumnData {
                    data: crate::sync::column::plain::PlainColumnData::empty(),
                    count: 0,
                },
            });
        }
        let offsets = ctx.read_offsets()?;
        let total_points = offsets[rows - 1] as usize;
        let saved = ctx.rows;
        ctx.rows = total_points;
        let points = Point::read_column(ctx)?;
        ctx.rows = saved;
        Ok(RingColumnData { offsets, points })
    }

    fn write_column(_data: &[Self], _buf: &mut Vec<u8>) -> Result<()> {
        Err(crate::sync::error::Error::Protocol(
            "Ring write not yet implemented".into(),
        ))
    }
}

// ───────────────────────────────────────────────
// Polygon = Array(Ring) — Vec<Ring>
// ───────────────────────────────────────────────

/// A polygon (one outer ring + optional inner rings).
/// Wire format: Array(Ring) = nested Array(Array(Point)).
#[derive(Debug, Clone, PartialEq)]
pub struct Polygon(pub Vec<Ring>);

impl ClickHouseValue for Polygon {
    fn ch_type_name() -> &'static str {
        "Array(Array(Tuple(Float64, Float64)))"
    }
    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::sync::error::Error::Protocol(
            "Polygon RowBinary read not supported".into(),
        ))
    }
    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Err(crate::sync::error::Error::Protocol(
            "Polygon RowBinary write not supported".into(),
        ))
    }
}

/// Column data for Polygon = nested Array(Ring).
pub struct PolygonColumnData<'a> {
    offsets: Vec<u64>,
    rings: RingColumnData<'a>,
}

impl<'a> PolygonColumnData<'a> {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
    fn ring_range(&self, index: usize) -> Result<(usize, usize)> {
        let start = if index == 0 {
            0
        } else {
            self.offsets[index - 1] as usize
        };
        let end = self.offsets[index] as usize;
        Ok((start, end))
    }
    pub fn get_polygon(&self, index: usize) -> Result<Polygon> {
        let (start, end) = self.ring_range(index)?;
        let mut rings = Vec::with_capacity(end - start);
        for i in start..end {
            rings.push(self.rings.get_ring(i)?);
        }
        Ok(Polygon(rings))
    }
}

impl<'a> ClickHouseColumnData<'a, Polygon> for PolygonColumnData<'a> {
    fn len(&self) -> usize {
        self.offsets.len()
    }
    fn get(&self, index: usize) -> Result<Polygon> {
        self.get_polygon(index)
    }
}

impl ClickHouseColumn for Polygon {
    type ColumnData<'a> = PolygonColumnData<'a>;
    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        if rows == 0 {
            return Ok(PolygonColumnData {
                offsets: Vec::new(),
                rings: RingColumnData {
                    offsets: Vec::new(),
                    points: PointColumnData {
                        data: crate::sync::column::plain::PlainColumnData::empty(),
                        count: 0,
                    },
                },
            });
        }
        let offsets = ctx.read_offsets()?;
        let total_rings = offsets[rows - 1] as usize;
        let saved = ctx.rows;
        ctx.rows = total_rings;
        let rings = Ring::read_column(ctx)?;
        ctx.rows = saved;
        Ok(PolygonColumnData { offsets, rings })
    }
    fn write_column(_data: &[Self], _buf: &mut Vec<u8>) -> Result<()> {
        Err(crate::sync::error::Error::Protocol(
            "Polygon write not yet implemented".into(),
        ))
    }
}

// ───────────────────────────────────────────────
// MultiPolygon = Array(Polygon) — Vec<Polygon>
// ───────────────────────────────────────────────

/// A multi-polygon (collection of polygons).
/// Wire format: Array(Polygon) = triple-nested Array(Array(Array(Point))).
#[derive(Debug, Clone, PartialEq)]
pub struct MultiPolygon(pub Vec<Polygon>);

impl ClickHouseValue for MultiPolygon {
    fn ch_type_name() -> &'static str {
        "Array(Array(Array(Tuple(Float64, Float64))))"
    }
    fn read_from<R: std::io::Read>(_r: &mut R) -> Result<Self> {
        Err(crate::sync::error::Error::Protocol(
            "MultiPolygon RowBinary read not supported".into(),
        ))
    }
    fn write_to<W: std::io::Write>(&self, _w: &mut W) -> Result<()> {
        Err(crate::sync::error::Error::Protocol(
            "MultiPolygon RowBinary write not supported".into(),
        ))
    }
}

/// Column data for MultiPolygon = nested Array(Polygon).
pub struct MultiPolygonColumnData<'a> {
    offsets: Vec<u64>,
    polygons: PolygonColumnData<'a>,
}

impl<'a> MultiPolygonColumnData<'a> {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
    fn poly_range(&self, index: usize) -> Result<(usize, usize)> {
        let start = if index == 0 {
            0
        } else {
            self.offsets[index - 1] as usize
        };
        let end = self.offsets[index] as usize;
        Ok((start, end))
    }
    pub fn get_multipolygon(&self, index: usize) -> Result<MultiPolygon> {
        let (start, end) = self.poly_range(index)?;
        let mut polys = Vec::with_capacity(end - start);
        for i in start..end {
            polys.push(self.polygons.get_polygon(i)?);
        }
        Ok(MultiPolygon(polys))
    }
}

impl<'a> ClickHouseColumnData<'a, MultiPolygon> for MultiPolygonColumnData<'a> {
    fn len(&self) -> usize {
        self.offsets.len()
    }
    fn get(&self, index: usize) -> Result<MultiPolygon> {
        self.get_multipolygon(index)
    }
}

impl ClickHouseColumn for MultiPolygon {
    type ColumnData<'a> = MultiPolygonColumnData<'a>;
    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        if rows == 0 {
            return Ok(MultiPolygonColumnData {
                offsets: Vec::new(),
                polygons: PolygonColumnData {
                    offsets: Vec::new(),
                    rings: RingColumnData {
                        offsets: Vec::new(),
                        points: PointColumnData {
                            data: crate::sync::column::plain::PlainColumnData::empty(),
                            count: 0,
                        },
                    },
                },
            });
        }
        let offsets = ctx.read_offsets()?;
        let total_polys = offsets[rows - 1] as usize;
        let saved = ctx.rows;
        ctx.rows = total_polys;
        let polygons = Polygon::read_column(ctx)?;
        ctx.rows = saved;
        Ok(MultiPolygonColumnData { offsets, polygons })
    }
    fn write_column(_data: &[Self], _buf: &mut Vec<u8>) -> Result<()> {
        Err(crate::sync::error::Error::Protocol(
            "MultiPolygon write not yet implemented".into(),
        ))
    }
}

// ───────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_read_write() {
        let pt = Point(1.5, 2.5);
        let mut buf = Vec::new();
        pt.write_to(&mut buf).expect("test operation failed");
        assert_eq!(buf.len(), 16);
        let mut cursor = std::io::Cursor::new(&buf[..]);
        let read = Point::read_from(&mut cursor).expect("test operation failed");
        assert_eq!(read.0, 1.5);
        assert_eq!(read.1, 2.5);
    }

    #[test]
    fn test_point_column() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1.0f64.to_le_bytes());
        buf.extend_from_slice(&3.0f64.to_le_bytes());
        buf.extend_from_slice(&2.0f64.to_le_bytes());
        buf.extend_from_slice(&4.0f64.to_le_bytes());

        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = Point::read_column(&mut ctx).expect("test operation failed");
        assert_eq!(col.len(), 2);
        assert_eq!(
            col.get_point(0).expect("test operation failed"),
            Point(1.0, 2.0)
        );
        assert_eq!(
            col.get_point(1).expect("test operation failed"),
            Point(3.0, 4.0)
        );
    }

    #[test]
    fn test_ring_column() {
        // Ring = Array(Point): 2 rings
        // Ring 0: [(1,2), (3,4)]  → offsets[0] = 2
        // Ring 1: [(5,6)]          → offsets[1] = 3
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u64.to_le_bytes()); // offsets[0] = 2
        buf.extend_from_slice(&3u64.to_le_bytes()); // offsets[1] = 3
        // Points: (1,2), (3,4), (5,6)
        buf.extend_from_slice(&1.0f64.to_le_bytes());
        buf.extend_from_slice(&3.0f64.to_le_bytes());
        buf.extend_from_slice(&5.0f64.to_le_bytes());
        buf.extend_from_slice(&2.0f64.to_le_bytes());
        buf.extend_from_slice(&4.0f64.to_le_bytes());
        buf.extend_from_slice(&6.0f64.to_le_bytes());

        let mut ctx = ReadColumnContext {
            rows: 2,
            pos: 0,
            buf: &buf,
        };
        let col = Ring::read_column(&mut ctx).expect("test operation failed");
        assert_eq!(col.len(), 2);
        let r0 = col.get_ring(0).expect("test operation failed");
        assert_eq!(r0.0, vec![Point(1.0, 2.0), Point(3.0, 4.0)]);
        let r1 = col.get_ring(1).expect("test operation failed");
        assert_eq!(r1.0, vec![Point(5.0, 6.0)]);
    }

    #[test]
    fn test_polygon_column() {
        // Polygon = Array(Ring): 1 polygon with 2 rings
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u64.to_le_bytes()); // offsets[0] = 2 rings
        // Ring 0: 2 points → offsets[0] = 2
        buf.extend_from_slice(&2u64.to_le_bytes());
        // Ring 1: 1 point → offsets[1] = 3
        buf.extend_from_slice(&3u64.to_le_bytes());
        // Points: (1,2), (3,4), (5,6)
        buf.extend_from_slice(&1.0f64.to_le_bytes());
        buf.extend_from_slice(&3.0f64.to_le_bytes());
        buf.extend_from_slice(&5.0f64.to_le_bytes());
        buf.extend_from_slice(&2.0f64.to_le_bytes());
        buf.extend_from_slice(&4.0f64.to_le_bytes());
        buf.extend_from_slice(&6.0f64.to_le_bytes());

        let mut ctx = ReadColumnContext {
            rows: 1,
            pos: 0,
            buf: &buf,
        };
        let col = Polygon::read_column(&mut ctx).expect("test operation failed");
        assert_eq!(col.len(), 1);
        let poly = col.get_polygon(0).expect("test operation failed");
        assert_eq!(poly.0.len(), 2);
        assert_eq!(poly.0[0].0, vec![Point(1.0, 2.0), Point(3.0, 4.0)]);
        assert_eq!(poly.0[1].0, vec![Point(5.0, 6.0)]);
    }

    #[test]
    fn test_multipolygon_column() {
        // MultiPolygon = Array(Polygon): 1 MP with 1 polygon, 1 ring, 2 points
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes()); // offsets[0] = 1 polygon
        buf.extend_from_slice(&1u64.to_le_bytes()); // polygon offsets[0] = 1 ring
        buf.extend_from_slice(&2u64.to_le_bytes()); // ring offsets[0] = 2 points
        buf.extend_from_slice(&1.0f64.to_le_bytes());
        buf.extend_from_slice(&3.0f64.to_le_bytes());
        buf.extend_from_slice(&2.0f64.to_le_bytes());
        buf.extend_from_slice(&4.0f64.to_le_bytes());

        let mut ctx = ReadColumnContext {
            rows: 1,
            pos: 0,
            buf: &buf,
        };
        let col = MultiPolygon::read_column(&mut ctx).expect("test operation failed");
        assert_eq!(col.len(), 1);
        let mp = col.get_multipolygon(0).expect("test operation failed");
        assert_eq!(mp.0.len(), 1);
        assert_eq!(mp.0[0].0.len(), 1);
        assert_eq!(mp.0[0].0[0].0, vec![Point(1.0, 2.0), Point(3.0, 4.0)]);
    }
}
