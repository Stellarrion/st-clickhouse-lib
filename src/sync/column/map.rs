use std::collections::HashMap;
use std::hash::Hash;

/// Untyped Map column data — raw byte slices for offsets, keys, values.
///
/// Used by [`super::AnyColumnData::Map`] for runtime-dispatched access.
/// The wire format is: N*8 byte offsets + keys blob + values blob.
#[derive(Debug, Clone)]
pub struct RawMapColumnData<'a> {
    pub offsets: Vec<u64>,
    pub keys_data: &'a [u8],
    pub values_data: &'a [u8],
}

use super::super::error::Result;
use super::super::protocol::block::ReadColumnContext;
use super::{ClickHouseColumn, ClickHouseColumnData, ClickHouseValue};

/// Column data for `Map(K, V)` — stored as `Array(Tuple(K, V))` on the wire.
///
/// Wire format (same as Array):
/// ```text
/// [N * 8 bytes UInt64 offsets] — cumulative, one per row
/// [K elements]                  — bulk-serialized keys
/// [V elements]                  — bulk-serialized values
/// ```
pub struct MapColumnData<'a, K: ClickHouseColumn + 'a, V: ClickHouseColumn + 'a> {
    /// Cumulative offsets. Owned Vec for safe alignment.
    offsets: Vec<u64>,
    keys: K::ColumnData<'a>,
    values: V::ColumnData<'a>,
}

impl<'a, K: ClickHouseColumn + 'a, V: ClickHouseColumn + 'a> MapColumnData<'a, K, V> {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    fn element_range(&self, index: usize) -> Result<(usize, usize)> {
        let start = if index == 0 {
            0
        } else {
            self.offsets.get(index - 1).copied().ok_or_else(|| {
                crate::sync::error::Error::Protocol("MapColumnData: index out of bounds".into())
            })? as usize
        };
        let end = self.offsets.get(index).copied().ok_or_else(|| {
            crate::sync::error::Error::Protocol("MapColumnData: index out of bounds".into())
        })? as usize;
        Ok((start, end))
    }
}

impl<'a, K, V> ClickHouseColumnData<'a, HashMap<K, V>> for MapColumnData<'a, K, V>
where
    K: ClickHouseColumn + Eq + Hash + 'static,
    V: ClickHouseColumn + 'static,
    K::ColumnData<'a>: ClickHouseColumnData<'a, K>,
    V::ColumnData<'a>: ClickHouseColumnData<'a, V>,
{
    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn get(&self, index: usize) -> Result<HashMap<K, V>> {
        let (start, end) = self.element_range(index)?;
        let mut map = HashMap::with_capacity(end - start);
        for i in start..end {
            let k = self.keys.get(i)?;
            let v = self.values.get(i)?;
            map.insert(k, v);
        }
        Ok(map)
    }
}

impl<K: ClickHouseValue + Eq + Hash + 'static, V: ClickHouseValue + 'static> ClickHouseValue
    for HashMap<K, V>
{
    fn ch_type_name() -> &'static str {
        "Map"
    }

    fn read_from<R: std::io::Read>(_reader: &mut R) -> Result<Self> {
        Err(crate::sync::error::Error::Protocol(
            "HashMap RowBinary read not supported".into(),
        ))
    }

    fn write_to<W: std::io::Write>(&self, _writer: &mut W) -> Result<()> {
        Err(crate::sync::error::Error::Protocol(
            "HashMap RowBinary write not supported".into(),
        ))
    }
}

impl<K, V> ClickHouseColumn for HashMap<K, V>
where
    K: ClickHouseColumn + Eq + Hash + 'static,
    V: ClickHouseColumn + 'static,
    K: ClickHouseValue,
    V: ClickHouseValue,
    HashMap<K, V>: 'static,
{
    type ColumnData<'a>
        = MapColumnData<'a, K, V>
    where
        K: 'a,
        V: 'a;

    fn read_column<'a>(ctx: &mut ReadColumnContext<'a>) -> Result<Self::ColumnData<'a>> {
        let rows = ctx.rows;
        if rows == 0 {
            return Ok(MapColumnData {
                offsets: Vec::new(),
                keys: K::read_column(ctx)?,
                values: V::read_column(ctx)?,
            });
        }
        let offsets = ctx.read_offsets()?;
        let total = offsets[rows - 1] as usize;
        let saved = ctx.rows;
        ctx.rows = total;
        let keys = K::read_column(ctx)?;
        let values = V::read_column(ctx)?;
        ctx.rows = saved;
        Ok(MapColumnData {
            offsets,
            keys,
            values,
        })
    }

    fn write_column(data: &[Self], buf: &mut Vec<u8>) -> Result<()> {
        let mut cumulative = 0u64;
        for map in data {
            cumulative += map.len() as u64;
            buf.extend_from_slice(&cumulative.to_le_bytes());
        }
        for map in data {
            for k in map.keys() {
                k.write_to(buf)?;
            }
        }
        for map in data {
            for v in map.values() {
                v.write_to(buf)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_string_uint64() {
        // Map(String, UInt64): 1 row with {"x": 42, "y": 99}
        // Wire: [offset:8] + [String keys] + [UInt64 values]
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u64.to_le_bytes()); // offset = 2
        // String keys: "x", "y"
        buf.push(1);
        buf.push(b'x');
        buf.push(1);
        buf.push(b'y');
        // UInt64 values: 42, 99
        buf.extend_from_slice(&42u64.to_le_bytes());
        buf.extend_from_slice(&99u64.to_le_bytes());

        let mut ctx = ReadColumnContext {
            rows: 1,
            pos: 0,
            buf: &buf,
        };
        let col = <HashMap<String, u64> as ClickHouseColumn>::read_column(&mut ctx)
            .expect("test operation failed");
        assert_eq!(col.len(), 1);
        let map = col.get(0).expect("test operation failed");
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("x"), Some(&42));
        assert_eq!(map.get("y"), Some(&99));
    }

    #[test]
    fn test_map_empty() {
        let buf = vec![];
        let mut ctx = ReadColumnContext {
            rows: 0,
            pos: 0,
            buf: &buf,
        };
        let col = <HashMap<String, u64> as ClickHouseColumn>::read_column(&mut ctx)
            .expect("test operation failed");
        assert_eq!(col.len(), 0);
    }
}
