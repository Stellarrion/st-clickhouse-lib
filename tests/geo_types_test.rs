//! Geo type roundtrip tests — Point, Ring, Polygon, MultiPolygon.
//!
//! ClickHouse geo types on the wire:
//! - Point: Tuple(Float64, Float64) — two f64 values per row
//! - Ring:  Array(Point) — UInt64 offsets + Point data
//! - Polygon: Array(Ring) — nested UInt64 offsets + Ring data
//! - MultiPolygon: Array(Polygon) — triple-nested

mod common;
use st_clickhouse::ClickHouseColumnData;
use st_clickhouse::column::{MultiPolygon, Point, Polygon, Ring};
use st_clickhouse::protocol::block::{Block, ColumnInfo};

// ═══════════════════════════════════════════════════════════════════
// test_point_roundtrip — read Tuple(Float64, Float64) as Point
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_point_roundtrip() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT tuple(1.5, 2.5) AS p")
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(block.row_count(), 1);
    assert!(block.column_count() > 0);

    let col = block.column::<Point>("p").expect("test operation failed");
    assert_eq!(col.len(), 1);
    let pt = col.get(0).expect("test operation failed");
    assert_eq!(pt.0, 1.5);
    assert_eq!(pt.1, 2.5);
    eprintln!("SUCCESS: Point roundtrip = ({}, {})!", pt.0, pt.1);
}

// ═══════════════════════════════════════════════════════════════════
// test_point_multi_row — multiple points in one column
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_point_multi_row() {
    let client = common::connect_client().await;
    let block = client
        .query("SELECT tuple(x, x * 10.0) AS p FROM (SELECT toFloat64(number) + 0.1 AS x FROM system.numbers LIMIT 5)")
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(block.row_count(), 5);
    let col = block.column::<Point>("p").expect("test operation failed");
    assert_eq!(col.len(), 5);

    // Row 0: (0.1, 1.0)
    assert_eq!(col.get(0).expect("test operation failed"), Point(0.1, 1.0));
    // Row 2: (2.1, 21.0)
    assert_eq!(col.get(2).expect("test operation failed"), Point(2.1, 21.0));
    // Row 4: (4.1, 41.0)
    assert_eq!(col.get(4).expect("test operation failed"), Point(4.1, 41.0));
    eprintln!("SUCCESS: Point multi-row (5 rows)!");
}

// ═══════════════════════════════════════════════════════════════════
// test_ring_roundtrip — read Array(Tuple(Float64, Float64)) as Ring
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_ring_roundtrip() {
    let client = common::connect_client().await;

    // Single closed ring: [(0,0),(1,0),(1,1),(0,0)] — first == last
    let block = client
        .query("SELECT CAST([(0.0,0.0),(1.0,0.0),(1.0,1.0),(0.0,0.0)], 'Array(Tuple(Float64, Float64))') AS ring")
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(block.row_count(), 1);
    let col = block.column::<Ring>("ring").expect("test operation failed");
    assert_eq!(col.len(), 1);

    let ring = col.get(0).expect("test operation failed");
    assert_eq!(ring.0.len(), 4);
    assert_eq!(ring.0[0], Point(0.0, 0.0));
    assert_eq!(ring.0[1], Point(1.0, 0.0));
    assert_eq!(ring.0[2], Point(1.0, 1.0));
    assert_eq!(ring.0[3], Point(0.0, 0.0));
    eprintln!("SUCCESS: Ring roundtrip ({} points)!", ring.0.len());
}

// ═══════════════════════════════════════════════════════════════════
// test_ring_multi_row — multiple rings in one column
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_ring_multi_row() {
    let client = common::connect_client().await;

    let block = client
        .query(
            "SELECT arrayJoin([
                CAST([(1.0,10.0),(3.0,30.0),(5.0,50.0)], 'Array(Tuple(Float64, Float64))'),
                CAST([(10.0,20.0),(30.0,40.0)], 'Array(Tuple(Float64, Float64))')
            ]) AS ring",
        )
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(block.row_count(), 2);
    let col = block.column::<Ring>("ring").expect("test operation failed");
    assert_eq!(col.len(), 2);

    // Ring 0: [(1,10),(3,30),(5,50)]
    let ring0 = col.get(0).expect("test operation failed");
    assert_eq!(ring0.0.len(), 3);
    assert_eq!(ring0.0[0], Point(1.0, 10.0));

    // Ring 1: [(10,20),(30,40)]
    let ring1 = col.get(1).expect("test operation failed");
    assert_eq!(ring1.0.len(), 2);
    assert_eq!(ring1.0[0], Point(10.0, 20.0));
    assert_eq!(ring1.0[1], Point(30.0, 40.0));

    eprintln!("SUCCESS: Ring multi-row (2 rings)!");
}

// ═══════════════════════════════════════════════════════════════════
// test_polygon_roundtrip — polygon without holes
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_polygon_roundtrip() {
    let client = common::connect_client().await;

    // Simple triangle polygon (just outer ring, no holes)
    let block = client
        .query("SELECT CAST([[(0.0,0.0),(2.0,0.0),(1.0,2.0),(0.0,0.0)]], 'Array(Array(Tuple(Float64, Float64)))') AS poly")
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(block.row_count(), 1);
    let col = block
        .column::<Polygon>("poly")
        .expect("test operation failed");
    assert_eq!(col.len(), 1);

    let poly = col.get(0).expect("test operation failed");
    assert_eq!(poly.0.len(), 1, "polygon should have 1 ring (outer)");
    let outer = &poly.0[0];
    assert_eq!(outer.0.len(), 4);
    assert_eq!(outer.0[0], Point(0.0, 0.0));
    assert_eq!(outer.0[1], Point(2.0, 0.0));
    assert_eq!(outer.0[2], Point(1.0, 2.0));
    assert_eq!(outer.0[3], Point(0.0, 0.0));

    eprintln!("SUCCESS: Polygon roundtrip (triangle)!");
}

// ═══════════════════════════════════════════════════════════════════
// test_polygon_with_holes — polygon with interior ring (hole)
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_polygon_with_holes() {
    let client = common::connect_client().await;

    // Outer ring: [(0,0),(5,0),(5,5),(0,5),(0,0)]
    // Inner ring (hole): [(1,1),(1,4),(4,4),(4,1),(1,1)]
    let block = client
        .query(
            "SELECT CAST([ \
                [(0.0,0.0),(5.0,0.0),(5.0,5.0),(0.0,5.0),(0.0,0.0)], \
                [(1.0,1.0),(1.0,4.0),(4.0,4.0),(4.0,1.0),(1.0,1.0)] \
            ], 'Array(Array(Tuple(Float64, Float64)))') AS poly",
        )
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(block.row_count(), 1);
    let col = block
        .column::<Polygon>("poly")
        .expect("test operation failed");
    let poly = col.get(0).expect("test operation failed");

    // 2 rings: outer + hole
    assert_eq!(
        poly.0.len(),
        2,
        "polygon should have 2 rings (outer + hole)"
    );

    let outer = &poly.0[0];
    assert_eq!(outer.0.len(), 5);
    assert_eq!(outer.0[0], Point(0.0, 0.0));
    assert_eq!(outer.0[4], Point(0.0, 0.0)); // closed

    let hole = &poly.0[1];
    assert_eq!(hole.0.len(), 5);
    assert_eq!(hole.0[0], Point(1.0, 1.0));
    assert_eq!(hole.0[4], Point(1.0, 1.0)); // closed

    eprintln!("SUCCESS: Polygon with hole (2 rings)!");
}

// ═══════════════════════════════════════════════════════════════════
// test_multipolygon_roundtrip — collection of multiple polygons
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multipolygon_roundtrip() {
    let client = common::connect_client().await;

    // MultiPolygon with 2 polygons:
    // Poly 0: triangle [(0,0),(2,0),(1,2),(0,0)]
    // Poly 1: square [(10,10),(10,20),(20,20),(20,10),(10,10)]
    let block = client
        .query(
            "SELECT CAST([ \
                [[(0.0,0.0),(2.0,0.0),(1.0,2.0),(0.0,0.0)]], \
                [[(10.0,10.0),(10.0,20.0),(20.0,20.0),(20.0,10.0),(10.0,10.0)]] \
            ], 'Array(Array(Array(Tuple(Float64, Float64))))') AS mp",
        )
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(block.row_count(), 1);
    let col = block
        .column::<MultiPolygon>("mp")
        .expect("test operation failed");
    let mp = col.get(0).expect("test operation failed");

    assert_eq!(mp.0.len(), 2, "multipolygon should have 2 polygons");

    // Poly 0: 1 ring (outer)
    let poly0 = &mp.0[0];
    assert_eq!(poly0.0.len(), 1);
    assert_eq!(poly0.0[0].0.len(), 4);
    assert_eq!(poly0.0[0].0[0], Point(0.0, 0.0));

    // Poly 1: 1 ring (outer)
    let poly1 = &mp.0[1];
    assert_eq!(poly1.0.len(), 1);
    assert_eq!(poly1.0[0].0.len(), 5);
    assert_eq!(poly1.0[0].0[0], Point(10.0, 10.0));

    eprintln!("SUCCESS: MultiPolygon roundtrip (2 polygons)!");
}

// ═══════════════════════════════════════════════════════════════════
// test_multipolygon_with_holes — MultiPolygon where one polygon has holes
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multipolygon_with_holes() {
    let client = common::connect_client().await;

    // Poly 0: square with hole
    //   Outer: [(0,0),(5,0),(5,5),(0,5),(0,0)]
    //   Hole:  [(1,1),(1,4),(4,4),(4,1),(1,1)]
    // Poly 1: simple triangle
    //   Outer: [(100,100),(102,100),(101,102),(100,100)]
    let block = client
        .query(
            "SELECT CAST([ \
                [[(0.0,0.0),(5.0,0.0),(5.0,5.0),(0.0,5.0),(0.0,0.0)], \
                 [(1.0,1.0),(1.0,4.0),(4.0,4.0),(4.0,1.0),(1.0,1.0)]], \
                [[(100.0,100.0),(102.0,100.0),(101.0,102.0),(100.0,100.0)]] \
            ], 'Array(Array(Array(Tuple(Float64, Float64))))') AS mp",
        )
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(block.row_count(), 1);
    let col = block
        .column::<MultiPolygon>("mp")
        .expect("test operation failed");
    let mp = col.get(0).expect("test operation failed");

    assert_eq!(mp.0.len(), 2);

    // Poly 0 has 2 rings (outer + hole)
    let poly0 = &mp.0[0];
    assert_eq!(poly0.0.len(), 2);
    assert_eq!(poly0.0[0].0.len(), 5); // outer ring
    assert_eq!(poly0.0[1].0.len(), 5); // hole ring

    // Poly 1 has 1 ring
    let poly1 = &mp.0[1];
    assert_eq!(poly1.0.len(), 1);
    assert_eq!(poly1.0[0].0.len(), 4);

    eprintln!("SUCCESS: MultiPolygon with holes (2 polys, one with hole)!");
}

// ═══════════════════════════════════════════════════════════════════
// test_geo_insert_select — Native insert + SELECT with Point type
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_geo_insert_select() {
    let client = common::connect_client().await;

    // Create temp table with Tuple(Float64, Float64) column (Point wire format)
    client
        .execute("DROP TABLE IF EXISTS st_geo_test")
        .await
        .expect("test operation failed");
    client
        .execute("CREATE TABLE st_geo_test (id UInt64, location Tuple(Float64, Float64)) ENGINE = Memory")
        .await
        .expect("test operation failed");

    // Begin Native INSERT
    let mut session = client
        .begin_insert("st_geo_test")
        .await
        .expect("test operation failed");

    // Build Point column data: 3 points as raw f64 pairs
    // Row 0: id=1, location=(1.0, 2.0)
    // Row 1: id=2, location=(3.0, 4.0)
    // Row 2: id=3, location=(5.0, 6.0)

    // id column (UInt64): 3 values
    let id_data: Vec<u8> = [1u64, 2u64, 3u64]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();

    // location column (Tuple(Float64, Float64)): ClickHouse Native stores all
    // x values first, then all y values.
    let loc_data: Vec<u8> = [1.0f64, 3.0, 5.0, 2.0, 4.0, 6.0]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();

    let block = Block {
        columns: vec![
            ColumnInfo {
                name: "id".into(),
                type_name: "UInt64".into(),
                data: bytes::Bytes::from(id_data),
                lc_materialized: bytes::Bytes::new(),
            },
            ColumnInfo {
                name: "location".into(),
                type_name: "Tuple(Float64, Float64)".into(),
                data: bytes::Bytes::from(loc_data),
                lc_materialized: bytes::Bytes::new(),
            },
        ],
        rows: 3,
    };

    session
        .send_data(&block)
        .await
        .expect("test operation failed");
    session.end().await.expect("test operation failed");
    eprintln!("Inserted 3 rows with Point data");

    // Read back and verify using Point type
    let result = client
        .query("SELECT id, location FROM st_geo_test ORDER BY id")
        .block()
        .await
        .expect("test operation failed");

    assert_eq!(result.row_count(), 3);

    // Read id column
    let ids = result.column::<u64>("id").expect("test operation failed");
    assert_eq!(ids.get(0).expect("test operation failed"), 1);
    assert_eq!(ids.get(1).expect("test operation failed"), 2);
    assert_eq!(ids.get(2).expect("test operation failed"), 3);

    // Read location column as Point
    let locs = result
        .column::<Point>("location")
        .expect("test operation failed");
    assert_eq!(locs.len(), 3);
    assert_eq!(locs.get(0).expect("test operation failed"), Point(1.0, 2.0));
    assert_eq!(locs.get(1).expect("test operation failed"), Point(3.0, 4.0));
    assert_eq!(locs.get(2).expect("test operation failed"), Point(5.0, 6.0));

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS st_geo_test")
        .await
        .expect("test operation failed");
    eprintln!("SUCCESS: Native insert + SELECT with Point type!");
}

// ═══════════════════════════════════════════════════════════════════
// test_empty_geo_columns — empty result sets for each geo type
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_empty_geo_columns() {
    let block = Block {
        columns: vec![ColumnInfo {
            name: "p".into(),
            type_name: "Tuple(Float64, Float64)".into(),
            data: bytes::Bytes::new(),
            lc_materialized: bytes::Bytes::new(),
        }],
        rows: 0,
    };
    let col = block.column::<Point>("p").expect("test operation failed");
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
    eprintln!("SUCCESS: Empty Point column!");

    let block = Block {
        columns: vec![ColumnInfo {
            name: "r".into(),
            type_name: "Array(Tuple(Float64, Float64))".into(),
            data: bytes::Bytes::new(),
            lc_materialized: bytes::Bytes::new(),
        }],
        rows: 0,
    };
    let col = block.column::<Ring>("r").expect("test operation failed");
    assert_eq!(col.len(), 0);
    eprintln!("SUCCESS: Empty Ring column!");

    let block = Block {
        columns: vec![ColumnInfo {
            name: "p".into(),
            type_name: "Array(Array(Tuple(Float64, Float64)))".into(),
            data: bytes::Bytes::new(),
            lc_materialized: bytes::Bytes::new(),
        }],
        rows: 0,
    };
    let col = block.column::<Polygon>("p").expect("test operation failed");
    assert_eq!(col.len(), 0);
    eprintln!("SUCCESS: Empty Polygon column!");

    let block = Block {
        columns: vec![ColumnInfo {
            name: "mp".into(),
            type_name: "Array(Array(Array(Tuple(Float64, Float64))))".into(),
            data: bytes::Bytes::new(),
            lc_materialized: bytes::Bytes::new(),
        }],
        rows: 0,
    };
    let col = block
        .column::<MultiPolygon>("mp")
        .expect("test operation failed");
    assert_eq!(col.len(), 0);
    eprintln!("SUCCESS: Empty MultiPolygon column!");
}
