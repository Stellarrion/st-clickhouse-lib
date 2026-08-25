//! Type roundtrip tests — INSERT then SELECT exact-value verification.
//!
//! For each supported ClickHouse data type:
//!   1. Create a table with the type
//!   2. INSERT known values via INSERT ... VALUES
//!   3. SELECT back
//!   4. Verify exact values match
//!
//! Requirements: ClickHouse server (external or local ClickHouse).

mod common;

#[macro_export]
macro_rules! scalar_roundtrip_test {
    ($name:ident, $table:literal, $ch_type:literal, $values:literal, $rust_ty:ty, $expected:expr, $success:literal) => {
        #[tokio::test]
        async fn $name() {
            let client = $crate::common::connect_client().await;
            client
                .execute(concat!("DROP TABLE IF EXISTS ", $table))
                .await
                .expect("test operation failed");
            client
                .execute(concat!(
                    "CREATE TABLE ",
                    $table,
                    " (val ",
                    $ch_type,
                    ") ENGINE = Memory"
                ))
                .await
                .expect("test operation failed");
            client
                .execute(concat!("INSERT INTO ", $table, " VALUES ", $values))
                .await
                .expect("test operation failed");

            let rows: Vec<($rust_ty,)> = client
                .query(concat!("SELECT val FROM ", $table, " ORDER BY val"))
                .fetch()
                .await
                .expect("test operation failed");
            assert_eq!(rows, $expected);
            eprintln!($success);
        }
    };
}

fn days_since_epoch(y: u16, m: u16, d: u16) -> u16 {
    let mut days = 0u32;
    for year in 1970..y as u32 {
        days += if is_leap(year) { 366 } else { 365 };
    }
    for month in 1..m as u32 {
        days += days_in_month(y as u32, month);
    }
    days += d as u32 - 1;
    days as u16
}

fn is_leap(y: u32) -> bool {
    (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400)
}

fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        },
        _ => 0,
    }
}

fn dt_secs(y: u16, mo: u16, d: u16, h: u32, mi: u32, s: u32) -> u32 {
    days_since_epoch(y, mo, d) as u32 * 86400 + h * 3600 + mi * 60 + s
}

mod type_roundtrip_test {
    mod all_types;
    mod complex;
    mod network;
    mod numeric;
    mod strings;
    mod temporal_decimal;
}
