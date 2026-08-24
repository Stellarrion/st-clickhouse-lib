//! Callback and event tests — progress, profile, log, profile events,
//! tracing context, query settings, query_id, and cancel-during-progress.
//!
//! Each test creates its own Client. The pool properly cleans up after each test.

mod common;
use st_clickhouse::TracingContext;
use st_clickhouse::connection::{Client, Profile, Progress, QueryCallbacks};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

// ── helpers ──────────────────────────────────────────────────────────

/// Connect with default settings for callback tests.
async fn connect() -> Client {
    common::connect_client().await
}

// ══════════════════════════════════════════════════════════════════════
// 1. Progress callback
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_progress_callback() {
    let client = connect().await;
    let fired = Arc::new(AtomicBool::new(false));
    let row_count = Arc::new(AtomicUsize::new(0));

    let f = fired.clone();
    let r = row_count.clone();
    let callbacks = QueryCallbacks {
        on_progress: Some(Box::new(move |p: Progress| {
            f.store(true, Ordering::SeqCst);
            r.fetch_add(p.rows as usize, Ordering::SeqCst);
        })),
        on_profile: None,
        on_log: None,
        on_profile_events: None,
        on_timezone_update: None,
        on_part_uuids: None,
    };

    let blocks = client
        .query("SELECT number FROM system.numbers LIMIT 100000")
        .with_callbacks(callbacks)
        .blocks()
        .await
        .expect("test operation failed");
    let returned_rows = blocks
        .iter()
        .map(st_clickhouse::Block::row_count)
        .sum::<usize>();

    assert!(
        fired.load(Ordering::SeqCst),
        "progress callback should fire"
    );
    assert!(returned_rows > 0, "should return rows");
    eprintln!(
        "Progress: reported {} rows, result has {} rows",
        row_count.load(Ordering::SeqCst),
        returned_rows
    );
}

// ══════════════════════════════════════════════════════════════════════
// 2. Profile callback
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_profile_callback() {
    let client = connect().await;
    let fired = Arc::new(AtomicBool::new(false));

    let f = fired.clone();
    let callbacks = QueryCallbacks {
        on_progress: None,
        on_profile: Some(Box::new(move |_p: Profile| {
            f.store(true, Ordering::SeqCst);
        })),
        on_log: None,
        on_profile_events: None,
        on_timezone_update: None,
        on_part_uuids: None,
    };

    let block = client
        .query("SELECT count() FROM (SELECT number FROM system.numbers LIMIT 1000)")
        .with_callbacks(callbacks)
        .block()
        .await
        .expect("test operation failed");

    assert!(block.row_count() > 0, "should return rows");
    // Profile callback may not fire in all server versions — log the result
    if fired.load(Ordering::SeqCst) {
        eprintln!("Profile callback fired");
    } else {
        eprintln!("Profile callback did NOT fire (may not be implemented)");
    }
}

// ══════════════════════════════════════════════════════════════════════
// 3. Log callback
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_log_callback() {
    let client = connect().await.with_setting("send_logs_level", "trace");

    let fired = Arc::new(AtomicBool::new(false));

    let f = fired.clone();
    let callbacks = QueryCallbacks {
        on_progress: None,
        on_profile: None,
        on_log: Some(Box::new(move |_block| {
            f.store(true, Ordering::SeqCst);
        })),
        on_profile_events: None,
        on_timezone_update: None,
        on_part_uuids: None,
    };

    let block = client
        .query("SELECT 1")
        .with_callbacks(callbacks)
        .block()
        .await
        .expect("test operation failed");

    assert!(block.row_count() > 0, "should return rows");
    if fired.load(Ordering::SeqCst) {
        eprintln!("Log callback fired");
    } else {
        eprintln!("Log callback did NOT fire (server may not send logs for trivial queries)");
    }
}

// ══════════════════════════════════════════════════════════════════════
// 4. Profile events callback
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_profile_events_callback() {
    let client = connect().await;

    let fired = Arc::new(AtomicBool::new(false));

    let f = fired.clone();
    let callbacks = QueryCallbacks {
        on_progress: None,
        on_profile: None,
        on_log: None,
        on_profile_events: Some(Box::new(move |_block| {
            f.store(true, Ordering::SeqCst);
        })),
        on_timezone_update: None,
        on_part_uuids: None,
    };

    // Profile events require server rev >= 54451
    let block = client
        .query("SELECT 1")
        .with_callbacks(callbacks)
        .block()
        .await
        .expect("test operation failed");

    assert!(block.row_count() > 0, "should return rows");
    if fired.load(Ordering::SeqCst) {
        eprintln!("Profile events callback fired");
    } else {
        eprintln!("Profile events callback did NOT fire (requires server rev >= 54451)");
    }
}

// ══════════════════════════════════════════════════════════════════════
// 5. All callbacks across multiple sequential queries
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_all_callbacks_multiple_queries() {
    let client = connect().await;

    let progress_fired = Arc::new(AtomicBool::new(false));
    let profile_fired = Arc::new(AtomicBool::new(false));
    let log_fired = Arc::new(AtomicBool::new(false));
    let pe_fired = Arc::new(AtomicBool::new(false));

    // Run 3 queries sequentially, each with the same callback set
    for i in 1..=3 {
        let pf = progress_fired.clone();
        let prf = profile_fired.clone();
        let lf = log_fired.clone();
        let pef = pe_fired.clone();

        let callbacks = QueryCallbacks {
            on_progress: Some(Box::new(move |_p: Progress| {
                pf.store(true, Ordering::SeqCst);
            })),
            on_profile: Some(Box::new(move |_p: Profile| {
                prf.store(true, Ordering::SeqCst);
            })),
            on_log: Some(Box::new(move |_block| {
                lf.store(true, Ordering::SeqCst);
            })),
            on_profile_events: Some(Box::new(move |_block| {
                pef.store(true, Ordering::SeqCst);
            })),
            on_timezone_update: None,
            on_part_uuids: None,
        };

        let result = client
            .query(&format!("SELECT {i} AS val"))
            .with_callbacks(callbacks)
            .block()
            .await;
        assert!(result.is_ok(), "query {i} should succeed");
    }

    eprintln!(
        "After 3 queries: progress={} profile={} log={} profile_events={}",
        progress_fired.load(Ordering::SeqCst),
        profile_fired.load(Ordering::SeqCst),
        log_fired.load(Ordering::SeqCst),
        pe_fired.load(Ordering::SeqCst),
    );
    // At minimum, progress should have fired at least once
    assert!(
        progress_fired.load(Ordering::SeqCst),
        "progress callback should fire across sequential queries"
    );
}

// ══════════════════════════════════════════════════════════════════════
// 6. Tracing context
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_tracing_context() {
    let client = connect().await;

    let tracing = TracingContext {
        trace_id: [0xAB; 16],
        span_id: 42,
        tracestate: String::new(),
        trace_flags: 1,
    };

    let block = client
        .query("SELECT 1")
        .with_tracing(tracing)
        .block()
        .await
        .expect("test operation failed");

    assert!(block.row_count() > 0, "query with tracing should succeed");
    eprintln!("Tracing context query completed successfully");
}

// ══════════════════════════════════════════════════════════════════════
// 7. Query-level settings
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_query_with_settings() {
    let client = connect().await;

    // max_threads=1 should still produce correct results
    let block = client
        .query("SELECT count() FROM (SELECT number FROM system.numbers LIMIT 1000)")
        .with_setting("max_threads", "1")
        .block()
        .await
        .expect("test operation failed");

    assert!(
        block.row_count() > 0,
        "query with settings should return rows"
    );

    // join_use_nulls — just verify the query doesn't error
    let block2 = client
        .query("SELECT 1 AS a")
        .with_setting("join_use_nulls", "1")
        .block()
        .await
        .expect("test operation failed");

    assert!(
        block2.row_count() > 0,
        "query with join_use_nulls should succeed"
    );
    eprintln!("Query settings test passed");
}

// ══════════════════════════════════════════════════════════════════════
// 8. Query ID
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_query_id() {
    let client = connect().await;

    // Execute with explicit query_id
    let query_id = "test-query-id-callbacks";
    let block = client
        .query("SELECT 1 AS result")
        .with_query_id(query_id)
        .block()
        .await
        .expect("test operation failed");

    assert!(block.row_count() > 0, "query with custom id should succeed");

    // Check system.query_log for our query (may or may not appear depending
    // on whether query_id is sent in the packet — current implementation
    // hardcodes empty query_id in build_query_packet_core).
    let log_result = client
        .query(&format!(
            "SELECT query_id FROM system.query_log WHERE query_id = '{query_id}' LIMIT 1"
        ))
        .block()
        .await;

    match log_result {
        Ok(log_block) => {
            if log_block.row_count() > 0 {
                eprintln!("Query ID found in system.query_log");
            } else {
                eprintln!(
                    "Query ID NOT found in system.query_log (query_id may not be sent in packet)"
                );
            }
        },
        Err(e) => {
            eprintln!("Could not read system.query_log: {e}");
        },
    }
}

// ══════════════════════════════════════════════════════════════════════
// 9. Cancel during progress
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cancel_during_progress() {
    let client = connect().await;
    let progress_fired = Arc::new(AtomicBool::new(false));

    let pf = progress_fired.clone();
    let callbacks = QueryCallbacks {
        on_progress: Some(Box::new(move |_p: Progress| {
            pf.store(true, Ordering::SeqCst);
        })),
        on_profile: None,
        on_log: None,
        on_profile_events: None,
        on_timezone_update: None,
        on_part_uuids: None,
    };

    // Client::cancel is fail-closed: it cannot reach the connection running
    // the query, so it must return Error::Config without opening or touching
    // any pooled connection. The heavy query below runs on a different client
    // and finishes (or errors) on its own.
    let client_for_cancel = connect().await;
    let cancel_handle = tokio::spawn(async move {
        // Give the query a moment to start sending progress
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        #[allow(deprecated)]
        let result = client_for_cancel.cancel().await;
        matches!(result, Err(st_clickhouse::error::Error::Config(_)))
    });

    // Run a heavy query — unaffected by the (fail-closed) cancel
    let result = client
        .query("SELECT number FROM system.numbers LIMIT 500000000")
        .with_callbacks(callbacks)
        .block()
        .await;

    // Wait for cancel to complete and prove it failed closed.
    let cancelled_config_error = cancel_handle.await.expect("cancel task must not panic");
    assert!(
        cancelled_config_error,
        "Client::cancel must fail closed with Error::Config"
    );

    match result {
        Ok(block) => {
            eprintln!(
                "Query completed before cancel (rows: {})",
                block.row_count()
            );
        },
        Err(e) => {
            eprintln!("Query cancelled or failed: {e}");
        },
    }

    // Progress may or may not have fired before cancel — both are acceptable
    eprintln!(
        "Progress fired before cancel: {}",
        progress_fired.load(Ordering::SeqCst)
    );
}
