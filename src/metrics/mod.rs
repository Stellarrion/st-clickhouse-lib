//! Basic Prometheus metrics for st-clickhouse.
//!
//! Tracks query counts, errors, pool utilization, and connection duration.
//! Emits Prometheus text format — no prometheus crate dependency needed.
//!
//! ```rust
//! use st_clickhouse::metrics::Metrics;
//! use std::sync::atomic::Ordering;
//! let metrics = Metrics::new();
//! metrics.queries_total.fetch_add(1, Ordering::Relaxed);
//! println!("{}", metrics.format());
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

/// Simple atomic metrics counters. Outputs Prometheus exposition format.
#[derive(Default)]
pub struct Metrics {
    /// Total queries executed.
    pub queries_total: AtomicU64,
    /// Queries that failed with an error.
    pub queries_errors: AtomicU64,
    /// Queries that were retried.
    pub queries_retried: AtomicU64,
    /// Total bytes received from the server.
    pub bytes_received: AtomicU64,
    /// Pool slot count.
    pub pool_slots: AtomicU64,
    /// Pool connections currently in use.
    pub pool_in_use: AtomicU64,
    /// Total connection errors.
    pub connection_errors: AtomicU64,
}

impl Metrics {
    pub const fn new() -> Self {
        Self {
            queries_total: AtomicU64::new(0),
            queries_errors: AtomicU64::new(0),
            queries_retried: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            pool_slots: AtomicU64::new(0),
            pool_in_use: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
        }
    }

    /// Emit metrics in Prometheus text format.
    pub fn format(&self) -> String {
        let mut out = String::new();
        push_metric(
            &mut out,
            "st_clickhouse_queries_total",
            "Total queries executed",
            self.queries_total.load(Ordering::Relaxed),
        );
        push_metric(
            &mut out,
            "st_clickhouse_queries_errors",
            "Queries that failed",
            self.queries_errors.load(Ordering::Relaxed),
        );
        push_metric(
            &mut out,
            "st_clickhouse_queries_retried",
            "Queries that were retried",
            self.queries_retried.load(Ordering::Relaxed),
        );
        push_metric(
            &mut out,
            "st_clickhouse_bytes_received",
            "Total bytes received",
            self.bytes_received.load(Ordering::Relaxed),
        );
        push_metric(
            &mut out,
            "st_clickhouse_pool_slots",
            "Pool connection slots",
            self.pool_slots.load(Ordering::Relaxed),
        );
        push_metric(
            &mut out,
            "st_clickhouse_pool_in_use",
            "Pool connections currently in use",
            self.pool_in_use.load(Ordering::Relaxed),
        );
        push_metric(
            &mut out,
            "st_clickhouse_connection_errors",
            "Connection errors",
            self.connection_errors.load(Ordering::Relaxed),
        );
        out
    }
}

pub(crate) struct QueryMetricGuard {
    metrics: Option<&'static Metrics>,
    count: u64,
    failed: bool,
}

impl QueryMetricGuard {
    pub(crate) fn new(metrics: Option<&'static Metrics>, count: u64) -> Self {
        if let Some(metrics) = metrics {
            metrics.queries_total.fetch_add(count, Ordering::Relaxed);
        }
        Self {
            metrics,
            count,
            failed: true,
        }
    }

    pub(crate) fn retry(&self) {
        if let Some(metrics) = self.metrics {
            metrics.queries_retried.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn succeed(mut self) {
        self.failed = false;
    }
}

impl Drop for QueryMetricGuard {
    fn drop(&mut self) {
        if let Some(metrics) = self.failed.then_some(self.metrics).flatten() {
            metrics
                .queries_errors
                .fetch_add(self.count, Ordering::Relaxed);
        }
    }
}

fn push_metric(out: &mut String, name: &str, desc: &str, value: u64) {
    use std::fmt::Write;
    let _ = write!(
        out,
        "# HELP {name} {desc}\n# TYPE {name} counter\n{name} {value}\n"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_format() {
        let m = Metrics::new();
        let output = m.format();
        assert!(output.contains("st_clickhouse_queries_total"));
        assert!(output.contains("counter"));
        assert!(output.contains("0"));
    }

    #[test]
    fn test_metrics_increment() {
        let m = Metrics::new();
        m.queries_total.fetch_add(5, Ordering::Relaxed);
        assert!(m.format().contains("st_clickhouse_queries_total 5"));
    }

    #[test]
    fn test_query_metric_guard_success_and_error() {
        let metrics = Box::leak(Box::new(Metrics::new()));

        let guard = QueryMetricGuard::new(Some(metrics), 1);
        guard.succeed();
        assert_eq!(metrics.queries_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.queries_errors.load(Ordering::Relaxed), 0);

        let guard = QueryMetricGuard::new(Some(metrics), 2);
        guard.retry();
        drop(guard);
        assert_eq!(metrics.queries_total.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.queries_errors.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.queries_retried.load(Ordering::Relaxed), 1);
    }
}
