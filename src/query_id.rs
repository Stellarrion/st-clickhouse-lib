//! Process-wide query-ID generation.
//!
//! Standard `st-ch-` query IDs are drawn from a single process-wide counter so
//! that a synchronous and an asynchronous query issued from the same process
//! can never mint the same ID. Per-module counters produced identical IDs,
//! which ClickHouse rejects with
//! `QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING` (216) when both queries are live
//! at once. Batch IDs intentionally keep their distinct `st-b-` prefix and
//! therefore their own counter state in `crate::connection::batch`.

use std::sync::atomic::{AtomicU64, Ordering};

const STANDARD_QUERY_ID_PREFIX: &[u8] = b"st-ch-";

static QUERY_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate the next standard `st-ch-` query ID into `buf`, returning its length.
///
/// Shared by the sync (`crate::sync`) and tokio (`crate::connection`) packet
/// builders; see the module docs for why there must be exactly one counter.
///
/// Concurrency: the counter uses `fetch_add`, so every call returns a distinct
/// slot even under races. The PID is read on every call rather than cached so a
/// process created with `fork` cannot inherit its parent's ID namespace.
/// `fetch_add` wraps silently and the ID masks the counter to its low 32 bits,
/// so IDs repeat only after 2^32 IDs from one process — the same window the
/// previous per-module counters had.
pub(crate) fn next_query_id(buf: &mut [u8; 22]) -> usize {
    next_query_id_with_prefix(buf, STANDARD_QUERY_ID_PREFIX, &QUERY_ID_COUNTER)
}

/// Generate a query ID with a custom `prefix` and counter state.
///
/// Used by batch IDs (`st-b-`), which stay distinct from standard query IDs.
pub(crate) fn next_query_id_with_prefix(
    buf: &mut [u8; 22], prefix: &[u8], counter: &AtomicU64,
) -> usize {
    buf[..prefix.len()].copy_from_slice(prefix);
    // Do not cache the PID: after `fork`, the child inherits static atomics but
    // must use its own query-ID namespace immediately.
    let process_prefix_value = (u64::from(std::process::id()) & 0xffff_ffff) << 32;
    let n = process_prefix_value | (counter.fetch_add(1, Ordering::Relaxed) & 0xffff_ffff);
    let mut started = false;
    let mut pos = prefix.len();
    for shift in (0..64).step_by(4).rev() {
        let digit = ((n >> shift) & 0x0f) as u8;
        if digit != 0 || started || shift == 0 {
            started = true;
            buf[pos] = if digit < 10 {
                b'0' + digit
            } else {
                b'a' + (digit - 10)
            };
            pos += 1;
        }
    }
    pos
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// The exact entry the synchronous builder uses (`SyncClient` packet
    /// construction calls this directly).
    fn sync_path_next(buf: &mut [u8; 22]) -> usize {
        next_query_id(buf)
    }

    /// The exact entry the asynchronous builder uses when no custom ID was
    /// supplied (`query_id_bytes(None, ..)` on the tokio connection path).
    #[cfg(feature = "tokio")]
    fn async_path_next(buf: &mut [u8; 22]) -> usize {
        crate::connection::query_packet::query_id_bytes(None, buf).len()
    }

    fn assert_standard_id_format(id: &str) {
        assert!(
            id.starts_with("st-ch-"),
            "query ID must start with 'st-ch-': {id}"
        );
        let suffix = id.strip_prefix("st-ch-").expect("prefix checked above");
        assert!(!suffix.is_empty(), "query ID must have a non-empty suffix");
        assert!(id.len() <= 22, "query ID must fit the 22-byte buffer: {id}");
        for c in suffix.chars() {
            assert!(
                c.is_ascii_digit() || ('a'..='f').contains(&c),
                "query ID suffix must be lowercase hex: {id}"
            );
        }
    }

    /// Regression for the sync/async collision: interleaving the sync and
    /// async builder paths must never mint the same ID. Before the shared
    /// counter, iteration N of both loops produced byte-identical IDs.
    #[test]
    fn interleaved_sync_and_async_ids_are_unique() {
        let mut seen = HashSet::new();
        let mut buf = [0u8; 22];
        for _ in 0..512 {
            let len = sync_path_next(&mut buf);
            let id = std::str::from_utf8(&buf[..len])
                .expect("sync query ID is ASCII")
                .to_owned();
            assert_standard_id_format(&id);
            assert!(seen.insert(id), "duplicate sync query ID after interleave");

            #[cfg(feature = "tokio")]
            {
                let len = async_path_next(&mut buf);
                let id = std::str::from_utf8(&buf[..len])
                    .expect("async query ID is ASCII")
                    .to_owned();
                assert_standard_id_format(&id);
                assert!(seen.insert(id), "duplicate async query ID after interleave");
            }
        }
    }

    /// Concurrent generation through both builder paths must stay unique:
    /// several threads mint IDs via the sync and async entries at once and
    /// the union across all threads must contain no duplicates.
    #[test]
    fn concurrent_sync_and_async_ids_are_unique() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 512;

        let all_ids: Vec<String> = std::thread::scope(|scope| {
            let mut results = Vec::with_capacity(THREADS);
            for _ in 0..THREADS {
                results.push(scope.spawn(|| {
                    let mut local = Vec::with_capacity(PER_THREAD * 2);
                    let mut buf = [0u8; 22];
                    for _ in 0..PER_THREAD {
                        let len = sync_path_next(&mut buf);
                        local.push(
                            std::str::from_utf8(&buf[..len])
                                .expect("sync query ID is ASCII")
                                .to_owned(),
                        );
                        #[cfg(feature = "tokio")]
                        {
                            let len = async_path_next(&mut buf);
                            local.push(
                                std::str::from_utf8(&buf[..len])
                                    .expect("async query ID is ASCII")
                                    .to_owned(),
                            );
                        }
                    }
                    local
                }));
            }
            results
                .into_iter()
                .flat_map(|handle| handle.join().expect("generator thread panicked"))
                .collect()
        });

        let expected = PER_THREAD * THREADS * if cfg!(feature = "tokio") { 2 } else { 1 };
        assert_eq!(
            all_ids.len(),
            expected,
            "every generated ID must be collected"
        );
        let unique: HashSet<&str> = all_ids.iter().map(String::as_str).collect();
        assert_eq!(
            unique.len(),
            all_ids.len(),
            "sync and async query IDs drawn concurrently must never collide"
        );
        for id in &all_ids {
            assert_standard_id_format(id);
        }
    }

    /// Both paths draw from one sequence: IDs taken back-to-back must share
    /// the process prefix and the later draw must be strictly greater. Exact
    /// consecutiveness is not asserted because parallel test threads also
    /// consume counter slots from the same process-wide state.
    #[cfg(feature = "tokio")]
    #[test]
    fn sync_and_async_share_one_counter_sequence() {
        let mut buf = [0u8; 22];
        let len = sync_path_next(&mut buf);
        let sync_id = std::str::from_utf8(&buf[..len])
            .expect("sync query ID is ASCII")
            .to_owned();

        #[cfg(feature = "tokio")]
        {
            let len = async_path_next(&mut buf);
            let async_id = std::str::from_utf8(&buf[..len])
                .expect("async query ID is ASCII")
                .to_owned();
            let s = sync_id.strip_prefix("st-ch-").expect("sync prefix checked");
            let a = async_id
                .strip_prefix("st-ch-")
                .expect("async prefix checked");
            let s_val = u64::from_str_radix(s, 16).expect("sync suffix is hex");
            let a_val = u64::from_str_radix(a, 16).expect("async suffix is hex");
            assert_eq!(
                s_val >> 32,
                a_val >> 32,
                "both paths must use the same process prefix"
            );
            assert!(
                a_val > s_val,
                "both paths must advance the same monotonically increasing counter"
            );
        }
    }

    /// Batch IDs keep their own `st-b-` namespace and never look like
    /// standard query IDs.
    #[test]
    fn batch_prefix_stays_distinct() {
        static BATCH_COUNTER: AtomicU64 = AtomicU64::new(1);
        let mut buf = [0u8; 22];
        let len = next_query_id_with_prefix(&mut buf, b"st-b-", &BATCH_COUNTER);
        let id = std::str::from_utf8(&buf[..len]).expect("batch query ID is ASCII");
        assert!(id.starts_with("st-b-"));
        assert!(!id.starts_with("st-ch-"));
    }
}
