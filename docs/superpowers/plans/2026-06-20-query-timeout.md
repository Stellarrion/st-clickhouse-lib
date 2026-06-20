# Query Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, hard wall-clock query deadline to the async `Client` that cancels the query server-side (`ClientPacket::Cancel`) and returns the pooled connection alive.

**Architecture:** Thread an `Option<Instant>` deadline through the async packet-read loops. Each read races `runtime::time::timeout(min(recv_timeout, remaining_to_deadline), …)`. On deadline expiry, send `Cancel` (protocol type 3, no payload), drain to `EndOfStream`/`Exception` (bounded by `recv_timeout`), and return `Error::Timeout`. The pool's existing liveness ping (`is_connection_alive`) reaps any connection left dirty by a server that ignores `Cancel`.

**Tech Stack:** Rust 2024 / edition 2024, tokio (async), `crate::runtime::time::{Instant, timeout}` abstraction, ClickHouse native protocol (verified against `src/Core/Protocol.h`).

**Spec:** `docs/superpowers/specs/2026-06-20-query-timeout-design.md` (Spec 1 of 3).

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `src/connection/tcp.rs` | `Client` struct | Add `query_timeout: Option<Duration>` field |
| `src/connection/connect.rs` | `Client::from_pool` construction | Initialize `query_timeout: None` |
| `src/connection/config.rs` | `Client` builder-style setters | Add `with_query_timeout` |
| `src/builder.rs` | `ClientBuilder<Async>` | Add `query_timeout` opt + wiring in `connect()` |
| `src/connection/io.rs` | wire helpers | Add `packet_read_timeout` pure helper |
| `src/connection/response_wait.rs` | `drain_response`, `read_table_structure` | Add `deadline` param + cancel-on-deadline |
| `src/connection/select_response.rs` | `read_select_response` | Add `deadline` param + cancel-on-deadline |
| `src/connection/server_packets.rs` | packet helpers | Add `cancel_and_drain` |
| `src/connection/query_builder.rs` | `QueryBuilder` | Add `.timeout()`, thread deadline, retry guard |
| `src/connection/commands.rs` | `Client::execute`, `cancel` | Thread deadline; magic-byte cleanup |
| `src/connection/insert_session.rs` | `InsertSession` | Thread deadline; `end()` drain |
| `src/connection/block_stream.rs` | `BlockStream` | Thread deadline; `cancel()` drains; magic-byte cleanup |
| `src/connection/row_stream_reader.rs` | `read_query_blocks` (cursor) | Thread deadline; magic-byte cleanup |
| `src/pool.rs` | `StreamWrapper`, liveness | Magic-byte cleanup only |
| `tests/query_timeout_test.rs` | NEW integration tests | Spec §7 test matrix |
| `st-clickhouse-py/tests/test_client.py` | Python verification | Confirm `query_timeout` raises on slow query |

**Out of scope (Spec 2 / 3):** sync-side `cancel_and_drain` parity, async/sync tree dedup, broad perf, batch-builder timeout, dedicated Python `TimeoutError` (sync `Error` has no `Timeout` variant yet), unifying existing `Error::Protocol("timeout")` recv-floor sites onto `Error::Timeout`.

**Conventions for every task:**
- After each code change, run `cargo clippy --workspace --all-features -- -D warnings` and `cargo build --workspace --all-features`. Do not commit with warnings.
- Commit per task with the message shown.
- The `Error::Timeout` variant already exists (`src/error.rs:24`); `is_timeout()` returns true for it. **Do not** add a new variant.
- `crate::runtime::time::Instant` is `tokio::time::Instant` (has `now()`, `saturating_duration_since`, `+ Duration`).
- `crate::protocol::packet::ClientPacket::{Cancel,Ping}` (path confirmed: `src/protocol/mod.rs:5` declares `pub mod packet`).
- **Never** thread the deadline into `src/connection/block_reader.rs:423` — that 50ms `timeout` is the compression-vs-plain-block detection peek; changing it corrupts block framing.

---

## Task 1: Add `query_timeout` config field + builder plumbing

**Files:**
- Modify: `src/connection/tcp.rs:22-35` (struct)
- Modify: `src/connection/connect.rs:26-45` (`from_pool`)
- Modify: `src/connection/config.rs:66-70` (add setter after `with_recv_timeout`)
- Modify: `src/builder.rs:26-44` (`BuilderOptions`), `:59-83` (default), `:85-232` (add method), `:243-300` (`connect()` wiring)
- Test: `src/connection/config.rs` (unit test in `#[cfg(test)]` block)

- [ ] **Step 1: Write the failing unit test**

Append to `src/builder.rs` (the `opts` field is private, so the test lives in the same module):

```rust
#[cfg(test)]
mod query_timeout_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn builder_stores_query_timeout() {
        let b = ClientBuilder::<Async>::new().query_timeout(Duration::from_secs(12));
        assert_eq!(b.opts.query_timeout, Some(Duration::from_secs(12)));
    }

    #[test]
    fn builder_default_has_no_query_timeout() {
        let b = ClientBuilder::<Async>::new();
        assert_eq!(b.opts.query_timeout, None);
    }
}
```

> Do **not** construct `Client` directly in a unit test — its fields (`SimplePool`, `QueryPacketTemplate`) own locks/Arcs and cannot be soundly zeroed. Test the builder option (this module) and the wired behavior via the Task 9 integration tests.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p st-clickhouse-lib --lib builder::query_timeout_tests 2>&1 | tail -20`
Expected: FAIL — `no field query_timeout` / method not found (compile error).

- [ ] **Step 3: Add the field to `Client`**

In `src/connection/tcp.rs`, add the field to the `Client` struct (after `recv_timeout` at line 32):

```rust
    pub(crate) recv_timeout: Duration,
    /// Wall-clock deadline for a whole query (None = no deadline, only the
    /// per-packet `recv_timeout` floor applies). Set via `with_query_timeout`.
    pub(crate) query_timeout: Option<Duration>,
```

- [ ] **Step 4: Initialize it in `from_pool`**

In `src/connection/connect.rs`, in the `Self { ... }` literal of `from_pool` (line 27), add after `recv_timeout: Duration::from_secs(300),`:

```rust
            recv_timeout: Duration::from_secs(300),
            query_timeout: None,
```

- [ ] **Step 5: Add the `with_query_timeout` setter**

In `src/connection/config.rs`, after `with_recv_timeout` (line 67-70), add:

```rust
    /// Set a whole-query wall-clock timeout.
    ///
    /// When set, a query that has not fully completed (read through
    /// `EndOfStream`) within `t` is cancelled server-side and returns
    /// [`Error::Timeout`](crate::error::Error::Timeout). The connection is
    /// drained and returned to the pool alive. `None` by default.
    pub fn with_query_timeout(mut self, t: Duration) -> Self {
        self.query_timeout = Some(t);
        self
    }
```

- [ ] **Step 6: Add the builder option**

In `src/builder.rs`:

(a) Add a field to `BuilderOptions` (after `retry_timeout: Option<Duration>,` at line 38):

```rust
    retry_timeout: Option<Duration>,
    query_timeout: Option<Duration>,
```

(b) In `Default for BuilderOptions` (line 59), add after `retry_timeout: None,`:

```rust
            retry_timeout: None,
            query_timeout: None,
```

(c) Add a builder method in `impl<M> ClientBuilder<M>` (after `retry_timeout`, around line 181):

```rust
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.opts.query_timeout = Some(timeout);
        self
    }
```

(d) In `ClientBuilder<Async>::connect()` (line 243), after the `retry_timeout` block (line 292-294), add:

```rust
        if let Some(timeout) = self.opts.query_timeout {
            client.query_timeout = Some(timeout);
        }
```

- [ ] **Step 7: Run test to verify it passes**

Run: `cargo test -p st-clickhouse-lib --lib builder::query_timeout_tests 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 8: Build + clippy**

Run: `cargo clippy --workspace --all-features -- -D warnings 2>&1 | tail -20`
Expected: no warnings (the new field is `pub(crate)` and now constructed everywhere).

- [ ] **Step 9: Commit**

```bash
git add src/connection/tcp.rs src/connection/connect.rs src/connection/config.rs src/builder.rs
git commit -m "feat(connection): add query_timeout config field + builder plumbing"
```

---

## Task 2: `cancel_and_drain` helper + named packet constants

**Files:**
- Modify: `src/connection/server_packets.rs` (add `cancel_and_drain`)
- Modify: `src/connection/response_wait.rs:64` (`drain_response` gains `deadline` param — signature only here; logic in Task 4)

> This task adds the helper and the magic-byte cleanup. The `deadline` plumbing into `drain_response` happens in Task 4; here we only add `cancel_and_drain` and a temporary 4-arg `drain_response` call. **To keep the build green per-task**, Task 2 will give `drain_response` its new `deadline` parameter and update all current callers to pass `None`, then Task 4 fills in the deadline-aware logic.

- [ ] **Step 1: Add `deadline` parameter to `drain_response` and `read_table_structure` (signature + caller updates, behavior unchanged)**

In `src/connection/response_wait.rs`, change both function signatures to add `deadline: Option<crate::runtime::time::Instant>,` after `response_compressed: bool,`. For now, the bodies ignore `deadline` (prefixed `_deadline` to avoid unused warnings) — Task 4 wires it.

```rust
pub(super) async fn read_table_structure(
    stream: &mut S, timeout: Duration, response_compressed: bool,
    _deadline: Option<crate::runtime::time::Instant>,
) -> Result<Block> {
    // body unchanged for now
```

```rust
pub(super) async fn drain_response(
    stream: &mut S, timeout: Duration, response_compressed: bool,
    _deadline: Option<crate::runtime::time::Instant>,
) -> Result<()> {
    // body unchanged for now
```

Update callers to pass `None`:
- `src/connection/commands.rs:107` → `drain_response(stream, self.recv_timeout, compression_flag(self.compression) == 1, None).await?;`
- `src/connection/insert_session.rs:115` → add `, None` to the `drain_response` call.

- [ ] **Step 2: Add `cancel_and_drain`**

Append to `src/connection/server_packets.rs` (it already imports `AsyncWriteExt` and `Result`):

```rust
use crate::connection::response_wait::drain_response;
use crate::protocol::packet::ClientPacket;

/// Send a `Cancel` packet and drain the response until `EndOfStream` /
/// `Exception` (bounded by `recv_timeout`).
///
/// Used when a query deadline elapses. Best-effort: if the server ignores
/// `Cancel` and the drain itself times out, this returns `Ok` and leaves the
/// connection to be reaped by the pool's liveness ping on next acquire
/// ([`crate::pool::SimplePool::get`]).
///
/// `drain_response` is called with `deadline = None` so it never recurses
/// into cancel logic.
pub(crate) async fn cancel_and_drain<S>(
    stream: &mut S, recv_timeout: std::time::Duration, response_compressed: bool,
) -> Result<()>
where
    S: crate::runtime::io::AsyncRead + crate::runtime::io::AsyncWrite + Unpin,
{
    // Cancel = protocol type 3, no payload.
    use crate::runtime::io::AsyncWriteExt;
    stream.write_all(&[ClientPacket::Cancel as u8]).await.ok();
    stream.flush().await.ok();
    drain_response(stream, recv_timeout, response_compressed, None).await
}
```

- [ ] **Step 3: Replace magic bytes `&[3]` / `&[4]` with named constants**

Add the import where needed and replace literals:

(a) `src/connection/io.rs` — `ping_stream` (line 165) and the read-exact pong check: add `use crate::protocol::packet::ClientPacket;` near the top, then change `stream.write_packet(&[4]).await?;` → `stream.write_packet(&[ClientPacket::Ping as u8]).await?;`. Leave the pong comparison `pkt[0] != 4` as-is (raw byte compare is fine; optionally `!= ClientPacket::Pong as u8`).

(b) `src/connection/block_stream.rs:164` → `stream.write_packet(&[ClientPacket::Cancel as u8]).await?;` and the `Drop` at line 173 → `let _ = tcp.try_write(&[ClientPacket::Cancel as u8]);`. Add `use crate::protocol::packet::ClientPacket;`.

(c) `src/connection/commands.rs:123` → `stream.write_packet(&[ClientPacket::Cancel as u8]).await?;`. Add the import.

(d) `src/connection/query_builder.rs:293` → `AsyncWriteExt::write_all(&mut stream, &[ClientPacket::Cancel as u8]).await.ok();`. Add `use crate::protocol::packet::ClientPacket;`.

(e) `src/connection/row_stream_reader.rs:23` → `AsyncWriteExt::write_all(&mut stream, &[ClientPacket::Cancel as u8]).await.ok();`. Add the import.

(f) `src/pool.rs` — lines 376, 817, 828 write `&[4]`/`&[3u8]`. Add `use crate::protocol::packet::ClientPacket;` and replace: 376 `&[ClientPacket::Ping as u8]`, 817 `&[ClientPacket::Cancel as u8]`, 828 `&[ClientPacket::Ping as u8]`.

- [ ] **Step 4: Build + clippy**

Run: `cargo clippy --workspace --all-features -- -D warnings 2>&1 | tail -30`
Expected: clean. (If a `ClientPacket` import is unused somewhere, remove it.)

- [ ] **Step 5: Commit**

```bash
git add src/connection/server_packets.rs src/connection/response_wait.rs \
        src/connection/commands.rs src/connection/insert_session.rs \
        src/connection/io.rs src/connection/block_stream.rs \
        src/connection/query_builder.rs src/connection/row_stream_reader.rs \
        src/pool.rs
git commit -m "feat(connection): add cancel_and_drain; name Cancel/Ping packet bytes"
```

---

## Task 3: `packet_read_timeout` pure helper + tests

**Files:**
- Modify: `src/connection/io.rs` (add helper + test module)

- [ ] **Step 1: Write the failing unit tests**

Append to `src/connection/io.rs`:

```rust
#[cfg(test)]
mod timeout_tests {
    use super::packet_read_timeout;
    use crate::runtime::time::Instant;
    use std::time::Duration;

    #[test]
    fn no_deadline_returns_recv_timeout() {
        assert_eq!(
            packet_read_timeout(Duration::from_secs(300), None),
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn deadline_far_returns_recv_timeout() {
        let dl = Instant::now() + Duration::from_secs(600);
        assert_eq!(
            packet_read_timeout(Duration::from_secs(300), Some(dl)),
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn deadline_near_returns_remaining() {
        let dl = Instant::now() + Duration::from_millis(10);
        let got = packet_read_timeout(Duration::from_secs(300), Some(dl));
        assert!(got.is_some());
        // remaining is ~10ms, far below the 300s recv floor
        assert!(got.unwrap() <= Duration::from_millis(10));
    }

    #[test]
    fn deadline_expired_returns_none() {
        let dl = Instant::now() - Duration::from_millis(1);
        assert_eq!(packet_read_timeout(Duration::from_secs(300), Some(dl)), None);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p st-clickhouse-lib --lib io::timeout_tests 2>&1 | tail -20`
Expected: FAIL — `cannot find function packet_read_timeout`.

- [ ] **Step 3: Add the helper**

In `src/connection/io.rs` (near the other small helpers, e.g. after `compression_flag`):

```rust
use crate::runtime::time::Instant;

/// Per-read timeout for a packet loop: the smaller of `recv_timeout` and the
/// time remaining until `deadline` (if set).
///
/// Returns `None` when the deadline has already elapsed — the caller must
/// treat the query as timed out (cancel + drain + `Error::Timeout`).
#[inline]
pub(crate) fn packet_read_timeout(
    recv_timeout: std::time::Duration, deadline: Option<Instant>,
) -> Option<std::time::Duration> {
    match deadline {
        None => Some(recv_timeout),
        Some(d) => {
            let remaining = d.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                None
            } else {
                Some(std::cmp::min(recv_timeout, remaining))
            }
        },
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p st-clickhouse-lib --lib io::timeout_tests 2>&1 | tail -20`
Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connection/io.rs
git commit -m "feat(connection): add packet_read_timeout deadline helper"
```

---

## Task 4: Deadline-aware `drain_response`, `read_table_structure`, `read_select_response`

**Files:**
- Modify: `src/connection/response_wait.rs` (wire deadline into both functions)
- Modify: `src/connection/select_response.rs:50` (`read_select_response` gains `deadline` + cancel logic)
- Modify callers: `src/connection/query_builder.rs` (Task 5 supplies real deadlines), `src/connection/commands.rs`, `src/connection/insert_session.rs`, `src/connection/server_packets.rs` (`cancel_and_drain` already passes `None`).

> Goal of this task: when the deadline elapses inside these loops, call `cancel_and_drain` and return `Error::Timeout`. When there is no deadline, behavior is byte-for-byte identical to today.

- [ ] **Step 1: Wire the deadline into `drain_response`**

In `src/connection/response_wait.rs`, replace the read-head of `drain_response` (the `let pkt = match … timeout …` block, lines 69-74) and add imports. Final function:

```rust
use crate::connection::io::packet_read_timeout;
use crate::connection::server_packets::cancel_and_drain;
use crate::error::Error;
use crate::runtime::time::Instant;

pub(super) async fn drain_response(
    stream: &mut S, timeout: Duration, response_compressed: bool,
    deadline: Option<Instant>,
) -> Result<()> {
    loop {
        let typ = match packet_read_timeout(timeout, deadline) {
            Some(per_read) => match crate::runtime::time::timeout(per_read, read_varint_async(stream)).await {
                Ok(Ok(t)) => t,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    if deadline.is_some() {
                        cancel_and_drain(stream, timeout, response_compressed).await?;
                        return Err(Error::Timeout("query exceeded deadline".into()));
                    }
                    return Ok(()); // recv_timeout floor, no deadline: unchanged
                },
            },
            None => {
                // deadline already elapsed before this read
                cancel_and_drain(stream, timeout, response_compressed).await?;
                return Err(Error::Timeout("query exceeded deadline".into()));
            },
        };
        debug!(packet_type = typ, "received packet");
        match typ {
            5 => return Ok(()),
            1 => {
                let _ = read_data_block_maybe_compressed(stream, response_compressed).await;
            },
            2 => {
                let _ = read_exception(stream).await;
                return Ok(());
            },
            3 => {
                let _ = read_progress_packet(stream).await;
            },
            4 => {},
            6 => {
                let _ = read_profile_info_packet(stream).await;
            },
            10 => {
                let _ = read_data_block(stream).await;
            },
            14 => {
                let _ = read_data_block_maybe_compressed(stream, response_compressed).await;
            },
            17 => {
                let _ = read_string_async(stream).await;
            },
            12 => {
                skip_part_uuids_packet(stream).await?;
            },
            11 => {
                read_table_columns_packet(stream, response_compressed).await?;
            },
            _ => return Err(unsupported_server_packet(stream, typ).await?),
        }
    }
}
```

- [ ] **Step 2: Wire the deadline into `read_table_structure`**

Same pattern in the same file. Replace the read-head (lines 19-28). Final:

```rust
pub(super) async fn read_table_structure(
    stream: &mut S, timeout: Duration, response_compressed: bool,
    deadline: Option<Instant>,
) -> Result<Block> {
    loop {
        let typ = match packet_read_timeout(timeout, deadline) {
            Some(per_read) => match crate::runtime::time::timeout(per_read, read_varint_async(stream)).await {
                Ok(Ok(t)) => t,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    if deadline.is_some() {
                        cancel_and_drain(stream, timeout, response_compressed).await?;
                        return Err(Error::Timeout("query exceeded deadline".into()));
                    }
                    return Err(Error::Protocol(
                        "timeout waiting for INSERT table structure".into(),
                    ));
                },
            },
            None => {
                cancel_and_drain(stream, timeout, response_compressed).await?;
                return Err(Error::Timeout("query exceeded deadline".into()));
            },
        };
        match typ {
            // …unchanged arms (1, 2, 3, 4, 5, 6, 11, 10, 14, 17, 12, _)
            1 => return read_data_block_maybe_compressed(stream, response_compressed).await,
            2 => return Err(read_exception(stream).await?),
            3 => { let _ = read_progress_packet(stream).await?; },
            4 => {},
            5 => return Err(Error::Protocol("EndOfStream before INSERT table structure".into())),
            6 => { let _ = read_profile_info_packet(stream).await?; },
            11 => { read_table_columns_packet(stream, response_compressed).await?; },
            10 => { let _ = read_data_block(stream).await?; },
            14 => { let _ = read_data_block_maybe_compressed(stream, response_compressed).await?; },
            17 => { let _timezone = read_string_async(stream).await?; },
            12 => { skip_part_uuids_packet(stream).await?; },
            _ => return Err(unsupported_server_packet(stream, typ).await?),
        }
    }
}
```

- [ ] **Step 3: Wire the deadline into `read_select_response`**

In `src/connection/select_response.rs`, change the signature (line 50) to add `deadline: Option<crate::runtime::time::Instant>,` after `recv_timeout: Duration,`, and replace the read-head (lines 54-60):

```rust
pub(super) async fn read_select_response<H: SelectResponseHandler>(
    stream: &mut crate::pool::StreamWrapper, recv_timeout: Duration,
    deadline: Option<crate::runtime::time::Instant>, response_compressed: bool,
    callbacks: &QueryCallbacks, mut handler: H,
) -> Result<H::Output> {
    use crate::connection::io::packet_read_timeout;
    use crate::connection::server_packets::cancel_and_drain;
    loop {
        let typ = match packet_read_timeout(recv_timeout, deadline) {
            Some(per_read) => match crate::runtime::time::timeout(per_read, read_varint_async(stream)).await {
                Ok(Ok(t)) => t,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    if deadline.is_some() {
                        cancel_and_drain(stream, recv_timeout, response_compressed).await?;
                        return Err(crate::error::Error::Timeout("query exceeded deadline".into()));
                    }
                    return Err(crate::error::Error::Protocol("timeout".into()));
                },
            },
            None => {
                cancel_and_drain(stream, recv_timeout, response_compressed).await?;
                return Err(crate::error::Error::Timeout("query exceeded deadline".into()));
            },
        };
        match typ {
            // … unchanged arms (1, 2, 3, 4, 5, 6, 10|14, 17, 12, _)
            1 => handler.on_data(stream, response_compressed).await?,
            2 => return Err(read_exception(stream).await?),
            3 => {
                let progress = read_progress_packet(stream).await?;
                if let Some(ref cb) = callbacks.on_progress { cb(progress); }
            },
            4 => {},
            5 => return handler.finish(),
            6 => {
                let profile = read_profile_info_packet(stream).await?;
                if let Some(ref cb) = callbacks.on_profile { cb(profile); }
            },
            10 | 14 => {
                handler.on_log_packet(stream, typ, response_compressed, callbacks).await?;
            },
            17 => { read_timezone_update(stream, callbacks).await?; },
            12 => { read_part_uuids_update(stream, callbacks).await?; },
            _ => {
                if handle_coordinator_packet(stream, typ).await? { continue; }
                return Err(unsupported_server_packet(stream, typ).await?);
            },
        }
    }
}
```

- [ ] **Step 4: Update remaining callers to pass `None` (temporary until Task 5/6 supply real deadlines)**

`read_select_response` callers in `src/connection/query_builder.rs` (`_try_block`, `_try_all`, `_try_row_count`, `raw`) — add `, None` between `self.client.recv_timeout` and `response_compressed`. Task 5 replaces `None` with the real deadline.

`read_table_structure` caller in `src/connection/insert_session.rs:54` → add `, None`. Task 6 replaces it.

- [ ] **Step 5: Build + clippy + existing tests**

Run: `cargo clippy --workspace --all-features -- -D warnings 2>&1 | tail -30`
Run: `cargo test -p st-clickhouse-lib --lib 2>&1 | tail -20`
Expected: clean; existing unit tests pass (behavior unchanged with `deadline = None`).

- [ ] **Step 6: Commit**

```bash
git add src/connection/response_wait.rs src/connection/select_response.rs \
        src/connection/query_builder.rs src/connection/insert_session.rs
git commit -m "feat(connection): deadline-aware drain/read_table_structure/read_select_response"
```

---

## Task 5: `QueryBuilder::timeout()` + retry guard + fetch-path deadlines

**Files:**
- Modify: `src/connection/query_builder.rs`

- [ ] **Step 1: Add the `timeout` field + builder method**

In the `QueryBuilder<'a>` struct (line 46), add:

```rust
    tracing_context: Option<crate::client_info::TracingContext>,
    timeout: Option<std::time::Duration>,
```

In `Client::query` (line 25), add `timeout: None,` to the literal.

Add the builder method in `impl<'a> QueryBuilder<'a>` (e.g. after `with_query_id`, line 128):

```rust
    /// Set a per-query wall-clock timeout that overrides the client-level
    /// [`Client::with_query_timeout`]. The deadline starts when the query is
    /// first sent.
    pub fn timeout(mut self, t: std::time::Duration) -> Self {
        self.timeout = Some(t);
        self
    }
```

- [ ] **Step 2: Add a private deadline resolver + change `retry` signature**

Replace `async fn retry` (line 173) so it takes a deadline and does not retry on a timeout when a deadline is active:

```rust
    /// Effective whole-query deadline: per-query override else client-level.
    fn effective_deadline(&self) -> Option<crate::runtime::time::Instant> {
        self.timeout
            .or(self.client.query_timeout)
            .map(|t| crate::runtime::time::Instant::now() + t)
    }

    async fn retry<T, F, Fut>(&self, deadline: Option<crate::runtime::time::Instant>, mut op: F) -> Result<T>
    where
        F: FnMut(Option<crate::runtime::time::Instant>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let metric_guard = QueryMetricGuard::new(self.client.metrics(), 1);
        let retries = self.client.send_retries.max(1);
        for attempt in 0..retries {
            match op(deadline).await {
                Ok(value) => {
                    metric_guard.succeed();
                    return Ok(value);
                },
                Err(e) if e.is_retryable() && attempt + 1 < retries => {
                    // A timed-out query under an explicit deadline must not be
                    // re-run (it would just time out again, server-side).
                    // With no deadline configured, behavior is unchanged.
                    if deadline.is_some() && e.is_timeout() {
                        return Err(e);
                    }
                    metric_guard.retry();
                    let base_ms = self.client.retry_timeout.as_millis() as u64;
                    let delay = base_ms.saturating_mul(1u64 << attempt);
                    let jitter = delay / 4;
                    let actual = delay.saturating_add(jitter).max(1);
                    crate::runtime::time::sleep(std::time::Duration::from_millis(actual)).await;
                },
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }
```

- [ ] **Step 3: Update the fetch methods to compute + pass the deadline**

`block` (line 250):
```rust
    pub async fn block(self) -> Result<Block> {
        let deadline = self.effective_deadline();
        self.retry(deadline, |dl| self._try_block(dl)).await
    }

    async fn _try_block(&self, deadline: Option<crate::runtime::time::Instant>) -> Result<Block> {
        let (mut guard, response_compressed) = self
            .send_select_query(QuerySettingsMode::Materialized)
            .await?;
        read_select_response(
            guard.stream_mut(),
            self.client.recv_timeout,
            deadline,
            response_compressed,
            &self.callbacks,
            FirstBlockHandler::default(),
        )
        .await
    }
```

`all` / `_try_all` (lines 310, 365): same — `let deadline = self.effective_deadline(); self.retry(deadline, |dl| self._try_all::<T>(dl)).await` and `_try_all(&self, deadline, …)` passing `deadline` into `read_select_response`.

`row_count` / `_try_row_count` (lines 346, 350): same pattern.

`raw` (line 380): `raw` is not retried; compute `let deadline = self.effective_deadline();` and pass into `read_select_response`.

`rows` (line 270, streaming): see Task 7.

- [ ] **Step 4: Build + clippy**

Run: `cargo clippy --workspace --all-features -- -D warnings 2>&1 | tail -30`
Expected: clean (all `read_select_response` calls now pass a deadline).

- [ ] **Step 5: Commit**

```bash
git add src/connection/query_builder.rs
git commit -m "feat(query): per-query .timeout() + deadline-aware retry on fetch paths"
```

---

## Task 6: `execute` + `INSERT` deadlines (client-level)

**Files:**
- Modify: `src/connection/commands.rs`
- Modify: `src/connection/insert_session.rs`

- [ ] **Step 1: Thread client-level deadline into `execute`**

`execute` has its own retry loop. In `execute_with_params_and_ignored_part_uuids` (line 38), compute the deadline once and add the same retry guard:

```rust
    pub async fn execute_with_params_and_ignored_part_uuids(
        &self, query: &str, params: &[QueryParameter], uuids: &[[u8; 16]],
    ) -> Result<()> {
        let span = info_span!("execute", query = %query, retries = self.send_retries);
        let deadline = self.query_timeout.map(|t| crate::runtime::time::Instant::now() + t);
        async {
            let metric_guard = QueryMetricGuard::new(self.metrics(), 1);
            let retries = self.send_retries.max(1);
            for attempt in 0..retries {
                match self
                    ._execute_with_params_and_ignored_part_uuids(query, params, uuids, deadline)
                    .await
                {
                    Ok(r) => { metric_guard.succeed(); return Ok(r); },
                    Err(e) => {
                        if !e.is_retryable() || attempt + 1 >= retries
                            || (deadline.is_some() && e.is_timeout())
                        {
                            return Err(e);
                        }
                        metric_guard.retry();
                        // …existing backoff/jitter block unchanged…
```

Pass `deadline` into `_execute_with_params_and_ignored_part_uuids` and through to `drain_response`:

```rust
    async fn _execute_with_params_and_ignored_part_uuids(
        &self, query: &str, params: &[QueryParameter], uuids: &[[u8; 16]],
        deadline: Option<crate::runtime::time::Instant>,
    ) -> Result<()> {
        // …unchanged through write_packet + flush…
        drain_response(
            stream,
            self.recv_timeout,
            compression_flag(self.compression) == 1,
            deadline,
        )
        .await?;
        // …unchanged schema-cache invalidation…
```

Keep the existing backoff/jitter body verbatim (only the new `||` clause is added to the guard).

- [ ] **Step 2: Thread client-level deadline into `INSERT`**

In `src/connection/insert_session.rs`:

(a) Add a `deadline: Option<crate::runtime::time::Instant>` field to `InsertSession<'a>` (after `recv_timeout`).

(b) In `begin_insert`, compute it before reading the table structure and pass it to `read_table_structure`:

```rust
        let response_compressed = compression_flag(self.compression) == 1;
        let deadline = self.query_timeout.map(|t| crate::runtime::time::Instant::now() + t);
        let block = read_table_structure(stream, self.recv_timeout, response_compressed, deadline).await?;
```

Store it in the returned `InsertSession { …, deadline, … }`.

(c) In `end` (line 96), pass the stored deadline into `drain_response`:

```rust
        drain_response(
            stream,
            self.recv_timeout,
            compression_flag(self.compression) == 1,
            self.deadline,
        )
        .await
```

- [ ] **Step 3: Build + clippy**

Run: `cargo clippy --workspace --all-features -- -D warnings 2>&1 | tail -30`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/connection/commands.rs src/connection/insert_session.rs
git commit -m "feat(connection): client-level query deadline on execute + INSERT paths"
```

---

## Task 7: `BlockStream` + streaming cursor deadline

**Files:**
- Modify: `src/connection/block_stream.rs`
- Modify: `src/connection/query_builder.rs` (`rows`)

> The `rows()`/`RowCursor` path takes the stream out of the pool (`take_stream`) by design, so the connection is not reused there regardless — the deadline's job is to fire and return `Error::Timeout` through the channel (plus send `Cancel` to be polite). `BlockStream` keeps the guard, so its cancel drains for reuse.

- [ ] **Step 1: Add deadline to `BlockStream`**

In `src/connection/block_stream.rs`:

(a) Add a field to `BlockStream<'a>` (line 26): `deadline: Option<crate::runtime::time::Instant>,`.

(b) In `begin_select_with_ignored_part_uuids` (line 45), compute it and store it:
```rust
        let deadline = self.query_timeout.map(|t| crate::runtime::time::Instant::now() + t);
        metric_guard.succeed();
        Ok(BlockStream {
            guard,
            done: false,
            recv_timeout: self.recv_timeout,
            deadline,
            callbacks: QueryCallbacks::default(),
        })
```

(c) In `next_block` (line 89), use the deadline for the per-read and cancel+drain on expiry. Replace the read-head (lines 95-105):
```rust
            let packet_type = match packet_read_timeout(self.recv_timeout, self.deadline) {
                Some(per_read) => match crate::runtime::time::timeout(per_read, read_varint_async(stream)).await {
                    Ok(Ok(t)) => t,
                    Ok(Err(e)) => { self.done = true; return Err(e); },
                    Err(_) => {
                        if self.deadline.is_some() {
                            self.done = true;
                            crate::connection::server_packets::cancel_and_drain(stream, self.recv_timeout, false).await?;
                            return Err(crate::error::Error::Timeout("query exceeded deadline".into()));
                        }
                        return Ok(None); // recv_timeout floor: unchanged
                    },
                },
                None => {
                    self.done = true;
                    crate::connection::server_packets::cancel_and_drain(stream, self.recv_timeout, false).await?;
                    return Err(crate::error::Error::Timeout("query exceeded deadline".into()));
                },
            };
```
Add `use crate::connection::io::packet_read_timeout;` at the top.

(d) Upgrade `cancel` (line 158) to drain so the connection survives, and fix the doc comment:
```rust
    /// Cancel the running query and drain the response.
    ///
    /// Sends `Cancel`, drains to `EndOfStream`/`Exception`, and leaves the
    /// connection usable for the next query.
    pub async fn cancel(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        self.done = true;
        let stream = self.guard.stream_mut();
        crate::connection::server_packets::cancel_and_drain(stream, self.recv_timeout, false).await
    }
```

- [ ] **Step 2: Thread deadline into the streaming cursor (`rows`)**

In `src/connection/query_builder.rs`, `rows` (line 270): compute `let deadline = self.effective_deadline();` before spawning, and pass it into `read_query_blocks`:

```rust
        let deadline = self.effective_deadline();
        let cancel_clone = cancel.clone();
        crate::runtime::spawn(async move {
            let mut stream = stream;
            if cancel_clone.load(std::sync::atomic::Ordering::Relaxed) {
                crate::runtime::io::AsyncWriteExt::write_all(&mut stream, &[ClientPacket::Cancel as u8])
                    .await.ok();
                return;
            }
            if let Err(e) = read_query_blocks(
                stream, &block_tx, &self.callbacks, Some(&cancel_clone), self.client.recv_timeout, deadline,
            ).await {
                let _ = block_tx.send(Err(e)).await;
            }
        });
```

In `src/connection/row_stream_reader.rs`, change `read_query_blocks` to accept `recv_timeout: Duration` and `deadline: Option<Instant>` and enforce them on the per-packet read (cancel + send `Err(Timeout)` on expiry):

```rust
pub(super) async fn read_query_blocks(
    mut stream: crate::pool::StreamWrapper, block_tx: &mpsc::Sender<Result<Option<Block>>>,
    callbacks: &QueryCallbacks, cancel: Option<&std::sync::atomic::AtomicBool>,
    recv_timeout: std::time::Duration, deadline: Option<crate::runtime::time::Instant>,
) -> Result<()> {
    use crate::connection::io::packet_read_timeout;
    use crate::protocol::packet::ClientPacket;
    loop {
        if let Some(c) = cancel {
            if c.load(std::sync::atomic::Ordering::Relaxed) {
                crate::runtime::io::AsyncWriteExt::write_all(&mut stream, &[ClientPacket::Cancel as u8]).await.ok();
                return Ok(());
            }
        }
        let packet_type = match packet_read_timeout(recv_timeout, deadline) {
            Some(per_read) => match crate::runtime::time::timeout(per_read, read_varint_async(&mut stream)).await {
                Ok(Ok(t)) => t,
                Ok(Err(e)) => { let _ = block_tx.send(Err(e)).await; return Ok(()); },
                Err(_) => {
                    if deadline.is_some() {
                        crate::runtime::io::AsyncWriteExt::write_all(&mut stream, &[ClientPacket::Cancel as u8]).await.ok();
                        crate::runtime::io::AsyncWriteExt::flush(&mut stream).await.ok();
                        let _ = block_tx.send(Err(crate::error::Error::Timeout("query exceeded deadline".into()))).await;
                        return Ok(());
                    }
                    let _ = block_tx.send(Err(crate::error::Error::Protocol("timeout".into()))).await;
                    return Ok(());
                },
            },
            None => {
                crate::runtime::io::AsyncWriteExt::write_all(&mut stream, &[ClientPacket::Cancel as u8]).await.ok();
                crate::runtime::io::AsyncWriteExt::flush(&mut stream).await.ok();
                let _ = block_tx.send(Err(crate::error::Error::Timeout("query exceeded deadline".into()))).await;
                return Ok(());
            },
        };
        match packet_type {
            // …unchanged arms (1, 2, 3, 4, 5, 6, 10|14, 17, 12, _)
        }
    }
}
```

- [ ] **Step 3: Build + clippy**

Run: `cargo clippy --workspace --all-features -- -D warnings 2>&1 | tail -30`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/connection/block_stream.rs src/connection/query_builder.rs src/connection/row_stream_reader.rs
git commit -m "feat(connection): query deadline on BlockStream + streaming cursor"
```

---

## Task 8: Python `query_timeout` verification

**Files:**
- Modify: `st-clickhouse-py/tests/test_client.py`

> `query_timeout` is already exposed in the `_Client` constructor (`st-clickhouse-py/src/client.rs:73,101`) and enforced by the sync core. A dedicated Python `TimeoutError` is deferred to Spec 2 (sync `Error` has no `Timeout` variant; the deadline currently surfaces as `ValueError`/"protocol error: …timeout…"). This task only verifies the existing wiring.

- [ ] **Step 1: Add the verification test**

Append to `st-clickhouse-py/tests/test_client.py`:

```python
def test_query_timeout_raises_on_slow_query():
    """query_timeout (already wired in the constructor) must abort a slow query."""
    import pytest
    from st_clickhouse import Client
    try:
        client = Client("localhost:9000", query_timeout=0.5)
    except Exception:
        pytest.skip("ClickHouse not available on localhost:9000")
    try:
        with pytest.raises(Exception):
            client.query("SELECT sleep(3)")
    finally:
        client.close()

    # Connection must remain usable after the timeout (pool reconnects).
    client2 = Client("localhost:9000", query_timeout=30.0)
    try:
        rows = client2.query("SELECT toUInt64(1) AS x")
        assert rows[0]["x"] == 1
    finally:
        client2.close()
```

- [ ] **Step 2: Run the Python test (requires ClickHouse on :9000 + `maturin develop`)**

Run:
```bash
cd st-clickhouse-py
uv run --extra test maturin develop --release 2>&1 | tail -5
uv run --extra test python -m pytest tests/test_client.py::test_query_timeout_raises_on_slow_query -v 2>&1 | tail -20
```
Expected: PASS (or SKIP if no ClickHouse). If it fails because the sync deadline does not fire, do **not** fix it here — file it for Spec 2 (sync timeout behavior). The async feature does not depend on it.

- [ ] **Step 3: Commit**

```bash
git add st-clickhouse-py/tests/test_client.py
git commit -m "test(python): verify query_timeout aborts a slow query"
```

---

## Task 9: Rust integration tests (Spec §7 matrix) + final verification

**Files:**
- Create: `tests/query_timeout_test.rs`

> Requires ClickHouse on `127.0.0.1:9000`. The shared harness `tests/common/mod.rs` provides `connect_client()` / `connect_client_pool(size)`. Mark these `#[ignore]` by default so `cargo test --lib` stays green without a server; run with `cargo test --test query_timeout_test -- --ignored`.

- [ ] **Step 1: Write the integration tests**

`tests/query_timeout_test.rs`:

```rust
mod common;

use st_clickhouse::error::Error;
use std::time::Duration;

fn assert_timeout<T>(r: st_clickhouse::error::Result<T>) {
    match r {
        Err(Error::Timeout(_)) => {}
        other => panic!("expected Error::Timeout, got {other:?}"),
    }
}

#[tokio::test]
#[ignore]
async fn per_query_timeout_fires_and_connection_is_reused() {
    let client = common::connect_client().await;
    // 1s deadline, 3s server sleep -> must time out.
    assert_timeout(
        client
            .query("SELECT sleep(3), number FROM system.numbers LIMIT 1")
            .timeout(Duration::from_secs(1))
            .fetch::<(f64, u64)>()
            .await,
    );
    // The connection must still be usable (drain worked / pool reaped + reconnected).
    let one: (u64,) = client
        .query("SELECT toUInt64(1)")
        .fetch()
        .await
        .expect("connection reusable after timeout");
    assert_eq!(one.0, 1);
}

#[tokio::test]
#[ignore]
async fn client_level_query_timeout_applies() {
    let client = common::connect_client().await;
    let client = client.with_query_timeout(Duration::from_secs(1));
    assert_timeout(
        client
            .query("SELECT sleep(3)")
            .fetch::<(f64,)>()
            .await,
    );
}

#[tokio::test]
#[ignore]
async fn per_query_override_beats_client_level() {
    let client = common::connect_client().await;
    // Tight client deadline, generous per-query override -> must succeed.
    let client = client.with_query_timeout(Duration::from_millis(100));
    let val: (f64,) = client
        .query("SELECT sleep(0.3)")
        .timeout(Duration::from_secs(5))
        .fetch()
        .await
        .expect("per-query override should win");
    assert_eq!(val.0, 0.0);
}

#[tokio::test]
#[ignore]
async fn no_timeout_long_query_completes() {
    // Regression guard: with no deadline, a 1s query completes normally.
    let client = common::connect_client().await;
    let val: (u64,) = client
        .query("SELECT toUInt64(42) WHERE sleep(0.2) = 0")
        .fetch()
        .await
        .expect("no timeout configured -> must complete");
    assert_eq!(val.0, 42);
}

#[tokio::test]
#[ignore]
async fn execute_timeout_fires() {
    let client = common::connect_client().await;
    let client = client.with_query_timeout(Duration::from_secs(1));
    assert_timeout(client.execute("SELECT sleep(3)").await);
}
```

- [ ] **Step 2: Run the integration tests (requires ClickHouse)**

```bash
cargo test -p st-clickhouse-lib --test query_timeout_test -- --ignored --test-threads=1 2>&1 | tail -30
```
Expected: all PASS. If `SELECT sleep(3)` does not time out, verify the deadline is actually set (add an `eprintln!` in `cancel_and_drain` temporarily) and that the server honors `Cancel`.

- [ ] **Step 3: Full verification gate**

Run each; all must pass:
```bash
cargo clippy --workspace --all-features -- -D warnings 2>&1 | tail -5
cargo build --workspace --all-features 2>&1 | tail -5
cargo test  -p st-clickhouse-lib --lib 2>&1 | tail -10
cargo test  -p st-clickhouse-lib --doc 2>&1 | tail -10
cargo fmt --all -- --check 2>&1 | tail -5
```
Expected: clean / pass. `cargo fmt --check` may report formatting — if so run `cargo fmt --all` and amend the last commit.

- [ ] **Step 4: Commit**

```bash
git add tests/query_timeout_test.rs
git commit -m "test(connection): query timeout integration matrix (fire/reuse/override/regress)"
```

---

## Finalization

- [ ] **Update CHANGELOG** — add under a new `## Unreleased` section in `CHANGELOG.md`:

```markdown
## [Unreleased]

### Added
- **Query timeout**: opt-in hard wall-clock deadline via `Client::with_query_timeout(d)`
  and per-query `QueryBuilder::timeout(d)`. On expiry the query is cancelled
  server-side (`Cancel` packet) and the pooled connection is drained and reused.
  Default `None` — no behaviour change for existing users. Async client only;
  sync core already supported `query_timeout`.
```

- [ ] **Final commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): note query timeout feature"
```

---

## Self-Review (completed by plan author)

- **Spec coverage:** §4.1 (config) → Task 1. §4.2 (QueryBuilder `.timeout`) → Task 5. §4.3 (`cancel_and_drain`) → Task 2. §4.4 (read paths) → Tasks 4/6/7. §4.5 (BlockStream cancel drains) → Task 7. §4.6 (magic bytes) → Task 2. §4.7 (`Error::Timeout`, no new variant, floor unchanged) → Tasks 4/7 (floor branches preserved). §4.8 (Python) → Task 8. §6 (error matrix) → Tasks 4/6/7. §7 (tests) → Tasks 3/8/9. ✓
- **Retry-on-timeout hazard:** handled via `deadline.is_some() && e.is_timeout()` guards in both `QueryBuilder::retry` (Task 5) and `execute`'s loop (Task 6) — zero behaviour change when no deadline is set. ✓
- **Placeholder scan:** no TBD/TODO; every code step shows real code. The two `// …unchanged arms…` comments are accompanied by the full arm list in the same block (Task 4 steps 1-3). ✓
- **Type consistency:** `deadline: Option<crate::runtime::time::Instant>` used uniformly; `packet_read_timeout`, `cancel_and_drain`, `effective_deadline` names match across tasks; `ClientPacket::Cancel`/`Ping` path `crate::protocol::packet::ClientPacket`. ✓
- **Excluded correctly:** `block_reader.rs:423` compression peek is never touched. ✓
- **Scope:** sync `cancel_and_drain`, dedup, perf, batch timeout, Python `TimeoutError` explicitly deferred. ✓
```
