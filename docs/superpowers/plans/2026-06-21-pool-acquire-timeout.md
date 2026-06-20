# Pool Acquire Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the wait for a free pool slot in `SimplePool::get()` with a configurable, opt-in `acquire_timeout`, failing fast with a distinct retryable `Error::PoolTimeout`. Default `None` = today's unbounded wait (zero behaviour change).

**Architecture:** One new `Option<Duration>` config field on `SimplePool`, one new `Error` variant, and a `crate::runtime::time::timeout` wrapper around the slot-lock acquisition in `get()`. Exposed via the existing builder/Client/URL plumbing (mirroring `send_timeout` and `with_query_timeout`). No background tasks, no `Arc` refactor, no new runtime API.

**Tech Stack:** Rust 2024, tokio async, `crate::runtime::time::timeout` (re-export of `tokio::time::timeout`), `crate::runtime::sync::Mutex` per-slot pool. Integration tests against a live ClickHouse (env `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`, `--ignored`).

**Spec:** `docs/superpowers/specs/2026-06-21-pool-robustness-design.md`

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `src/error.rs` | Crate error enum + predicates | Add `PoolTimeout(String)`, `Display`, `is_pool_timeout()`, add to `is_retryable()` |
| `src/pool.rs` | Connection pool + slot acquisition | Add `acquire_timeout` field + `set_acquire_timeout`; wrap `get()` slot lock in `timeout`; fix stale module/struct doc |
| `src/connection/config.rs` | `Client` consuming setters | Add `with_acquire_timeout(Duration)` |
| `src/builder.rs` | `ClientBuilder` + URL parsing | Add `acquire_timeout` opt, `.acquire_timeout(d)` builder method, URL `acquire_timeout=`, wire into `connect()` |
| `tests/pool_acquire_timeout_test.rs` | Live-server integration tests (new) | Acquire-timeout-fires + no-regression |
| `CHANGELOG.md` | Release notes | Bullet under `[Unreleased]` → `Added` |

Decomposition rationale: each task touches one or two files and produces an independently-verifiable commit. The behavioural core (Task 4) is isolated to `get()` and has a server-free unit test, so it can be verified without a live ClickHouse. Integration tests (Task 5) are the end-to-end proof.

---

### Task 1: `Error::PoolTimeout` variant

**Files:**
- Modify: `src/error.rs:8-29` (enum), `src/error.rs:31-50` (Display), `src/error.rs:61-92` (predicates)
- Test: `src/error.rs` (append `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing tests**

Append to the end of `src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_timeout_is_pool_timeout_only() {
        assert!(Error::PoolTimeout("no slot".into()).is_pool_timeout());
        assert!(!Error::Timeout("query".into()).is_pool_timeout());
        assert!(!Error::ConnectionClosed("x".into()).is_pool_timeout());
    }

    #[test]
    fn pool_timeout_is_retryable_but_not_timeout() {
        let e = Error::PoolTimeout("no slot".into());
        assert!(e.is_retryable(), "PoolTimeout must stay retryable");
        assert!(!e.is_timeout(), "PoolTimeout must NOT match is_timeout");
    }

    #[test]
    fn pool_timeout_display() {
        assert_eq!(
            Error::PoolTimeout("no slot".to_owned()).to_string(),
            "pool acquire timeout: no slot"
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib error::tests`
Expected: COMPILE ERROR — `Error::PoolTimeout` and `is_pool_timeout` do not exist.

- [ ] **Step 3: Add the variant, Display arm, and predicate**

In the `Error` enum (after the `Config(String)` arm at `src/error.rs:28`):

```rust
    /// A pool slot could not be acquired within `acquire_timeout`.
    PoolTimeout(String),
```

In the `Display` impl (after the `Config` arm at `src/error.rs:47`):

```rust
            Error::PoolTimeout(msg) => write!(f, "pool acquire timeout: {msg}"),
```

In the `impl Error` block, add a new predicate after `is_timeout` (`src/error.rs:63-65`):

```rust
    /// Returns `true` if this is a pool-acquire timeout.
    pub fn is_pool_timeout(&self) -> bool {
        matches!(self, Error::PoolTimeout(_))
    }
```

Add `PoolTimeout` to `is_retryable` (`src/error.rs:86-91`). **Do not** add it to `is_timeout` (it must not collide with the query-timeout retry guard — see spec §2):

```rust
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Io(_)
                | Error::Timeout(_)
                | Error::ConnectionClosed(_)
                | Error::Protocol(_)
                | Error::PoolTimeout(_)
        )
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib error::tests`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/error.rs
git commit -m "feat(error): add retryable PoolTimeout variant"
```

---

### Task 2: `acquire_timeout` config on `SimplePool` + doc fix

**Files:**
- Modify: `src/pool.rs:1-10` (module doc), `src/pool.rs:481-484` (struct doc), `src/pool.rs:485-515` (struct), `src/pool.rs:519-546` (`new`), `~src/pool.rs:558` (`set_*` area)
- Test: `src/pool.rs` existing `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing tests**

Add to the existing `mod tests` in `src/pool.rs` (after `test_set_send_timeout`):

```rust
    #[test]
    fn test_acquire_timeout_defaults_none() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let pool = SimplePool::new(vec![addr], 2);
        assert!(pool.acquire_timeout.is_none());
    }

    #[test]
    fn test_set_acquire_timeout() {
        let addr = "127.0.0.1:9000"
            .parse::<std::net::SocketAddr>()
            .expect("test operation failed");
        let mut pool = SimplePool::new(vec![addr], 2);
        pool.set_acquire_timeout(Some(Duration::from_millis(50)));
        assert_eq!(pool.acquire_timeout, Some(Duration::from_millis(50)));
        pool.set_acquire_timeout(None);
        assert!(pool.acquire_timeout.is_none());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib pool::tests::test_acquire_timeout`
Expected: COMPILE ERROR — no field `acquire_timeout`, no method `set_acquire_timeout`.

- [ ] **Step 3: Add the field, default, setter, and fix the docs**

3a. Add the field to `SimplePool` (`src/pool.rs`, after `connect_timeout: Option<Duration>,` at line 492):

```rust
    /// Max wait for a free slot in `get()` (None = unbounded, today's behaviour).
    acquire_timeout: Option<Duration>,
```

3b. Initialise it in `SimplePool::new` (after `connect_timeout: None,` at line 529):

```rust
            acquire_timeout: None,
```

3c. Add the setter near `set_send_timeout` (`src/pool.rs:558-561`):

```rust
    /// Set the max wait for a free pool slot. `None` = unbounded (default).
    pub(crate) fn set_acquire_timeout(&mut self, t: Option<Duration>) {
        self.acquire_timeout = t;
    }
```

3d. Replace the **false** module doc (`src/pool.rs:1-10`) — there is no `Semaphore`:

```rust
//! Connection pool with per-slot async mutexes and round-robin selection.
//!
//! Architecture:
//!   - Per-slot `crate::runtime::sync::Mutex` — guards each `Option<Connection>`
//!   - Atomic round-robin index (`next_idx`) — assigns slots without a free-list
//!     lock
//!
//! No blocking mutex in any async path. Each concurrent user typically locks a
//! different slot; when more callers contend than there are slots, the wait for
//! a free slot is optionally bounded by `acquire_timeout` (default: unbounded).
//! `PoolGuard::drop` drops the slot guard, waking the next waiter.
```

3e. Fix the struct doc (`src/pool.rs:481-484`) — replace "A semaphore-based pool…":

```rust
/// A pool of ClickHouse connections with per-slot async mutexes.
///
/// Slots are assigned round-robin via an atomic counter. Each slot holds
/// `Option<Connection>` and is lazily connected on first use.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib pool::tests`
Expected: all pool unit tests pass (including the 2 new ones).

- [ ] **Step 5: Commit**

```bash
git add src/pool.rs
git commit -m "feat(pool): add acquire_timeout config + fix module doc"
```

---

### Task 3: Expose `acquire_timeout` on Client / builder / URL

**Files:**
- Modify: `src/connection/config.rs:60-64` (add `with_acquire_timeout`)
- Modify: `src/builder.rs:39` (BuilderOptions field), `src/builder.rs:77` (Default), `src/builder.rs:185-188` (builder method), `src/builder.rs:276-278` (`connect()` wiring), `src/builder.rs:516-517` (URL parse)
- Test: `src/builder.rs` (append `mod acquire_timeout_tests`)

- [ ] **Step 1: Write the failing tests**

Append to `src/builder.rs` (after the `query_timeout_tests` mod):

```rust
#[cfg(test)]
mod acquire_timeout_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn builder_stores_acquire_timeout() {
        let b = ClientBuilder::<Async>::new().acquire_timeout(Duration::from_millis(250));
        assert_eq!(b.opts.acquire_timeout, Some(Duration::from_millis(250)));
    }

    #[test]
    fn builder_default_has_no_acquire_timeout() {
        let b = ClientBuilder::<Async>::new();
        assert_eq!(b.opts.acquire_timeout, None);
    }

    #[test]
    fn url_parses_acquire_timeout() {
        let b = ClientBuilder::<Async>::from_url(
            "clickhouse://honne:honne@127.0.0.1:9000?acquire_timeout=50ms",
        )
        .expect("url should parse");
        assert_eq!(b.opts.acquire_timeout, Some(Duration::from_millis(50)));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib builder::acquire_timeout_tests`
Expected: COMPILE ERROR — no method `acquire_timeout`, no field `opts.acquire_timeout`.

- [ ] **Step 3: Wire the builder, Client setter, and URL parse**

3a. `BuilderOptions` field (`src/builder.rs`, after `query_timeout: Option<Duration>,` at line 39):

```rust
    acquire_timeout: Option<Duration>,
```

3b. `BuilderOptions::default` (`src/builder.rs`, after `query_timeout: None,` at line 77):

```rust
            acquire_timeout: None,
```

3c. Builder method (`src/builder.rs`, after the `query_timeout` method at lines 185-188, inside `impl<M> ClientBuilder<M>`):

```rust
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.opts.acquire_timeout = Some(timeout);
        self
    }
```

3d. URL parse arm (`src/builder.rs`, in `parse_query`, after the `"send_timeout"` arm at line 517):

```rust
            "acquire_timeout" => opts.acquire_timeout = Some(parse_duration(&value)?),
```

3e. `connect()` wiring (`src/builder.rs`, after the `send_timeout` block at lines 276-278 — set it on the **pool** before `new_connected`, exactly like `connect_timeout`/`send_timeout`):

```rust
        if let Some(timeout) = self.opts.acquire_timeout {
            pool.set_acquire_timeout(Some(timeout));
        }
```

3f. Client consuming setter (`src/connection/config.rs`, after `with_send_timeout` at lines 61-64 — mirror it):

```rust
    /// Set the maximum time to wait for a free pool slot.
    ///
    /// When set, an acquisition that cannot get a free slot within `t` returns
    /// [`Error::PoolTimeout`](crate::error::Error::PoolTimeout) (retryable).
    /// `None` by default — unbounded wait, today's behaviour.
    pub fn with_acquire_timeout(mut self, t: Duration) -> Self {
        self.pool.set_acquire_timeout(Some(t));
        self
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib builder::acquire_timeout_tests`
Expected: 3 passed.

- [ ] **Step 5: Build + clippy the crate to confirm the wiring compiles**

Run: `cargo build && cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/builder.rs src/connection/config.rs
git commit -m "feat(client): expose acquire_timeout on builder/Client/URL"
```

---

### Task 4: Bound slot acquisition in `get()` with `acquire_timeout`

**Files:**
- Modify: `src/pool.rs:760` (the `let mut slot_guard = self.slots[idx].lock().await;` line inside `get()`)
- Test: `src/pool.rs` existing `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing test (server-free: hold the only slot)**

Add to the existing `mod tests` in `src/pool.rs`:

```rust
    /// Server-free proof that `get()` honours `acquire_timeout`: hold the only
    /// slot from the test, so `get()` cannot acquire it and must time out.
    /// The outer probe bound makes RED fail fast instead of hanging.
    #[tokio::test]
    async fn test_acquire_timeout_returns_pool_timeout_when_slot_contended() {
        let addr: std::net::SocketAddr = "127.0.0.1:9000"
            .parse()
            .expect("test operation failed");
        let mut pool = SimplePool::new(vec![addr], 1);
        pool.set_acquire_timeout(Some(Duration::from_millis(20)));
        // Occupy the single slot; `get()` (round-robin idx 0) cannot lock it.
        let _held = pool.slots[0].lock().await;

        let res = crate::runtime::time::timeout(Duration::from_secs(2), pool.get()).await;
        match res {
            Ok(Err(crate::error::Error::PoolTimeout(_))) => {},
            other => panic!("expected PoolTimeout, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run the test to verify it fails (fast)**

Run: `cargo test --lib pool::tests::test_acquire_timeout_returns_pool_timeout_when_slot_contended`
Expected: FAIL within ~2 s — `get()` ignores `acquire_timeout`, blocks on the held lock, the outer probe times out → `Err(Elapsed)` → panic "expected PoolTimeout, got Err(Elapsed)". (If it instead hangs, kill it — that also confirms the wiring is missing.)

- [ ] **Step 3: Implement the timeout wrapper**

Replace the line at `src/pool.rs:760`:

```rust
        let mut slot_guard = self.slots[idx].lock().await;
```

with:

```rust
        let mut slot_guard = match self.acquire_timeout {
            Some(t) => match crate::runtime::time::timeout(t, self.slots[idx].lock()).await {
                Ok(g) => g,
                Err(_) => {
                    if let Some(m) = self.metrics {
                        m.connection_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(crate::error::Error::PoolTimeout(format!(
                        "no connection slot available within {t:?}"
                    )));
                }
            },
            None => self.slots[idx].lock().await,
        };
```

The rest of `get()` (lazy-connect, ttl/stale/alive checks, reconnect) is unchanged. `acquire_timeout` bounds **only** the slot-lock wait; connect/ping inside the lock keep being bounded by `connect_timeout` and the 1 s liveness ping as today.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --lib pool::tests::test_acquire_timeout_returns_pool_timeout_when_slot_contended`
Expected: PASS in ~20 ms.

- [ ] **Step 5: Run the full lib test suite + clippy**

Run: `cargo test --lib && cargo clippy --all-targets -- -D warnings`
Expected: all pass, no warnings.

- [ ] **Step 6: Commit**

```bash
git add src/pool.rs
git commit -m "feat(pool): bound slot acquire with acquire_timeout in get()"
```

---

### Task 5: Integration tests against a live ClickHouse

**Files:**
- Create: `tests/pool_acquire_timeout_test.rs`

These are `#[ignore]` (need a live server). Run with:
`CLICKHOUSE_USER=honne CLICKHOUSE_PASSWORD=honne cargo test --test pool_acquire_timeout_test -- --ignored`

- [ ] **Step 1: Create the integration test file**

Create `tests/pool_acquire_timeout_test.rs`. (In an integration test — outside the crate — `crate::runtime::time` is `pub(crate)`, so sleep uses `tokio::time` directly. tokio is present via `#[tokio::test]`.)

```rust
mod common;

use st_clickhouse::error::Error;
use std::sync::Arc;
use std::time::Duration;

/// A size-1 pool with a 50 ms acquire timeout: a slow query occupies the only
/// slot, so a concurrent acquire must fail fast with `PoolTimeout`. Afterwards
/// the pool is not starved — a fresh query succeeds.
#[tokio::test]
#[ignore]
async fn acquire_timeout_fires_under_contention() {
    let client = Arc::new(
        common::connect_client_pool(1)
            .await
            .with_acquire_timeout(Duration::from_millis(50)),
    );

    // Slow query grabs and holds the single slot for ~2 s.
    let slow = {
        let c = client.clone();
        tokio::spawn(async move { c.query("SELECT sleep(2)").fetch::<(u8,)>().await })
    };
    // Let the slow query acquire the slot first.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Concurrent acquire on the same single-slot pool must time out.
    let probe = client.query("SELECT 1").fetch::<(u8,)>().await;
    match probe {
        Err(Error::PoolTimeout(_)) => {},
        other => panic!("expected PoolTimeout, got {other:?}"),
    }

    // After the slow query releases the slot, a fresh query must succeed.
    let _ = slow.await.expect("slow task panicked");
    let one: (u8,) = client
        .query("SELECT toUInt8(1)")
        .fetch()
        .await
        .expect("pool usable after slow query finishes");
    assert_eq!(one.0, 1);
}

/// Regression guard: with no `acquire_timeout` (default), concurrent queries on
/// a tiny pool simply queue — never a spurious `PoolTimeout`.
#[tokio::test]
#[ignore]
async fn no_acquire_timeout_queues_instead_of_failing() {
    let client = Arc::new(common::connect_client_pool(1).await);

    let a = {
        let c = client.clone();
        tokio::spawn(async move { c.query("SELECT toUInt8(7)").fetch::<(u8,)>().await })
    };
    let b = {
        let c = client.clone();
        tokio::spawn(async move { c.query("SELECT toUInt8(8)").fetch::<(u8,)>().await })
    };

    let ra = a.await.expect("task a panicked");
    let rb = b.await.expect("task b panicked");
    assert!(ra.is_ok(), "no spurious PoolTimeout: {ra:?}");
    assert!(rb.is_ok(), "no spurious PoolTimeout: {rb:?}");
}
```

- [ ] **Step 2: Build the test binary to verify it compiles**

Run: `cargo build --test pool_acquire_timeout_test`
Expected: compiles. If `Client` fails `Send + Sync` for `Arc<Client>`/`tokio::spawn`, surface it (do not suppress) and report — the async client is expected to be `Send + Sync`.

- [ ] **Step 3: Run the integration tests against the live server**

Run: `CLICKHOUSE_USER=honne CLICKHOUSE_PASSWORD=honne cargo test --test pool_acquire_timeout_test -- --ignored`
Expected: 2 passed. `acquire_timeout_fires_under_contention` returns `PoolTimeout` in ~50 ms; `no_acquire_timeout_queues_instead_of_failing` both queries succeed.

- [ ] **Step 4: Commit**

```bash
git add tests/pool_acquire_timeout_test.rs
git commit -m "test(pool): acquire-timeout integration matrix"
```

---

### Task 6: CHANGELOG + full verification

**Files:**
- Modify: `CHANGELOG.md` (`## [Unreleased]` → `### Added`)

- [ ] **Step 1: Add the changelog bullet**

In `CHANGELOG.md`, under `## [Unreleased]` → `### Added`, after the existing Query timeout bullet, add:

```markdown
- **Pool acquire timeout**: bound the wait for a free pool slot via
  `Client::with_acquire_timeout(d)` / `ClientBuilder::acquire_timeout(d)` / URL
  `acquire_timeout=`. Returns the retryable `Error::PoolTimeout` when no slot is
  free in time. Default `None` (unbounded — unchanged). Async client only.
```

- [ ] **Step 2: Run the complete verification matrix**

Run (lib unit tests — no server needed):
```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --lib
```
Expected: fmt clean, clippy clean, all lib tests pass.

Run (integration — live server):
```bash
CLICKHOUSE_USER=honne CLICKHOUSE_PASSWORD=honne cargo test --test pool_acquire_timeout_test -- --ignored
```
Expected: 2 passed.

- [ ] **Step 3: Re-read the original request and confirm each requirement**

Check against spec §3 (`docs/superpowers/specs/2026-06-21-pool-robustness-design.md`):
- §3.1 `Error::PoolTimeout` + `is_pool_timeout` + retryable-not-timeout → Task 1 ✓
- §3.2 `acquire_timeout` field, `None` default, `get()` wrapper, metric bump → Tasks 2 & 4 ✓
- §3.3 builder + Client setter + URL parse → Task 3 ✓
- §3.4 module doc fix → Task 2 ✓
- §6 testing (unit + integration) → Tasks 1/2/3/4 unit + Task 5 integration ✓

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): pool acquire timeout"
```

---

## Self-Review Notes

**Spec coverage:** every spec section (§3.1 error, §3.2 pool, §3.3 client/builder/URL, §3.4 doc, §6 testing) maps to a task (see Task 6 Step 3). Non-goals (reaper, elastic pool, separate max-lifetime, sync/Python) are correctly absent.

**Deviations from spec (both lean, both consistent with the codebase):**
1. Spec §3.3 said wire `acquire_timeout` in `connect()` "after `new_connected`". Task 3e wires it on the **pool before** `new_connected` (next to `connect_timeout`/`send_timeout` at `builder.rs:273-278`) — the established pattern for pool-owned config, since `acquire_timeout` lives on `SimplePool`, not `Client`.
2. Spec §3.3 listed `Client::with_acquire_timeout(d)` → `self.pool.set_acquire_timeout(Some(d))`. Task 3f adds it as a **consuming** `mut self -> Self` setter (mirroring `with_send_timeout` at `config.rs:61-64`), matching the `with_query_timeout` precedent rather than a `&mut self` method.

**Placeholder scan:** none — all steps contain real code and exact commands.

**Type/name consistency:** field `acquire_timeout` (Task 2), setter `set_acquire_timeout(Option<Duration>)` (Task 2), builder method `acquire_timeout(Duration)` (Task 3), Client `with_acquire_timeout(Duration)` (Task 3), URL key `acquire_timeout` (Task 3), error `PoolTimeout(String)` + `is_pool_timeout()` (Task 1) — all referenced consistently across tasks. `crate::runtime::time::timeout(Duration, F)` returns `Result<F::Output, Elapsed>` (verified at `src/runtime/tokio_runtime.rs:26`).
