# Pool Acquire Timeout — Design

**Status:** Approved (brainstorm 2026-06-21; scope reduced from "pool robustness" after review)
**Scope:** Async `SimplePool` (`src/pool.rs`). Async-only — the sync core has no pool, and Python
rides the sync core, so no Python exposure.

---

## 1. Motivation

`SimplePool::get()` acquires a slot with `self.slots[idx].lock().await` — an **unbounded** wait.
Under contention (more concurrent acquirers than slots, or while a slot is held during a slow
reconnect / liveness-ping inside `get()`), a caller hangs forever waiting for a slot. There is no
way to bound that wait or fail fast.

### Goal
- Bound the slot-acquire wait with a configurable `acquire_timeout`; fail fast with a distinct,
  retryable error.
- Zero behaviour change for users who don't set it (`None` default = today's unbounded wait).

### Non-goals (deliberately out of scope)
- **Idle reaper / background task** — considered and **dropped**. `get()` already recycles stale
  connections on every acquire (`connection_expired` ttl check + `is_connection_alive` ping). A
  reaper only helps low-traffic idle pools, where an idle stale connection sitting in a slot costs
  ~nothing (the next acquire recycles it). The reaper's machinery (`Arc<slots>`, spawned task,
  shutdown lifecycle, spawn-ordering) is not justified by that marginal value. Revisit only if a
  concrete idle-pool problem is demonstrated.
- **Elastic / burst pool** — fail fast instead.
- **Separate `max-lifetime` knob** — `ttl` already covers max-lifetime at acquire time.
- Sync core / Python (no pool).
- In-flight query cancellation (`.timeout()` covers the time-bounded case).

---

## 2. Approach

Wrap the slot-lock in `get()` with `tokio::time::timeout`. One new config field, one new error
variant. Nothing else changes.

### Why a new `Error::PoolTimeout` (not reusing `Error::Timeout`)
An acquire timeout is operationally different from a query timeout: it's transient (a slot may
free up) and **must remain retryable** even when the query has a deadline set. The query-timeout
feature's retry guard suppresses retry on `is_timeout()` when a deadline is set
(`deadline.is_some() && e.is_timeout()`); if an acquire timeout reused `Error::Timeout`, that guard
would wrongly prevent retrying a transient pool-exhaustion. A distinct variant avoids the collision.

---

## 3. Component changes

### 3.1 Error (`src/error.rs`)
```rust
/// A pool slot could not be acquired within `acquire_timeout`.
PoolTimeout(String),
```
- `Display`: `"pool acquire timeout: {msg}"`.
- New `pub fn is_pool_timeout(&self) -> bool`.
- **Add `Error::PoolTimeout(_)` to `is_retryable()`** (a slot may free up — transient), alongside
  the existing `Timeout`/`Io`/`ConnectionClosed`/`Protocol`.
- It is NOT `is_timeout()` (that stays `Error::Timeout` only) — so the query-timeout retry guard
  never matches it. Correct by construction.

### 3.2 Pool (`src/pool.rs`)
- Add `acquire_timeout: Option<Duration>` to `SimplePool` (default `None`).
- `SimplePool::new`: `acquire_timeout: None`.
- `set_acquire_timeout(&mut self, Option<Duration>)`.
- In `get()` (~line 756), replace `let mut slot_guard = self.slots[idx].lock().await;` with:
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
The rest of `get()` (lazy-connect, ttl/stale/alive checks, reconnect) is unchanged.
`acquire_timeout` bounds **only the slot-lock wait** — the documented "hang under contention".
Connect/ping inside the lock stay bounded by `connect_timeout` and the 1s liveness ping as today.

### 3.3 Client + builder
- `Client::with_acquire_timeout(d)` → `self.pool.set_acquire_timeout(Some(d))`.
- `ClientBuilder::acquire_timeout(d)` + `BuilderOptions.acquire_timeout` + URL-parse
  `acquire_timeout` (alongside the existing `recv_timeout`/`send_timeout`/etc.).
- Wire in `ClientBuilder<Async>::connect()` after `new_connected`.

### 3.4 Housekeeping
Fix the stale module doc at `src/pool.rs:1` (claims `crate::runtime::sync::Semaphore`; the impl is
per-slot `AsyncMutex` + round-robin `AtomicUsize`). One comment edit while the file is open.

---

## 4. Data flow
```
get() ─► timeout(acquire_timeout, slots[idx].lock())
       ├─ Ok(guard) → connect / ping / reconnect (unchanged) → PoolGuard
       └─ Elapsed   → Error::PoolTimeout (retryable; bumps connection_errors metric)

acquire_timeout == None → slots[idx].lock().await (unchanged)
```

## 5. Error handling
| Situation | Result |
|---|---|
| No slot within `acquire_timeout` | `Error::PoolTimeout` (retryable; `connection_errors++`) |
| `acquire_timeout` unset | unbounded wait — unchanged |
| Retry guard interaction | `PoolTimeout` is `is_retryable()` but not `is_timeout()`, so the query-timeout no-retry guard never matches it — acquire timeouts retry correctly even with a deadline set |

## 6. Testing (`tests/pool_acquire_timeout_test.rs`, `#[ignore]` — needs live server)
- **Acquire timeout fires:** build a size-1 client with `.acquire_timeout(50ms)`; run
  `SELECT sleep(2)` to occupy the single slot, and concurrently issue a second query — the second
  returns `Error::PoolTimeout` within ~50ms. After the slow query finishes, a new query succeeds
  (no permanent starvation).
- **No regression:** default client (no `acquire_timeout`) — concurrent queries on a small pool
  queue normally (no spurious `PoolTimeout`).
Unit (no server): `is_pool_timeout()` true only for `PoolTimeout`; `PoolTimeout` is `is_retryable()`
and NOT `is_timeout()`; `SimplePool::new` defaults `acquire_timeout` to `None`.

## 7. Roadmap context
Gap #2 (pool robustness), reduced to its essential, acute part (acquire timeout). Gap #1 (query
cancellation) was dropped — `.timeout()` covers it. Followed by JWT/OIDC auth (#3), async
`quota_key` parity (#4), session affinity/transactions (#5).
