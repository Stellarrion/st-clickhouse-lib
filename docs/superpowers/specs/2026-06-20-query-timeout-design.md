# Query Timeout — Design

**Status:** Approved (brainstorm 2026-06-20)
**Spec:** 1 of 3 in the query-timeout / dedup / perf roadmap
**Scope:** Async native-protocol client (`st_clickhouse` crate). Sync core and Python noted where they interact.

---

## 1. Motivation

The async client has **no whole-query timeout**. Its only receive-side protection is
`recv_timeout` (default **300 s**, set in `src/connection/connect.rs:41`), applied *per individual
packet read* via `crate::runtime::time::timeout(recv_timeout, read_varint_async(stream))` at six
call sites (`response_wait`, `block_stream`, `block_reader`, `select_response`, `metadata`).

Because the timer re-arms on every packet, **a query that trickles data slower than 300 s per
packet never times out** — it can run essentially forever. There is no wall-clock cap on the
query as a whole.

The sync core already has a real deadline (`src/sync/client.rs` computes
`Instant::now() + config.query_timeout` at five call sites; `with_query_timeout` on
`src/sync/config.rs:192`). The async side is behind.

### Goals

- Add an opt-in **hard wall-clock deadline** for async queries, matching the sync core's
  `query_timeout` semantics and the user's mental model (cf. PostgreSQL `statement_timeout`).
- On deadline expiry, **cancel the query server-side and reuse the pooled connection** — do not
  forfeit it.
- Expose the timeout at two levels: client-wide default + per-query override.
- No behaviour change for existing users (default `None`).

### Non-goals (deferred to later roadmap specs)

- **Spec 2 — dedup:** full convergence of the parallel async (`src/connection`, `src/column`,
  `src/protocol`) and sync (`src/sync`) trees, including a shared `cancel_and_drain` and
  deadline implementation. Sync-side `cancel_and_drain` parity lands there.
- **Spec 3 — perf:** broad performance work.
- This spec does **not** add a stall/idle timeout or per-phase timeouts. One knob, one semantic.

---

## 2. Protocol basis (verified against ClickHouse source)

Authoritative source: `src/Core/Protocol.h` in `ClickHouse/ClickHouse` (master):

```
namespace Client {
    enum Enum {
        Hello = 0,
        Query = 1,
        Data  = 2,
        Cancel = 3,   // Cancel the query execution.
        ...
    };
}
```

and the protocol comment:

> *"The client can also send Cancel packet — a request to cancel the query. In this case the
> server can stop executing the query and return incomplete data, but **the client must still
> read until EndOfStream** packet."*

Facts that govern the design:

1. **`Cancel` is packet type `3` with no payload** — just the type varint.
2. **No protocol-revision gate** — `Cancel` is ancient; safe for all supported revisions
   (24.x → 26.4).
3. After `Cancel`, the server emits its normal termination sequence: remaining `Data` fragments
   (possibly incomplete), then `EndOfStream` (5) **or** `Exception` (2). The client **must**
   drain until one of those two, or the connection cannot be reused.
4. The server's cancellation is **best-effort** ("can stop"). If it ignores `Cancel`, the drain
   must be bounded so the client is never stuck waiting.

This matches the existing enum in `shared/packet.rs:68` (`ClientPacket::Cancel = 3`), which is
currently **defined but never referenced by name** — the few call sites send it as a raw
`&[3]` byte literal.

### Existing cancel-sending code (async)

Cancel is *already sent* in three places, but **none drain**, so each forfeits the connection:

- `src/connection/block_stream.rs:158` — `BlockStream::cancel()`: writes `&[3]`, flushes, returns.
  Doc comment: *"The stream is no longer usable after cancellation."*
- `src/connection/commands.rs:120` — `Client::cancel()`: writes `&[3]`, flushes, returns.
- `src/connection/block_stream.rs:170` — `BlockStream::Drop`: best-effort
  `tcp.try_write(&[3])` (non-async, fire-and-forget).

So the gaps this spec closes are precisely: **(a) the deadline itself on async, and
(b) a bounded drain after `Cancel` so the connection survives.**

---

## 3. Approach

**Per-read deadline, reusing the existing `runtime::time::timeout` pattern, plus a new
`cancel_and_drain` helper.**

An `Option<Instant>` deadline is threaded through the async read paths. Each packet read races
`runtime::time::timeout(min(recv_timeout, remaining_to_deadline), read_next())`. When the
deadline elapses, the code calls `cancel_and_drain` and returns `Error::Timeout`.

### Why this approach

- **Reuses the exact pattern already used six times in the codebase** for `recv_timeout`. No new
  runtime API surface.
- **Runtime-portable.** `crate::runtime::time::timeout` is already abstracted over the `tokio`
  and `smol` features (`src/runtime/tokio_runtime.rs` re-exports `tokio::time::{Instant, sleep,
  timeout}`). The deadline is pure `Instant` arithmetic — no `select!`/`sleep_until` needed, so
  no tokio-vs-smol macro divergence to paper over.
- **Drain composes cleanly** because each read owns the stream borrow for one packet at a time;
  the cancel-and-drain runs in the same context.

### Rejected alternatives

- **Wrap the whole query future in `timeout(deadline, query)`.** On expiry the future owns the
  pooled-connection borrow; sending `Cancel` and draining from outside the borrow is not
  expressible without a `Drop`-based cleanup hack. Cannot satisfy "reuse connection."
- **`select!` against `sleep_until(deadline)`.** Requires adding `select`/`sleep_until` to the
  runtime abstraction; tokio and smol differ on the `select!` macro. Adds surface for no gain
  over the per-read pattern.

---

## 4. Component changes

### 4.1 Config (`src/connection/tcp.rs`, `src/builder.rs`)

- Add `query_timeout: Option<Duration>` to the async `Client` struct (alongside `recv_timeout`
  at `tcp.rs:29`).
- Add `with_query_timeout(d: Duration)` to the builder, mirroring sync. Default `None`.
- The existing `recv_timeout` (300 s, per-packet) stays **unchanged** as the floor safety net.

### 4.2 QueryBuilder (`src/connection/query_builder.rs`)

- Add `.timeout(d: Duration)` (per-query override).
- **Effective deadline resolution** at query start:
  `per_query.timeout.or(client.query_timeout)`. If `Some`, compute `deadline = now + d`.
- `None` everywhere ⇒ identical to today (no timeout).

### 4.3 New helper: `cancel_and_drain`

New async function (location: `src/connection/server_packets.rs`, which already groups
packet-level helpers, or a new `src/connection/cancel.rs`):

```text
async fn cancel_and_drain<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    recv_timeout: Duration,
) -> Result<()>
```

Behaviour:

1. `stream.write_packet(&[ClientPacket::Cancel as u8]).await?; stream.flush().await?;`
2. Loop: read packet type with `runtime::time::timeout(recv_timeout, read_varint_async(stream))`.
   - `EndOfStream (5)` or `Exception (2)` ⇒ stop, return `Ok(())` (the `Exception` is the
     cancellation notice — not surfaced; the caller returns `Timeout`).
   - Any other packet (`Data`, `Progress`, `Log`, …) ⇒ read-and-discard its body, continue.
   - `Err(Elapsed)` (server ignored `Cancel` past `recv_timeout`) ⇒ return `Err` so the caller
     marks the connection unhealthy; the pool's `is_connection_alive` reaps it on next acquire.
     **Never hang.**

### 4.4 Read-path integration

Thread `deadline: Option<Instant>` and replace the fixed `timeout(recv_timeout, …)` with a
computed per-read bound at each site:

```text
let per_read = match deadline {
    Some(d) => {
        let remaining = d.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // deadline already reached → fall through to cancel_and_drain
            // + Error::Timeout (same as an elapsed read below)
        }
        std::cmp::min(recv_timeout, remaining)
    }
    None => recv_timeout,
};
```

On `Err(Elapsed)` with an active deadline ⇒ `cancel_and_drain(stream, recv_timeout).await?;
return Err(Error::Timeout(...))`.

Sites to update (all in `src/connection/`): `response_wait.rs` (`read_table_structure`,
`drain_response`), `block_reader.rs`, `block_stream.rs`, `select_response.rs`,
`row_stream_reader.rs`, `metadata.rs`, `insert_session.rs` (table-structure read + EOS ack).

The `deadline` is plumbed from `QueryBuilder` → the read functions (the call sites already pass
`recv_timeout` positionally; `deadline` is an added parameter).

### 4.5 Upgrade `BlockStream::cancel()` (`block_stream.rs:158`)

- Route through `cancel_and_drain` so the connection survives.
- Fix the doc comment ("no longer usable" → "query cancelled; connection remains usable").

### 4.6 Magic-byte cleanup (in-scope, minor)

Replace raw literals with named enum casts at the cancel/ping sites for readability and to make
grep find them:

- `&[3]` → `&[ClientPacket::Cancel as u8]` (`block_stream.rs:163`, `commands.rs:123`, and the
  `Drop` `try_write`).
- `&[4]` → `&[ClientPacket::Ping as u8]` (`io.rs:164`, `ops.rs:9`, `pool.rs:375`).

### 4.7 Error

The **new** deadline path returns the existing `Error::Timeout(String)` (`src/error.rs:44`);
`is_timeout()` and `is_retryable()` already return `true` for it. No new variant.

> Note: several existing recv-timeout sites currently return `Error::Protocol("timeout")`
> (e.g. `select_response.rs:58`, `block_stream.rs`, `metadata.rs`). Unifying those onto
> `Error::Timeout` is a **behaviour change** (different variant for callers matching on it) and
> is deliberately **left out** of this spec to honour the "no behaviour change" goal. It is a
> candidate for Spec 2 (dedup).

### 4.8 Python bindings (`st-clickhouse-py`)

Python uses the **sync core** (`st_clickhouse::sync::*`), which already has `query_timeout`. The
work here is plumbing only: expose `query_timeout` (and a per-query equivalent if the sync query
API supports it) on the PyO3 `Client` constructor and surface `TimeoutError` in the Python error
hierarchy. Async-Python is unaffected (it rides the sync core via the thread bridge).

> Note: adding `cancel_and_drain` to the **sync** core is deferred to Spec 2 (dedup). Today the
> sync core sets a socket read-timeout and lets the read fail on deadline; it does not send
> `Cancel`. That is an existing limitation, not a regression introduced here.

---

## 5. Data flow

```
query start
  → effective = per_query.timeout.or(client.query_timeout)
  → deadline  = effective.map(|d| Instant::now() + d)
  → read loop:
       per_read = deadline.map(min(recv_timeout, remaining)).unwrap_or(recv_timeout)
       match timeout(per_read, read_next()) {
           Ok(pkt)  => handle pkt (Data/Progress/Log/…/EndOfStream),
           Err(Elapsed) if deadline_set => {
               cancel_and_drain(stream, recv_timeout).await?;
               return Err(Error::Timeout("query exceeded {effective:?}"));   // NEW path
           }
           Err(Elapsed) /* recv_timeout floor, no query deadline */ =>
               // UNCHANGED from today: keep each site's existing behaviour
               // (most currently return Error::Protocol("timeout")).
               // NOT remapped to Error::Timeout here — see §4.7.
       }
  → PoolGuard drops cleanly → connection returned to pool ALIVE → next query reuses it
```

---

## 6. Error handling

| Situation | Result |
|---|---|
| Deadline elapses | `Error::Timeout`; `cancel_and_drain` ran; **connection reused** |
| `cancel_and_drain` drain stalls past `recv_timeout` (server ignores Cancel) | `Error::Timeout`; connection marked unhealthy; pool reaps via `is_connection_alive` on next acquire |
| `Cancel` write/flush fails | `Error::Timeout`; connection is bad; pool reaps on next acquire |
| Server returns `Exception` during drain | Treated as successful cancel drain; caller still returns `Error::Timeout` (the timeout is the user-visible cause) |
| No query deadline configured (`recv_timeout` floor only) | Identical to today, **including the floor's current error variant** (most sites: `Error::Protocol("timeout")`). Not remapped to `Error::Timeout` — see §4.7. |

**Invariant:** a timed-out query path never hangs and never silently keeps a query running
server-side when the server honours `Cancel`.

---

## 7. Testing

### Unit

- Effective-deadline resolution: per-query overrides client-level; both `None` ⇒ `None`.
- `min(recv_timeout, remaining)` edge cases (remaining < recv, remaining > recv, remaining == 0).
- `cancel_and_drain` packet sequence against a mock `AsyncRead + AsyncWrite` stream: asserts
  `Cancel` sent, then consumes a scripted `Data → EndOfStream` and returns `Ok`; asserts an
  `Exception` terminator also returns `Ok`; asserts a non-terminating stream returns `Err` after
  `recv_timeout`.
- Magic-byte replacement: cancel/ping bytes equal the enum discriminants.

### Integration (testcontainers, ClickHouse 26.x)

Uses the existing `tests/common/mod.rs` harness.

1. **Timeout fires + connection reused.** `SELECT sleep(3)` with `.timeout(1s)` ⇒
   `Err(is_timeout())`. Immediately run `SELECT 1` on the **same client/pool** ⇒ succeeds. This
   is the central proof that drain → reuse works.
2. **Client-level default.** `with_query_timeout(1s)` applies to all queries; no per-query set.
3. **Per-query overrides client-level.** Client `1s`, query `.timeout(10s)` on `SELECT sleep(2)`
   ⇒ succeeds.
4. **Streaming cursor.** `SELECT sleep(0.05)` from a cursor with a short client timeout ⇒
   `Timeout` mid-stream; subsequent `SELECT 1` reuses the connection.
5. **No regression.** Long query with **no** timeout configured completes normally.
6. **Insert path.** `INSERT` whose server-side EOS ack exceeds the deadline ⇒ `Timeout`; pool
   recovers.

### Negative / robustness

- Drain bounded by `recv_timeout`: simulate a server that ignores `Cancel` (artificially tiny
  `recv_timeout` for the drain) ⇒ `Timeout` returns promptly, connection is reaped. No hang.

---

## 8. Open questions for the implementation plan

- Exact param-threading shape for `deadline` through the read functions (positional vs a small
  `ReadOpts { recv_timeout, deadline }` struct). A struct reduces future churn if more per-read
  knobs appear — decide in the plan.
- Whether `.timeout(Duration)` on `QueryBuilder` should also be exposed on the `BatchBuilder`
  (batch sends multiple queries in one round-trip). Default: yes, applied to the whole batch.
- Naming: `.timeout()` vs `.with_timeout()` for the per-query builder method — match the
  prevailing builder convention in `query_builder.rs`.

---

## 9. Roadmap context

This is **Spec 1 of 3**, sequenced:

1. **Query timeout (this spec)** — async deadline + `cancel_and_drain` + Python plumbing.
2. **Dedup** — converge the parallel async/sync trees; fold sync `cancel_and_drain` parity and a
   shared deadline implementation in here.
3. **Perf** — broad performance work on the read hot paths.

Each spec gets its own brainstorm → plan → implement cycle.
