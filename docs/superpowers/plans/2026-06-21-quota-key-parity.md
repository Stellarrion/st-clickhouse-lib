# Async `quota_key` Parity Implementation Plan

> **For agentic workers:** This is mechanical parity with the sync core. Steps use
> checkbox (`- [ ]`) syntax. Every change is a one-line `&str` param + pass-through.

**Goal:** Make the async client send a configurable `quota_key` (ClientInfo protocol
field + handshake addendum) instead of a hardcoded `""`, matching the sync core.

**Architecture:** Source of truth is `SimplePool.quota_key` (alongside
`user`/`password`/`database`) — required because the handshake-addendum site
(`pool.rs:355`) runs in the pool's connect path with no `Client`. The value is threaded
as a `&str` param through the internal template-build chain to the ClientInfo encoder.
The public `write_client_info` keeps its signature (non-breaking; defaults to `""`).

**Tech Stack:** Rust 2024, tokio async, native ClickHouse protocol.

---

## Design decisions (approved)

1. **URL `?quota_key=`** → protocol ClientInfo field (parity with sync). Previously it
   fell through to the `settings` map (sent as a CH *setting*). Behavior change, more
   correct. The arm is added **before** the settings fallback in `parse_query`.
2. **`with_quota_key` / `set_quota_key` bumps config generation** → existing pooled
   connections reconnect so the handshake addendum carries the new key (same as
   `set_database` / `set_credentials`).
3. **Default `""`** → identical wire bytes when unset; zero behavior change for
   existing users.
4. **Public `write_client_info(buf, rev, tracing)` signature unchanged** (it is
   re-exported via `pub mod client_info`). It defaults `quota_key` to `""`. All real
   query paths go through the threaded template chain, which carries the configured
   value.

## Wire sites (async, currently hardcoded `""`)

- `src/client_info.rs:44` — template path (`build_client_info_template`)
- `src/client_info.rs:104` — direct path (`write_client_info_with_query_id`)
- `src/pool.rs:355` — handshake addendum (`connect_raw`)

Sync parity: `src/sync/client_info.rs:69` + `src/sync/client.rs:184`.

---

## Task 1: Pool plumbing

**Files:** `src/pool.rs`

- [ ] Add field `quota_key: String` to `SimplePool` (after `database`, ~:498).
- [ ] Initialize `quota_key: String::new()` in `new()` (~:537).
- [ ] Add `pub(crate) fn set_quota_key(&mut self, key: &str) { self.quota_key = key.to_owned(); self.bump_config_generation(); }` (mirror `set_database`, ~:634).
- [ ] Add `pub(crate) fn quota_key(&self) -> &str { &self.quota_key }`.
- [ ] Add `quota_key: &'a str` to `RawConnectConfig` (~:297).
- [ ] Write `config.quota_key` at `pool.rs:355` instead of `""`.
- [ ] Add `quota_key: &self.quota_key,` to **both** `RawConnectConfig { .. }` sites (tls ~:708, non-tls ~:720).
- [ ] **Test:** unit test `set_quota_key` stores + accessor returns; default `""`.

## Task 2: Thread `quota_key` through the template-build chain + callers

**Files:** `src/client_info.rs`, `src/connection/io.rs`, `src/connection/query_packet.rs`,
`src/connection/batch.rs`, `src/connection/commands.rs`, `src/connection/block_stream.rs`,
`src/connection/insert_session.rs`, `src/connection/connect.rs`, `src/connection/config.rs`

- [ ] `build_client_info_template(rev)` → `(rev, quota_key: &str)`; write `quota_key` at
      line 44.
- [ ] `build_query_packet_common_template(settings, compression, rev)` → `+ quota_key`;
      pass to `build_client_info_template`.
- [ ] `build_query_packet_template(settings, compression, rev)` → `+ quota_key`; pass to
      `build_query_packet_common_template`.
- [ ] `build_query_packet_from_cached_or_revision(...)` → `+ quota_key: &str`; pass to
      `build_query_packet_template` at the rebuild site.
- [ ] `build_batch_query_packet_template(settings, compression, rev)` → `+ quota_key`;
      pass to `build_query_packet_common_template`.
- [ ] **Callers** pass `self.pool.quota_key()` (or `self.client.pool.quota_key()` for
      batch):
      - `commands.rs:99`, `block_stream.rs:57`, `insert_session.rs:39`
      - `batch.rs:126` (`self.client.pool.quota_key()`)
- [ ] `connect.rs::from_pool`: read `pool.quota_key()` before the move; pass to the
      placeholder `build_query_packet_template`.
- [ ] `config.rs::refresh_query_template`: pass `self.pool.quota_key()`.
- [ ] **Test:** unit test — `build_client_info_template(REV, "tenant-42")` encodes
      `tenant-42` at the quota_key position (assert the bytes appear after the revision
      varint, gated by `DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO`).

## Task 3: Client + builder entry points

**Files:** `src/connection/config.rs`, `src/builder.rs`

- [ ] `Client::with_quota_key(mut self, key: &str) -> Self`:
      `self.pool.set_quota_key(key); self.refresh_query_template(); self`
- [ ] `BuilderOptions.quota_key: String` + `String::new()` in `Default`.
- [ ] `ClientBuilder::quota_key(mut self, key: impl Into<String>) -> Self`.
- [ ] URL arm `"quota_key" => opts.quota_key = value` (before the `_` settings fallback).
- [ ] `connect()`: `pool.set_quota_key(&self.opts.quota_key);` (before `new_connected`).
- [ ] **Tests:** builder stores; default empty; URL `?quota_key=` parses (separate test
      module mirroring `acquire_timeout_tests`).

## Task 4: Integration smoke test + CHANGELOG + memory

**Files:** `tests/quota_key_test.rs`, `CHANGELOG.md`, memory

- [ ] `tests/quota_key_test.rs` — `#[ignore]` live test: client with
      `with_quota_key("...")`, `SELECT toUInt8(1)` succeeds (quota accounting is hard to
      observe; this proves no protocol break). Uses `common::connect_client`.
- [ ] `CHANGELOG.md` — entry under `[Unreleased]/Added`.
- [ ] Update `gap-roadmap-progress` memory: #4 done.
- [ ] **Verify:** `cargo clippy --all-targets --all-features -- -D warnings`;
      `cargo fmt --check`; `cargo test --lib`; run the `#[ignore]` integration test.

## Self-Review notes

- `refresh_query_template(&mut self)` reads `self.pool.quota_key()` (`&str` into
  `&self.pool`) and `&self.settings`, then assigns `self.query_template` — disjoint
  borrows, the `&str` is copied into bytes inside the builder, no lifetime conflict.
- `from_pool(pool)` moves `pool` into `Self`; read `let quota_key = pool.quota_key().to_owned();`
  first, pass `&quota_key` to the placeholder builder.
- `clippy::panic`/`clippy::unwrap_used` are `deny` in this repo (test code included) —
  use `assert!(matches!(..))` + `.expect("msg")`, never `panic!`/`unwrap()`. See
  [[clippy-panic-test-convention]].
