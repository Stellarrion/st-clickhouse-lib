# Contributing

## Versioning

This section is the authoritative versioning manifest. CI enforces it; the
enforcement lives in `.github/workflows/ci.yml` (jobs `version-check` and
`version-bump-check`).

### Release model

- **Merging to `main` IS the release.** A push to `main` runs the full
  validation matrix and then publishes every artifact whose version is not
  already on the registries (idempotent skips otherwise).
- The annotated `vX.Y.Z` git tag is a **bookmark** created after publishing
  (`tag-release` job). It is not the publish trigger.
- Manual/back-compat path: pushing a `v*` tag also triggers publishing.

### Version sources (exactly four, must always agree)

1. `Cargo.toml` (`st-clickhouse-lib`)
2. `derive/Cargo.toml` (`st-clickhouse-derive`)
3. `st-clickhouse-py/Cargo.toml` (`st-clickhouse-py`)
4. `st-clickhouse-py/pyproject.toml` (the wheel version)

The `version-check` job fails any run where these differ.

### Bump rules (enforced before merge)

Every PR into `main` must carry a version that is **strictly greater** than
the version currently published on crates.io (`version-bump-check` job,
compared against the crates.io API, semver-aware). Consequences:

- Code change -> bump. There is no "land on main without releasing".
- Docs/chore/test-only change -> patch bump. Cheap and keeps the invariant.
- Never reuse or lower an already-published version.
- The PR's own manifests are what gets validated (CI checks out the PR merge
  ref), and exactly that version publishes on merge.

Semver selection:

- **Patch** (`0.3.1`): fixes, tests, docs, CI, dependency bumps with no API
  change.
- **Minor** (`0.4.0`): new features, new APIs, deprecations. May carry
  breaking changes while the project is `0.x` (pre-1.0 semver), but every
  breaking change MUST be listed under a `### Changed (BREAKING*)` heading
  in `CHANGELOG.md`.
- **Major** (`1.0.0`): reserved for the stability commitment.

### Changelog rule

Every released version MUST have a `## [X.Y.Z] — YYYY-MM-DD` section in
`CHANGELOG.md`; the PR gate fails if the header for the PR's version is
missing. Keep an empty `## [Unreleased]` section on top for post-tag work.

### Local helper

```bash
# all four manifests at once:
sed -i 's/^version = "X.Y.Z"/version = "A.B.C"/' \
  Cargo.toml derive/Cargo.toml st-clickhouse-py/Cargo.toml \
  st-clickhouse-py/pyproject.toml
```
