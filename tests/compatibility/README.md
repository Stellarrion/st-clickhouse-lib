# Compatibility Testing

This directory contains scripts for testing `st-clickhouse-lib` against multiple ClickHouse server versions.

## Usage

```bash
# Test against ClickHouse 24.8, 25.8, 26.4, and current latest
./tests/compatibility/run.sh
```

The script:
1. Starts a Docker container for each ClickHouse version
2. Waits for it to become healthy
3. Runs the full Rust workspace test suite with all features
4. Reports pass/fail per version
5. Cleans up containers

## Prerequisites

- Docker
- curl (for health checks)
- Rust toolchain

## Version Coverage

| Version | Protocol Revision | Notes |
|---------|------------------|-------|
| 24.8    | 54483            | Current minimum supported revision |
| 25.8    | 54483+           | Stable LTS coverage |
| 26.4    | 54483+           | CH 26.x pinned coverage |
| latest  | 54483+           | Current published ClickHouse image |

## Adding a Version

Edit `run.sh` and add the version tag to the `VERSIONS` array:

```bash
VERSIONS=("24.8" "25.8" "26.4" "latest")
```

## Known Version Differences

- `DateTime64` scale handling changed between 24.x and 25.x
- `Variant`/`Dynamic`/`JSON` types were introduced in 25.x
- Protocol revision 54483 is the minimum required for all features
