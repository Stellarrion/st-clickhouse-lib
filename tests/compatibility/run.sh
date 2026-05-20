#!/usr/bin/env bash
# Compatibility test runner for st-clickhouse.
# Runs the full integration test suite against multiple ClickHouse versions.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERSIONS=("24.8" "25.8" "26.4" "latest")
RESULTS=()

cleanup() {
    local version="$1"
    echo "  Cleaning up ClickHouse ${version}..."
    docker rm -f "ch-compat-${version}" 2>/dev/null || true
}

cleanup_all() {
    for v in "${VERSIONS[@]}"; do
        cleanup "$v"
    done
}
trap cleanup_all EXIT

for version in "${VERSIONS[@]}"; do
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Testing ClickHouse ${version}"
    echo "═══════════════════════════════════════════════════════════"

    # Start container
    cleanup "$version"
    docker run -d --name "ch-compat-${version}" \
        -p 9000:9000 \
        -p 8123:8123 \
        -e CLICKHOUSE_SKIP_USER_SETUP=1 \
        "clickhouse/clickhouse-server:${version}" 2>/dev/null

    echo "  Waiting for ClickHouse ${version} to become healthy..."
    for i in $(seq 1 30); do
        if curl -s http://127.0.0.1:8123/ping >/dev/null 2>&1; then
            echo "  ClickHouse ${version} is healthy (attempt ${i})"
            break
        fi
        if [ "$i" -eq 30 ]; then
            echo "  FAIL: ClickHouse ${version} did not become healthy"
            RESULTS+=("${version}: FAIL (health check)")
            cleanup "$version"
            continue 2
        fi
        sleep 2
    done

    # Small extra wait for handshake to be ready
    sleep 2

    # Run tests
    cd "$PROJECT_DIR"
    if cargo test --workspace --all-features -- --test-threads=4 2>&1; then
        echo "  PASS: All tests passed on ClickHouse ${version}"
        RESULTS+=("${version}: PASS")
    else
        echo "  FAIL: Some tests failed on ClickHouse ${version}"
        RESULTS+=("${version}: FAIL")
    fi

    cleanup "$version"
done

# Summary
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  COMPATIBILITY RESULTS"
echo "═══════════════════════════════════════════════════════════"
for result in "${RESULTS[@]}"; do
    echo "  ${result}"
done

# Exit with failure if any version failed
for result in "${RESULTS[@]}"; do
    if [[ "$result" == *FAIL* ]]; then
        exit 1
    fi
done
echo "  All versions passed!"
