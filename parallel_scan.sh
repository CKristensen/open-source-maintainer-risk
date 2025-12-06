#!/bin/bash
# Package registry scan script - fetches top packages from NPM, PyPI, and Maven
# Analyzes maintainer risk in parallel
# Usage: ./parallel_scan.sh

set -e

TOKEN="${GITHUB_TOKEN}"
LIBRARIES_IO_KEY="${LIBRARIES_IO_API_KEY}"

if [ -z "$TOKEN" ]; then
    echo "Error: GitHub token required. Set GITHUB_TOKEN env var."
    echo "  export GITHUB_TOKEN=ghp_your_token_here"
    exit 1
fi

echo "============================================================"
echo "🔍 Open Source Maintainer Risk Scanner"
echo "============================================================"
echo ""
echo "Starting parallel scans of package registries..."
echo ""

# Create temp files for output
NPM_LOG=$(mktemp)
PYPI_LOG=$(mktemp)
MAVEN_LOG=$(mktemp)

# Cleanup on exit
cleanup() {
    rm -f "$NPM_LOG" "$PYPI_LOG" "$MAVEN_LOG"
}
trap cleanup EXIT

# Start NPM scan in background
echo "📦 [NPM] Starting scan of top 2000 packages..."
(
    uv run risk-tool scan-npm --limit 2000 --min-downloads 100000 2>&1
) > "$NPM_LOG" &
NPM_PID=$!

# Start PyPI scan in background
echo "🐍 [PyPI] Starting scan of top 2000 packages..."
(
    uv run risk-tool scan-pypi --limit 2000 --min-downloads 500000 2>&1
) > "$PYPI_LOG" &
PYPI_PID=$!

# Start Maven scan in background (if API key available)
if [ -n "$LIBRARIES_IO_KEY" ]; then
    echo "☕ [Maven] Starting scan of top 1000 packages..."
    (
        uv run risk-tool scan-maven --limit 1000 --min-dependents 1000 2>&1
    ) > "$MAVEN_LOG" &
    MAVEN_PID=$!
else
    echo "⚠️  [Maven] Skipping - LIBRARIES_IO_API_KEY not set"
    echo "   Get a free key at: https://libraries.io/api"
    MAVEN_PID=""
fi

echo ""
echo "Waiting for scans to complete..."
echo ""

# Wait for all background jobs and report status
FAILED=0

# Wait for NPM
if wait $NPM_PID; then
    echo "✅ [NPM] Scan complete"
else
    echo "❌ [NPM] Scan failed"
    FAILED=1
fi
echo "------------------------------------------------------------"
cat "$NPM_LOG"
echo ""

# Wait for PyPI
if wait $PYPI_PID; then
    echo "✅ [PyPI] Scan complete"
else
    echo "❌ [PyPI] Scan failed"
    FAILED=1
fi
echo "------------------------------------------------------------"
cat "$PYPI_LOG"
echo ""

# Wait for Maven (if started)
if [ -n "$MAVEN_PID" ]; then
    if wait $MAVEN_PID; then
        echo "✅ [Maven] Scan complete"
    else
        echo "❌ [Maven] Scan failed"
        FAILED=1
    fi
    echo "------------------------------------------------------------"
    cat "$MAVEN_LOG"
    echo ""
fi

echo "============================================================"
if [ $FAILED -eq 0 ]; then
    echo "✅ All scans complete! Results saved to risk_report.db"
else
    echo "⚠️  Some scans failed. Check logs above for details."
fi
echo "   Run 'uv run risk-tool explore' to view results."
echo "============================================================"
