#!/bin/bash
# NPM scan script - fetches top NPM packages and analyzes maintainer risk
# Usage: ./parallel_scan.sh [github_token] [limit] [min_downloads]

TOKEN="${1:-$GITHUB_TOKEN}"
LIMIT="${2:-1000}"
MIN_DOWNLOADS="${3:-10000}"

if [ -z "$TOKEN" ]; then
    echo "Error: GitHub token required. Pass as argument or set GITHUB_TOKEN env var."
    echo "Usage: ./parallel_scan.sh <github_token> [limit] [min_downloads]"
    echo ""
    echo "Options:"
    echo "  limit         Number of top NPM packages to scan (default: 1000)"
    echo "  min_downloads Minimum weekly downloads filter (default: 10000)"
    exit 1
fi

echo "Starting NPM package scan..."
echo "  Limit: $LIMIT packages"
echo "  Min downloads: $MIN_DOWNLOADS/week"
echo "============================================================"

# Scan top NPM packages
uv run risk-tool scan-npm --token "$TOKEN" --limit "$LIMIT" --min-downloads "$MIN_DOWNLOADS"

echo "============================================================"
echo "Scan complete! Results saved to risk_report.db"
echo "Run 'uv run risk-tool explore' to view results."
