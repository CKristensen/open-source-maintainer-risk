#!/bin/bash
# Parallel scan script - runs 5 scans for popular languages simultaneously
# Usage: ./parallel_scan.sh <github_token>

TOKEN="${1:-$GITHUB_TOKEN}"

if [ -z "$TOKEN" ]; then
    echo "Error: GitHub token required. Pass as argument or set GITHUB_TOKEN env var."
    echo "Usage: ./parallel_scan.sh <github_token>"
    exit 1
fi

echo "Starting parallel scans for top 5 languages (stars:>2000, limit 1000)..."
echo "============================================================"

# Run 5 scans in parallel using background processes
uv run risk-tool scan --token "$TOKEN" --limit 1000 --query "language:python stars:>2000" &
uv run risk-tool scan --token "$TOKEN" --limit 1000 --query "language:javascript stars:>2000" &
uv run risk-tool scan --token "$TOKEN" --limit 1000 --query "language:typescript stars:>2000" &
uv run risk-tool scan --token "$TOKEN" --limit 1000 --query "language:go stars:>2000" &
uv run risk-tool scan --token "$TOKEN" --limit 1000 --query "language:rust stars:>2000" &

# Wait for all background jobs to complete
wait

echo "============================================================"
echo "All scans complete! Results saved to risk_report.db"
echo "Run 'uv run risk-tool explore' to view results."
