#!/usr/bin/env bash
# verify_endpoints.sh — quick CURL checks for mounted debug routes
set -euo pipefail
BASE_URL="${1:-http://localhost:8000}"
echo "Hitting $BASE_URL/views/debug/connection"
curl -sS "$BASE_URL/views/debug/connection" | jq . || curl -sS "$BASE_URL/views/debug/connection"
echo
echo "Hitting $BASE_URL/views/debug/diagnose"
curl -sS "$BASE_URL/views/debug/diagnose" | jq . || curl -sS "$BASE_URL/views/debug/diagnose"
echo
