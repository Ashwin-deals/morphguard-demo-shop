#!/usr/bin/env bash
# Drive the running demo shop with curl: 30 normal requests, then every chaos
# mode for a chaos user (X-User 9001) and a normal user (X-User 100).
# Writes demo_shop.phases (phase name + log line range) for check_log.py.
#
#   ./verify_chaos.sh [base_url]        default http://127.0.0.1:8001
set -uo pipefail
B=${1:-http://127.0.0.1:8001}
DIR=$(cd "$(dirname "$0")" && pwd)
LOG="$DIR/demo_shop.log"
PHASES="$DIR/demo_shop.phases"
: > "$PHASES"

lines() { wc -l < "$LOG" | tr -d ' '; }
req() {  # method path user
  curl -s -o /dev/null -m 15 -w "  %{http_code} %{size_download}B %{time_total}s  $1 $2 (X-User ${3:-none})\n" \
       -X "$1" ${3:+-H "X-User: $3"} "$B$2"
}
phase() { echo "$1 $2 $(lines)" >> "$PHASES"; }
chaos() { curl -s -X POST "$B/_chaos/$1"; echo; }

curl -s -o /dev/null --retry 20 --retry-delay 1 --retry-connrefused -m 5 "$B/_chaos/status" || { echo "app not reachable at $B"; exit 1; }

echo "== baseline: 30 requests, chaos off"
start=$(lines)
for spec in "GET / 101" "GET /api/products 101" "GET /api/products/551 101" "POST /api/cart 101" "GET /api/cart 101" \
            "POST /api/checkout 101" "GET / 102" "GET /static/app.js 102" "POST /accounts/login 102" "GET /api/products/12 102" \
            "GET /api/orders/4643 102" "GET /api/reports/7 103" "GET /api/products 103" "POST /api/cart 103" "POST /api/checkout 103" \
            "GET /api/orders/88 103" "GET / 104" "GET /api/products/954 104" "GET /api/cart 104" "POST /accounts/login 104" \
            "GET /static/app.js 104" "GET /api/products 105" "GET /api/reports/12 105" "GET /api/products/3885 105" \
            "GET /api/orders/1 106" "POST /api/cart 106" "GET /" "GET /api/nope 106" "GET /api/checkout 107" "GET /api/products/77 107"; do
  set -- $spec; req "$1" "$2" "${3:-}"
done
echo "$start" > /dev/null; echo "baseline $start $(lines)" >> "$PHASES"

echo "== /_chaos endpoints (never logged)"
before=$(lines)
curl -s "$B/_chaos/status"; echo
echo -n "  POST /_chaos/bogus -> "; curl -s -o /dev/null -w "%{http_code}\n" -X POST "$B/_chaos/bogus"
echo -n "  GET  /_chaos/off   -> "; curl -s -o /dev/null -w "%{http_code}\n" "$B/_chaos/off"
echo "  log lines added by /_chaos calls: $(( $(lines) - before ))"

for mode in 500_burst slow db_errors; do
  echo "== chaos $mode"
  echo -n "  POST /_chaos/$mode -> "; chaos "$mode"
  echo -n "  GET  /_chaos/status -> "; curl -s "$B/_chaos/status"; echo
  for user in 9001 100; do
    start=$(lines)
    case $mode in
      500_burst) req POST /api/checkout $user; req GET /api/orders/4643 $user; req POST /api/cart $user; req GET /api/cart $user ;;
      slow)      req GET /api/reports/12 $user; req GET /api/products/551 $user; req GET /api/orders/1 $user ;;
      db_errors) req GET /api/orders/4643 $user; req GET /api/orders/88 $user; req POST /api/checkout $user ;;
    esac
    echo "$mode-$user $start $(lines)" >> "$PHASES"
  done
done
echo -n "== POST /_chaos/off -> "; chaos off
