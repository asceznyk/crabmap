#!/bin/bash
set -e

cargo build

results=$(mktemp)

setsid ./target/debug/crabstore \
  --pvolumes localhost:4001,localhost:4002,localhost:4003,localhost:4004,localhost:4005 \
  --dbfile /tmp/test.db \
  run &
PID=$!

cleanup() {
  echo "Stopping crabstore..."
  kill -- "-$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  rm -f /tmp/test.db "$results"
}
trap cleanup EXIT

sleep 1

echo "Running concurrent PUT test..."

for i in {1..100}; do
  curl -s -o /dev/null -w "%{http_code}\n" \
    -X PUT --data "value-$i" \
    http://localhost:4000/foo >> "$results" &
done

set +e
wait
wait_status=$?
set -e
echo "wait status: $wait_status"

echo "ABOUT TO COUNT"

created=$(grep -c '^201$' "$results" || true)
conflicts=$(grep -c '^409$' "$results" || true)
forbidden=$(grep -c '^403$' "$results" || true)

echo "201: $created"
echo "409: $conflicts"
echo "403: $forbidden"

if [ "$created" -ne 1 ]; then
  echo "FAIL: expected 1 CREATED, got $created"
  exit 1
fi

if [ $((conflicts + forbidden)) -ne 99 ]; then
  echo "FAIL: expected 99 rejected, got $((conflicts + forbidden))"
  exit 1
fi

echo "PASS"

