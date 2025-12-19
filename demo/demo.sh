#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG="${ROOT}/demo/local.toml"

NODE="${ROOT}/target/release/dwbase-node"
CLI="${ROOT}/target/release/dwbase-cli"

if [[ ! -f "$CFG" ]]; then
  echo "Config not found: $CFG" >&2
  exit 1
fi

if [[ ! -x "$NODE" || ! -x "$CLI" ]]; then
  echo "Building release binaries..."
  (cd "$ROOT" && cargo build --release)
fi

echo "=== DWBase Demo: Local Cognitive Memory ==="
echo "Using config: $CFG"
echo "Node:         $NODE"
echo "CLI:          $CLI"

echo
echo "=== 1) Starting node ==="
"$NODE" --config "$CFG" >"${ROOT}/demo/node.log" 2>&1 &
NODE_PID=$!

cleanup() {
  echo
  echo "=== Stopping node (PID $NODE_PID) ==="
  kill "$NODE_PID" >/dev/null 2>&1 || true
  wait "$NODE_PID" >/dev/null 2>&1 || true
  echo "Node logs saved to demo/node.log"
}
trap cleanup EXIT
sleep 2

echo
echo "=== 2) Writing memory ==="
echo '  -> Remembering latency event {"service":"checkout","latency_ms":820} (importance 0.9, label latency)'
curl -s -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"incident:42","worker":"cli","kind":"observation","timestamp":"","importance":0.9,"payload_json":"{\"service\":\"checkout\",\"latency_ms\":820}","vector":null,"flags":[],"labels":["latency"],"links":[]}' >/dev/null

echo '  -> Remembering error event {"service":"checkout","error":"timeout"} (importance 0.8, label error)'
curl -s -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"incident:42","worker":"cli","kind":"observation","timestamp":"","importance":0.8,"payload_json":"{\"service\":\"checkout\",\"error\":\"timeout\"}","vector":null,"flags":[],"labels":["error"],"links":[]}' >/dev/null

echo
echo "=== 3) Ask ==="
echo 'Question: "Why is checkout slow?"'
"$CLI" ask incident:42 --question "Why is checkout slow?"

echo
echo "=== 4) Inject untrusted ==="
echo '  -> Remembering untrusted latency event {"service":"checkout","latency_ms":5} (worker untrusted-worker)'
curl -s -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"incident:42","worker":"untrusted-worker","kind":"observation","timestamp":"","importance":0.9,"payload_json":"{\"service\":\"checkout\",\"latency_ms\":5}","vector":null,"flags":[],"labels":["latency"],"links":[]}' >/dev/null

echo
echo "=== 5) Ask again ==="
"$CLI" ask incident:42 --question "What is the current checkout latency?"

echo
echo "=== 6) Replay ==="
"$CLI" replay incident:42 --limit 20

echo
echo "=== Demo complete ==="
echo "Tip: tail -f demo/node.log"
