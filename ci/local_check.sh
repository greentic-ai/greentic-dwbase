#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> cargo fmt"
cargo fmt

echo "==> cargo clippy"
cargo clippy --workspace --exclude component-dwbase --exclude dwbase-pack-runner -- -D warnings

echo "==> cargo test"
cargo test --workspace --exclude component-dwbase --exclude dwbase-pack-runner

echo "==> ensure wasm32-wasip2 target"
rustup target add wasm32-wasip2

echo "==> build component (wasm32-wasip2)"
cargo build -p component-dwbase --target wasm32-wasip2 --features component-wasm

echo "==> build dwbase-memory pack"
"$ROOT/packs/dwbase-memory/build.sh"

echo "==> schema/manifest sync check"
python3 "$ROOT/tools/check_schema_sync.py"

echo "==> cargo package (publish dry-run)"
OFFLINE="${CARGO_NET_OFFLINE:-0}"
if [[ "${OFFLINE}" == "1" ]]; then
  echo "  -> skipping cargo package (offline mode)"
  exit 0
fi
CRATES=(
  dwbase-core
  dwbase-metrics
  dwbase-engine
  dwbase-storage-sled
  dwbase-vector-hnsw
  dwbase-stream-local
  dwbase-security
  dwbase-embedder-dummy
  dwbase-swarm
  dwbase-swarm-nats
  dwbase-node
  dwbase-cli
  component-dwbase
  dwbase-pack-runner
)
for crate in "${CRATES[@]}"; do
  echo "  -> packaging ${crate}"
  if [[ "${crate}" == "component-dwbase" ]]; then
    CARGO_BUILD_TARGET=wasm32-wasip2 cargo package -p "${crate}" --allow-dirty
  else
    cargo package -p "${crate}" --allow-dirty
  fi
done
