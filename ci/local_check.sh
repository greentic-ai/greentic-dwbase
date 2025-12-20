#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Normalize CARGO_NET_OFFLINE to bool strings for cargo.
if [[ "${CARGO_NET_OFFLINE:-}" == "1" ]]; then
  export CARGO_NET_OFFLINE=true
fi

if [[ "${CARGO_NET_OFFLINE:-}" != "true" ]]; then
  echo "==> install greentic tools (if missing)"
  command -v greentic-component >/dev/null 2>&1 || cargo install greentic-component --version 0.4
  command -v packc >/dev/null 2>&1 || cargo install packc --version 0.4
  command -v cargo-component >/dev/null 2>&1 || cargo install cargo-component --version 0.21
else
  echo "==> offline mode: skipping tool installs"
fi

echo "==> cargo fmt"
cargo fmt

echo "==> cargo clippy"
cargo clippy --workspace --exclude component-dwbase --exclude dwbase-pack-runner -- -D warnings

echo "==> cargo test"
cargo test --workspace --exclude component-dwbase --exclude dwbase-pack-runner

if [[ "${CARGO_NET_OFFLINE:-}" == "true" ]]; then
  echo "==> offline mode: skipping component build, pack build, schema check, and packaging"
  exit 0
fi

echo "==> ensure wasm32-wasip2 target"
rustup target add wasm32-wasip2

echo "==> build component (wasm32-wasip2)"
cargo component build -p component-dwbase --release --target wasm32-wasip2 --features component-wasm

COMPONENT_WASM="$ROOT/target/wasm32-wasip2/release/component_dwbase.wasm"
COMPONENT_MANIFEST="$ROOT/target/wasm32-wasip2/release/component.manifest.json"

echo "==> stage component manifest"
cp "$ROOT/crates/component-dwbase/component.manifest.json" "$COMPONENT_MANIFEST"
greentic-component hash --wasm "$COMPONENT_WASM" "$COMPONENT_MANIFEST"

echo "==> stage component into pack"
cp "$COMPONENT_WASM" "$ROOT/packs/dwbase-gtpack/components/component_dwbase.wasm"
cp "$COMPONENT_MANIFEST" "$ROOT/packs/dwbase-gtpack/components/component.manifest.json"

echo "==> build gtpack with packc"
packc build --in "$ROOT/packs/dwbase-gtpack" --gtpack-out "$ROOT/packs/dwbase-gtpack/dist/dwbase.gtpack"

echo "==> greentic-component doctor"
greentic-component doctor "$COMPONENT_MANIFEST"

echo "==> packc verify"
DEV_KEY="$ROOT/packs/dwbase-gtpack/dist/dev-component.key"
DEV_PUB="$ROOT/packs/dwbase-gtpack/dist/dev-component.pub"
openssl genpkey -algorithm Ed25519 -out "$DEV_KEY"
openssl pkey -in "$DEV_KEY" -pubout -out "$DEV_PUB"
packc sign --pack "$ROOT/packs/dwbase-gtpack" --manifest "$ROOT/packs/dwbase-gtpack/dist/manifest.cbor" --key "$DEV_KEY" --offline
packc verify --pack "$ROOT/packs/dwbase-gtpack" --manifest "$ROOT/packs/dwbase-gtpack/dist/manifest.cbor" --key "$DEV_PUB" --offline

echo "==> schema/manifest sync check"
python3 "$ROOT/tools/check_schema_sync.py"

echo "==> cargo package (publish dry-run)"
OFFLINE="${CARGO_NET_OFFLINE:-0}"
if [[ "${OFFLINE}" == "1" || "${OFFLINE}" == "true" ]]; then
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
