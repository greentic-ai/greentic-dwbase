#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PACK_DIR="$ROOT/packs/dwbase-memory"
DIST_DIR="$PACK_DIR/dist"
COMPONENT_DIR="$PACK_DIR/components/component-dwbase"

mkdir -p "$DIST_DIR"
mkdir -p "$COMPONENT_DIR"

echo "==> Building component-dwbase (wasm32-wasip2, release)"
cargo build -p component-dwbase --release --target wasm32-wasip2 --features component-wasm

WASM_SRC="$ROOT/target/wasm32-wasip2/release/component_dwbase.wasm"
if [[ ! -f "$WASM_SRC" ]]; then
  echo "ERROR: expected wasm artifact at: $WASM_SRC" >&2
  echo "Hint: ensure wasm32-wasip2 target is installed: rustup target add wasm32-wasip2" >&2
  exit 1
fi

echo "==> Staging component artifact + manifest"
cp "$ROOT/crates/component-dwbase/component.manifest.json" "$COMPONENT_DIR/component.manifest.json"
cp "$WASM_SRC" "$COMPONENT_DIR/component.wasm"

# Validate component with greentic-component doctor if installed
if command -v greentic-component >/dev/null 2>&1; then
  echo "==> greentic-component doctor"
  greentic-component doctor "$COMPONENT_DIR/component.wasm" || true
fi

OUT="$DIST_DIR/dwbase-memory.gtpack"
rm -f "$OUT"

echo "==> Packaging $OUT (zip format with .gtpack extension)"
(
  cd "$PACK_DIR"
  # Keep the archive inspectable; avoid embedding repo root paths.
  zip -qr "$OUT" \
    pack.json \
    README.md \
    config/default.env \
    flows/memory_demo.json \
    components/component-dwbase/component.manifest.json \
    components/component-dwbase/component.wasm
)

echo "==> Done: $OUT"
