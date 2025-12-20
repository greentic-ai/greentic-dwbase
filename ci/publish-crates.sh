#!/usr/bin/env bash
set -euo pipefail

# Publish workspace crates to crates.io in dependency order.
# Usage: ./ci/publish-crates.sh [--dry-run]

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DRYRUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
  DRYRUN="--dry-run"
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
  echo "==> publishing ${crate} ${DRYRUN}"
  set +e
  if [[ "${crate}" == "component-dwbase" ]]; then
    OUTPUT=$(CARGO_BUILD_TARGET=wasm32-wasip2 cargo publish -p "${crate}" ${DRYRUN} 2>&1)
  else
    OUTPUT=$(cargo publish -p "${crate}" ${DRYRUN} 2>&1)
  fi
  STATUS=$?
  set -e
  if [[ $STATUS -ne 0 ]]; then
    if echo "$OUTPUT" | grep -qi "already uploaded"; then
      echo "    ${crate} already published; skipping"
    else
      echo "$OUTPUT"
      exit $STATUS
    fi
  fi
done
