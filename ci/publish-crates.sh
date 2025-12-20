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

# Prefer explicit token if provided; cargo will also honor ~/.cargo/credentials.
TOKEN_ARG=""
if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  TOKEN_ARG="--token ${CARGO_REGISTRY_TOKEN}"
elif [[ -n "${CARGO_REGISTRIES_CRATES_IO_TOKEN:-}" ]]; then
  TOKEN_ARG="--token ${CARGO_REGISTRIES_CRATES_IO_TOKEN}"
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
    OUTPUT=$(CARGO_BUILD_TARGET=wasm32-wasip2 cargo publish -p "${crate}" ${DRYRUN} ${TOKEN_ARG} 2>&1)
  else
    OUTPUT=$(cargo publish -p "${crate}" ${DRYRUN} ${TOKEN_ARG} 2>&1)
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
