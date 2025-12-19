#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PACK_DIR="$ROOT/packs/dwbase-memory"
DIST_PACK="$PACK_DIR/dist/dwbase-memory.gtpack"

USER_TEXT="${1:-}"
if [[ -z "$USER_TEXT" ]]; then
  echo "Usage: $0 \"<user_text>\"" >&2
  exit 2
fi

if [[ ! -f "$DIST_PACK" ]]; then
  echo "Pack not built; building..." >&2
  "$PACK_DIR/build.sh"
fi

# Load default env (safe-by-default), exporting all variables.
set -a
source "$PACK_DIR/config/default.env"
set +a

echo "==> Running pack flow twice to demonstrate persistence (DWBASE_DATA_DIR=$DWBASE_DATA_DIR)"

cargo run -p dwbase-pack-runner -- \
  --pack "$DIST_PACK" \
  --flow memory_demo \
  --input "user_text=$USER_TEXT" >/dev/null

cargo run -p dwbase-pack-runner -- \
  --pack "$DIST_PACK" \
  --flow memory_demo \
  --input "user_text=What do you remember about me?" \
  --json

