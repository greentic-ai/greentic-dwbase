# dwbase-memory.gtpack (demo)

Greentic pack bundling `component-dwbase` plus a simple demo flow that shows “LLM long-term memory” semantics:

- remember key facts from the input
- ask memory before responding
- write a summary atom

This pack is designed to be fully reproducible locally (no cloud dependencies).

## Build the `.gtpack`
From repo root:
```bash
./packs/dwbase-memory/build.sh
```

This produces `packs/dwbase-memory/dist/dwbase-memory.gtpack`.

Notes:
- Requires Rust target `wasm32-wasip2` for the component build:
  - `rustup target add wasm32-wasip2`

## Run the demo flow (manual)
The canonical flow definition lives at `packs/dwbase-memory/flows/memory_demo.json` and is intended to be runnable from a pack/flow runner (not just a JSON placeholder).

The component uses environment variables for config; a safe-by-default config template is in:
- `packs/dwbase-memory/config/default.env`

### Run with the included host-side runner (recommended for this repo)
This repo ships a minimal runner (`dwbase-pack-runner`) that loads the pack/flow JSON and executes steps against `component-dwbase` in-process.

Install (local):
- Build the pack: `./packs/dwbase-memory/build.sh`
- Copy the pack to your preferred location (optional): `cp packs/dwbase-memory/dist/dwbase-memory.gtpack ./dwbase-memory.gtpack`
- You can always run it directly from `packs/dwbase-memory/dist/` (no global install step is required by the included runner).

Build the `.gtpack`:
```bash
./packs/dwbase-memory/build.sh
```

Load defaults (env/secrets):
```bash
set -a
source ./packs/dwbase-memory/config/default.env
set +a
```

Run the flow twice (same `DWBASE_DATA_DIR`) and observe persistence:
```bash
DWBASE_DATA_DIR=./packs/dwbase-memory/demo-data \
DWBASE_TENANT_ID=demo \
cargo run -p dwbase-pack-runner -- \
  --pack ./packs/dwbase-memory/dist/dwbase-memory.gtpack \
  --flow memory_demo \
  --input user_text="I live in Berlin and my favorite color is green."

DWBASE_DATA_DIR=./packs/dwbase-memory/demo-data \
DWBASE_TENANT_ID=demo \
cargo run -p dwbase-pack-runner -- \
  --pack ./packs/dwbase-memory/dist/dwbase-memory.gtpack \
  --flow memory_demo \
  --input user_text="What do you remember about me?"
```

The runner prints a JSON object containing step outputs (including `replay_recent`), which should include atoms from the previous run.

Shortcut script:
```bash
./packs/dwbase-memory/run.sh "I live in Berlin and my favorite color is green."
```

### Legacy fallback (no pack runner)
To verify persistence across runs without pack tooling, you can run the included local demo example:
```bash
DWBASE_DATA_DIR=./packs/dwbase-memory/demo-data \
DWBASE_TENANT_ID=demo \
cargo run -p component-dwbase --example memory_demo -- "I live in Berlin and my favorite color is green."
```

Run it twice (same `DWBASE_DATA_DIR`). The second run should retrieve the previously remembered facts and summary.
