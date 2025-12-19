# Developer: CI checks to keep WIT/pack/schema in sync

Run the same checks locally that CI enforces:

- `cargo fmt --check`
- `cargo clippy -- -D warnings`
- `cargo test`
- `rustup target add wasm32-wasip2` (one-time)
- `cargo build -p component-dwbase --target wasm32-wasip2`
- `./packs/dwbase-memory/build.sh`
- `python3 tools/check_schema_sync.py` (validates `component.manifest.json` JSON and compares the operation list to `wit/dwbase-core.wit` exports; allowlists `dwbase.world.list` and WIT-only ops `dwbase.observe`, `dwbase.metrics_snapshot`)

These commands fail fast when the WASI build breaks, the pack drifts, or the manifest/WIT exports diverge.
