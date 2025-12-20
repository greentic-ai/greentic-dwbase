# Developer: CI checks to keep WIT/pack/schema in sync

Run the same checks locally that CI enforces:

- `cargo fmt --check`
- `cargo clippy -- -D warnings`
- `cargo test`
- `rustup target add wasm32-wasip2` (one-time)
- `cargo component build -p component-dwbase --target wasm32-wasip2 --features component-wasm`
- `greentic-component hash --wasm target/wasm32-wasip2/release/component_dwbase.wasm target/wasm32-wasip2/release/component.manifest.json`
- `greentic-component doctor target/wasm32-wasip2/release/component.manifest.json`
- `packc build --in packs/dwbase-gtpack --gtpack-out packs/dwbase-gtpack/dist/dwbase.gtpack`
- `packc sign --pack packs/dwbase-gtpack --manifest packs/dwbase-gtpack/dist/manifest.cbor --key <ed25519-priv> --offline`
- `packc verify --pack packs/dwbase-gtpack --manifest packs/dwbase-gtpack/dist/manifest.cbor --key <ed25519-pub> --offline`
- `python3 tools/check_schema_sync.py` (validates the Greentic world + manifest basics)

These commands fail fast when the WASI build breaks, the pack drifts, or the manifest/WIT exports diverge.

Shortcut: `ci/local_check.sh` runs exactly what CI runs (fmt, clippy, tests, wasm32-wasip2 build, pack build, schema sync).
