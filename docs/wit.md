# WIT Contracts

DWBase publishes a single WIT package (`dwbase:core`) describing the engine surface so hosts/guests can call remember/ask/replay/observe without re-defining the schema.

- `wit/dwbase-core.wit` — world `engine` with inlined types (`atom`, `new-atom`, `atom-filter`, `question`, `answer`). Types match `crates/dwbase-core` and `crates/dwbase-engine`.
- `wit/dwbase-types.wit` — reference type-only view (not imported to avoid parser issues).

Generated bindings live in:
- `crates/dwbase-wit-host` — Rust host adapter.
- `crates/dwbase-wit-guest` — WASM guest helper (`ClientConfig` etc.).

Validate contracts:
```bash
cargo test -p dwbase-wit-host
cargo test -p dwbase-wit-guest
wasm-tools print wit/dwbase-core.wit   # optional inspection
```

ANN is best-effort: guests should send `new-atom.vector` if they want vector search to activate (default embedder returns `None`).
