# Developer: WIT

DWBase exposes a WIT package for host/guest integration without reimplementing the HTTP API.

## Contracts
- `wit/dwbase-core.wit` (package `dwbase:core`): defines `engine` interface with `remember`, `ask`, `observe`, `replay` plus all atom/question/answer types. Types are inlined (no imports) for compatibility with current `wit-parser`.
- `wit/dwbase-types.wit`: shared shapes only; kept for reference.

Engine signature (simplified):
```wit
remember: func(atom: new-atom) -> atom-id
ask: func(question: question) -> answer
observe: func(atom: atom)
replay: func(target-world: world-key, filter: atom-filter) -> list<atom>
```

## Bindings
- Host (`crates/dwbase-wit-host`): adapts the WIT `engine` world to Rust traits, validates WIT parsing in tests.
- Guest (`crates/dwbase-wit-guest`): re-exports generated types and `ClientConfig { endpoint: String }` for WASM guests. Used by `examples/wasm-worker`.

## Validation commands
```bash
cargo test -p dwbase-wit-host
cargo test -p dwbase-wit-guest
wasm-tools print wit/dwbase-core.wit   # optional inspection
```

## Notes
- No trust/ranking logic lives in WIT; it mirrors the Rust engine types exactly.
- The dummy embedder means ANN is skipped unless the guest supplies vectors in `new-atom.vector`.
