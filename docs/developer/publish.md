# Publishing to crates.io

Workspace crates now carry the required metadata (license, description, repository, readme, etc.) and workspace dependencies include version + path so `cargo publish` can succeed.

Publish in dependency order:

```bash
./ci/publish-crates.sh --dry-run   # sanity check
./ci/publish-crates.sh             # real publish
```

Order (as scripted): core → metrics → engine → storage → vector → stream → security → embedder → swarm → swarm-nats → node → cli → component → pack-runner.

Notes:
- All crates share the workspace version. Bump `version` in `Cargo.toml` (workspace.package) and commit before publishing.
- `component-dwbase` publishes the WASI bindings crate; the actual `.wasm` is shipped via GitHub releases/pack artifacts, not crates.io.
- If you only want to publish a subset, run `cargo publish -p <crate> [--dry-run]` manually but keep dependency order (e.g., publish `dwbase-core` before `dwbase-engine`).
