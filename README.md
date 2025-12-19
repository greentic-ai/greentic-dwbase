# DWBase Workspace

DWBase is the first database built for digital workers and agentic agents—and the first one written almost entirely by an AI in ~24 hours of vibing code. It’s an immutable “atom” memory with reflex-style recall, built-in policy/metrics, WASI components, and swarm awareness.

## Why it’s different
- Agent-first atoms: immutable, world-scoped records with labels/flags/links and recency-weighted recall.
- Reflex pipeline: remember → embed (optional) → index → streams → swarm replication hooks.
- WIT everywhere: ships a `component-dwbase` WASI world with ask/remember/replay/health/metrics for host-agnostic embedding.
- Observe streams: backpressure-aware subscriptions with drop/backoff metrics and replay protection.
- Storage: sled-backed append-only log with secondary indexes, repair, and retention policy hooks.
- Vector: optional HNSW per-world ANN for reranking.
- Security: capability-aware policies, rate limits, and replication allow/deny lists.
- Swarm: durable peer membership, capability-aware replication filters, and replay protection.
- Ops/UX: Prometheus-compatible metrics snapshots, CLI + HTTP node, devnet tooling, pack build for components.
- Built by AI: ~24 hours of uninterrupted AI pairing produced the current stack.

## Install the CLI fast (cargo-binstall)
Prebuilt binaries are published for macOS (arm64 + x86_64), Linux (x86_64-gnu), and Windows (x86_64-msvc).
```bash
cargo binstall dwbase-cli

# binary name
dwbase --help
```

From source:
```bash
rustup target add wasm32-wasip2
cargo build -p dwbase-cli
```

## Quick start (remember, ask, replay)
```bash
# from repo root
cp config.example.toml config.toml
cargo run -p dwbase-node -- --config ./config.toml

# remember (HTTP). Leave timestamp empty to auto-fill; importance must be 0.0–1.0.
curl -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"demo","worker":"cli","kind":"observation","timestamp":"","importance":0.9,"payload_json":"{\"text\":\"checkout latency 820ms\"}","vector":null,"flags":[],"labels":["latency"],"links":[]}'

# ask (CLI); prompts for text if omitted
dwbase ask demo --question "Why is checkout slow?"

# replay most recent atoms
dwbase replay demo --limit 5

# list known worlds tracked since node start
dwbase list-worlds
```

Expected ask output (pretty JSON): supporting atoms sorted by timestamp, then importance; `text` is `"answer-pending"` because the engine returns raw supporting atoms.

## Concepts (v1)
- **Atom:** immutable record with `id`, `world`, `worker`, `kind`, ISO-8601 `timestamp`, `importance` (0.0–1.0), `payload_json` string, optional vector, flags/labels/links.
- **World:** namespace for atoms (e.g., `incident:42`); storage, vector, and streams are world-scoped.
- **remember:** append an atom; vector is embedded if missing; published to streams and reflex index.
- **ask:** return ranked atoms for a question (recency-first, then importance) using reflex index, storage scan, and optional ANN vector search.
- **observe:** publish an already-built atom to subscribers (no persistence).
- **replay:** ordered scan of stored atoms with filters (world, kinds, labels, flags, since/until, limit).

## What DWBase is not
- Not a relational or document database.
- Not a general-purpose vector store (ANN is in-memory per world; no durability).
- Not multi-node or replicated (single Axum node; swarm layer handles replication hooks separately).

## Workspace layout
- `crates/dwbase-core` — atom model.
- `crates/dwbase-engine` — orchestrator for remember/ask/observe/replay (+ optional metrics).
- `crates/dwbase-storage-sled` — sled-backed append-only storage with secondary index.
- `crates/dwbase-vector-hnsw` — in-memory HNSW ANN per world.
- `crates/dwbase-stream-local` — in-process pub/sub with backpressure metrics.
- `crates/dwbase-security` — capabilities + rate limits; trust scores recorded to metrics only.
- `crates/dwbase-node` — Axum HTTP API.
- `crates/dwbase-cli` — HTTP client (binary name: `dwbase`).
- `crates/dwbase-wit-host` / `crates/dwbase-wit-guest` — WIT bindings.
- `crates/dwbase-embedder-dummy`, `dwbase-metrics`, `dwbase-swarm` — adapters and scaffolding.
- `crates/component-dwbase` — WASI component + manifest; `packs/dwbase-memory` builds a runnable pack.

## Learn more
- `docs/overview.md` — DWBase in one page.
- `docs/cli.md` — CLI commands with real outputs.
- `docs/developer/` — architecture, engine, storage, vector, security, WIT details.
- `examples/` — runnable scenarios (`basic_memory`, `observe_stream`, `replay_simulation`, `worlds_and_forks`, `wasm-worker`).

## Building and testing
```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```
