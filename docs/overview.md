# DWBase in One Page

DWBase is an append-only atom store for agent memory. Atoms live in worlds and can be remembered, asked over, observed on a stream, or replayed from storage. Retrieval is recency-first with optional ANN vector search.

## Core pieces
- **Atom:** `{id, world, worker, kind, timestamp (RFC3339), importance 0–1, payload_json, vector?, flags[], labels[], links[]}` (immutable).
- **World:** namespace boundary; storage, vector index, and stream subscriptions are keyed by `WorldKey`.
- **Engine (`crates/dwbase-engine`):** orchestrates remember/ask/observe/replay; reflex index caches recent atoms; ANN uses `VectorEngine` if embeddings exist.
- **Node (`crates/dwbase-node`):** Axum HTTP API exposing `/remember`, `/ask`, `/replay`, `/worlds`, `/atoms/{id}`.
- **CLI (`crates/dwbase-cli`):** wraps the HTTP API for ask/replay/list/inspect.
- **Storage (`crates/dwbase-storage-sled`):** sled-backed append-only log per world.
- **Vector (`crates/dwbase-vector-hnsw`):** in-memory HNSW per world (no durability).
- **Streams (`crates/dwbase-stream-local`):** in-process pub/sub with poll semantics.
- **Security (`crates/dwbase-security`):** capabilities, rate limits, importance cap; trust scores are recorded to metrics only.

## What DWBase is / is not
- Is: single-node append-only memory with recent-first recall and optional ANN.
- Is not: relational/document DB, durable vector store, or distributed system.

## Flows

Remember:
```
new atom -> gatekeeper (caps/rate/importance) -> embed if missing
         -> vector.upsert (when vector present) -> storage.append
         -> reflex_index.insert -> stream.publish
```

Ask:
```
question -> gatekeeper -> reflex_index.filter (recency window)
         -> storage.scan fallback -> optional embed+vector.search
         -> fetch atoms by id -> dedupe -> sort (newest, then importance)
         -> Answer { world, text:"answer-pending", supporting_atoms }
```

Replay: ordered scan from storage with filters (world, kinds, labels, flags, since/until, limit).

Observe: publish an existing atom to subscribers (no persistence).

## Usage snapshot
```bash
cargo run -p dwbase-node -- --config ./config.toml
curl -X POST http://127.0.0.1:8080/remember -H "content-type: application/json" -d '{"world":"demo",...}'
cargo run -p dwbase-cli -- ask demo --question "What changed?"
cargo run -p dwbase-cli -- replay demo --limit 5
```

See `docs/developer/` for internals and `examples/` for runnable scenarios.
