# Developer: Architecture

Top-level crates:
- `dwbase-core`: atom types, ids, importance validation.
- `dwbase-engine`: orchestrator tying `StorageEngine`, `VectorEngine`, `StreamEngine`, `Gatekeeper`, and `Embedder` with a reflex index.
- Adapters: `dwbase-storage-sled`, `dwbase-vector-hnsw`, `dwbase-stream-local`, `dwbase-embedder-dummy`, `dwbase-security`.
- Entry points: `dwbase-node` (Axum HTTP API) and `dwbase-cli` (HTTP client). Metrics are gated behind the `metrics` feature.

```text
[CLI/WIT Host] --HTTP--> [dwbase-node] --calls--> [DWBaseEngine]
                                     |             |-- StorageEngine (sled)
                                     |             |-- VectorEngine (HNSW, in-memory)
                                     |             |-- StreamEngine (local channels)
                                     |             |-- Gatekeeper (caps, rate limit)
                                     |             |-- Embedder (dummy)
                                     |             '-- ReflexIndex (recent atoms)
                                     '-- metrics (optional)
```

## HTTP surface (dwbase-node)
- `POST /remember` → `DWBaseEngine::remember(NewAtom)`; auto-assigns id/timestamp if empty.
- `POST /ask` → `DWBaseEngine::ask(Question)`; returns `Answer { world, text:"answer-pending", supporting_atoms }`.
- `POST /replay` → `DWBaseEngine::replay(WorldKey, AtomFilter)`; ordered scan.
- `GET /worlds` → worlds seen since process start (in-memory list).
- `GET /atoms/{id}` → lookup via storage.

## Engine flows

Remember:
```
NewAtom
  -> Gatekeeper.check_remember (world/kind/labels/import./rate)
  -> fill id + timestamp (if missing)
  -> Embedder.embed(payload_json) when vector is None
  -> VectorEngine.upsert(world, id, vector) if vector present
  -> StorageEngine.append(atom)
  -> ReflexIndex.insert(atom)   // in-memory recency cache
  -> StreamEngine.publish(atom) // subscribers only
```

Ask:
```
Question
  -> Gatekeeper.check_ask
  -> ReflexIndex.filter(recency window)
  -> if empty: StorageEngine.scan(world, filter)
  -> optional: embed(question.text) + VectorEngine.search(world, query, k=10, filter)
  -> StorageEngine.get_by_ids for ANN hits
  -> dedupe by AtomId
  -> sort by timestamp desc, then importance desc
  -> Answer{text:"answer-pending", supporting_atoms}
```

Replay: `StorageEngine.scan(world, filter)` with ordered log traversal.

Observe: publishes an already-built atom to `StreamEngine` (no storage).

## World isolation and durability
- Storage, vector index, and stream subscriptions are world-scoped via `WorldKey`.
- Vector index is in-memory only; storage is durable via sled.
- Reflex index is an in-memory recency cache; window configured in `ReflexIndexConfig`.

## Configuration entry points
- Node config: `config.toml` (`listen`, `data_dir`, security caps/rate limits, embedder selection).
- Default embedder: dummy (returns `None` so vector search is skipped unless a vector is supplied).
- Metrics feature: records remember/ask latency, index freshness, reflex GC evictions, trust distribution snapshot.
