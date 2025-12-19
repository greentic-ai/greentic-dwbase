# Developer: Engine

Crate: `crates/dwbase-engine`. The engine is generic over adapters and owns a per-process `ReflexIndex` for recency filtering.

## Core types and traits
- Data: `NewAtom`, `Atom`, `AtomKind`, `AtomFilter`, `Question`, `Answer`, `WorldAction`.
- Traits: `StorageEngine`, `VectorEngine`, `StreamEngine`, `Gatekeeper`, `Embedder`.
- Orchestrator: `DWBaseEngine<S, V, T, G, E>` with helpers `remember`, `ask`, `observe`, `replay`, `get_atoms`.

## Remember pipeline
```text
NewAtom
  -> Gatekeeper.check_remember (world/kind/labels/importance/rate)
  -> assign AtomId (atom-<u64>) + timestamp if empty (now, RFC3339)
  -> embedder.embed(payload_json) if vector is None
  -> VectorEngine.upsert(world, id, vector) when vector present
  -> StorageEngine.append(atom)
  -> ReflexIndex.insert(atom)  // in-memory buckets by second
  -> StreamEngine.publish(atom)
```

Notes:
- Importance is expected in [0.0, 1.0]; gatekeeper may cap via config.
- Links/flags/labels are passed through unchanged; no semantics enforced by the engine.

## Ask pipeline
```text
Question { world, text, filter? }
  -> Gatekeeper.check_ask
  -> ReflexIndex.filter(recency window; respects kinds/labels/flags/since)
  -> if empty: StorageEngine.scan(world, filter or default)
  -> optional ANN:
       if embed(question.text) returns Some(vec):
         ids = VectorEngine.search(world, vec, k=10, filter)
         candidates += StorageEngine.get_by_ids(ids)
  -> dedupe by AtomId
  -> sort by timestamp desc, then importance desc
  -> Answer { world, text: "answer-pending", supporting_atoms }
```

Rationale:
- Reflex index avoids storage scans for “recent” questions; window defaults to 300s, buckets are 1s.
- ANN is best-effort; dummy embedder returns `None` so vector search is skipped unless a vector is provided on remember.

## Replay, observe, and world actions
- `replay(world, filter)` → `StorageEngine.scan` ordered by append log; respects kinds/labels/flags/since/until/limit.
- `observe(atom)` → publish to streams only; useful for simulations where persistence is not needed.
- `get_atoms(ids)` → storage lookup; used by node `/atoms/{id}` and ANN fetch path.
- `manage_world` → create/archive/resume worlds by writing a `world_meta` reflection (status label + optional description/labels); idempotent when the target status is already set.
- `worlds()` → lists active worlds; `worlds_filtered(true)` includes archived worlds.
- Policy atoms are read from the target world, `policy:<world>` (per-world), and `tenant:<id>/policy` (tenant defaults) for retention/min-importance/replication hints.

## Reflex index
- `ReflexIndex` buckets atoms per world; bucket id = `timestamp_seconds / bucket_width_seconds`.
- `ReflexIndexConfig { recency_seconds: 300, bucket_width_seconds: 1 }` by default.
- Filters support world, kinds, labels, author, min_importance, since, exclude_flags.
- GC runs on insert to keep only recent buckets; metrics can record evictions if enabled.

## Metrics (feature `metrics`)
- Records remember and ask latency, index freshness (age between atom timestamp and now), and reflex GC evictions.
- Trust distribution is emitted from node startup using the `TrustStore` snapshot (informational only).
