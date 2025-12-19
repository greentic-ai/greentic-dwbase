# Developer: Vector

Crate: `crates/dwbase-vector-hnsw`. Implements `VectorEngine` using in-memory HNSW per world.

## Index layout and metadata
- One HNSW graph per `WorldKey`; created on first insert.
- Dimension is inferred from the first insert per world; mismatches return `DwbaseError::Vector`.
- Distance: L2 (Euclidean).
- `VectorMetadata { kind: Option<AtomKind>, labels: Vec<String>, flags: Vec<String> }` stored alongside each point; used to honor filters.

```text
world -> { dim, HNSW graph, meta: point_id -> (AtomId, metadata) }
```

## API behavior
- `upsert(world, atom_id, vector)` → inserts a new point; no deletion/update semantics beyond overwrite of metadata slot.
- `search(world, query, k, filter)`:
  - world mismatch in filter returns empty vector.
  - dimension mismatch errors.
  - HNSW search (ef_search=200) yields nearest points; results are filtered using metadata against `kinds`, `labels`, `flags`.
  - If all ANN hits are filtered out, it falls back to returning the first matching metadata entries (deterministic order) to avoid empty answers.
- `rebuild` is a no-op for this impl.

## Engine integration
- Remember: engine calls `upsert` when an atom has a vector (either supplied or embedded).
- Ask: if `Embedder::embed(question.text)` returns `Some`, engine calls `search(world, query_vec, k=10, filter)` and fetches atoms via storage for returned ids before merging/reranking.

## Limits
- No persistence; indexes are empty after restart.
- No cross-world ANN; callers must supply the world.
- Metadata mirrors only kind/labels/flags; temporal or importance filtering is handled in the engine after atoms are fetched.

## Swapping implementations
`HnswVectorEngine` is behind the `VectorEngine` trait. Drop-in replacements can provide persistent or remote ANN without changing engine code.
