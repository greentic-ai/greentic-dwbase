# Performance & Load Testing

This suite measures DWBase locally to catch regressions. It is **measurement-only** (no tuning).

## Benchmarks (`cargo bench -p dwbase-bench`)
- Criterion benchmarks in `crates/dwbase-bench/benches/engine.rs`
  - `remember_single_atom`
  - `remember_batch_atoms` (10/100/1000)
  - `ask_hot_path` (reflex)
  - `ask_vector_path` (ANN)
  - `observe_throughput`
  - `replay_throughput`
- Components: sled storage, HNSW vector index, local stream engine, gatekeeper (open caps), simple embedder. Runs on a temp sled directory.
- Async benches use `tokio` via `to_async`.

Run:
```bash
cargo bench -p dwbase-bench
```

## Load tests (`cargo test -p dwbase-bench -- --nocapture`)
- Located in `crates/dwbase-bench/tests/perf.rs`
  - Sustained remember rate (~3s window)
  - Concurrent ask + remember
  - Observe fan-out to N subscribers
- Tests print ops/sec and p95 latencies. Soft targets:
  - remember p95 < 2ms (local)
  - ask hot path p95 < 3ms
  - ask full path p95 < 10ms
- If targets are exceeded, a `[perf-warning]` line is logged; tests do **not** fail.

Run:
```bash
cargo test -p dwbase-bench -- --nocapture --ignored   # if you want to gate on CI duration
```

## Notes / Known Limits
- Benchmarks and tests use local sled files and in-memory indexes; no network/swarm benchmarks yet.
- Metrics are not emitted; results are printed to stdout.
- CI runners are expected to handle the short (≈3s) load tests; adjust durations if flaky.
