# dwbase-node

Minimal HTTP node for DWBase, intended for local development.

## Running

From the workspace root:

```bash
cp config.example.toml config.toml
cargo run -p dwbase-node -- --config ./config.toml
```

Metrics/tracing are enabled by default via the `metrics` feature; disable with
`--no-default-features` if you want a minimal build. To collect metrics, install a recorder
compatible with the `metrics` crate (e.g., a Prometheus exporter) before running.

Example `config.toml`:

```toml
[node]
listen = "127.0.0.1:8080"
data_dir = "./data"
embedder = "dummy"

[node.security]
read_worlds = []
write_worlds = []
importance_cap = 1.0
remember_per_sec = 10
ask_per_min = 60
```

Empty `read_worlds` / `write_worlds` allow all worlds; populate with world keys to restrict access.

## API

- `POST /remember` — body: `NewAtom` (dwbase-engine JSON). Returns `AtomId`.
- `POST /ask` — body: `Question`. Returns `Answer`.
- `POST /replay` — body: `{ "world": "...", "filter": { ... } }`. Returns list of `Atom`.
- `GET /atoms/{id}` — returns `Atom` if found (searches across worlds).
- `GET /worlds` — returns known worlds observed since the node started.
