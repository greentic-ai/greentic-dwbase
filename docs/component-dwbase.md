# component-dwbase Runbook

DWBase node (`dwbase-node`) is a single-process append-only memory service. This runbook lists the health endpoints, emitted metrics, configuration knobs, and common troubleshooting steps for production ops.

## Multi-tenant safety defaults (component-dwbase)
component-dwbase is intended to be callable by LLMs. It ships with “safe by default” tenant scoping:
- Tenant id: `DWBASE_TENANT_ID` (or `GREENTIC_TENANT_ID`); defaults to `default`.
- World namespace rule (default): writes/reads must be within `tenant:<id>/...`.
  - Example: `tenant:acme/incident:1234`
  - Override (explicit): `DWBASE_ALLOW_READ_WORLDS`, `DWBASE_ALLOW_WRITE_WORLDS`, `DWBASE_ALLOW_READ_PREFIXES`, `DWBASE_ALLOW_WRITE_PREFIXES`.

Safety limits (defaults; override via env):
- Payload size: `DWBASE_MAX_PAYLOAD_BYTES` (default `65536`).
- Importance cap: `DWBASE_IMPORTANCE_CAP` (default `0.7`).
- Allowed kinds: `DWBASE_ALLOWED_KINDS` (default `observation,reflection`).
- Allowed labels: `DWBASE_ALLOWED_LABELS` (default: none allowed).
  - Allow policy labels: `DWBASE_ALLOW_POLICY_LABELS=1` (enables labels with prefix `policy:`).

## Structured tool errors (v2 operations)
The original v1 tool operations predate structured error handling. For backwards compatibility they may signal failure via “empty” values:
- `dwbase.remember` returns `""` on failure
- `dwbase.observe_start` returns `0` on failure
- `dwbase.observe_poll` returns `[]` on failure

Prefer the v2 operations in new flows/hosts:
- `dwbase.remember_v2`
- `dwbase.ask_v2` (adds `warnings`)
- `dwbase.observe_start_v2`
- `dwbase.observe_poll_v2`
- `dwbase.observe_stop_v2`
- `dwbase.health_v2`

### Result shape
v2 operations return a result object with exactly one of:
- `{"ok": ...}`
- `{"err": { "code": "...", "message": "...", "details_json": "..."? }}`

### Error codes
Stable `code` values (do not parse `message`):
- `capability_denied`: cross-tenant world access or disallowed world prefixes
- `invalid_input`: missing required fields (ex: observe_start without `filter.world_key`)
- `payload_too_large`: payload exceeds `DWBASE_MAX_PAYLOAD_BYTES`
- `importance_cap`: importance exceeds `DWBASE_IMPORTANCE_CAP`
- `kind_not_allowed`: kind not listed in `DWBASE_ALLOWED_KINDS`
- `label_not_allowed`: labels denied by default or not listed in `DWBASE_ALLOWED_LABELS`
- `invalid_handle`: unknown/expired observe stream handle (or `0`)
- `storage_error`: storage init/IO failures (ex: data dir not writable)
- `internal_error`: unexpected engine/replication failures

### Warning semantics (`ask_v2`)
`dwbase.ask_v2` returns `warnings: []` on success when the node is healthy. When the node is degraded (storage/index not ready, or disk pressure per `dwbase.health`), it adds a warning like:
- `degraded: disk usage high`

## Observe streams (flow-friendly)
component-dwbase exposes poll-based observe streams for Greentic flows:
- `dwbase.observe_start(filter) -> handle`
- `dwbase.observe_poll(handle, max) -> atoms`
- `dwbase.observe_stop(handle)`
- `dwbase.observe_stats(handle) -> { queued_count, dropped_count, last_event_ms, warnings }`

Local semantics:
- Streams receive atoms from local `remember/observe` and from `ingest_remote_atoms` (replication).
- Poll returns atoms in publish order (best-effort, bounded queue).

### Backpressure + drop policy
Observe queues are bounded per handle, with explicit drop behavior:
- Queue capacity: `DWBASE_OBSERVE_QUEUE_CAPACITY` (default `10000`)
- Drop policy: `DWBASE_OBSERVE_DROP_POLICY`:
  - `drop_oldest` (default): evict the oldest queued item to make room
  - `drop_newest`: drop the incoming event when the queue is full

When drops occur:
- `dwbase.observe_stats(...).dropped_count` increases
- `dwbase.observe_stats(...).warnings` includes `events_dropped`
- Metrics are emitted: `dwbase.observe.dropped_total`, `dwbase.observe.queue_depth`

### Optional durability (cursor catch-up)
For restart-safe “catch up” behavior, enable durable observe cursors:
- `DWBASE_OBSERVE_DURABLE=1`
- `DWBASE_OBSERVE_DURABLE_CATCHUP_LIMIT` (default `1000`, bounds catch-up enqueue)

When enabled, `observe_start` will:
- load the last-seen cursor for the world (stored under `DWBASE_DATA_DIR/_observe/cursors/...`)
- enqueue any atoms appended after that cursor (bounded), so a new subscription can pick up events that happened while the stream was not active

Optional NATS cross-node observe:
- Enable publishing compact atom events: `DWBASE_OBSERVE_NATS_BROADCAST=1` (requires swarm bus initialized; typically with `DWBASE_SWARM_ENABLE=1`)
- Enable subscribing to per-world event subjects: `DWBASE_OBSERVE_NATS_SUBSCRIBE=1`
- Subject mapping: `dwbase.world.<hex(world_key)>.events` (world key is included in payload; subject uses hex token to avoid leaking/escaping issues)

## Health signals
component-dwbase exposes an operator-facing health snapshot via the tool surface:
- `dwbase.health() -> { status, message, storage_ok, index_ok, disk_*, sync_lag_ms, quarantine_count }`

Ready criteria:
- storage is readable (`storage_ok=true`)
- index metadata (if present) is usable (`index_ok=true`)

Degraded criteria:
- storage/index not ready, or
- `disk_pressure=degraded` (see disk model below)

### Disk model (WASI-friendly)
Health reports disk state with explicit “unknown vs configured vs filesystem” semantics:
- `disk_used_bytes`: current DWBase data dir footprint (recursive size)
- `disk_free_bytes` / `disk_total_bytes`:
  - If `DWBASE_MAX_DISK_MB` is set: interpreted as a **DWBase budget**, so `total=max_disk` and `free=max_disk - disk_used_bytes`.
  - Else, if filesystem stats are available on the host: `total/free` reflect the filesystem containing `DWBASE_DATA_DIR`.
  - Else: both are `0` (capacity unknown).
- `disk_used_percent`:
  - If `DWBASE_MAX_DISK_MB` is set: `disk_used_bytes / disk_total_bytes * 100`.
  - Else if filesystem stats are available: `(fs_total - fs_free) / fs_total * 100` (overall filesystem usage).
  - Else: `0`.
- `disk_pressure`: derived signal:
  - `unknown` when capacity is unknown
  - `ok` when below warn threshold
  - `warn` when above `DWBASE_HEALTH_DISK_WARN_PCT` (default `80`)
  - `degraded` when above `DWBASE_HEALTH_DISK_DEGRADED_PCT` (default `90`)

Failure modes / overrides:
- WASI runtimes may not expose filesystem stats; set `DWBASE_MAX_DISK_MB` to get deterministic pressure behavior.
- For tests/diagnostics you can force-disable filesystem stats with `DWBASE_HEALTH_DISABLE_FS_STATS=1`.

## Metrics
Emitted via the `metrics` crate (compatible with Greentic telemetry when the host installs a recorder/exporter):
- `dwbase.remember.latency_ms` (histogram)
- `dwbase.ask.latency_ms` (histogram)
- `dwbase.index.freshness_ms` (gauge)
- `dwbase.observe.dropped_total` (counter; total dropped observe events due to backpressure)
- `dwbase.observe.queue_depth` (gauge; best-effort current depth)
- `dwbase.gc.evictions` (counter)
- `dwbase.trust.score{entity=<worker|node>}` (gauge)
- `dwbase.sync.lag_ms` (gauge; time since last remote ingest, `0` when idle)
- `dwbase.quarantine.count` (gauge; currently `0`, quarantine not implemented)
- `dwbase.disk.used_bytes`, `dwbase.disk.total_bytes`, `dwbase.disk.used_percent` (gauges from `dwbase.health`; total uses `DWBASE_MAX_DISK_MB` when set)

Tracing (`tracing_subscriber::fmt`) is initialized with the metrics feature; set `RUST_LOG=info` for node logs.

## Configuration knobs (`config.toml`)
```toml
[node]
listen = "127.0.0.1:8080"
data_dir = "./data"
embedder = "dummy"          # only dummy is available

[node.security]
read_worlds = []            # empty = allow all
write_worlds = []           # empty = allow all
importance_cap = 1.0        # drop remembers above this
remember_per_sec = 10
ask_per_min = 60

[node.health]
disk_usage_degraded = 0.9   # mark degraded when >= 90% used
```
Values map directly to `NodeConfig` (`health.disk_usage_degraded` is a fraction, not percent).

## Tenant scoping rules
- Worlds are identified by `WorldKey` (opaque string). Storage, vector index, and streams are world-scoped.
- Security caps (`read_worlds`/`write_worlds`) constrain which worlds a worker can read/write; empty lists mean allow-all.
- Importance is validated/capped to `[0,1]` and optionally capped by `importance_cap`.

## NATS discovery / swarm
- Presence/discovery is available (PR-33):
  - publishes periodic hello on `dwbase.node.hello`
  - maintains a TTL-expiring peer table in-process
  - enable by setting `NATS_URL`/`GREENTIC_NATS_URL` or `DWBASE_PRESENCE_MOCK=1` (tests/local)
- Selective replication skeleton over NATS inbox messaging (PR-35):
  - nodes broadcast subscription intent on `dwbase.swarm.broadcast` (`SubscriptionIntent { patterns: [...] }`)
  - atoms are replicated as direct messages to per-node inbox subjects: `dwbase.swarm.<node_id>.inbox`
  - v1 intentionally does **not** use per-world NATS subjects (simpler and avoids leaking world names via subjects)

### Replication configuration (component-dwbase)
- Enable replication loops: set `DWBASE_SWARM_ENABLE=1`
- Subscribe to worlds (comma-separated patterns): set `DWBASE_SUBSCRIBE_WORLDS=world-x,incident:*`
- Use mock transport (in-memory, no network): set `DWBASE_SWARM_MOCK=1`
- Use real NATS transport: set `NATS_URL=...` (requires building with `--features nats`)
- Swarm state (subscriptions/peers) is durably persisted to `<DWBASE_DATA_DIR>/swarm.json` and reloaded on restart; inboxes are bounded and drop duplicates/replayed batches.

### Metrics (WASI-friendly)
- A Prometheus exporter is embedded in-process (no HTTP server). Call `dwbase.metrics_snapshot()` from the component API to get JSON with counters/gauges/histograms plus Prometheus text; forward or scrape that text with your host telemetry agent.
- Metric names follow `dwbase.*` (e.g., `dwbase.remember.latency_ms`, `dwbase.observe.queue_depth`, `dwbase.disk.used_bytes`).
- Hosts can poll the snapshot on an interval and push to OTLP/Prom-compatible collectors without requiring the component to bind a port.

### Policy atoms (minimal)
Policy atoms have two conventional homes:
- Tenant defaults: `tenant:<id>/policy` (used by the component and replication policy).
- Per-world overrides: `policy:<world>` (emitted by node/CLI tooling).

Labels (all prefixed `policy:`):
- Retention: `retention_days=<n>`, `retention_min_days=<n>` (tenant floor), `min_importance=<f32>`
- Replication controls (enforced by component replication policy):
  - `replication_allow=<world_prefix>`
  - `replication_deny=<world_prefix>` (deny wins; if any allow rules exist, an allow match is required)

### Security note
- Incoming replication is filtered by local subscription patterns; if a node does not subscribe to a world, it will not ingest it by default.
- Production deployments should ensure nodes only subscribe to worlds they are authorized to read (capability-aware policy hooks exist in `dwbase-swarm-nats` for tighter enforcement).

## Troubleshooting
- **Component won’t start:** verify `DWBASE_DATA_DIR` permissions and that the WASI filesystem mapping includes it; enable logs via `RUST_LOG=info`.
- **Health = degraded (disk):** check `dwbase.health().disk_used_percent`. Free space or raise `DWBASE_MAX_DISK_MB` / `DWBASE_HEALTH_DISK_DEGRADED_PCT` temporarily (not recommended long term).
- **High latency (ask/remember):** inspect `dwbase.ask.latency_ms`/`dwbase.remember.latency_ms`; storage is local-disk backed.
- **No peers / high lag:** confirm `NATS_URL`/`GREENTIC_NATS_URL` and (if used) `DWBASE_SWARM_ENABLE=1`. `dwbase.sync.lag_ms` reflects time since last remote ingest.
- **Disk usage unknown:** if `DWBASE_MAX_DISK_MB` is unset, health will report `disk_total_bytes=0` and percent `0` (capacity unknown).

## Upgrade notes
- Metrics are feature-gated (`metrics` is enabled by default); disable with `--no-default-features` if you run without telemetry.
- Health endpoint is new; dashboards should watch `/health.status` and the disk gauges.
- ANN, swarm sync, and quarantine logic are not yet implemented; metrics will read `0` until those features ship.
