# Developer: Security

Crate: `crates/dwbase-security`. Provides capabilities, rate limits, and a simple trust store. The engine wires `LocalGatekeeper` by default.

## Components
- `Capabilities { read_worlds, write_worlds, labels_write, kinds_write, importance_cap, rate_limits, offline_policy }`
- `RateLimits { remember_per_sec: Option<f64>, ask_per_min: Option<f64> }` implemented with token buckets keyed per worker (remember) or global (ask).
- `TrustStore` holds worker/node scores; currently only emitted to metrics (no enforcement).
- `LocalGatekeeper` implements `Gatekeeper` trait using a clock so tests can mock time.

## Checks
- `check_remember`:
  - world must be allowed by `write_worlds` (if non-empty)
  - kind and labels must be in allow-lists (if provided)
  - importance must be <= `importance_cap` (if set)
  - rate limit enforced per worker using `remember_per_sec`
- `check_ask`:
  - world must be allowed by `read_worlds` (if non-empty)
  - rate limit enforced globally using `ask_per_min` (converted to per-second bucket)
- `check_world_action`: uses same world gating; engine stub does not call it yet.

Rate limiting is deterministic: buckets refill on every call based on elapsed time; capacity defaults to the configured rate.

## Node configuration
`config.toml` under `[node.security]` maps directly to `Capabilities`:
```toml
[node.security]
read_worlds = []      # empty = allow all
write_worlds = []     # empty = allow all
importance_cap = 1.0  # drop remembers above this
remember_per_sec = 10
ask_per_min = 60
```
Trust scores from the config (if provided) are exported to metrics when the `metrics` feature is enabled but do not influence ranking or authorization.
