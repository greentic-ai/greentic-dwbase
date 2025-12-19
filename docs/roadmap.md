# DWBase Roadmap

## v1.1 (planned)
- Pluggable real embedder backend (replace dummy).
- Metrics exporter (Prometheus/OpenTelemetry) and dashboards.
- Node auth hardening: API tokens and stricter read/write caps.
- CLI enrichments: batch remember, streaming tail, richer filters.

## v1.2 (planned)
- Swarm/orchestration scaffold promotion to active scheduler.
- Storage/index maintenance routines (compaction, index rebuild).
- Event/webhook integrations for stream delivery beyond local channels.
- Performance tuning guided by PR-15 benchmarks and perf tests.
