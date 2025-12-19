# Example: Replay Simulation (time travel)

Record events in a world and replay them through time windows using CLI filters.

## Start the node
```bash
cp config.example.toml config.toml
cargo run -p dwbase-node -- --config ./config.toml
```

## Remember time-stamped events
```bash
curl -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"sim","worker":"sim","kind":"observation","timestamp":"2024-01-01T00:00:00Z","importance":0.4,"payload_json":"{\"tick\":1}","vector":null,"flags":[],"labels":["tick"],"links":[]}'

curl -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"sim","worker":"sim","kind":"observation","timestamp":"2024-01-01T00:05:00Z","importance":0.6,"payload_json":"{\"tick\":2}","vector":null,"flags":[],"labels":["tick"],"links":[]}'
```

## Replay a window
```bash
cargo run -p dwbase-cli -- replay sim --limit 10 --since "2024-01-01T00:01:00Z"
```
Expected output (only the second atom is after the since filter):
```json
[
  {"id":"atom-2","world":"sim","timestamp":"2024-01-01T00:05:00Z","payload_json":"{\"tick\":2}","labels":["tick"],"...":"..."}
]
```

## Inspect a specific id (optional)
```bash
cargo run -p dwbase-cli -- inspect atom-1
```

Adjust `since`/`limit` to simulate other replay windows. If you send more atoms with different kinds or labels, use `--since`/`--limit` plus JSON `AtomFilter` fields via HTTP to replay subsets.
