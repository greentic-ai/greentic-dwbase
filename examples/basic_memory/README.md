# Example: Basic Memory (remember → ask → replay)

Scenario: append two observations to world `demo`, ask a question, and replay the history using the node + CLI.

## Run the node
```bash
cp config.example.toml config.toml
cargo run -p dwbase-node -- --config ./config.toml
```

## Remember observations (HTTP)
```bash
curl -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"demo","worker":"cli","kind":"observation","timestamp":"","importance":0.9,"payload_json":"{\"text\":\"checkout latency 820ms\"}","vector":null,"flags":[],"labels":["latency"],"links":[]}'

curl -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"demo","worker":"cli","kind":"observation","timestamp":"","importance":0.7,"payload_json":"{\"text\":\"checkout error timeout\"}","vector":null,"flags":[],"labels":["error"],"links":[]}'
```
Each call returns an id like `"atom-1"`, `"atom-2"`. Timestamps are auto-filled if empty.

## Ask a question (CLI)
```bash
cargo run -p dwbase-cli -- ask demo --question "Why is checkout slow?"
```
Expected (pretty JSON):
```json
{
  "world": "demo",
  "text": "answer-pending",
  "supporting_atoms": [
    {"id":"atom-2","payload_json":"{\"text\":\"checkout error timeout\"}", "...": "..."},
    {"id":"atom-1","payload_json":"{\"text\":\"checkout latency 820ms\"}", "...": "..."}
  ]
}
```
Atoms are sorted by timestamp (newest first), then importance.

## Replay history (CLI)
```bash
cargo run -p dwbase-cli -- replay demo --limit 5
```
Outputs the remembered atoms in append order.

## List worlds (CLI)
```bash
cargo run -p dwbase-cli -- list-worlds
```
Shows `["demo"]` for this scenario.
