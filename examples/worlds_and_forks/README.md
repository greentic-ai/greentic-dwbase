# Example: Worlds and Forks (importance-aware)

Demonstrates per-world isolation and how importance breaks ties when timestamps match.

## Start node
```bash
cp config.example.toml config.toml
cargo run -p dwbase-node -- --config ./config.toml
```

## Remember observations in two worlds
```bash
# World alpha, low importance (fixed timestamp to show ordering)
curl -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"alpha","worker":"ops","kind":"observation","timestamp":"2024-01-01T00:00:00Z","importance":0.2,"payload_json":"{\"text\":\"alpha old info\"}","vector":null,"flags":[],"labels":["alpha"],"links":[]}'

# World beta, high importance (same timestamp)
curl -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"beta","worker":"ops","kind":"observation","timestamp":"2024-01-01T00:00:00Z","importance":0.9,"payload_json":"{\"text\":\"beta critical\"}","vector":null,"flags":[],"labels":["beta"],"links":[]}'
```

## Ask in each world
```bash
cargo run -p dwbase-cli -- ask alpha --question "What's happening?"
cargo run -p dwbase-cli -- ask beta --question "What's happening?"
```

Expected (pretty JSON, truncated):
```json
{
  "world": "beta",
  "text": "answer-pending",
  "supporting_atoms": [
    {"id":"atom-2","importance":0.9,"payload_json":"{\"text\":\"beta critical\"}", "...":"..."}
  ]
}
{
  "world": "alpha",
  "text": "answer-pending",
  "supporting_atoms": [
    {"id":"atom-1","importance":0.2,"payload_json":"{\"text\":\"alpha old info\"}", "...":"..."}
  ]
}
```
Importance influences ordering only after timestamp ties; setting the same timestamp above makes the higher-importance atom appear first. Trust scores are not used for ranking in v1.

## List worlds
```bash
cargo run -p dwbase-cli -- list-worlds
```
Shows both `alpha` and `beta`, illustrating independent world histories.
