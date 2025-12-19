# Example: WASM Worker (dwbase-wit-guest)

Minimal WASM guest that surfaces a client config plus sample remember/ask payloads.

## Build
```bash
cd examples/wasm-worker
rustup target add wasm32-unknown-unknown
cargo build --target wasm32-unknown-unknown
```
The module is `target/wasm32-unknown-unknown/debug/wasm_worker.wasm`.

## Exports
- `client_config() -> ClientConfig` with `endpoint = "http://127.0.0.1:8080"`.
- `sample_payload() -> String` with JSON `{"text":"hello from wasm"}`.
- `sample_question() -> String` with `"What did the wasm worker say?"`.

## Using from a host
Load the WASM with your runtime (e.g., `wasmtime`) and call the exports, then drive the HTTP API:
```text
// pseudo-host flow
let payload = call_export("sample_payload"); // JSON string
// remember
curl -X POST http://127.0.0.1:8080/remember -H "content-type: application/json" \
  -d format!("{{\"world\":\"wasm\",\"worker\":\"guest\",\"kind\":\"observation\",\"timestamp\":\"\",\"importance\":0.8,\"payload_json\":{payload:?},\"vector\":null,\"flags\":[],\"labels\":[],\"links\":[]}}");
// ask
cargo run -p dwbase-cli -- ask wasm --question "$(call_export("sample_question"))"
```
Expected: ask returns an answer where `supporting_atoms[0].payload_json` is the remembered WASM string.

You can also feed the exports into `dwbase-wit-host` by implementing its `EngineApi` against the HTTP node and invoking `remember`/`ask` through the adapter. Adjust `client_config()` to point at a remote node if needed.
