# Example: Observe + Stream Fan-out

This standalone crate shows publishing atoms and receiving them via the local stream engine.

## Run
```bash
cd examples/observe_stream
cargo run
```

Expected output:
```
received {"event":"obs-0"}
received {"event":"obs-1"}
received {"event":"obs-2"}
```

Details:
- Uses `LocalStreamEngine` subscriptions (in-process channels).
- Publishes atoms via `engine.observe` (no storage replay).
- Uses a temp sled path; nothing persists.
