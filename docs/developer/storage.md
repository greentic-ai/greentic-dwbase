# Developer: Storage

Crate: `crates/dwbase-storage-sled`. Provides a sled-backed `StorageEngine` with append-only ordering per world.

## Layout
- Atom bodies: `world/{world}/atoms/{atom_id}` → bincode-serialized `Atom`.
- Append log: `world/{world}/log/{seq}` → `atom_id` (zero-padded, monotonic per world).
- Atom id index: `idx/atom/{atom_id}` → `AtomIndexEntry { world, seq }`.
- `StorageStats` counts atoms/vectors by scanning atoms.

## Behavior
- `append(atom)` → writes body + log entry (allocates next sequence); flushes if `flush_on_write`.
- `append_atoms(world, Vec<Atom>)` → helper used internally for batch writes/tests.
- `get_by_ids(ids)` → looks up `idx/atom/{id}` to jump directly to the owning world; falls back to a world scan only when the index is missing/stale.
- `scan(world, filter)` → iterates the log in order and filters on kinds, labels, flags, since/until, world (optional), and limit. Ordering is append order.
- `stats(world)` → counts atoms and vectors present in that world.

```text
replay(world, filter)
  -> scan_prefix("world/{world}/log/")
  -> for each seq: load atom by id
  -> apply filters; stop on limit
```

## Filters honored
- `kinds`: must contain atom.kind if non-empty.
- `labels` / `flags`: all listed values must be present on the atom.
- `since` / `until`: compared as strings on RFC3339 timestamps (consistent ordering).
- `limit`: stops iteration early.

## Durability and performance notes
- sled handles fsync; `flush_on_write` defaults to true for safer demos.
- No compaction/GC; log grows unbounded.
- `get_by_ids` prefers the secondary index; the index is rebuilt on startup and can be refreshed manually via `dwbase storage reindex` (CLI).

## Configuration
`SledConfig { path: PathBuf, flush_on_write: bool, encryption_enabled: bool, key_id: Option<String> }`
- When `encryption_enabled` is true, atoms are AES-256-GCM encrypted using the key fetched via the `KeyProvider` (node/CLI use `EnvKeyProvider`, reading hex from `DWBASE_KEY_<KEY_ID>`).
- Node reads `data_dir` from `config.toml` and sets `flush_on_write=true`.
- Tests use `tempfile::TempDir`.
