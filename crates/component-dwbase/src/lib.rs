//! component-dwbase: WASI component exposing DWBase operations with local persistence.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use std::{path::PathBuf, sync::Arc};

use dwbase_core::{
    Atom, AtomId, AtomKind, Importance, Link, LinkKind, Timestamp, WorkerKey, WorldKey,
};
use dwbase_embedder_dummy::DummyEmbedder;
use dwbase_engine::{AtomFilter, DWBaseEngine, NewAtom, Question, StorageEngine, StreamEngine};
use dwbase_security::{Capabilities, LocalGatekeeper, TrustStore};
use dwbase_swarm_nats::replication::Replicator;
use dwbase_swarm_nats::replication::WorldAccessPolicy;
#[cfg(feature = "nats")]
use dwbase_swarm_nats::swarm::NatsBus;
use dwbase_swarm_nats::swarm::{MockBus, NatsSwarmTransport};
use dwbase_swarm_nats::world_events::{
    decode_event_batch, world_events_subject, WorldEventBroadcaster,
};
#[cfg(feature = "nats")]
use dwbase_swarm_nats::AsyncNats;
use dwbase_swarm_nats::{
    now_rfc3339, start_presence_loop, MockNats, NatsClient, NodeHello, PeerTable,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use once_cell::sync::OnceCell;
use parking_lot::Mutex as ParkingMutex;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use wit_bindgen::generate;

generate!({
    path: "../../wit/dwbase-core.wit",
    world: "core",
});

use exports::dwbase::core::engine::{
    self, Answer, Atom as WitAtom, AtomFilter as WitAtomFilter, AtomKind as WitAtomKind,
    NewAtom as WitNewAtom, Question as WitQuestion,
};

type ComponentEngine =
    DWBaseEngine<FsStorage, NoVector, LocalStream, LocalGatekeeper, DummyEmbedder>;

static ENGINE: OnceCell<Mutex<ComponentEngine>> = OnceCell::new();
static PEERS: OnceCell<PeerTable> = OnceCell::new();
static SWARM: OnceCell<Arc<Replicator>> = OnceCell::new();
static STREAM: OnceCell<LocalStream> = OnceCell::new();
static BUS: OnceCell<Arc<dyn dwbase_swarm_nats::swarm::SwarmBus>> = OnceCell::new();
static BROADCASTER: OnceCell<WorldEventBroadcaster> = OnceCell::new();
static EVENT_SUBS: OnceCell<ParkingMutex<HashSet<String>>> = OnceCell::new();
static LAST_REMOTE_INGEST_MS: AtomicU64 = AtomicU64::new(0);
static PROM_HANDLE: OnceCell<Option<PrometheusHandle>> = OnceCell::new();

fn now_ms() -> u64 {
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()) as u64
}

fn install_metrics_recorder() -> Option<&'static PrometheusHandle> {
    PROM_HANDLE
        .get_or_init(|| PrometheusBuilder::new().install_recorder().ok())
        .as_ref()
}

#[derive(Default)]
struct ParsedMetrics {
    counters: Vec<engine::MetricPoint>,
    gauges: Vec<engine::MetricPoint>,
    histograms: Vec<engine::HistogramMetric>,
}

fn parse_labels(raw: &str) -> Vec<engine::MetricLabel> {
    if raw.trim().is_empty() {
        return Vec::new();
    }
    let mut labels = Vec::new();
    for pair in raw.split(',') {
        if let Some((k, v)) = pair.split_once('=') {
            let val = v.trim().trim_matches('"').replace("\\\"", "\"");
            labels.push(engine::MetricLabel {
                key: k.to_string(),
                value: val,
            });
        }
    }
    labels.sort_by(|a, b| a.key.cmp(&b.key));
    labels
}

fn labels_key(labels: &[engine::MetricLabel]) -> String {
    labels
        .iter()
        .map(|l| format!("{}={}", l.key, l.value))
        .collect::<Vec<_>>()
        .join("|")
}

#[derive(Default)]
struct HistAccum {
    labels: Vec<engine::MetricLabel>,
    buckets: Vec<engine::HistogramBucket>,
    sum: Option<f64>,
    count: Option<f64>,
}

fn parse_prometheus(text: &str) -> ParsedMetrics {
    use std::collections::HashMap;

    let mut types: HashMap<String, String> = HashMap::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            if let Some((name, ty)) = rest.trim().split_once(' ') {
                types.insert(name.trim().to_string(), ty.trim().to_string());
            }
        }
    }

    let mut parsed = ParsedMetrics::default();
    let mut hist_map: HashMap<(String, String), HistAccum> = HashMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name_part, val_part)) = line.split_once(' ') else {
            continue;
        };
        let value: f64 = match val_part.trim().parse() {
            Ok(v) => v,
            Err(_) => continue,
        };

        let (name, labels) = if let Some((n, raw_labels)) = name_part.split_once('{') {
            let clean = raw_labels.trim_end_matches('}');
            (n.to_string(), parse_labels(clean))
        } else {
            (name_part.to_string(), Vec::new())
        };

        // Histogram buckets/sum/count use suffixed names; group by base name + labels (minus le).
        if name.ends_with("_bucket") {
            let base = name.trim_end_matches("_bucket").to_string();
            let mut base_labels = labels.clone();
            let le_idx = base_labels.iter().position(|l| l.key == "le");
            let le = le_idx
                .and_then(|idx| {
                    base_labels
                        .get(idx)
                        .and_then(|l| l.value.parse::<f64>().ok())
                })
                .unwrap_or(f64::INFINITY);
            if let Some(idx) = le_idx {
                base_labels.remove(idx);
            }
            let key = labels_key(&base_labels);
            let entry = hist_map
                .entry((base.clone(), key))
                .or_insert_with(|| HistAccum {
                    labels: base_labels.clone(),
                    ..Default::default()
                });
            entry
                .buckets
                .push(engine::HistogramBucket { le, count: value });
            continue;
        }
        if name.ends_with("_sum") {
            let base = name.trim_end_matches("_sum").to_string();
            let key = labels_key(&labels);
            let entry = hist_map
                .entry((base.clone(), key))
                .or_insert_with(|| HistAccum {
                    labels: labels.clone(),
                    ..Default::default()
                });
            entry.sum = Some(value);
            continue;
        }
        if name.ends_with("_count") {
            let base = name.trim_end_matches("_count").to_string();
            let key = labels_key(&labels);
            let entry = hist_map
                .entry((base.clone(), key))
                .or_insert_with(|| HistAccum {
                    labels: labels.clone(),
                    ..Default::default()
                });
            entry.count = Some(value);
            continue;
        }

        match types.get(&name).map(|s| s.as_str()) {
            Some("counter") => parsed.counters.push(engine::MetricPoint {
                name: name.clone(),
                labels: labels.clone(),
                value,
            }),
            Some("gauge") => parsed.gauges.push(engine::MetricPoint {
                name: name.clone(),
                labels: labels.clone(),
                value,
            }),
            _ => parsed.gauges.push(engine::MetricPoint {
                name: name.clone(),
                labels: labels.clone(),
                value,
            }),
        }
    }

    for ((name, _key), mut acc) in hist_map {
        acc.buckets
            .sort_by(|a, b| a.le.partial_cmp(&b.le).unwrap_or(std::cmp::Ordering::Equal));
        parsed.histograms.push(engine::HistogramMetric {
            name,
            labels: acc.labels,
            buckets: acc.buckets,
            sum: acc.sum.unwrap_or(0.0),
            count: acc.count.unwrap_or(0.0),
        });
    }

    parsed
}

fn max_disk_bytes() -> u64 {
    std::env::var("DWBASE_MAX_DISK_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|mb| mb * 1024 * 1024)
        .unwrap_or(0)
}

fn health_disk_warn_percent() -> f32 {
    std::env::var("DWBASE_HEALTH_DISK_WARN_PCT")
        .ok()
        .and_then(|v| v.parse::<f32>().ok())
        .unwrap_or(80.0)
}

fn health_disk_degraded_percent() -> f32 {
    std::env::var("DWBASE_HEALTH_DISK_DEGRADED_PCT")
        .ok()
        .and_then(|v| v.parse::<f32>().ok())
        .unwrap_or(90.0)
}

fn health_disable_fs_stats() -> bool {
    std::env::var("DWBASE_HEALTH_DISABLE_FS_STATS").is_ok()
}

fn index_rebuild_warn_ms() -> u64 {
    std::env::var("DWBASE_INDEX_REBUILD_WARN_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(60)
        .saturating_mul(1000)
}

fn fs_capacity_bytes(path: &PathBuf) -> Option<(u64, u64)> {
    if health_disable_fs_stats() {
        return None;
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let total = fs2::total_space(path).ok()?;
        let free = fs2::available_space(path).ok()?;
        Some((total, free))
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = path;
        None
    }
}

fn dir_size_bytes(path: &PathBuf) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    for ent in entries.flatten() {
        let p = ent.path();
        if let Ok(md) = ent.metadata() {
            if md.is_file() {
                total = total.saturating_add(md.len());
            } else if md.is_dir() {
                total = total.saturating_add(dir_size_bytes(&p));
            }
        }
    }
    total
}

fn compute_health(
    engine: &DWBaseEngine<FsStorage, NoVector, LocalStream, LocalGatekeeper, DummyEmbedder>,
) -> engine::HealthSnapshot {
    let storage_ok = engine.storage_ready();
    let index_ok = engine.index_status().iter().all(|m| m.ready);

    let data = data_dir();
    let used_bytes = dir_size_bytes(&data);
    let configured_total_bytes = max_disk_bytes();
    let mut disk_total_bytes = 0u64;
    let mut disk_free_bytes = 0u64;
    let mut disk_used_percent = 0.0f32;
    let mut pressure_basis = "unknown";

    if configured_total_bytes > 0 {
        disk_total_bytes = configured_total_bytes;
        disk_free_bytes = configured_total_bytes.saturating_sub(used_bytes);
        disk_used_percent = (used_bytes as f32 / configured_total_bytes as f32) * 100.0;
        pressure_basis = "configured";
    } else if let Some((fs_total, fs_free)) = fs_capacity_bytes(&data) {
        disk_total_bytes = fs_total;
        disk_free_bytes = fs_free;
        if fs_total > 0 {
            disk_used_percent =
                ((fs_total.saturating_sub(fs_free)) as f32 / fs_total as f32) * 100.0;
        }
        pressure_basis = "filesystem";
    }

    let warn_pct = health_disk_warn_percent();
    let degraded_pct = health_disk_degraded_percent();
    let disk_pressure = if disk_total_bytes == 0 {
        "unknown".to_string()
    } else if disk_used_percent >= degraded_pct {
        "degraded".to_string()
    } else if disk_used_percent >= warn_pct {
        "warn".to_string()
    } else {
        "ok".to_string()
    };

    let last_remote = LAST_REMOTE_INGEST_MS.load(Ordering::Relaxed);
    let lag_ms = if last_remote == 0 {
        0
    } else {
        now_ms().saturating_sub(last_remote)
    };

    // Placeholder quarantine metric.
    let quarantine_count = 0u64;

    if disk_total_bytes > 0 {
        let used_for_metrics = if pressure_basis == "filesystem" {
            disk_total_bytes.saturating_sub(disk_free_bytes)
        } else {
            used_bytes
        };
        dwbase_metrics::record_disk_usage(used_for_metrics, disk_total_bytes);
    }
    dwbase_metrics::record_sync_lag(Duration::from_millis(lag_ms));
    dwbase_metrics::record_quarantine_count(quarantine_count);

    let mut status = "ready".to_string();
    let mut message = "ok".to_string();
    if let Some(lag) = engine.max_index_rebuild_lag_ms() {
        if lag > index_rebuild_warn_ms() {
            status = "degraded".into();
            message = format!("index rebuild lag {lag}ms");
        }
    }
    if !storage_ok || !index_ok {
        status = "degraded".into();
        message = "storage or index not ready".into();
    } else if disk_pressure == "degraded" {
        status = "degraded".into();
        message = match pressure_basis {
            "filesystem" => "disk pressure degraded (filesystem)".into(),
            "configured" => "disk pressure degraded (configured capacity)".into(),
            _ => "disk pressure degraded".into(),
        };
    } else if disk_pressure == "warn" {
        message = match pressure_basis {
            "filesystem" => "disk pressure warn (filesystem)".into(),
            "configured" => "disk pressure warn (configured capacity)".into(),
            _ => "disk pressure warn".into(),
        };
    }

    engine::HealthSnapshot {
        status,
        message,
        storage_ok,
        index_ok,
        disk_used_bytes: used_bytes,
        disk_free_bytes,
        disk_total_bytes,
        disk_used_percent,
        disk_pressure,
        sync_lag_ms: lag_ms,
        quarantine_count,
    }
}

fn data_dir() -> PathBuf {
    std::env::var("DWBASE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./dwbase-data"))
}

fn tenant_id() -> String {
    std::env::var("DWBASE_TENANT_ID")
        .or_else(|_| std::env::var("GREENTIC_TENANT_ID"))
        .unwrap_or_else(|_| "default".into())
}

fn tenant_prefix(id: &str) -> String {
    format!("tenant:{id}/")
}

fn tool_error(
    code: &str,
    message: impl Into<String>,
    details_json: Option<String>,
) -> engine::ToolError {
    engine::ToolError {
        code: code.to_string(),
        message: message.into(),
        details_json,
    }
}

fn err_invalid_input(message: impl Into<String>) -> engine::ToolError {
    tool_error("invalid_input", message, None)
}

fn err_capability_denied(message: impl Into<String>) -> engine::ToolError {
    tool_error("capability_denied", message, None)
}

fn err_invalid_handle(message: impl Into<String>) -> engine::ToolError {
    tool_error("invalid_handle", message, None)
}

fn err_storage(message: impl Into<String>) -> engine::ToolError {
    tool_error("storage_error", message, None)
}

fn map_validation_error(message: String) -> engine::ToolError {
    let lower = message.to_ascii_lowercase();
    if lower.starts_with("write denied") || lower.starts_with("read denied") {
        return err_capability_denied(message);
    }
    if lower.starts_with("payload too large") {
        return tool_error("payload_too_large", message, None);
    }
    if lower.starts_with("importance") {
        return tool_error("importance_cap", message, None);
    }
    if lower.starts_with("kind") {
        return tool_error("kind_not_allowed", message, None);
    }
    if lower.starts_with("label") || lower.contains("labels not permitted") {
        return tool_error("label_not_allowed", message, None);
    }
    err_invalid_input(message)
}

fn effective_worker(input: &str) -> String {
    std::env::var("DWBASE_WORKER_ID")
        .or_else(|_| std::env::var("GREENTIC_WORKER_ID"))
        .unwrap_or_else(|_| {
            if input.trim().is_empty() {
                "llm".into()
            } else {
                input.to_string()
            }
        })
}

fn subscription_patterns() -> Vec<String> {
    let raw = std::env::var("DWBASE_SUBSCRIBE_WORLDS")
        .or_else(|_| std::env::var("DWBASE_SUBSCRIBE_PATTERNS"))
        .unwrap_or_default();
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn observe_queue_capacity() -> usize {
    std::env::var("DWBASE_OBSERVE_QUEUE_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(10_000)
}

fn observe_drop_policy() -> ObserveDropPolicy {
    let raw = std::env::var("DWBASE_OBSERVE_DROP_POLICY").unwrap_or_else(|_| "drop_oldest".into());
    match raw.to_ascii_lowercase().as_str() {
        "drop_newest" | "newest" => ObserveDropPolicy::DropNewest,
        _ => ObserveDropPolicy::DropOldest,
    }
}

fn observe_durable_enabled() -> bool {
    std::env::var("DWBASE_OBSERVE_DURABLE").is_ok()
}

fn observe_durable_catchup_limit() -> usize {
    std::env::var("DWBASE_OBSERVE_DURABLE_CATCHUP_LIMIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1_000)
}

#[derive(Clone, Debug)]
struct SecurityConfig {
    tenant_id: String,
    enforce_tenant_namespace: bool,
    allow_read_worlds: Vec<String>,
    allow_write_worlds: Vec<String>,
    allow_read_prefixes: Vec<String>,
    allow_write_prefixes: Vec<String>,
    payload_max_bytes: usize,
    importance_cap: f32,
    allowed_kinds: Vec<AtomKind>,
    allowed_labels: Vec<String>,
    allow_policy_labels: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            tenant_id: tenant_id(),
            enforce_tenant_namespace: true,
            allow_read_worlds: Vec::new(),
            allow_write_worlds: Vec::new(),
            allow_read_prefixes: Vec::new(),
            allow_write_prefixes: Vec::new(),
            payload_max_bytes: 64 * 1024,
            importance_cap: 0.7,
            allowed_kinds: vec![AtomKind::Observation, AtomKind::Reflection],
            allowed_labels: Vec::new(),
            allow_policy_labels: false,
        }
    }
}

fn parse_csv_env(key: &str) -> Vec<String> {
    std::env::var(key)
        .ok()
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

impl SecurityConfig {
    fn from_env() -> Self {
        let mut cfg = SecurityConfig::default();
        if let Ok(v) = std::env::var("DWBASE_ENFORCE_TENANT_NAMESPACE") {
            cfg.enforce_tenant_namespace = v != "0" && !v.eq_ignore_ascii_case("false");
        }
        cfg.allow_read_worlds = parse_csv_env("DWBASE_ALLOW_READ_WORLDS");
        cfg.allow_write_worlds = parse_csv_env("DWBASE_ALLOW_WRITE_WORLDS");
        cfg.allow_read_prefixes = parse_csv_env("DWBASE_ALLOW_READ_PREFIXES");
        cfg.allow_write_prefixes = parse_csv_env("DWBASE_ALLOW_WRITE_PREFIXES");

        if let Ok(v) = std::env::var("DWBASE_MAX_PAYLOAD_BYTES") {
            if let Ok(n) = v.parse::<usize>() {
                cfg.payload_max_bytes = n;
            }
        }
        if let Ok(v) = std::env::var("DWBASE_IMPORTANCE_CAP") {
            if let Ok(n) = v.parse::<f32>() {
                cfg.importance_cap = n;
            }
        }
        let kinds = parse_csv_env("DWBASE_ALLOWED_KINDS");
        if !kinds.is_empty() {
            cfg.allowed_kinds = kinds
                .iter()
                .filter_map(|k| match k.to_ascii_lowercase().as_str() {
                    "observation" => Some(AtomKind::Observation),
                    "reflection" => Some(AtomKind::Reflection),
                    "plan" => Some(AtomKind::Plan),
                    "action" => Some(AtomKind::Action),
                    "message" => Some(AtomKind::Message),
                    _ => None,
                })
                .collect();
        }
        cfg.allowed_labels = parse_csv_env("DWBASE_ALLOWED_LABELS");
        cfg.allow_policy_labels = std::env::var("DWBASE_ALLOW_POLICY_LABELS").is_ok();
        cfg
    }
}

fn matches_prefixes(prefixes: &[String], world: &str) -> bool {
    prefixes.iter().any(|p| world.starts_with(p))
}

fn validate_world_for_write(world: &str) -> Result<(), String> {
    let cfg = SecurityConfig::from_env();
    if cfg.allow_write_worlds.iter().any(|w| w == world) {
        return Ok(());
    }
    if matches_prefixes(&cfg.allow_write_prefixes, world) {
        return Ok(());
    }
    if cfg.enforce_tenant_namespace {
        let prefix = tenant_prefix(&cfg.tenant_id);
        if !world.starts_with(&prefix) {
            return Err(format!(
                "write denied: world must be within {prefix} (set DWBASE_ALLOW_WRITE_WORLDS/DWBASE_ALLOW_WRITE_PREFIXES to override)"
            ));
        }
    }
    Ok(())
}

fn validate_world_for_read(world: &str) -> Result<(), String> {
    let cfg = SecurityConfig::from_env();
    if cfg.allow_read_worlds.iter().any(|w| w == world) {
        return Ok(());
    }
    if matches_prefixes(&cfg.allow_read_prefixes, world) {
        return Ok(());
    }
    if cfg.enforce_tenant_namespace {
        let prefix = tenant_prefix(&cfg.tenant_id);
        if !world.starts_with(&prefix) {
            return Err(format!(
                "read denied: world must be within {prefix} (set DWBASE_ALLOW_READ_WORLDS/DWBASE_ALLOW_READ_PREFIXES to override)"
            ));
        }
    }
    Ok(())
}

fn validate_new_atom(new_atom: &NewAtom) -> Result<(), String> {
    let cfg = SecurityConfig::from_env();
    validate_world_for_write(&new_atom.world.0)?;
    if new_atom.payload_json.len() > cfg.payload_max_bytes {
        return Err(format!(
            "payload too large: {} bytes > max {} (DWBASE_MAX_PAYLOAD_BYTES)",
            new_atom.payload_json.len(),
            cfg.payload_max_bytes
        ));
    }
    if new_atom.importance.get() > cfg.importance_cap {
        return Err(format!(
            "importance {} exceeds cap {} (DWBASE_IMPORTANCE_CAP)",
            new_atom.importance.get(),
            cfg.importance_cap
        ));
    }
    if !cfg.allowed_kinds.contains(&new_atom.kind) {
        return Err(format!(
            "kind {:?} not permitted (DWBASE_ALLOWED_KINDS)",
            new_atom.kind
        ));
    }
    for label in &new_atom.labels {
        if cfg.allow_policy_labels && label.starts_with("policy:") {
            continue;
        }
        if !cfg.allowed_labels.is_empty() && cfg.allowed_labels.contains(label) {
            continue;
        }
        if cfg.allowed_labels.is_empty() {
            return Err("labels not permitted by default (set DWBASE_ALLOWED_LABELS or DWBASE_ALLOW_POLICY_LABELS)".into());
        }
        return Err(format!("label not permitted: {label}"));
    }
    Ok(())
}

fn init_engine_v2() -> Result<&'static Mutex<ComponentEngine>, engine::ToolError> {
    ENGINE.get_or_try_init(|| {
        let _ = SecurityConfig::from_env();
        let _ = install_metrics_recorder();
        let dir = data_dir();
        let storage = FsStorage::new(dir.clone())
            .map_err(|e| err_storage(format!("init storage failed: {e}")))?;
        let vector = NoVector;
        let stream = LocalStream::new();
        let _ = STREAM.set(stream.clone());
        let gatekeeper = LocalGatekeeper::new(Capabilities::default(), TrustStore::default());
        let embedder = DummyEmbedder::new();
        let engine = DWBaseEngine::new(storage, vector, stream, gatekeeper, embedder);
        maybe_start_presence(&engine);
        maybe_start_swarm(&engine);
        Ok(Mutex::new(engine))
    })
}

fn init_engine() -> &'static Mutex<ComponentEngine> {
    init_engine_v2().expect("engine init")
}

fn node_id() -> String {
    std::env::var("DWBASE_NODE_ID").unwrap_or_else(|_| "component-dwbase".into())
}

fn maybe_start_presence(
    engine: &DWBaseEngine<FsStorage, NoVector, LocalStream, LocalGatekeeper, DummyEmbedder>,
) {
    // Start only when explicitly configured.
    if std::env::var("NATS_URL").is_err()
        && std::env::var("GREENTIC_NATS_URL").is_err()
        && std::env::var("DWBASE_PRESENCE_MOCK").is_err()
    {
        return;
    }
    let table = PEERS.get_or_init(PeerTable::default);
    let client: Arc<dyn NatsClient> = if std::env::var("DWBASE_PRESENCE_MOCK").is_ok() {
        Arc::new(MockNats::default())
    } else {
        #[cfg(feature = "nats")]
        {
            std::env::var("NATS_URL")
                .or_else(|_| std::env::var("GREENTIC_NATS_URL"))
                .ok()
                .and_then(|url| {
                    AsyncNats::connect(&url)
                        .ok()
                        .map(|c| Arc::new(c) as Arc<dyn NatsClient>)
                })
                .unwrap_or_else(|| Arc::new(MockNats::default()))
        }
        #[cfg(not(feature = "nats"))]
        {
            Arc::new(MockNats::default())
        }
    };
    let hello = hello_from_engine(engine);
    let ttl = std::time::Duration::from_secs(30);
    start_presence_loop(client, hello, table.clone(), ttl);
}

fn maybe_start_swarm(
    engine: &DWBaseEngine<FsStorage, NoVector, LocalStream, LocalGatekeeper, DummyEmbedder>,
) {
    // Opt-in: start only when explicitly enabled or when subscription patterns are provided.
    let patterns = subscription_patterns();
    let enabled = std::env::var("DWBASE_SWARM_ENABLE").is_ok() || !patterns.is_empty();
    if !enabled {
        return;
    }

    let self_id = dwbase_swarm::PeerId::new(node_id());
    let bus: Arc<dyn dwbase_swarm_nats::swarm::SwarmBus> =
        if std::env::var("DWBASE_SWARM_MOCK").is_ok() {
            Arc::new(MockBus::default()) as Arc<dyn dwbase_swarm_nats::swarm::SwarmBus>
        } else {
            #[cfg(feature = "nats")]
            {
                std::env::var("NATS_URL")
                    .or_else(|_| std::env::var("GREENTIC_NATS_URL"))
                    .ok()
                    .and_then(|url| {
                        NatsBus::connect(&url)
                            .ok()
                            .map(|b| Arc::new(b) as Arc<dyn dwbase_swarm_nats::swarm::SwarmBus>)
                    })
                    .unwrap_or_else(|| {
                        Arc::new(MockBus::default()) as Arc<dyn dwbase_swarm_nats::swarm::SwarmBus>
                    })
            }
            #[cfg(not(feature = "nats"))]
            {
                Arc::new(MockBus::default()) as Arc<dyn dwbase_swarm_nats::swarm::SwarmBus>
            }
        };

    let _ = BUS.set(bus.clone());
    let _ = EVENT_SUBS.set(ParkingMutex::new(HashSet::new()));
    if std::env::var("DWBASE_OBSERVE_NATS_BROADCAST").is_ok() {
        let _ = BROADCASTER.set(WorldEventBroadcaster::new(bus.clone(), node_id(), 200.0));
    }

    let transport = NatsSwarmTransport::new(bus, self_id, 200.0).expect("init swarm transport");
    let policy: Arc<dyn WorldAccessPolicy> = Arc::new(ComponentWorldAccessPolicy::new(data_dir()));
    let swarm_state = data_dir().join("swarm.json");
    let replicator = Arc::new(
        Replicator::with_policy_and_state(
            transport,
            patterns,
            std::time::Duration::from_secs(30),
            policy,
            Some(swarm_state),
            512,
            std::time::Duration::from_secs(300),
        )
        .expect("init replicator"),
    );
    let _ = SWARM.set(replicator.clone());
    start_replication_loop(engine, replicator);
}

#[derive(Clone)]
struct ComponentWorldAccessPolicy {
    data_dir: PathBuf,
    cache: Arc<Mutex<PolicyCache>>,
}

struct PolicyCache {
    last_loaded: std::time::Instant,
    deny_prefixes: Vec<String>,
    allow_prefixes: Vec<String>,
    min_retention_days: Option<u64>,
}

impl Default for PolicyCache {
    fn default() -> Self {
        Self {
            last_loaded: std::time::Instant::now() - std::time::Duration::from_secs(3600),
            deny_prefixes: Vec::new(),
            allow_prefixes: Vec::new(),
            min_retention_days: None,
        }
    }
}

impl ComponentWorldAccessPolicy {
    fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            cache: Arc::new(Mutex::new(PolicyCache::default())),
        }
    }

    fn refresh_cache(&self) {
        let mut cache = self.cache.lock().unwrap();
        if cache.last_loaded.elapsed() < std::time::Duration::from_secs(1) {
            return;
        }
        cache.last_loaded = std::time::Instant::now();

        let policy_world = format!("{}policy", tenant_prefix(&tenant_id()));
        let path = self.data_dir.join("atoms.json");
        let Ok(bytes) = fs::read(&path) else {
            return;
        };
        let Ok(persisted) = serde_json::from_slice::<Persisted>(&bytes) else {
            return;
        };
        let Some(atoms) = persisted.atoms.get(&policy_world) else {
            cache.deny_prefixes.clear();
            cache.allow_prefixes.clear();
            cache.min_retention_days = None;
            return;
        };
        let mut deny = Vec::new();
        let mut allow = Vec::new();
        let mut min_retention_days = None;
        for atom in atoms {
            for label in atom.labels() {
                if let Some(pat) = label.strip_prefix("policy:replication_deny=") {
                    deny.push(pat.to_string());
                }
                if let Some(pat) = label.strip_prefix("policy:replication_allow=") {
                    allow.push(pat.to_string());
                }
                if let Some(v) = label.strip_prefix("policy:retention_min_days=") {
                    if let Ok(n) = v.parse::<u64>() {
                        min_retention_days = Some(n);
                    }
                }
            }
        }
        cache.deny_prefixes = deny;
        cache.allow_prefixes = allow;
        cache.min_retention_days = min_retention_days;
    }

    fn policy_allows_replication(&self, world: &str) -> bool {
        self.refresh_cache();
        let cache = self.cache.lock().unwrap();
        let denied = cache.deny_prefixes.iter().any(|p| world.starts_with(p));
        if denied {
            return false;
        }
        if cache.allow_prefixes.is_empty() {
            return true;
        }
        cache.allow_prefixes.iter().any(|p| world.starts_with(p))
    }
}

impl WorldAccessPolicy for ComponentWorldAccessPolicy {
    fn can_send_world(&self, world: &str, _to: &dwbase_swarm::PeerId) -> bool {
        validate_world_for_read(world).is_ok() && self.policy_allows_replication(world)
    }

    fn can_receive_world(&self, world: &str, _from: &dwbase_swarm::PeerId) -> bool {
        validate_world_for_read(world).is_ok() && self.policy_allows_replication(world)
    }
}

fn start_replication_loop(
    engine: &DWBaseEngine<FsStorage, NoVector, LocalStream, LocalGatekeeper, DummyEmbedder>,
    replicator: Arc<Replicator>,
) {
    // The engine instance is owned by a static mutex; we only need the address here to re-lock it inside the thread.
    let _engine_ptr = engine as *const _;
    std::thread::spawn(move || loop {
        let _ = replicator.announce();
        let _ = replicator.poll_inbox();
        while let Some((_from, batch)) = replicator.poll_atom_batch() {
            if let Some(engine_mutex) = ENGINE.get() {
                let guard = engine_mutex.lock().unwrap();
                if futures::executor::block_on(guard.ingest_remote_atoms(batch.atoms)).is_ok() {
                    LAST_REMOTE_INGEST_MS.store(now_ms(), Ordering::Relaxed);
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    });
}

fn hello_from_engine(
    engine: &DWBaseEngine<FsStorage, NoVector, LocalStream, LocalGatekeeper, DummyEmbedder>,
) -> NodeHello {
    NodeHello {
        node_id: node_id(),
        endpoint: "component-local".into(),
        worlds_served: engine
            .storage
            .worlds()
            .unwrap_or_default()
            .into_iter()
            .map(|w| w.0)
            .collect(),
        trust_score: 1.0,
        started_at: now_rfc3339(),
        version: env!("CARGO_PKG_VERSION").into(),
    }
}

pub fn peers() -> Vec<NodeHello> {
    PEERS.get().map(|t| t.peers()).unwrap_or_default()
}

#[derive(Clone)]
struct LocalStream {
    inner: Arc<ParkingMutex<LocalStreamInner>>,
}

#[derive(Clone, Copy, Debug)]
enum ObserveDropPolicy {
    DropOldest,
    DropNewest,
}

struct LocalStreamInner {
    next_handle: AtomicU64,
    subs: HashMap<u64, LocalSubscription>,
}

struct LocalSubscription {
    world: WorldKey,
    filter: AtomFilter,
    queue: VecDeque<Atom>,
    capacity: usize,
    drop_policy: ObserveDropPolicy,
    dropped_total: u64,
    last_event_ms: u64,
}

impl LocalStream {
    fn new() -> Self {
        Self {
            inner: Arc::new(ParkingMutex::new(LocalStreamInner {
                next_handle: AtomicU64::new(1),
                subs: HashMap::new(),
            })),
        }
    }

    fn matches_filter(atom: &Atom, filter: &AtomFilter) -> bool {
        if let Some(world) = &filter.world {
            if atom.world() != world {
                return false;
            }
        }
        if !filter.kinds.is_empty() && !filter.kinds.contains(atom.kind()) {
            return false;
        }
        if !filter.labels.is_empty() && !filter.labels.iter().all(|l| atom.labels().contains(l)) {
            return false;
        }
        if !filter.flags.is_empty() && !filter.flags.iter().all(|f| atom.flags().contains(f)) {
            return false;
        }
        if let Some(since) = &filter.since {
            if atom.timestamp().0 < since.0 {
                return false;
            }
        }
        if let Some(until) = &filter.until {
            if atom.timestamp().0 > until.0 {
                return false;
            }
        }
        true
    }

    fn push_atom(&self, atom: Atom) {
        let mut guard = self.inner.lock();
        for sub in guard.subs.values_mut() {
            if &sub.world != atom.world() {
                continue;
            }
            if !Self::matches_filter(&atom, &sub.filter) {
                continue;
            }
            Self::enqueue(sub, atom.clone());
        }
    }

    fn enqueue(sub: &mut LocalSubscription, atom: Atom) {
        if sub.queue.len() >= sub.capacity {
            sub.dropped_total = sub.dropped_total.saturating_add(1);
            dwbase_metrics::record_observe_dropped(1);
            match sub.drop_policy {
                ObserveDropPolicy::DropOldest => {
                    let _ = sub.queue.pop_front();
                }
                ObserveDropPolicy::DropNewest => {
                    dwbase_metrics::record_observe_queue_depth(sub.queue.len() as u64);
                    return;
                }
            }
        }
        sub.queue.push_back(atom);
        sub.last_event_ms = now_ms();
        dwbase_metrics::record_observe_queue_depth(sub.queue.len() as u64);
    }

    fn poll_n(&self, handle: u64, max: usize) -> Vec<Atom> {
        let mut out = Vec::new();
        let mut guard = self.inner.lock();
        let Some(sub) = guard.subs.get_mut(&handle) else {
            return out;
        };
        for _ in 0..max {
            if let Some(a) = sub.queue.pop_front() {
                out.push(a);
            } else {
                break;
            }
        }
        out
    }

    fn has_handle(&self, handle: u64) -> bool {
        self.inner.lock().subs.contains_key(&handle)
    }

    fn world_for_handle(&self, handle: u64) -> Option<WorldKey> {
        self.inner.lock().subs.get(&handle).map(|s| s.world.clone())
    }

    fn stats_for_handle(&self, handle: u64) -> Option<(usize, u64, u64)> {
        let guard = self.inner.lock();
        let sub = guard.subs.get(&handle)?;
        Some((sub.queue.len(), sub.dropped_total, sub.last_event_ms))
    }

    fn push_to_handle(&self, handle: u64, atom: Atom) -> bool {
        let mut guard = self.inner.lock();
        let Some(sub) = guard.subs.get_mut(&handle) else {
            return false;
        };
        if &sub.world != atom.world() {
            return false;
        }
        if !Self::matches_filter(&atom, &sub.filter) {
            return false;
        }
        Self::enqueue(sub, atom);
        true
    }
}

impl dwbase_engine::StreamEngine for LocalStream {
    type Handle = u64;

    fn publish(&self, atom: &Atom) -> dwbase_engine::Result<()> {
        self.push_atom(atom.clone());
        Ok(())
    }

    fn subscribe(
        &self,
        world: &WorldKey,
        filter: AtomFilter,
    ) -> dwbase_engine::Result<Self::Handle> {
        let handle = self
            .inner
            .lock()
            .next_handle
            .fetch_add(1, Ordering::Relaxed);
        let mut guard = self.inner.lock();
        guard.subs.insert(
            handle,
            LocalSubscription {
                world: world.clone(),
                filter,
                queue: VecDeque::new(),
                capacity: observe_queue_capacity(),
                drop_policy: observe_drop_policy(),
                dropped_total: 0,
                last_event_ms: 0,
            },
        );
        Ok(handle)
    }

    fn poll(&self, handle: &Self::Handle) -> dwbase_engine::Result<Option<Atom>> {
        Ok(self.poll_n(*handle, 1).pop())
    }

    fn stop(&self, handle: Self::Handle) -> dwbase_engine::Result<()> {
        self.inner.lock().subs.remove(&handle);
        Ok(())
    }
}

#[derive(Default)]
struct NoVector;
impl dwbase_engine::VectorEngine for NoVector {
    fn upsert(
        &self,
        _world: &WorldKey,
        _atom_id: &AtomId,
        _vector: &[f32],
    ) -> dwbase_engine::Result<()> {
        Ok(())
    }

    fn search(
        &self,
        _world: &WorldKey,
        _query: &[f32],
        _k: usize,
        _filter: &AtomFilter,
    ) -> dwbase_engine::Result<Vec<AtomId>> {
        Ok(Vec::new())
    }

    fn rebuild(&self, _world: &WorldKey) -> dwbase_engine::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Persisted {
    atoms: HashMap<String, Vec<Atom>>,
}

#[derive(Debug)]
struct FsStorage {
    root: PathBuf,
    data: Mutex<HashMap<WorldKey, Vec<Atom>>>,
}

impl FsStorage {
    fn new(root: PathBuf) -> dwbase_engine::Result<Self> {
        if !root.exists() {
            fs::create_dir_all(&root)
                .map_err(|e| dwbase_engine::DwbaseError::Storage(e.to_string()))?;
        }
        let path = root.join("atoms.json");
        let data = if path.exists() {
            let bytes =
                fs::read(&path).map_err(|e| dwbase_engine::DwbaseError::Storage(e.to_string()))?;
            if bytes.is_empty() {
                HashMap::new()
            } else {
                let persisted: Persisted = serde_json::from_slice(&bytes)
                    .map_err(|e| dwbase_engine::DwbaseError::Storage(e.to_string()))?;
                persisted
                    .atoms
                    .into_iter()
                    .map(|(k, v)| (WorldKey::new(k), v))
                    .collect()
            }
        } else {
            HashMap::new()
        };
        Ok(Self {
            root,
            data: Mutex::new(data),
        })
    }

    fn persist(&self, data: &HashMap<WorldKey, Vec<Atom>>) -> dwbase_engine::Result<()> {
        let path = self.root.join("atoms.json");
        let persistable = Persisted {
            atoms: data.iter().map(|(k, v)| (k.0.clone(), v.clone())).collect(),
        };
        let bytes = serde_json::to_vec_pretty(&persistable)
            .map_err(|e| dwbase_engine::DwbaseError::Storage(e.to_string()))?;
        fs::write(path, bytes).map_err(|e| dwbase_engine::DwbaseError::Storage(e.to_string()))?;
        Ok(())
    }

    fn matches_filter(atom: &Atom, filter: &AtomFilter) -> bool {
        if let Some(world) = &filter.world {
            if atom.world() != world {
                return false;
            }
        }
        if !filter.kinds.is_empty() && !filter.kinds.contains(atom.kind()) {
            return false;
        }
        if !filter.labels.is_empty() && !filter.labels.iter().all(|l| atom.labels().contains(l)) {
            return false;
        }
        if !filter.flags.is_empty() && !filter.flags.iter().all(|f| atom.flags().contains(f)) {
            return false;
        }
        if let Some(since) = &filter.since {
            if atom.timestamp().0 < since.0 {
                return false;
            }
        }
        if let Some(until) = &filter.until {
            if atom.timestamp().0 > until.0 {
                return false;
            }
        }
        true
    }
}

impl StorageEngine for FsStorage {
    fn append(&self, atom: Atom) -> dwbase_engine::Result<()> {
        let mut guard = self.data.lock().unwrap();
        guard.entry(atom.world().clone()).or_default().push(atom);
        self.persist(&guard)
    }

    fn get_by_ids(&self, ids: &[AtomId]) -> dwbase_engine::Result<Vec<Atom>> {
        let guard = self.data.lock().unwrap();
        let mut out = Vec::new();
        for atoms in guard.values() {
            for atom in atoms {
                if ids.contains(atom.id()) {
                    out.push(atom.clone());
                }
            }
        }
        Ok(out)
    }

    fn scan(&self, world: &WorldKey, filter: &AtomFilter) -> dwbase_engine::Result<Vec<Atom>> {
        let guard = self.data.lock().unwrap();
        let list = guard.get(world).cloned().unwrap_or_default();
        let mut out = Vec::new();
        for atom in list {
            if Self::matches_filter(&atom, filter) {
                out.push(atom);
                if let Some(limit) = filter.limit {
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn stats(&self, world: &WorldKey) -> dwbase_engine::Result<dwbase_engine::StorageStats> {
        let guard = self.data.lock().unwrap();
        let atoms = guard.get(world);
        let atom_count = atoms.map(|v| v.len()).unwrap_or(0);
        let vector_count = atoms
            .map(|v| v.iter().filter(|a| a.vector().is_some()).count())
            .unwrap_or(0);
        Ok(dwbase_engine::StorageStats {
            atom_count,
            vector_count,
        })
    }

    fn list_ids_in_window(
        &self,
        world: &WorldKey,
        window: &dwbase_engine::TimeWindow,
    ) -> dwbase_engine::Result<Vec<AtomId>> {
        let guard = self.data.lock().unwrap();
        let mut ids = Vec::new();
        if let Some(atoms) = guard.get(world) {
            for atom in atoms {
                if let Ok(dt) = OffsetDateTime::parse(
                    atom.timestamp().0.as_str(),
                    &time::format_description::well_known::Rfc3339,
                ) {
                    let ms = (dt.unix_timestamp_nanos() / 1_000_000) as i64;
                    if ms >= window.start_ms && ms <= window.end_ms {
                        ids.push(atom.id().clone());
                    }
                }
            }
        }
        Ok(ids)
    }

    fn delete_atoms(&self, world: &WorldKey, ids: &[AtomId]) -> dwbase_engine::Result<usize> {
        let mut guard = self.data.lock().unwrap();
        let mut removed = 0usize;
        if let Some(vec) = guard.get_mut(world) {
            let before = vec.len();
            vec.retain(|a| !ids.contains(a.id()));
            removed = before - vec.len();
        }
        self.persist(&guard)?;
        Ok(removed)
    }

    fn worlds(&self) -> dwbase_engine::Result<Vec<WorldKey>> {
        let guard = self.data.lock().unwrap();
        Ok(guard.keys().cloned().collect())
    }
}

fn to_atom(kind: WitAtomKind) -> AtomKind {
    match kind {
        WitAtomKind::Observation => AtomKind::Observation,
        WitAtomKind::Reflection => AtomKind::Reflection,
        WitAtomKind::Plan => AtomKind::Plan,
        WitAtomKind::Action => AtomKind::Action,
        WitAtomKind::Message => AtomKind::Message,
    }
}

fn to_wit_atom(atom: &Atom) -> WitAtom {
    WitAtom {
        id: atom.id().0.clone(),
        world_key: atom.world().0.clone(),
        worker: atom.worker().0.clone(),
        kind: match atom.kind() {
            AtomKind::Observation => WitAtomKind::Observation,
            AtomKind::Reflection => WitAtomKind::Reflection,
            AtomKind::Plan => WitAtomKind::Plan,
            AtomKind::Action => WitAtomKind::Action,
            AtomKind::Message => WitAtomKind::Message,
        },
        timestamp: atom.timestamp().0.clone(),
        importance: atom.importance().get(),
        payload_json: atom.payload_json().to_string(),
        vector: atom.vector().map(|v| v.to_vec()),
        flags_list: atom.flags().to_vec(),
        labels: atom.labels().to_vec(),
        links: atom.links().iter().map(|l| l.target.0.clone()).collect(),
    }
}

fn to_filter(filter: WitAtomFilter) -> AtomFilter {
    AtomFilter {
        world: filter.world_key.map(WorldKey::new),
        kinds: filter.kinds.into_iter().map(to_atom).collect(),
        labels: filter.labels,
        flags: filter.flag_filter,
        since: filter.since.map(Timestamp::new),
        until: filter.until.map(Timestamp::new),
        limit: filter.limit.map(|v| v as usize),
    }
}

fn warnings_from_health(snapshot: &engine::HealthSnapshot) -> Vec<String> {
    if snapshot.status == "ready" {
        Vec::new()
    } else if snapshot.message.is_empty() {
        vec!["degraded".into()]
    } else {
        vec![format!("degraded: {}", snapshot.message)]
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ObserveCursor {
    last_atom_id: String,
    last_timestamp: String,
    updated_at_ms: u64,
}

fn world_token(world_key: &str) -> String {
    hex::encode(world_key.as_bytes())
}

fn observe_cursor_path(world: &WorldKey) -> PathBuf {
    data_dir()
        .join("_observe")
        .join("cursors")
        .join(world_token(&world.0))
        .join("cursor.json")
}

fn read_observe_cursor(world: &WorldKey) -> Option<ObserveCursor> {
    let path = observe_cursor_path(world);
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_observe_cursor(world: &WorldKey, cursor: &ObserveCursor) {
    let path = observe_cursor_path(world);
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(bytes) = serde_json::to_vec(cursor) {
        let _ = fs::write(path, bytes);
    }
}

fn durable_catchup_atoms(
    engine: &ComponentEngine,
    world: &WorldKey,
    filter: &AtomFilter,
    cursor: Option<&ObserveCursor>,
) -> Vec<Atom> {
    let mut out = Vec::new();
    let Ok(all) = engine.storage.scan(
        world,
        &AtomFilter {
            world: Some(world.clone()),
            kinds: Vec::new(),
            labels: Vec::new(),
            flags: Vec::new(),
            since: None,
            until: None,
            limit: None,
        },
    ) else {
        return out;
    };

    let start_idx = cursor
        .and_then(|c| all.iter().position(|a| a.id().0 == c.last_atom_id))
        .map(|i| i + 1)
        .unwrap_or(0);

    for atom in all.into_iter().skip(start_idx) {
        if !LocalStream::matches_filter(&atom, filter) {
            continue;
        }
        out.push(atom);
        if out.len() >= observe_durable_catchup_limit() {
            break;
        }
    }
    out
}

pub struct Component;
impl engine::Guest for Component {
    fn remember(atom: WitNewAtom) -> String {
        let start = Instant::now();
        let engine = init_engine();
        let guard = engine.lock().unwrap();
        let new_atom = NewAtom {
            world: WorldKey::new(atom.world_key),
            worker: WorkerKey::new(effective_worker(&atom.worker)),
            kind: to_atom(atom.kind),
            timestamp: Timestamp::new(if atom.timestamp.is_empty() {
                OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
            } else {
                atom.timestamp
            }),
            importance: Importance::clamped(atom.importance),
            payload_json: atom.payload_json,
            vector: atom.vector,
            flags: atom.flags_list,
            labels: atom.labels,
            links: atom
                .links
                .into_iter()
                .map(|id| Link {
                    target: AtomId::new(id),
                    kind: LinkKind::References,
                })
                .collect(),
        };
        if let Err(_e) = validate_new_atom(&new_atom) {
            return String::new();
        }
        let id = match futures::executor::block_on(guard.remember(new_atom)) {
            Ok(v) => v.0,
            Err(_) => return String::new(),
        };
        dwbase_metrics::record_remember_latency(start.elapsed());
        dwbase_metrics::record_index_freshness(Duration::from_millis(0));
        let repl = SWARM.get().cloned();
        let broadcaster = BROADCASTER.get();
        let atom = guard
            .get_atoms(&[AtomId::new(id.clone())])
            .ok()
            .and_then(|mut v| v.pop());
        drop(guard);
        if let Some(atom) = atom {
            if let Some(repl) = repl {
                let _ = repl.replicate_new_atom(atom.clone());
            }
            if let Some(b) = broadcaster {
                let _ = b.publish_atom(atom);
            }
        }
        id
    }

    fn ask(question: WitQuestion) -> Answer {
        let start = Instant::now();
        if validate_world_for_read(&question.world_key).is_err() {
            return Answer {
                world_key: question.world_key,
                text: "capability denied".into(),
                supporting_atoms: Vec::new(),
            };
        }
        let engine = init_engine();
        let guard = engine.lock().unwrap();
        let q = Question {
            world: WorldKey::new(question.world_key),
            text: question.text,
            filter: question.filter.map(to_filter),
        };
        let ans = futures::executor::block_on(guard.ask(q)).expect("ask");
        dwbase_metrics::record_ask_latency(start.elapsed());
        Answer {
            world_key: ans.world.0,
            text: ans.text,
            supporting_atoms: ans.supporting_atoms.iter().map(to_wit_atom).collect(),
        }
    }

    fn observe(atom: WitAtom) {
        let start = Instant::now();
        let engine = init_engine();
        let guard = engine.lock().unwrap();
        let new_atom = NewAtom {
            world: WorldKey::new(atom.world_key.clone()),
            worker: WorkerKey::new(effective_worker(&atom.worker)),
            kind: to_atom(atom.kind),
            timestamp: Timestamp::new(atom.timestamp),
            importance: Importance::clamped(atom.importance),
            payload_json: atom.payload_json,
            vector: atom.vector,
            flags: atom.flags_list,
            labels: atom.labels,
            links: atom
                .links
                .into_iter()
                .map(|id| Link {
                    target: AtomId::new(id),
                    kind: LinkKind::References,
                })
                .collect(),
        };
        if validate_new_atom(&new_atom).is_err() {
            return;
        }
        let id = futures::executor::block_on(guard.remember(new_atom))
            .ok()
            .map(|v| v.0);
        dwbase_metrics::record_remember_latency(start.elapsed());
        dwbase_metrics::record_index_freshness(Duration::from_millis(0));
        let repl = SWARM.get().cloned();
        let broadcaster = BROADCASTER.get();
        let atom = id
            .clone()
            .and_then(|id| guard.get_atoms(&[AtomId::new(id)]).ok())
            .and_then(|mut v| v.pop());
        drop(guard);
        if let Some(atom) = atom {
            if let Some(repl) = repl {
                let _ = repl.replicate_new_atom(atom.clone());
            }
            if let Some(b) = broadcaster {
                let _ = b.publish_atom(atom);
            }
        }
    }

    fn replay(target_world: String, filter: WitAtomFilter) -> Vec<WitAtom> {
        if validate_world_for_read(&target_world).is_err() {
            return Vec::new();
        }
        let engine = init_engine();
        let guard = engine.lock().unwrap();
        let filter = to_filter(filter);
        futures::executor::block_on(guard.replay(WorldKey::new(target_world), filter))
            .expect("replay")
            .iter()
            .map(to_wit_atom)
            .collect()
    }

    fn observe_start(filter: WitAtomFilter) -> u64 {
        let engine = init_engine();
        let filter = to_filter(filter);
        let Some(world) = filter.world.clone() else {
            return 0;
        };
        if validate_world_for_read(&world.0).is_err() {
            return 0;
        }
        let stream = STREAM.get_or_init(LocalStream::new).clone();
        let handle = stream.subscribe(&world, filter).unwrap_or(0);
        if handle == 0 {
            return 0;
        }

        if observe_durable_enabled() {
            let guard = engine.lock().unwrap();
            let cursor = read_observe_cursor(&world);
            let catchup = durable_catchup_atoms(
                &guard,
                &world,
                &AtomFilter {
                    world: Some(world.clone()),
                    kinds: Vec::new(),
                    labels: Vec::new(),
                    flags: Vec::new(),
                    since: None,
                    until: None,
                    limit: None,
                },
                cursor.as_ref(),
            );
            drop(guard);
            for atom in catchup {
                let _ = stream.push_to_handle(handle, atom);
            }
        }

        // Optional cross-node observe: subscribe to per-world NATS event subject if a bus is available.
        if std::env::var("DWBASE_OBSERVE_NATS_SUBSCRIBE").is_ok() {
            if let Some(bus) = BUS.get() {
                let subject = world_events_subject(&world.0);
                let subs = EVENT_SUBS.get_or_init(|| ParkingMutex::new(HashSet::new()));
                let mut guard = subs.lock();
                if guard.insert(subject.clone()) {
                    let stream = stream.clone();
                    let self_node = node_id();
                    let _ = bus.subscribe(
                        &subject,
                        Box::new(move |_sub, bytes, _reply_to| {
                            let Ok(batch) = decode_event_batch(&bytes) else {
                                return;
                            };
                            for ev in batch.events {
                                if ev.from_node == self_node {
                                    continue;
                                }
                                stream.push_atom(ev.atom);
                            }
                        }),
                    );
                }
            }
        }

        handle
    }

    fn observe_poll(handle: u64, max: u32) -> Vec<WitAtom> {
        if handle == 0 {
            return Vec::new();
        }
        let _ = init_engine();
        let stream = STREAM.get_or_init(LocalStream::new);
        let atoms = stream.poll_n(handle, max as usize);
        if observe_durable_enabled() {
            if let Some(world) = stream.world_for_handle(handle) {
                if let Some(last) = atoms.last() {
                    write_observe_cursor(
                        &world,
                        &ObserveCursor {
                            last_atom_id: last.id().0.clone(),
                            last_timestamp: last.timestamp().0.clone(),
                            updated_at_ms: now_ms(),
                        },
                    );
                }
            }
        }
        atoms.iter().map(to_wit_atom).collect()
    }

    fn observe_stop(handle: u64) {
        if handle == 0 {
            return;
        }
        let _ = init_engine();
        let stream = STREAM.get_or_init(LocalStream::new);
        let _ = stream.stop(handle);
    }

    fn observe_stats(handle: u64) -> engine::ObserveStatsSnapshot {
        if handle == 0 {
            return engine::ObserveStatsSnapshot {
                handle,
                queued_count: 0,
                dropped_count: 0,
                last_event_ms: 0,
                warnings: vec!["invalid_handle".into()],
            };
        }
        let stream = STREAM.get_or_init(LocalStream::new);
        let Some((queued, dropped, last_event_ms)) = stream.stats_for_handle(handle) else {
            return engine::ObserveStatsSnapshot {
                handle,
                queued_count: 0,
                dropped_count: 0,
                last_event_ms: 0,
                warnings: vec!["invalid_handle".into()],
            };
        };
        let mut warnings = Vec::new();
        if dropped > 0 {
            warnings.push("events_dropped".into());
        }
        engine::ObserveStatsSnapshot {
            handle,
            queued_count: queued as u64,
            dropped_count: dropped,
            last_event_ms,
            warnings,
        }
    }

    fn health() -> engine::HealthSnapshot {
        let engine = init_engine();
        let guard = engine.lock().unwrap();
        compute_health(&guard)
    }

    fn remember_v2(atom: WitNewAtom) -> Result<String, engine::ToolError> {
        let start = Instant::now();
        let engine = init_engine_v2()?;
        let guard = engine.lock().unwrap();

        let new_atom = NewAtom {
            world: WorldKey::new(atom.world_key),
            worker: WorkerKey::new(effective_worker(&atom.worker)),
            kind: to_atom(atom.kind),
            timestamp: Timestamp::new(if atom.timestamp.is_empty() {
                OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
            } else {
                atom.timestamp
            }),
            importance: Importance::clamped(atom.importance),
            payload_json: atom.payload_json,
            vector: atom.vector,
            flags: atom.flags_list,
            labels: atom.labels,
            links: atom
                .links
                .into_iter()
                .map(|id| Link {
                    target: AtomId::new(id),
                    kind: LinkKind::References,
                })
                .collect(),
        };

        if let Err(msg) = validate_new_atom(&new_atom) {
            return Err(map_validation_error(msg));
        }

        let id = futures::executor::block_on(guard.remember(new_atom))
            .map_err(|e| err_storage(format!("remember failed: {e}")))?
            .0;

        dwbase_metrics::record_remember_latency(start.elapsed());
        dwbase_metrics::record_index_freshness(Duration::from_millis(0));

        let repl = SWARM.get().cloned();
        let broadcaster = BROADCASTER.get();
        let atom = guard
            .get_atoms(&[AtomId::new(id.clone())])
            .ok()
            .and_then(|mut v| v.pop());
        drop(guard);

        if let Some(atom) = atom {
            if let Some(repl) = repl {
                let _ = repl.replicate_new_atom(atom.clone());
            }
            if let Some(b) = broadcaster {
                let _ = b.publish_atom(atom);
            }
        }

        Ok(id)
    }

    fn ask_v2(question: WitQuestion) -> Result<engine::AnswerV2, engine::ToolError> {
        let start = Instant::now();
        if let Err(msg) = validate_world_for_read(&question.world_key) {
            return Err(map_validation_error(msg));
        }
        let engine = init_engine_v2()?;
        let guard = engine.lock().unwrap();
        let q = Question {
            world: WorldKey::new(question.world_key),
            text: question.text,
            filter: question.filter.map(to_filter),
        };
        let ans = futures::executor::block_on(guard.ask(q))
            .map_err(|e| tool_error("internal_error", format!("ask failed: {e}"), None))?;
        dwbase_metrics::record_ask_latency(start.elapsed());
        let health = compute_health(&guard);
        Ok(engine::AnswerV2 {
            world_key: ans.world.0,
            text: ans.text,
            supporting_atoms: ans.supporting_atoms.iter().map(to_wit_atom).collect(),
            warnings: warnings_from_health(&health),
        })
    }

    fn observe_start_v2(filter: WitAtomFilter) -> Result<u64, engine::ToolError> {
        let engine = init_engine_v2()?;
        let filter = to_filter(filter);
        let Some(world) = filter.world.clone() else {
            return Err(err_invalid_input(
                "observe_start requires filter.world_key to be set",
            ));
        };
        if let Err(msg) = validate_world_for_read(&world.0) {
            return Err(map_validation_error(msg));
        }
        let stream = STREAM.get_or_init(LocalStream::new).clone();
        let handle = stream
            .subscribe(&world, filter)
            .map_err(|e| tool_error("internal_error", format!("subscribe failed: {e}"), None))?;

        if observe_durable_enabled() {
            let guard = engine.lock().unwrap();
            let cursor = read_observe_cursor(&world);
            let catchup = durable_catchup_atoms(
                &guard,
                &world,
                &AtomFilter {
                    world: Some(world.clone()),
                    kinds: Vec::new(),
                    labels: Vec::new(),
                    flags: Vec::new(),
                    since: None,
                    until: None,
                    limit: None,
                },
                cursor.as_ref(),
            );
            drop(guard);
            for atom in catchup {
                let _ = stream.push_to_handle(handle, atom);
            }
        }

        if std::env::var("DWBASE_OBSERVE_NATS_SUBSCRIBE").is_ok() {
            if let Some(bus) = BUS.get() {
                let subject = world_events_subject(&world.0);
                let subs = EVENT_SUBS.get_or_init(|| ParkingMutex::new(HashSet::new()));
                let mut guard = subs.lock();
                if guard.insert(subject.clone()) {
                    let stream = stream.clone();
                    let self_node = node_id();
                    let _ = bus.subscribe(
                        &subject,
                        Box::new(move |_sub, bytes, _reply_to| {
                            let Ok(batch) = decode_event_batch(&bytes) else {
                                return;
                            };
                            for ev in batch.events {
                                if ev.from_node == self_node {
                                    continue;
                                }
                                stream.push_atom(ev.atom);
                            }
                        }),
                    );
                }
            }
        }

        Ok(handle)
    }

    fn observe_poll_v2(handle: u64, max: u32) -> Result<Vec<WitAtom>, engine::ToolError> {
        if handle == 0 {
            return Err(err_invalid_handle("handle must be non-zero"));
        }
        let _ = init_engine_v2()?;
        let stream = STREAM.get_or_init(LocalStream::new);
        if !stream.has_handle(handle) {
            return Err(err_invalid_handle("unknown observe handle"));
        }
        let atoms = stream.poll_n(handle, max as usize);
        if observe_durable_enabled() {
            if let Some(world) = stream.world_for_handle(handle) {
                if let Some(last) = atoms.last() {
                    write_observe_cursor(
                        &world,
                        &ObserveCursor {
                            last_atom_id: last.id().0.clone(),
                            last_timestamp: last.timestamp().0.clone(),
                            updated_at_ms: now_ms(),
                        },
                    );
                }
            }
        }
        Ok(atoms.iter().map(to_wit_atom).collect())
    }

    fn observe_stop_v2(handle: u64) -> Result<bool, engine::ToolError> {
        if handle == 0 {
            return Err(err_invalid_handle("handle must be non-zero"));
        }
        let _ = init_engine_v2()?;
        let stream = STREAM.get_or_init(LocalStream::new);
        if !stream.has_handle(handle) {
            return Err(err_invalid_handle("unknown observe handle"));
        }
        stream
            .stop(handle)
            .map_err(|e| tool_error("internal_error", format!("stop failed: {e}"), None))?;
        Ok(true)
    }

    fn observe_stats_v2(handle: u64) -> Result<engine::ObserveStatsSnapshot, engine::ToolError> {
        if handle == 0 {
            return Err(err_invalid_handle("handle must be non-zero"));
        }
        let _ = init_engine_v2()?;
        let stream = STREAM.get_or_init(LocalStream::new);
        if !stream.has_handle(handle) {
            return Err(err_invalid_handle("unknown observe handle"));
        }
        Ok(<Component as engine::Guest>::observe_stats(handle))
    }

    fn health_v2() -> Result<engine::HealthSnapshot, engine::ToolError> {
        let engine = init_engine_v2()?;
        let guard = engine.lock().unwrap();
        Ok(compute_health(&guard))
    }

    fn metrics_snapshot() -> Result<engine::MetricsSnapshotData, engine::ToolError> {
        let handle = install_metrics_recorder()
            .or_else(|| PROM_HANDLE.get().and_then(|h| h.as_ref()))
            .ok_or_else(|| {
                tool_error(
                    "internal_error",
                    "metrics recorder unavailable",
                    Some("install metrics recorder failed".into()),
                )
            })?;
        metrics::gauge!("dwbase.component.up").set(1.0);
        let text = handle.render();
        let parsed = parse_prometheus(&text);
        Ok(engine::MetricsSnapshotData {
            format: "prometheus".into(),
            prometheus: text,
            counters: parsed.counters,
            gauges: parsed.gauges,
            histograms: parsed.histograms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dwbase_swarm_nats::replication::replicator_with_bus;
    use tempfile::TempDir;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static TEST_DIR: once_cell::sync::OnceCell<TempDir> = once_cell::sync::OnceCell::new();

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn reset_security_env() {
        for key in [
            "DWBASE_TENANT_ID",
            "GREENTIC_TENANT_ID",
            "DWBASE_WORKER_ID",
            "GREENTIC_WORKER_ID",
            "DWBASE_ENFORCE_TENANT_NAMESPACE",
            "DWBASE_ALLOW_READ_WORLDS",
            "DWBASE_ALLOW_WRITE_WORLDS",
            "DWBASE_ALLOW_READ_PREFIXES",
            "DWBASE_ALLOW_WRITE_PREFIXES",
            "DWBASE_MAX_PAYLOAD_BYTES",
            "DWBASE_IMPORTANCE_CAP",
            "DWBASE_ALLOWED_KINDS",
            "DWBASE_ALLOWED_LABELS",
            "DWBASE_ALLOW_POLICY_LABELS",
            "DWBASE_OBSERVE_QUEUE_CAPACITY",
            "DWBASE_OBSERVE_DROP_POLICY",
            "DWBASE_OBSERVE_DURABLE",
            "DWBASE_OBSERVE_DURABLE_CATCHUP_LIMIT",
            "DWBASE_HEALTH_DISABLE_FS_STATS",
        ] {
            std::env::remove_var(key);
        }
    }

    fn set_shared_data_dir() {
        let dir = TEST_DIR.get_or_init(|| TempDir::new().unwrap());
        std::env::set_var("DWBASE_DATA_DIR", dir.path());
    }

    #[test]
    fn remember_then_ask_roundtrip() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        let token = format!("hello-{}", now_ms());
        let atom_id = <Component as engine::Guest>::remember(WitNewAtom {
            world_key: "tenant:default/w1".into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:00Z".into(),
            importance: 0.5,
            payload_json: format!(r#"{{"note":"{}"}}"#, token),
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });
        assert!(!atom_id.is_empty());
        let atoms_path = data_dir().join("atoms.json");
        let bytes = fs::read(&atoms_path).expect("atoms.json should exist after remember");
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            text.contains(&atom_id),
            "atoms.json should contain remembered atom id"
        );

        let answer = <Component as engine::Guest>::ask(WitQuestion {
            world_key: "tenant:default/w1".into(),
            text: token,
            filter: None,
        });
        assert_eq!(answer.world_key, "tenant:default/w1");
        assert!(!answer.text.is_empty());
    }

    #[test]
    fn observe_start_poll_stop_receives_atoms_in_order() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        std::env::set_var("DWBASE_OBSERVE_QUEUE_CAPACITY", "100");
        std::env::set_var("DWBASE_OBSERVE_DROP_POLICY", "drop_oldest");

        let handle = <Component as engine::Guest>::observe_start(WitAtomFilter {
            world_key: Some("tenant:default/obs".into()),
            kinds: vec![],
            labels: vec![],
            flag_filter: vec![],
            since: None,
            until: None,
            limit: None,
        });
        assert!(handle > 0);

        let a1 = <Component as engine::Guest>::remember(WitNewAtom {
            world_key: "tenant:default/obs".into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:00Z".into(),
            importance: 0.4,
            payload_json: r#"{"n":1}"#.into(),
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });
        let a2 = <Component as engine::Guest>::remember(WitNewAtom {
            world_key: "tenant:default/obs".into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:01Z".into(),
            importance: 0.4,
            payload_json: r#"{"n":2}"#.into(),
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });

        let mut got = <Component as engine::Guest>::observe_poll(handle, 10);
        if got.len() < 2 {
            got.extend(<Component as engine::Guest>::observe_poll(handle, 10));
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].id, a1);
        assert_eq!(got[1].id, a2);

        <Component as engine::Guest>::observe_stop(handle);
    }

    #[test]
    fn observe_receives_remotely_ingested_atoms() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();

        let handle = <Component as engine::Guest>::observe_start(WitAtomFilter {
            world_key: Some("tenant:default/obs-remote".into()),
            kinds: vec![],
            labels: vec![],
            flag_filter: vec![],
            since: None,
            until: None,
            limit: None,
        });
        assert!(handle > 0);

        let atom = Atom::builder(
            AtomId::new("remote-1"),
            WorldKey::new("tenant:default/obs-remote"),
            WorkerKey::new("peer"),
            AtomKind::Observation,
            Timestamp::new("2024-01-01T00:00:00Z"),
            Importance::clamped(0.5),
            r#"{"remote":true}"#,
        )
        .build();

        let engine = init_engine();
        let guard = engine.lock().unwrap();
        futures::executor::block_on(guard.ingest_remote_atoms(vec![atom])).unwrap();
        drop(guard);

        let got = <Component as engine::Guest>::observe_poll(handle, 10);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, "remote-1");

        <Component as engine::Guest>::observe_stop(handle);
    }

    #[test]
    fn presence_discovery_with_mock() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        let client = Arc::new(MockNats::default()) as Arc<dyn NatsClient>;
        let table_a = PeerTable::default();
        let table_b = PeerTable::default();
        let engine_a = init_engine();
        let engine_b = init_engine();
        let hello_a = hello_from_engine(&engine_a.lock().unwrap());
        let mut hello_b = hello_from_engine(&engine_b.lock().unwrap());
        hello_b.node_id = "other-node".into();
        let ttl = std::time::Duration::from_millis(100);
        start_presence_loop(client.clone(), hello_a, table_a.clone(), ttl);
        start_presence_loop(client, hello_b, table_b.clone(), ttl);
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(
            table_a.peers().iter().any(|p| p.node_id == "other-node"),
            "table A should see node B"
        );
        assert!(
            table_b.peers().iter().any(|p| p.node_id == node_id()),
            "table B should see node A"
        );
    }

    #[test]
    fn selective_replication_only_ingests_subscribed_worlds() {
        let _lock = env_lock();
        reset_security_env();
        let bus = Arc::new(MockBus::default()) as Arc<dyn dwbase_swarm_nats::swarm::SwarmBus>;

        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let dir_c = TempDir::new().unwrap();

        let engine_a = DWBaseEngine::new(
            FsStorage::new(dir_a.path().to_path_buf()).unwrap(),
            NoVector,
            LocalStream::new(),
            LocalGatekeeper::new(Capabilities::default(), TrustStore::default()),
            DummyEmbedder::new(),
        );
        let engine_b = DWBaseEngine::new(
            FsStorage::new(dir_b.path().to_path_buf()).unwrap(),
            NoVector,
            LocalStream::new(),
            LocalGatekeeper::new(Capabilities::default(), TrustStore::default()),
            DummyEmbedder::new(),
        );
        let engine_c = DWBaseEngine::new(
            FsStorage::new(dir_c.path().to_path_buf()).unwrap(),
            NoVector,
            LocalStream::new(),
            LocalGatekeeper::new(Capabilities::default(), TrustStore::default()),
            DummyEmbedder::new(),
        );

        let repl_a = replicator_with_bus(bus.clone(), "node-a", vec![]).unwrap();
        let repl_b =
            replicator_with_bus(bus.clone(), "node-b", vec!["tenant:default/world-x".into()])
                .unwrap();
        let repl_c = replicator_with_bus(bus.clone(), "node-c", vec![]).unwrap();

        repl_b.announce().unwrap();

        // Node A writes an atom to world-x and replicates it.
        let atom_id = futures::executor::block_on(engine_a.remember(NewAtom {
            world: WorldKey::new("tenant:default/world-x"),
            worker: WorkerKey::new("w"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new("2024-01-01T00:00:00Z"),
            importance: Importance::clamped(0.5),
            payload_json: r#"{"note":"hello"}"#.into(),
            vector: None,
            flags: vec![],
            labels: vec![],
            links: vec![],
        }))
        .unwrap();

        let mut atoms = engine_a.get_atoms(&[atom_id.clone()]).unwrap();
        let atom = atoms.pop().expect("atom exists");
        repl_a.replicate_new_atom(atom).unwrap();

        // Node B should receive and ingest; Node C should not.
        repl_b.poll_inbox().unwrap();
        repl_c.poll_inbox().unwrap();

        while let Some((_from, batch)) = repl_b.poll_atom_batch() {
            futures::executor::block_on(engine_b.ingest_remote_atoms(batch.atoms)).unwrap();
        }
        while let Some((_from, batch)) = repl_c.poll_atom_batch() {
            futures::executor::block_on(engine_c.ingest_remote_atoms(batch.atoms)).unwrap();
        }

        let ans_b = futures::executor::block_on(engine_b.ask(Question {
            world: WorldKey::new("tenant:default/world-x"),
            text: "hello?".into(),
            filter: None,
        }))
        .unwrap();
        assert!(
            ans_b.supporting_atoms.iter().any(|a| a.id() == &atom_id),
            "node-b should contain replicated atom"
        );

        let ans_c = futures::executor::block_on(engine_c.ask(Question {
            world: WorldKey::new("tenant:default/world-x"),
            text: "hello?".into(),
            filter: None,
        }))
        .unwrap();
        assert!(
            ans_c.supporting_atoms.iter().all(|a| a.id() != &atom_id),
            "node-c should not contain replicated atom"
        );
    }

    #[test]
    fn attempt_to_write_foreign_world_is_denied() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        std::env::set_var("DWBASE_TENANT_ID", "acme");

        let atom_id = <Component as engine::Guest>::remember(WitNewAtom {
            world_key: "tenant:other/w1".into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:00Z".into(),
            importance: 0.5,
            payload_json: r#"{"note":"nope"}"#.into(),
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });
        assert!(atom_id.is_empty(), "write should be denied");
    }

    #[test]
    fn oversize_payload_is_rejected() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        std::env::set_var("DWBASE_MAX_PAYLOAD_BYTES", "10");

        let atom_id = <Component as engine::Guest>::remember(WitNewAtom {
            world_key: "tenant:default/w1".into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:00Z".into(),
            importance: 0.1,
            payload_json: r#"{"note":"this is definitely >10 bytes"}"#.into(),
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });
        assert!(atom_id.is_empty(), "oversize payload should be rejected");
    }

    #[test]
    fn health_ready_by_default() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        std::env::set_var("DWBASE_MAX_DISK_MB", "100");
        let h = <Component as engine::Guest>::health();
        assert!(h.storage_ok);
        assert!(h.index_ok);
        assert_eq!(h.status, "ready");
        assert_eq!(h.disk_pressure, "ok");
    }

    #[test]
    fn health_capacity_unknown_stays_ready_when_ok() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        std::env::remove_var("DWBASE_MAX_DISK_MB");
        std::env::set_var("DWBASE_HEALTH_DISABLE_FS_STATS", "1");

        let h = <Component as engine::Guest>::health();
        assert!(h.storage_ok);
        assert!(h.index_ok);
        assert_eq!(h.status, "ready");
        assert_eq!(h.disk_pressure, "unknown");
    }

    #[test]
    fn health_degrades_when_configured_capacity_exceeded() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        std::env::set_var("DWBASE_HEALTH_DISABLE_FS_STATS", "1");
        std::env::set_var("DWBASE_MAX_DISK_MB", "1");

        let dir = data_dir();
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("pressure.bin"), vec![0u8; 2 * 1024 * 1024]).unwrap();

        let h = <Component as engine::Guest>::health();
        assert_eq!(h.disk_pressure, "degraded");
        assert_eq!(h.status, "degraded");
    }

    #[test]
    fn metrics_snapshot_renders_prometheus() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        let _ = install_metrics_recorder();
        dwbase_metrics::record_observe_dropped(1);

        let snap = <Component as engine::Guest>::metrics_snapshot().expect("metrics");
        assert_eq!(snap.format, "prometheus");
        println!("metrics snapshot text:\n{}", snap.prometheus);
        assert!(
            !snap.prometheus.is_empty(),
            "prometheus text should be present"
        );
    }

    fn atom_for_world(id: &str, world: &str, ts: &str) -> Atom {
        Atom::builder(
            AtomId::new(id),
            WorldKey::new(world),
            WorkerKey::new("w"),
            AtomKind::Observation,
            Timestamp::new(ts),
            Importance::clamped(0.5),
            r#"{"x":true}"#,
        )
        .build()
    }

    #[test]
    fn observe_drop_oldest_is_deterministic_fifo() {
        let _lock = env_lock();
        reset_security_env();
        std::env::set_var("DWBASE_OBSERVE_QUEUE_CAPACITY", "2");
        std::env::set_var("DWBASE_OBSERVE_DROP_POLICY", "drop_oldest");

        let stream = LocalStream::new();
        let world = WorldKey::new("tenant:default/obs-drop-oldest");
        let handle = stream
            .subscribe(
                &world,
                AtomFilter {
                    world: Some(world.clone()),
                    kinds: vec![],
                    labels: vec![],
                    flags: vec![],
                    since: None,
                    until: None,
                    limit: None,
                },
            )
            .unwrap();

        assert!(stream.push_to_handle(
            handle,
            atom_for_world("a1", &world.0, "2024-01-01T00:00:00Z")
        ));
        assert!(stream.push_to_handle(
            handle,
            atom_for_world("a2", &world.0, "2024-01-01T00:00:01Z")
        ));
        assert!(stream.push_to_handle(
            handle,
            atom_for_world("a3", &world.0, "2024-01-01T00:00:02Z")
        ));

        let got = stream.poll_n(handle, 10);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].id().0, "a2");
        assert_eq!(got[1].id().0, "a3");

        let (queued, dropped, _last) = stream.stats_for_handle(handle).unwrap();
        assert_eq!(queued, 0);
        assert_eq!(dropped, 1);
    }

    #[test]
    fn observe_drop_newest_is_deterministic_fifo() {
        let _lock = env_lock();
        reset_security_env();
        std::env::set_var("DWBASE_OBSERVE_QUEUE_CAPACITY", "2");
        std::env::set_var("DWBASE_OBSERVE_DROP_POLICY", "drop_newest");

        let stream = LocalStream::new();
        let world = WorldKey::new("tenant:default/obs-drop-newest");
        let handle = stream
            .subscribe(
                &world,
                AtomFilter {
                    world: Some(world.clone()),
                    kinds: vec![],
                    labels: vec![],
                    flags: vec![],
                    since: None,
                    until: None,
                    limit: None,
                },
            )
            .unwrap();

        assert!(stream.push_to_handle(
            handle,
            atom_for_world("a1", &world.0, "2024-01-01T00:00:00Z")
        ));
        assert!(stream.push_to_handle(
            handle,
            atom_for_world("a2", &world.0, "2024-01-01T00:00:01Z")
        ));
        assert!(stream.push_to_handle(
            handle,
            atom_for_world("a3", &world.0, "2024-01-01T00:00:02Z")
        ));

        let got = stream.poll_n(handle, 10);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].id().0, "a1");
        assert_eq!(got[1].id().0, "a2");

        let (queued, dropped, _last) = stream.stats_for_handle(handle).unwrap();
        assert_eq!(queued, 0);
        assert_eq!(dropped, 1);
    }

    #[test]
    fn observe_durable_cursor_catches_up_after_restart_like_gap() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        std::env::set_var("DWBASE_OBSERVE_DURABLE", "1");

        let world = "tenant:default/obs-durable";
        let handle1 = <Component as engine::Guest>::observe_start(WitAtomFilter {
            world_key: Some(world.into()),
            kinds: vec![],
            labels: vec![],
            flag_filter: vec![],
            since: None,
            until: None,
            limit: None,
        });
        assert!(handle1 > 0);

        let _a1 = <Component as engine::Guest>::remember(WitNewAtom {
            world_key: world.into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:00Z".into(),
            importance: 0.4,
            payload_json: r#"{"n":1}"#.into(),
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });
        let _a2 = <Component as engine::Guest>::remember(WitNewAtom {
            world_key: world.into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:01Z".into(),
            importance: 0.4,
            payload_json: r#"{"n":2}"#.into(),
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });

        let got = <Component as engine::Guest>::observe_poll(handle1, 10);
        assert_eq!(got.len(), 2);
        <Component as engine::Guest>::observe_stop(handle1);

        // Atom remembered while no observe subscription is active.
        let a3 = <Component as engine::Guest>::remember(WitNewAtom {
            world_key: world.into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:02Z".into(),
            importance: 0.4,
            payload_json: r#"{"n":3}"#.into(),
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });
        assert!(!a3.is_empty());

        let handle2 = <Component as engine::Guest>::observe_start(WitAtomFilter {
            world_key: Some(world.into()),
            kinds: vec![],
            labels: vec![],
            flag_filter: vec![],
            since: None,
            until: None,
            limit: None,
        });
        assert!(handle2 > 0);

        let got = <Component as engine::Guest>::observe_poll(handle2, 10);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, a3);
        <Component as engine::Guest>::observe_stop(handle2);
    }

    #[test]
    fn remember_v2_capability_denied_is_structured() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();

        let res = <Component as engine::Guest>::remember_v2(WitNewAtom {
            world_key: "tenant:other/w1".into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:00Z".into(),
            importance: 0.5,
            payload_json: r#"{"note":"nope"}"#.into(),
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });

        let err = res.expect_err("should deny cross-tenant write");
        assert_eq!(err.code, "capability_denied");
    }

    #[test]
    fn remember_v2_oversize_payload_is_structured() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();
        std::env::set_var("DWBASE_MAX_PAYLOAD_BYTES", "10");

        let payload = "x".repeat(20);
        let res = <Component as engine::Guest>::remember_v2(WitNewAtom {
            world_key: "tenant:default/w1".into(),
            worker: "worker-1".into(),
            kind: WitAtomKind::Observation,
            timestamp: "2024-01-01T00:00:00Z".into(),
            importance: 0.1,
            payload_json: payload,
            vector: None,
            flags_list: vec![],
            labels: vec![],
            links: vec![],
        });

        let err = res.expect_err("should reject oversize payload");
        assert_eq!(err.code, "payload_too_large");
    }

    #[test]
    fn observe_v2_invalid_handle_is_structured() {
        let _lock = env_lock();
        reset_security_env();
        set_shared_data_dir();

        let res = <Component as engine::Guest>::observe_poll_v2(91254, 1);
        let err = res.expect_err("invalid handle should error");
        assert_eq!(err.code, "invalid_handle");

        let res = <Component as engine::Guest>::observe_stop_v2(91254);
        let err = res.expect_err("invalid handle should error");
        assert_eq!(err.code, "invalid_handle");
    }
}
