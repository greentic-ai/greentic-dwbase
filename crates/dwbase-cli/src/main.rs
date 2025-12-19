//! Command-line interface for interacting with a DWBase node over HTTP.

use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use dwbase_core::{Atom, Timestamp, WorldKey};
use dwbase_embedder_dummy::DummyEmbedder;
use dwbase_engine::{Answer, AtomFilter, DWBaseEngine, IndexMetadata, NewAtom, Question};
use dwbase_security::{Capabilities, LocalGatekeeper, TrustStore};
use dwbase_storage_sled::{EnvKeyProvider, SledConfig, SledStorage};
use dwbase_stream_local::LocalStreamEngine;
use dwbase_vector_hnsw::HnswVectorEngine;
use humantime::parse_duration;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tar::Builder;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use toml::{value::Table, Value};
use walkdir::WalkDir;
use zstd::stream::{Decoder, Encoder};

#[derive(Parser)]
#[command(author, version, about = "DWBase CLI", long_about = None)]
struct Cli {
    /// Base URL of the running dwbase-node (e.g., http://127.0.0.1:8080)
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    api: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List known worlds observed by the node
    ListWorlds,
    /// Ask a question in a world; prompts for text when omitted
    Ask {
        /// World key to query
        world: String,
        /// Question text; if omitted, an interactive prompt is shown
        #[arg(short, long)]
        question: Option<String>,
    },
    /// Replay atoms from a world
    Replay {
        /// World key to replay
        world: String,
        /// Optional limit on returned atoms
        #[arg(short, long)]
        limit: Option<usize>,
        /// Optional ISO-8601 timestamp to filter since
        #[arg(long)]
        since: Option<String>,
    },
    /// Inspect a single atom by id
    Inspect {
        /// Atom id to fetch
        id: String,
    },
    /// Deploy helper for local/devnet nodes
    Deploy {
        #[command(subcommand)]
        command: DeployCommands,
    },
    /// Storage utilities
    Storage {
        #[command(subcommand)]
        command: StorageCommands,
    },
    /// Index utilities
    Index {
        #[command(subcommand)]
        command: IndexCommands,
    },
    /// World management and policy
    World {
        #[command(subcommand)]
        command: WorldCommands,
    },
    /// Backup and restore data directories
    Backup {
        #[command(subcommand)]
        command: BackupCommands,
    },
    /// Extended testing and soak harnesses
    Test {
        #[command(subcommand)]
        command: TestCommands,
    },
}

#[derive(Subcommand)]
enum DeployCommands {
    /// Run a single node locally
    Local {
        /// Path to config; defaults to deploy/local.toml
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Spawn N local nodes with distinct ports/data dirs
    Devnet {
        /// Number of nodes to spawn
        #[arg(long, default_value = "3")]
        nodes: usize,
        /// Starting port number
        #[arg(long, default_value = "7000")]
        base_port: u16,
    },
    /// Stop running devnet nodes (reads .dwbase/devnet/state.json)
    Stop,
    /// Remove devnet data directories (requires --yes)
    Clean {
        /// Proceed without prompt
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum WorldCommands {
    /// Create a world (adds world meta)
    Create {
        /// World key to create
        world: String,
        /// Optional description
        #[arg(long)]
        description: Option<String>,
        /// Optional labels to attach to the world meta (comma-separated)
        #[arg(long)]
        labels: Option<String>,
    },
    /// Archive a world (excluded from default listings)
    Archive {
        /// World key to archive
        world: String,
    },
    /// Resume an archived world
    Resume {
        /// World key to resume
        world: String,
    },
    /// Set policy atoms for a world
    Policy {
        /// Target world for policy
        world: String,
        /// Retention days (policy:retention_days)
        #[arg(long)]
        retention_days: Option<u64>,
        /// Minimum importance threshold
        #[arg(long)]
        min_importance: Option<f32>,
        /// Replication allow prefixes (comma-separated)
        #[arg(long)]
        replication_allow: Option<String>,
        /// Replication deny prefixes (comma-separated)
        #[arg(long)]
        replication_deny: Option<String>,
    },
}

#[derive(Subcommand)]
enum StorageCommands {
    /// Validate sled storage logs and truncate corrupt tails
    Check {
        /// Path to sled database directory
        #[arg(long, default_value = ".dwbase/local/data")]
        path: PathBuf,
    },
    /// Rebuild sled secondary indexes (atom id lookup)
    Reindex {
        /// Path to sled database directory
        #[arg(long, default_value = ".dwbase/local/data")]
        path: PathBuf,
    },
}

#[derive(Subcommand)]
enum IndexCommands {
    /// Show index metadata/status from a running node
    Status,
}

#[derive(Subcommand)]
enum BackupCommands {
    /// Create a compressed snapshot of the data directory
    Create {
        /// Path to the data directory to snapshot
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        /// Output snapshot file (.tzst)
        #[arg(long)]
        out: PathBuf,
    },
    /// Restore a snapshot into a data directory
    Restore {
        /// Input snapshot file (.tzst)
        #[arg(long, value_name = "SNAPSHOT")]
        input: PathBuf,
        /// Target data directory to restore into
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        /// Overwrite an existing non-empty directory
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum TestCommands {
    /// Run a mixed-workload soak test with failure injection
    Soak {
        /// Duration (e.g., 5m, 30m)
        #[arg(long, default_value = "5m")]
        duration: String,
        /// Optional data directory to reuse; defaults to temp dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Report output path (JSON)
        #[arg(long, default_value = "soak-report.json")]
        report: PathBuf,
    },
}

#[derive(Debug, Serialize)]
struct ReplayRequest {
    world: WorldKey,
    #[serde(default)]
    filter: AtomFilter,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotMeta {
    format_version: u32,
    created_at: String,
    index_metadata: Vec<IndexMetadata>,
}

#[derive(Debug, Serialize)]
struct SoakReport {
    duration_secs: u64,
    remembers: usize,
    asks: usize,
    remember_p50_ms: f64,
    remember_p95_ms: f64,
    ask_p50_ms: f64,
    ask_p95_ms: f64,
    disk_bytes_start: u64,
    disk_bytes_end: u64,
    memory_bytes_end: u64,
    restart_injected: bool,
    corruption_injected: bool,
    partition_simulated_secs: u64,
    errors: Vec<String>,
}

async fn list_worlds(client: &Client, api: &str) -> Result<()> {
    let resp: Vec<WorldKey> = client
        .get(format!("{api}/worlds"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}

fn parse_csv(input: Option<String>) -> Vec<String> {
    input
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

async fn manage_world(
    client: &Client,
    api: &str,
    action: &str,
    world: String,
    description: Option<String>,
    labels: Vec<String>,
) -> Result<()> {
    let req = serde_json::json!({
        "action": action,
        "world": world,
        "description": description,
        "labels": labels,
    });
    let res = client
        .post(format!("{api}/worlds/manage"))
        .json(&req)
        .send()
        .await?
        .error_for_status()?;
    println!("World {action} ok (status {})", res.status());
    Ok(())
}

async fn set_policy(
    client: &Client,
    api: &str,
    world: String,
    retention_days: Option<u64>,
    min_importance: Option<f32>,
    replication_allow: Vec<String>,
    replication_deny: Vec<String>,
) -> Result<()> {
    if retention_days.is_none()
        && min_importance.is_none()
        && replication_allow.is_empty()
        && replication_deny.is_empty()
    {
        anyhow::bail!("set at least one policy field");
    }
    let req = serde_json::json!({
        "world": world,
        "retention_days": retention_days,
        "min_importance": min_importance,
        "replication_allow": replication_allow,
        "replication_deny": replication_deny,
    });
    let res = client
        .post(format!("{api}/worlds/policy"))
        .json(&req)
        .send()
        .await?
        .error_for_status()?;
    let id: serde_json::Value = res.json().await?;
    println!("Policy atom id: {}", id["0"].as_str().unwrap_or_default());
    Ok(())
}

async fn ask(client: &Client, api: &str, world: String, question: Option<String>) -> Result<()> {
    let text = match question {
        Some(q) if !q.trim().is_empty() => q,
        _ => prompt("Question> ")?,
    };
    let body = Question {
        world: WorldKey::new(world),
        text,
        filter: None,
    };
    let answer: Answer = client
        .post(format!("{api}/ask"))
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    println!("{}", serde_json::to_string_pretty(&answer)?);
    Ok(())
}

async fn replay(
    client: &Client,
    api: &str,
    world: String,
    limit: Option<usize>,
    since: Option<String>,
) -> Result<()> {
    let filter = AtomFilter {
        limit,
        since: since.map(Timestamp::new),
        ..Default::default()
    };
    let body = ReplayRequest {
        world: WorldKey::new(world),
        filter,
    };
    let atoms: Vec<Atom> = client
        .post(format!("{api}/replay"))
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    println!("{}", serde_json::to_string_pretty(&atoms)?);
    Ok(())
}

async fn inspect(client: &Client, api: &str, id: String) -> Result<()> {
    let atom: Option<Atom> = client
        .get(format!("{api}/atoms/{id}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    match atom {
        Some(a) => println!("{}", serde_json::to_string_pretty(&a)?),
        None => println!("Atom {id} not found"),
    }
    Ok(())
}

fn prompt(label: &str) -> Result<String> {
    print!("{label}");
    io::stdout().flush().ok();
    let mut buf = String::new();
    io::stdin().read_line(&mut buf)?;
    let s = buf.trim().to_owned();
    if s.is_empty() {
        Err(anyhow!("input cannot be empty"))
    } else {
        Ok(s)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct NodeState {
    id: String,
    port: u16,
    pid: u32,
    data_dir: PathBuf,
    log_path: PathBuf,
    config_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct DevnetState {
    nodes: Vec<NodeState>,
}

#[derive(Debug, Deserialize)]
struct IndexStatus {
    world: String,
    version: u64,
    embedder_version: String,
    last_rebuilt: String,
    ready: bool,
    #[serde(default)]
    rebuilding: bool,
    #[serde(default)]
    progress: f32,
}

#[derive(Clone, Default)]
struct PidTracker {
    inner: Arc<Mutex<Vec<u32>>>,
    armed: Arc<AtomicBool>,
}

impl PidTracker {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
            armed: Arc::new(AtomicBool::new(true)),
        }
    }

    fn push(&self, pid: u32) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.push(pid);
        }
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
    }

    fn kill_all(&self, reason: &str) {
        let pids = self
            .inner
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        if pids.is_empty() {
            return;
        }
        for pid in pids {
            let _ = kill_pid(pid);
        }
        eprintln!("{reason}");
    }
}

impl Drop for PidTracker {
    fn drop(&mut self) {
        if self.armed.load(Ordering::SeqCst) {
            self.kill_all("cleaned up devnet child processes");
        }
    }
}

#[derive(Debug, Deserialize)]
struct ConfigPreview {
    node: Option<NodePreview>,
}

#[derive(Debug, Deserialize)]
struct NodePreview {
    listen: Option<String>,
    data_dir: Option<PathBuf>,
}

fn state_path() -> PathBuf {
    devnet_root().join("state.json")
}

fn devnet_root() -> PathBuf {
    base_dir().join("devnet")
}

fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("create dir {}", path.display()))
}

fn base_dir() -> PathBuf {
    env::var("DWBASE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(".dwbase"))
}

fn default_devnet_config(listen: u16, data_dir: &Path) -> Value {
    let mut security = Table::new();
    security.insert("read_worlds".into(), Value::Array(vec![]));
    security.insert("write_worlds".into(), Value::Array(vec![]));
    security.insert("importance_cap".into(), Value::Float(1.0));
    security.insert("remember_per_sec".into(), Value::Float(10.0));
    security.insert("ask_per_min".into(), Value::Float(60.0));

    let mut health = Table::new();
    health.insert("disk_usage_degraded".into(), Value::Float(0.9));

    let mut node = Table::new();
    node.insert(
        "listen".into(),
        Value::String(format!("127.0.0.1:{listen}")),
    );
    node.insert(
        "data_dir".into(),
        Value::String(data_dir.to_string_lossy().into_owned()),
    );
    node.insert("embedder".into(), Value::String("dummy".into()));
    node.insert("security".into(), Value::Table(security));
    node.insert("health".into(), Value::Table(health));

    let mut root = Table::new();
    root.insert("node".into(), Value::Table(node));
    Value::Table(root)
}

fn write_config(path: &Path, listen: u16, data_dir: &Path) -> Result<()> {
    let template_path = PathBuf::from("deploy/devnet.toml");
    let mut doc: Value = if template_path.exists() {
        let raw = fs::read_to_string(&template_path)
            .with_context(|| format!("read template {}", template_path.display()))?;
        toml::from_str(&raw)
            .with_context(|| format!("parse template {}", template_path.display()))?
    } else {
        default_devnet_config(listen, data_dir)
    };

    let table = doc
        .as_table_mut()
        .ok_or_else(|| anyhow!("config root must be a table"))?;
    let node = table
        .entry("node")
        .or_insert_with(|| Value::Table(Table::new()))
        .as_table_mut()
        .ok_or_else(|| anyhow!("node config must be a table"))?;

    node.insert(
        "listen".into(),
        Value::String(format!("127.0.0.1:{listen}")),
    );
    node.insert(
        "data_dir".into(),
        Value::String(data_dir.to_string_lossy().into_owned()),
    );
    node.entry("embedder")
        .or_insert_with(|| Value::String("dummy".into()));

    let rendered = toml::to_string_pretty(&doc)?;
    fs::write(path, rendered).with_context(|| format!("write config {}", path.display()))
}

fn spawn_node(config: &Path, log_path: &Path) -> Result<u32> {
    if let Some(parent) = log_path.parent() {
        ensure_dir(parent)?;
    }
    let log = fs::File::create(log_path)?;
    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("-p")
        .arg("dwbase-node")
        .arg("--")
        .arg("--config")
        .arg(config)
        .env("DWBASE_DATA_DIR", base_dir().to_string_lossy().into_owned());

    if let Ok(val) = env::var("DWBASE_MAX_RAM_MB") {
        if !val.is_empty() {
            cmd.env("DWBASE_MAX_RAM_MB", val);
        }
    }
    if let Ok(val) = env::var("DWBASE_MAX_DISK_MB") {
        if !val.is_empty() {
            cmd.env("DWBASE_MAX_DISK_MB", val);
        }
    }

    let child = cmd
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()
        .context("spawn dwbase-node")?;
    Ok(child.id())
}

fn read_config_preview(path: &Path) -> Result<Option<NodePreview>> {
    let raw = fs::read_to_string(path)?;
    let cfg: ConfigPreview =
        toml::from_str(&raw).with_context(|| format!("parse config preview {}", path.display()))?;
    Ok(cfg.node)
}

fn load_state() -> Result<Option<DevnetState>> {
    let path = state_path();
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path)?;
    let st: DevnetState = serde_json::from_str(&text)?;
    Ok(Some(st))
}

fn save_state(state: &DevnetState) -> Result<()> {
    let path = state_path();
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let text = serde_json::to_string_pretty(state)?;
    fs::write(&path, text)?;
    Ok(())
}

fn clear_state_file() -> Result<()> {
    let path = state_path();
    if path.exists() {
        fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
    }
    Ok(())
}

fn load_state_pruned() -> Result<Option<DevnetState>> {
    let Some(mut state) = load_state()? else {
        return Ok(None);
    };
    let before = state.nodes.len();
    state.nodes.retain(|n| pid_running(n.pid));
    if state.nodes.is_empty() {
        let _ = clear_state_file();
        return Ok(None);
    }
    if state.nodes.len() != before {
        save_state(&state)?;
    }
    Ok(Some(state))
}

fn install_ctrlc_handler(tracker: PidTracker) -> Result<()> {
    ctrlc::set_handler(move || {
        tracker.kill_all("received Ctrl+C; stopping devnet children");
        std::process::exit(130);
    })
    .context("install Ctrl+C handler")
}

fn storage_check(path: PathBuf) -> Result<()> {
    let mut cfg = SledConfig::new(path);
    cfg.flush_on_write = false;
    let _storage = SledStorage::open(cfg, Arc::new(EnvKeyProvider))?;
    println!("storage check OK");
    Ok(())
}

fn storage_reindex(path: PathBuf) -> Result<()> {
    let mut cfg = SledConfig::new(path);
    cfg.flush_on_write = false;
    let storage = SledStorage::open(cfg, Arc::new(EnvKeyProvider))?;
    let rebuilt = storage.rebuild_index()?;
    println!("rebuilt atom index entries: {rebuilt}");
    Ok(())
}

async fn index_status(api: &str) -> Result<()> {
    let client = Client::new();
    let statuses: Vec<IndexStatus> = client
        .get(format!("{api}/index/status"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    println!(
        "{:<20} {:<8} {:<15} {:<25} {:<6} {:<10} {:<8}",
        "World", "Version", "Embedder", "Last Rebuilt", "Ready", "Rebuilding", "Progress"
    );
    for st in statuses {
        println!(
            "{:<20} {:<8} {:<15} {:<25} {:<6} {:<10} {:<8.0}",
            st.world,
            st.version,
            st.embedder_version,
            st.last_rebuilt,
            st.ready,
            st.rebuilding,
            st.progress * 100.0
        );
    }
    Ok(())
}

async fn fetch_index_metadata(api: &str) -> Result<Vec<IndexMetadata>> {
    let client = Client::new();
    let resp = client
        .get(format!("{api}/index/status"))
        .send()
        .await?
        .error_for_status()?;
    Ok(resp.json().await?)
}

fn create_snapshot(data_dir: &Path, out: &Path, meta: &SnapshotMeta) -> Result<()> {
    if !data_dir.exists() {
        return Err(anyhow!("data dir {} not found", data_dir.display()));
    }
    if out.exists() {
        return Err(anyhow!(
            "output snapshot {} already exists; delete or choose another path",
            out.display()
        ));
    }
    let file = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let encoder = Encoder::new(file, 0)?;
    let mut builder = Builder::new(encoder);
    builder
        .append_dir_all("data", data_dir)
        .with_context(|| format!("add {}", data_dir.display()))?;
    let meta_bytes = serde_json::to_vec_pretty(meta)?;
    let mut header = tar::Header::new_gnu();
    header.set_path("metadata.json")?;
    header.set_size(meta_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder.append(&header, meta_bytes.as_slice())?;
    let encoder = builder.into_inner()?;
    encoder.finish()?;
    Ok(())
}

fn restore_snapshot(input: &Path, data_dir: &Path, force: bool) -> Result<SnapshotMeta> {
    if !input.exists() {
        return Err(anyhow!("snapshot {} not found", input.display()));
    }
    let staging = data_dir.with_extension("restore_tmp");
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .with_context(|| format!("remove existing staging {}", staging.display()))?;
    }
    fs::create_dir_all(&staging)?;

    let file = fs::File::open(input).with_context(|| format!("open {}", input.display()))?;
    let decoder = Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);
    archive
        .unpack(&staging)
        .with_context(|| "unpack snapshot".to_string())?;

    let meta_path = staging.join("metadata.json");
    let meta: SnapshotMeta = if meta_path.exists() {
        let bytes = fs::read(&meta_path)?;
        serde_json::from_slice(&bytes)?
    } else {
        SnapshotMeta {
            format_version: 1,
            created_at: "unknown".into(),
            index_metadata: Vec::new(),
        }
    };

    if data_dir.exists() && data_dir.read_dir()?.next().is_some() && !force {
        return Err(anyhow!(
            "data dir {} is not empty; pass --force to overwrite",
            data_dir.display()
        ));
    }
    if data_dir.exists() {
        fs::remove_dir_all(data_dir).with_context(|| format!("remove {}", data_dir.display()))?;
    }
    let restored_data = staging.join("data");
    if !restored_data.exists() {
        return Err(anyhow!("snapshot missing data/ payload"));
    }
    if let Some(parent) = data_dir.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::rename(&restored_data, data_dir)
        .with_context(|| format!("move restored data into {}", data_dir.display()))?;
    let audit_meta = data_dir.join(".snapshot_metadata.json");
    let _ = fs::write(&audit_meta, serde_json::to_vec_pretty(&meta)?);
    let _ = fs::remove_dir_all(&staging);
    Ok(meta)
}

async fn backup_create(api: &str, data_dir: PathBuf, out: PathBuf) -> Result<()> {
    let timestamp = OffsetDateTime::now_utc().format(&Rfc3339)?;
    let mut meta = SnapshotMeta {
        format_version: 1,
        created_at: timestamp,
        index_metadata: Vec::new(),
    };
    match fetch_index_metadata(api).await {
        Ok(m) => meta.index_metadata = m,
        Err(e) => {
            eprintln!("warning: could not fetch index metadata: {e}");
        }
    };
    create_snapshot(&data_dir, &out, &meta)?;
    println!(
        "snapshot created at {} from {} ({} index records)",
        out.display(),
        data_dir.display(),
        meta.index_metadata.len()
    );
    Ok(())
}

fn backup_restore(input: PathBuf, data_dir: PathBuf, force: bool) -> Result<()> {
    let meta = restore_snapshot(&input, &data_dir, force)?;
    println!(
        "restored snapshot into {} (created {}, {} index records)",
        data_dir.display(),
        meta.created_at,
        meta.index_metadata.len()
    );
    Ok(())
}

fn dir_size_bytes(path: &Path) -> u64 {
    let mut total = 0u64;
    for entry in WalkDir::new(path).into_iter().flatten() {
        if let Ok(md) = entry.metadata() {
            if md.is_file() {
                total = total.saturating_add(md.len());
            }
        }
    }
    total
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((pct / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx]
}

fn corrupt_tail(data_dir: &Path, world: &WorldKey) -> Result<()> {
    let db = sled::open(data_dir)?;
    let prefix = format!("world/{}/log/", world.0);
    if let Some(Ok((key, _))) = db.scan_prefix(prefix.as_bytes()).next_back() {
        db.insert(key, b"badframe".to_vec())?;
        db.flush()?;
    }
    Ok(())
}

fn rss_bytes() -> u64 {
    0
}

async fn soak_test(duration: Duration, data_dir: Option<PathBuf>, report: PathBuf) -> Result<()> {
    let tmp_dir;
    let base_dir = if let Some(dir) = data_dir {
        fs::create_dir_all(&dir)?;
        dir
    } else {
        tmp_dir = tempfile::tempdir()?;
        tmp_dir.path().to_path_buf()
    };
    let world = WorldKey::new("soak");

    let mut sled_cfg = SledConfig::new(&base_dir);
    sled_cfg.flush_on_write = true;
    let mut engine = {
        let storage = SledStorage::open(sled_cfg.clone(), Arc::new(EnvKeyProvider))?;
        let vector = HnswVectorEngine::new();
        let stream = LocalStreamEngine::new();
        let gatekeeper = LocalGatekeeper::new(Capabilities::default(), TrustStore::default());
        let embedder = DummyEmbedder::new();
        DWBaseEngine::new(storage, vector, stream, gatekeeper, embedder)
    };

    let start_disk = dir_size_bytes(&base_dir);
    let start = tokio::time::Instant::now();
    let end_at = start + duration;

    let mut remember_lat = Vec::new();
    let mut ask_lat = Vec::new();
    let mut errors = Vec::new();
    let mut injected_restart = false;
    let mut injected_corrupt = false;
    let mut partition_pause = Duration::from_secs(0);

    while tokio::time::Instant::now() < end_at {
        let atom_id = format!("soak-{}", uuid::Uuid::new_v4());
        let new_atom = NewAtom {
            world: world.clone(),
            worker: dwbase_core::WorkerKey::new("soak-worker"),
            kind: dwbase_core::AtomKind::Observation,
            timestamp: dwbase_core::Timestamp::new(""),
            importance: dwbase_core::Importance::clamped(0.5),
            payload_json: format!(r#"{{"id":"{atom_id}"}}"#),
            vector: None,
            flags: vec![],
            labels: vec![],
            links: vec![],
        };
        let t0 = tokio::time::Instant::now();
        match engine.remember(new_atom).await {
            Ok(_) => remember_lat.push(t0.elapsed().as_secs_f64() * 1000.0),
            Err(e) => errors.push(format!("remember: {e}")),
        }

        let t1 = tokio::time::Instant::now();
        let q = dwbase_engine::Question {
            world: world.clone(),
            text: "status?".into(),
            filter: None,
        };
        match engine.ask(q).await {
            Ok(_) => ask_lat.push(t1.elapsed().as_secs_f64() * 1000.0),
            Err(e) => errors.push(format!("ask: {e}")),
        }

        let elapsed = start.elapsed();
        if !injected_corrupt && elapsed > duration / 3 {
            if let Err(e) = corrupt_tail(&base_dir, &world) {
                errors.push(format!("corrupt: {e}"));
            } else {
                injected_corrupt = true;
            }
        }
        if !injected_restart && elapsed > duration * 2 / 3 {
            injected_restart = true;
            drop(engine);
            let storage = SledStorage::open(sled_cfg.clone(), Arc::new(EnvKeyProvider))?;
            let vector = HnswVectorEngine::new();
            let stream = LocalStreamEngine::new();
            let gatekeeper = LocalGatekeeper::new(Capabilities::default(), TrustStore::default());
            let embedder = DummyEmbedder::new();
            engine = DWBaseEngine::new(storage, vector, stream, gatekeeper, embedder);
        }

        if elapsed > duration / 2 && partition_pause.is_zero() {
            let pause = Duration::from_secs(2);
            partition_pause = pause;
            tokio::time::sleep(pause).await;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let end_disk = dir_size_bytes(&base_dir);
    let remember_p50 = percentile(&remember_lat, 50.0);
    let remember_p95 = percentile(&remember_lat, 95.0);
    let ask_p50 = percentile(&ask_lat, 50.0);
    let ask_p95 = percentile(&ask_lat, 95.0);

    let report_data = SoakReport {
        duration_secs: duration.as_secs(),
        remembers: remember_lat.len(),
        asks: ask_lat.len(),
        remember_p50_ms: remember_p50,
        remember_p95_ms: remember_p95,
        ask_p50_ms: ask_p50,
        ask_p95_ms: ask_p95,
        disk_bytes_start: start_disk,
        disk_bytes_end: end_disk,
        memory_bytes_end: rss_bytes(),
        restart_injected: injected_restart,
        corruption_injected: injected_corrupt,
        partition_simulated_secs: partition_pause.as_secs(),
        errors,
    };

    fs::write(&report, serde_json::to_vec_pretty(&report_data)?)?;
    println!(
        "Soak test complete: {} remembers, {} asks. Report at {}",
        report_data.remembers,
        report_data.asks,
        report.display()
    );
    Ok(())
}

fn pid_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        Command::new("kill")
            .arg("-0")
            .arg(format!("{}", pid))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn kill_pid(pid: u32) -> bool {
    #[cfg(unix)]
    {
        Command::new("kill")
            .arg(format!("{}", pid))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn deploy_local(config: Option<PathBuf>) -> Result<()> {
    let cfg = config.unwrap_or_else(|| PathBuf::from("deploy/local.toml"));
    if !cfg.exists() {
        anyhow::bail!("config {} not found", cfg.display());
    }
    let preview = read_config_preview(&cfg)?;
    let base = base_dir().join("local");
    ensure_dir(&base)?;
    let log_path = base.join("dwbase-node.log");
    let pid = spawn_node(&cfg, &log_path)?;
    println!("local node started");
    if let Some(node) = preview {
        if let Some(listen) = node.listen {
            println!("  listen: {listen}");
        }
        if let Some(data_dir) = node.data_dir {
            println!("  data: {}", data_dir.display());
        }
    }
    println!("  pid: {pid}");
    println!("  config: {}", cfg.display());
    println!("  logs: {}", log_path.display());
    Ok(())
}

fn deploy_devnet(nodes: usize, base_port: u16) -> Result<()> {
    if nodes == 0 {
        anyhow::bail!("--nodes must be > 0");
    }
    if let Some(existing) = load_state_pruned()? {
        if !existing.nodes.is_empty() {
            anyhow::bail!("devnet state exists at {}; run `dwbase deploy stop` or `dwbase deploy clean --yes` first", state_path().display());
        }
    }
    let base = devnet_root();
    ensure_dir(&base)?;
    let tracker = PidTracker::new();
    install_ctrlc_handler(tracker.clone())?;
    let mut state = DevnetState::default();
    for idx in 0..nodes {
        let id = format!("node-{}", idx + 1);
        let port = base_port + idx as u16;
        let node_dir = base.join(&id);
        let data_dir = node_dir.join("data");
        let log_path = node_dir.join("dwbase-node.log");
        let cfg_path = node_dir.join("config.toml");
        ensure_dir(&data_dir)?;
        write_config(&cfg_path, port, &data_dir)?;
        let pid = spawn_node(&cfg_path, &log_path)?;
        tracker.push(pid);
        state.nodes.push(NodeState {
            id,
            port,
            pid,
            data_dir,
            log_path,
            config_path: cfg_path,
        });
    }
    save_state(&state)?;
    tracker.disarm();
    println!("devnet started:");
    println!("Node  ID         Port   Data Dir");
    println!("----  ---------  -----  -------------------------");
    for (idx, n) in state.nodes.iter().enumerate() {
        println!(
            "{:<4}  {:<9}  {:<5}  {}",
            (b'A' + idx as u8) as char,
            n.id,
            n.port,
            n.data_dir.display()
        );
    }
    println!("state file: {}", state_path().display());
    println!("logs/configs under {}", base.display());
    Ok(())
}

fn stop_devnet() -> Result<()> {
    let Some(state) = load_state()? else {
        println!("no devnet state found");
        return Ok(());
    };
    let mut running = Vec::new();
    let mut stale = Vec::new();
    for n in state.nodes {
        if pid_running(n.pid) {
            running.push(n);
        } else {
            stale.push(n);
        }
    }
    if running.is_empty() {
        println!("no running devnet processes; clearing state");
        clear_state_file()?;
        return Ok(());
    }

    for n in running {
        if kill_pid(n.pid) {
            println!("stopped {} (pid {})", n.id, n.pid);
        } else {
            println!("failed to stop {} (pid {})", n.id, n.pid);
        }
    }
    if !stale.is_empty() {
        println!(
            "stale entries ignored for already-dead PIDs: {}",
            stale.len()
        );
    }
    clear_state_file()?;
    Ok(())
}

fn clean_devnet(yes: bool) -> Result<()> {
    if !yes {
        anyhow::bail!("refusing to clean without --yes");
    }
    if let Err(err) = stop_devnet() {
        eprintln!("warning: attempted to stop devnet before cleaning but hit: {err}");
    }
    let base = devnet_root();
    if base.exists() {
        fs::remove_dir_all(&base).with_context(|| format!("remove {}", base.display()))?;
        println!("removed {}", base.display());
    } else {
        println!("nothing to clean at {}", base.display());
    }
    let _ = clear_state_file();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::ListWorlds => {
            let client = Client::new();
            list_worlds(&client, &cli.api).await
        }
        Commands::Ask { world, question } => {
            let client = Client::new();
            ask(&client, &cli.api, world, question).await
        }
        Commands::Replay {
            world,
            limit,
            since,
        } => {
            let client = Client::new();
            replay(&client, &cli.api, world, limit, since).await
        }
        Commands::Inspect { id } => {
            let client = Client::new();
            inspect(&client, &cli.api, id).await
        }
        Commands::Deploy { command } => match command {
            DeployCommands::Local { config } => deploy_local(config),
            DeployCommands::Devnet { nodes, base_port } => deploy_devnet(nodes, base_port),
            DeployCommands::Stop => stop_devnet(),
            DeployCommands::Clean { yes } => clean_devnet(yes),
        },
        Commands::Storage { command } => match command {
            StorageCommands::Check { path } => storage_check(path),
            StorageCommands::Reindex { path } => storage_reindex(path),
        },
        Commands::Index { command } => match command {
            IndexCommands::Status => index_status(&cli.api).await,
        },
        Commands::World { command } => match command {
            WorldCommands::Create {
                world,
                description,
                labels,
            } => {
                let client = Client::new();
                let labels = parse_csv(labels);
                manage_world(&client, &cli.api, "create", world, description, labels).await
            }
            WorldCommands::Archive { world } => {
                let client = Client::new();
                manage_world(&client, &cli.api, "archive", world, None, Vec::new()).await
            }
            WorldCommands::Resume { world } => {
                let client = Client::new();
                manage_world(&client, &cli.api, "resume", world, None, Vec::new()).await
            }
            WorldCommands::Policy {
                world,
                retention_days,
                min_importance,
                replication_allow,
                replication_deny,
            } => {
                let client = Client::new();
                let allow = parse_csv(replication_allow);
                let deny = parse_csv(replication_deny);
                set_policy(
                    &client,
                    &cli.api,
                    world,
                    retention_days,
                    min_importance,
                    allow,
                    deny,
                )
                .await
            }
        },
        Commands::Backup { command } => match command {
            BackupCommands::Create { data_dir, out } => {
                backup_create(&cli.api, data_dir, out).await
            }
            BackupCommands::Restore {
                input,
                data_dir,
                force,
            } => backup_restore(input, data_dir, force),
        },
        Commands::Test { command } => match command {
            TestCommands::Soak {
                duration,
                data_dir,
                report,
            } => {
                let dur = parse_duration(&duration)?;
                soak_test(dur, data_dir, report).await
            }
        },
    }
    .context("command failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use dwbase_core::{AtomId, AtomKind, Importance, WorkerKey};
    use dwbase_embedder_dummy::DummyEmbedder;
    use dwbase_engine::{DWBaseEngine, NewAtom};
    use dwbase_security::{Capabilities, LocalGatekeeper, TrustStore};
    use dwbase_storage_sled::{DummyKeyProvider, SledConfig, SledStorage};
    use dwbase_stream_local::LocalStreamEngine;
    use dwbase_vector_hnsw::HnswVectorEngine;
    use tempfile::TempDir;

    fn build_engine(
        data_dir: &Path,
    ) -> DWBaseEngine<
        SledStorage,
        HnswVectorEngine,
        LocalStreamEngine,
        LocalGatekeeper,
        DummyEmbedder,
    > {
        let storage = SledStorage::open(
            SledConfig::new(data_dir),
            Arc::new(DummyKeyProvider::default()),
        )
        .expect("sled");
        let vector = HnswVectorEngine::new();
        let stream = LocalStreamEngine::new();
        let gatekeeper = LocalGatekeeper::new(Capabilities::default(), TrustStore::default());
        let embedder = DummyEmbedder::new();
        DWBaseEngine::new(storage, vector, stream, gatekeeper, embedder)
    }

    #[tokio::test]
    async fn snapshot_roundtrip_restores_atoms_and_ask() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        let engine = build_engine(&data_dir);

        let world = WorldKey::new("backup-world");
        let new_atom = NewAtom {
            world: world.clone(),
            worker: WorkerKey::new("w1"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new("2024-01-01T00:00:00Z"),
            importance: Importance::clamped(0.5),
            payload_json: r#"{"note":"hello"}"#.into(),
            vector: None,
            flags: Vec::new(),
            labels: Vec::new(),
            links: Vec::new(),
        };
        let id = engine.remember(new_atom).await.expect("remember");

        let snapshot = tmp.path().join("snapshot.tzst");
        let meta = SnapshotMeta {
            format_version: 1,
            created_at: "now".into(),
            index_metadata: engine.index_status(),
        };
        create_snapshot(&data_dir, &snapshot, &meta).expect("create snapshot");

        fs::remove_dir_all(&data_dir).expect("wipe");
        restore_snapshot(&snapshot, &data_dir, true).expect("restore");

        let engine2 = build_engine(&data_dir);
        let answer = engine2
            .ask(Question {
                world: world.clone(),
                text: "hello?".into(),
                filter: None,
            })
            .await
            .expect("ask");
        let seen: Vec<_> = answer
            .supporting_atoms
            .iter()
            .map(|a| a.id().clone())
            .collect();
        assert!(
            seen.contains(&AtomId::new(id.0.clone())),
            "restored engine should return stored atom"
        );
    }
}
