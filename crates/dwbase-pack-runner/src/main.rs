use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use component_dwbase::exports::dwbase::core::engine as wit;
use component_dwbase::Component;
use serde::Deserialize;
use serde_json::Value;
use tempfile::TempDir;

#[derive(Parser, Debug)]
#[command(name = "dwbase-pack-runner")]
#[command(
    about = "Minimal host-side runner for dwbase-memory.gtpack (Greentic-style pack/flow JSON)."
)]
struct Args {
    /// Path to a pack directory (packs/dwbase-memory) or a .gtpack file.
    #[arg(long)]
    pack: PathBuf,

    /// Flow name inside the pack (e.g. memory_demo).
    #[arg(long)]
    flow: String,

    /// Flow inputs, as key=value (repeatable), e.g. --input user_text="hello"
    #[arg(long, value_parser = parse_kv, action = clap::ArgAction::Append)]
    input: Vec<(String, String)>,

    /// Print outputs as JSON.
    #[arg(long, default_value_t = true)]
    json: bool,
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    let Some((k, v)) = s.split_once('=') else {
        return Err("expected key=value".into());
    };
    Ok((k.trim().to_string(), v.to_string()))
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct PackFile {
    #[serde(rename = "apiVersion")]
    api_version: String,
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    flows: Vec<PackFlowRef>,
}

#[derive(Debug, Deserialize)]
struct PackFlowRef {
    name: String,
    file: String,
}

#[derive(Debug, Deserialize)]
struct FlowFile {
    #[serde(rename = "apiVersion")]
    api_version: String,
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    inputs: BTreeMap<String, FlowInput>,
    steps: Vec<FlowStep>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct FlowInput {
    #[serde(rename = "type")]
    _type: String,
    #[serde(default)]
    description: String,
}

#[derive(Debug, Deserialize)]
struct FlowStep {
    id: String,
    op: String,
    #[serde(default)]
    with: Value,
}

fn read_json(path: &Path) -> Result<Value> {
    let bytes = std::fs::read(path).with_context(|| format!("read {path:?}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse json {path:?}"))
}

fn read_pack(pack_root: &Path) -> Result<(PackFile, PathBuf)> {
    let pack_path = pack_root.join("pack.json");
    let pack_json = read_json(&pack_path)?;
    let pack: PackFile = serde_json::from_value(pack_json).context("decode pack.json")?;
    Ok((pack, pack_path))
}

fn select_flow(pack_root: &Path, pack: &PackFile, flow_name: &str) -> Result<(FlowFile, PathBuf)> {
    let flow_ref = pack
        .flows
        .iter()
        .find(|f| f.name == flow_name)
        .ok_or_else(|| anyhow!("flow not found in pack: {flow_name}"))?;
    let flow_path = pack_root.join(&flow_ref.file);
    let flow_json = read_json(&flow_path)?;
    let flow: FlowFile = serde_json::from_value(flow_json).context("decode flow json")?;
    Ok((flow, flow_path))
}

fn substitute_inputs(value: &mut Value, inputs: &BTreeMap<String, String>) {
    match value {
        Value::String(s) => {
            for (k, v) in inputs {
                let needle = format!("${{inputs.{k}}}");
                if s.contains(&needle) {
                    *s = s.replace(&needle, v);
                }
            }
        }
        Value::Array(arr) => {
            for v in arr {
                substitute_inputs(v, inputs);
            }
        }
        Value::Object(map) => {
            for (_, v) in map.iter_mut() {
                substitute_inputs(v, inputs);
            }
        }
        _ => {}
    }
}

fn expect_object<'a>(value: &'a Value, ctx: &str) -> Result<&'a serde_json::Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{ctx}: expected object"))
}

fn get_string(obj: &serde_json::Map<String, Value>, key: &str) -> Result<String> {
    obj.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("missing or invalid string field: {key}"))
}

fn get_f32(obj: &serde_json::Map<String, Value>, key: &str) -> Result<f32> {
    obj.get(key)
        .and_then(|v| v.as_f64())
        .map(|n| n as f32)
        .ok_or_else(|| anyhow!("missing or invalid number field: {key}"))
}

fn get_u32_opt(obj: &serde_json::Map<String, Value>, key: &str) -> Option<u32> {
    obj.get(key).and_then(|v| v.as_u64()).map(|n| n as u32)
}

fn parse_kind(s: &str) -> Result<wit::AtomKind> {
    match s {
        "observation" => Ok(wit::AtomKind::Observation),
        "reflection" => Ok(wit::AtomKind::Reflection),
        "plan" => Ok(wit::AtomKind::Plan),
        "action" => Ok(wit::AtomKind::Action),
        "message" => Ok(wit::AtomKind::Message),
        _ => Err(anyhow!("unknown kind: {s}")),
    }
}

fn parse_new_atom(with: Value) -> Result<wit::NewAtom> {
    let obj = expect_object(&with, "remember.with")?.clone();
    Ok(wit::NewAtom {
        world_key: get_string(&obj, "world_key")?,
        worker: get_string(&obj, "worker")?,
        kind: parse_kind(get_string(&obj, "kind")?.as_str())?,
        timestamp: obj
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        importance: get_f32(&obj, "importance")?,
        payload_json: get_string(&obj, "payload_json")?,
        vector: None,
        flags_list: obj
            .get("flags_list")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        labels: obj
            .get("labels")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        links: obj
            .get("links")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    })
}

fn atom_to_json(a: &wit::Atom) -> Value {
    serde_json::json!({
        "id": a.id,
        "world_key": a.world_key,
        "worker": a.worker,
        "kind": format!("{:?}", a.kind).to_ascii_lowercase(),
        "timestamp": a.timestamp,
        "importance": a.importance,
        "payload_json": a.payload_json,
        "vector": a.vector,
        "flags_list": a.flags_list,
        "labels": a.labels,
        "links": a.links,
    })
}

fn answer_v2_to_json(a: &wit::AnswerV2) -> Value {
    let atoms: Vec<Value> = a.supporting_atoms.iter().map(atom_to_json).collect();
    serde_json::json!({
        "world_key": a.world_key,
        "text": a.text,
        "supporting_atoms": atoms,
        "warnings": a.warnings,
    })
}

fn parse_filter(obj: &serde_json::Map<String, Value>) -> wit::AtomFilter {
    wit::AtomFilter {
        world_key: obj
            .get("world_key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        kinds: Vec::new(),
        labels: Vec::new(),
        flag_filter: Vec::new(),
        since: None,
        until: None,
        limit: get_u32_opt(obj, "limit"),
    }
}

fn parse_question(with: Value) -> Result<wit::Question> {
    let obj = expect_object(&with, "ask.with")?;
    let filter = obj
        .get("filter")
        .and_then(|v| v.as_object())
        .map(parse_filter);
    Ok(wit::Question {
        world_key: get_string(obj, "world_key")?,
        text: get_string(obj, "text")?,
        filter,
    })
}

fn parse_replay(with: Value) -> Result<(String, wit::AtomFilter)> {
    let obj = expect_object(&with, "replay.with")?;
    let target_world = get_string(obj, "target_world")?;
    let filter = obj
        .get("filter")
        .and_then(|v| v.as_object())
        .map(parse_filter)
        .unwrap_or(wit::AtomFilter {
            world_key: Some(target_world.clone()),
            kinds: Vec::new(),
            labels: Vec::new(),
            flag_filter: Vec::new(),
            since: None,
            until: None,
            limit: None,
        });
    Ok((target_world, filter))
}

fn run_step(op: &str, with: Value) -> Result<Value> {
    match op {
        "dwbase.remember_v2" => {
            let atom = parse_new_atom(with)?;
            let res = <Component as wit::Guest>::remember_v2(atom);
            Ok(match res {
                Ok(id) => serde_json::json!({ "ok": { "id": id } }),
                Err(e) => {
                    serde_json::json!({ "err": { "code": e.code, "message": e.message, "details_json": e.details_json } })
                }
            })
        }
        "dwbase.ask_v2" => {
            let q = parse_question(with)?;
            let res = <Component as wit::Guest>::ask_v2(q);
            Ok(match res {
                Ok(ans) => serde_json::json!({ "ok": answer_v2_to_json(&ans) }),
                Err(e) => {
                    serde_json::json!({ "err": { "code": e.code, "message": e.message, "details_json": e.details_json } })
                }
            })
        }
        "dwbase.replay" => {
            let (world, filter) = parse_replay(with)?;
            let atoms = <Component as wit::Guest>::replay(world, filter);
            Ok(Value::Array(atoms.iter().map(atom_to_json).collect()))
        }
        other => Err(anyhow!("unsupported op: {other}")),
    }
}

fn unpack_pack(pack_path: &Path) -> Result<(TempDir, PathBuf)> {
    let dir = tempfile::tempdir().context("create temp dir")?;
    let root = dir.path().to_path_buf();
    let status = Command::new("unzip")
        .arg("-q")
        .arg(pack_path)
        .arg("-d")
        .arg(dir.path())
        .status()
        .context("running unzip")?;
    if !status.success() {
        return Err(anyhow!("unzip failed with status: {status}"));
    }
    Ok((dir, root))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let inputs: BTreeMap<String, String> = args.input.into_iter().collect();

    let (temp, pack_root) = if args.pack.is_dir() {
        (None, args.pack.clone())
    } else {
        let (dir, root) = unpack_pack(&args.pack)?;
        (Some(dir), root)
    };

    let (pack, _pack_path) = read_pack(&pack_root)?;
    if pack.api_version != "greentic.pack/v1" {
        return Err(anyhow!("unsupported pack apiVersion: {}", pack.api_version));
    }
    let (flow, _flow_path) = select_flow(&pack_root, &pack, &args.flow)?;
    if flow.api_version != "greentic.flow/v1" {
        return Err(anyhow!("unsupported flow apiVersion: {}", flow.api_version));
    }

    // Validate required inputs exist (best-effort).
    for (k, v) in &flow.inputs {
        if v._type == "string" && !inputs.contains_key(k) {
            return Err(anyhow!("missing required input: {k}"));
        }
    }

    let mut step_outputs: BTreeMap<String, Value> = BTreeMap::new();
    for step in flow.steps {
        let mut with = step.with;
        substitute_inputs(&mut with, &inputs);
        let out = run_step(&step.op, with)
            .with_context(|| format!("running step {} ({})", step.id, step.op))?;
        step_outputs.insert(step.id, out);
    }

    let result = serde_json::json!({
        "pack": { "name": pack.name, "version": pack.version },
        "flow": { "name": flow.name, "description": flow.description },
        "outputs": step_outputs,
    });

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("{result}");
    }

    drop(temp);
    Ok(())
}
