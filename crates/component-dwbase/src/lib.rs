//! component-dwbase: Greentic component shim for DWBase.
//!
//! Implements `greentic:component@0.5.0` using greentic-interfaces-guest. DWBase
//! logic stays internal; this adapter currently echoes inputs and can be
//! extended to call into DWBase APIs.

use greentic_interfaces_guest::component::node::{self, Guest, InvokeResult, StreamEvent};
use serde_json::json;

pub struct DwbaseComponent;

impl Guest for DwbaseComponent {
    fn get_manifest() -> String {
        json!({
            "name": "dwbase",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "DWBase component shim (greentic:component@0.5.0)",
            "capabilities": ["invoke", "invoke-stream"],
            "tags": ["dwbase", "memory", "agent"],
        })
        .to_string()
    }

    fn on_start(_ctx: node::ExecCtx) -> Result<node::LifecycleStatus, String> {
        Ok(node::LifecycleStatus::Ok)
    }

    fn on_stop(_ctx: node::ExecCtx, _reason: String) -> Result<node::LifecycleStatus, String> {
        Ok(node::LifecycleStatus::Ok)
    }

    fn invoke(_ctx: node::ExecCtx, op: String, input: String) -> InvokeResult {
        // Minimal adapter: echo the request; extend to call DWBase APIs per op.
        let payload = json!({
            "op": op,
            "input": input,
            "status": "ok",
        });
        InvokeResult::Ok(payload.to_string())
    }

    fn invoke_stream(_ctx: node::ExecCtx, op: String, input: String) -> Vec<StreamEvent> {
        vec![
            StreamEvent::Data(json!({"op": op, "input": input, "chunk": 0}).to_string()),
            StreamEvent::Done,
        ]
    }
}

greentic_interfaces_guest::export_component_node!(DwbaseComponent);
