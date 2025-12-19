#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import List, Dict, Any

ROOT = Path(__file__).resolve().parents[1]
CODEX_DIR = ROOT / ".codex"
STATE_PATH = CODEX_DIR / "STATE.json"

PR_RE = re.compile(r"^PR-(\d{2})\.md$")

def die(msg: str, code: int = 1) -> None:
    print(f"[codex_next] {msg}", file=sys.stderr)
    raise SystemExit(code)

def load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        die(f"Missing {STATE_PATH}. Run PR-01 steps to create it.")
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))

def save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")

def list_pr_files() -> List[Path]:
    if not CODEX_DIR.exists():
        die(f"Missing {CODEX_DIR}. Create .codex directory and PR files.")
    prs = []
    for p in sorted(CODEX_DIR.iterdir()):
        if p.is_file() and PR_RE.match(p.name):
            prs.append(p)
    if not prs:
        die("No PR-*.md files found in .codex/")
    return prs

def next_pr(state: Dict[str, Any]) -> Path | None:
    done = set(state.get("done", []))
    for p in list_pr_files():
        pr_key = p.stem  # PR-01
        if pr_key not in done:
            return p
    return None

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", action="store_true", help="Show current status")
    ap.add_argument("--done", type=str, help="Mark PR done, e.g. PR-02")
    ap.add_argument("--show", type=str, help="Show specific PR, e.g. PR-03")
    args = ap.parse_args()

    state = load_state()

    if args.status:
        prs = [p.stem for p in list_pr_files()]
        done = set(state.get("done", []))
        pending = [p for p in prs if p not in done]
        print("== Codex PR Status ==")
        print(f"Done:    {sorted(done)}")
        print(f"Pending: {pending}")
        print(f"Current: {state.get('current')}")
        return

    if args.done:
        pr = args.done.strip()
        if pr not in state.get("done", []):
            state.setdefault("done", []).append(pr)
        state["current"] = None
        save_state(state)
        print(f"[codex_next] Marked done: {pr}")
        return

    if args.show:
        pr = args.show.strip()
        pr_path = CODEX_DIR / f"{pr}.md"
        if not pr_path.exists():
            die(f"Not found: {pr_path}")
        state["current"] = pr
        save_state(state)
        print(pr_path.read_text(encoding="utf-8"))
        return

    p = next_pr(state)
    if p is None:
        print("[codex_next] All PRs are done ✅")
        return

    state["current"] = p.stem
    save_state(state)
    print(p.read_text(encoding="utf-8"))

if __name__ == "__main__":
    main()
