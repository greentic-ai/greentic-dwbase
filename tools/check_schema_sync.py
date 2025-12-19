#!/usr/bin/env python3
"""
Best-effort schema/WIT sync check for component-dwbase.

Validates:
- component.manifest.json parses as JSON
- manifest operations map to WIT exports (prefix dwbase.; hyphens -> underscores)
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "crates/component-dwbase/component.manifest.json"
WIT = ROOT / "wit/dwbase-core.wit"

# These ops are intentionally absent/present across boundaries.
ALLOWED_MANIFEST_ONLY = {"dwbase.world.list"}
ALLOWED_WIT_ONLY = {"dwbase.observe", "dwbase.metrics_snapshot"}


def load_manifest_ops() -> set[str]:
    try:
        data = json.loads(MANIFEST.read_text())
    except json.JSONDecodeError as exc:
        print(f"manifest JSON invalid: {exc}", file=sys.stderr)
        sys.exit(1)
    if not isinstance(data, dict):
        print("manifest JSON is not an object", file=sys.stderr)
        sys.exit(1)
    ops = []
    for entry in data.get("operations", []):
        name = entry.get("name") if isinstance(entry, dict) else None
        if isinstance(name, str):
            ops.append(name)
    return set(ops)


def load_wit_ops() -> set[str]:
    text = WIT.read_text()
    func_pattern = re.compile(r"^\s*([a-z0-9-]+):\s*func", re.MULTILINE)
    names = func_pattern.findall(text)
    return {f"dwbase.{n.replace('-', '_')}" for n in names}


def main() -> int:
    manifest_ops = load_manifest_ops()
    wit_ops = load_wit_ops()

    missing_in_manifest = sorted(wit_ops - manifest_ops - ALLOWED_WIT_ONLY)
    extra_in_manifest = sorted(manifest_ops - wit_ops - ALLOWED_MANIFEST_ONLY)

    ok = True
    if missing_in_manifest:
        print("operations missing from manifest (present in WIT):")
        for name in missing_in_manifest:
            print(f"  - {name}")
        ok = False
    if extra_in_manifest:
        print("operations present in manifest but not in WIT:")
        for name in extra_in_manifest:
            print(f"  - {name}")
        ok = False

    if ok:
        print(
            f"schema sync ok: {len(manifest_ops)} manifest ops, "
            f"{len(wit_ops)} wit exports (allowed diff: "
            f"{len(ALLOWED_MANIFEST_ONLY)} manifest-only, "
            f"{len(ALLOWED_WIT_ONLY)} wit-only)"
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
