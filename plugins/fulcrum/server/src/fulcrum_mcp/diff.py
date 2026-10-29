"""Diff two schema definitions: a unified text diff plus a field-level summary."""

from __future__ import annotations

import difflib
import json
import re
from dataclasses import dataclass, field
from typing import Any

_PROTO_FIELD = re.compile(
    r"^\s*(?:(optional|repeated|required)\s+)?([\w.<>, ]+?)\s+(\w+)\s*=\s*(\d+)\s*(?:\[[^\]]*\])?\s*;", re.M
)


@dataclass
class DiffResult:
    changed: bool
    unified: str
    structural: list[tuple[str, str, str]] = field(default_factory=list)


def _normalize(definition: str) -> str:
    try:
        return json.dumps(json.loads(definition), indent=2, sort_keys=True)
    except (ValueError, TypeError):
        return definition.strip()


def _avro_fields(schema: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten record fields (including nested records) to {dotted.name: field-json}."""
    out: dict[str, Any] = {}
    if isinstance(schema, dict) and schema.get("type") == "record":
        for f in schema.get("fields", []):
            name = f"{prefix}{f.get('name')}"
            out[name] = {k: v for k, v in f.items() if k != "name"}
            ftype = f.get("type")
            candidates = ftype if isinstance(ftype, list) else [ftype]
            for cand in candidates:
                if isinstance(cand, dict) and cand.get("type") == "record":
                    out.update(_avro_fields(cand, f"{name}."))
    return out


def _json_fields(schema: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not isinstance(schema, dict):
        return out
    required = set(schema.get("required", []))
    for name, prop in (schema.get("properties") or {}).items():
        key = f"{prefix}{name}"
        info = dict(prop) if isinstance(prop, dict) else {"schema": prop}
        info["required"] = name in required
        nested = info.pop("properties", None)
        out[key] = info
        if nested is not None:
            out.update(_json_fields(prop, f"{key}."))
    return out


def _proto_fields(definition: str) -> dict[str, Any]:
    return {
        m.group(3): {"label": m.group(1) or "", "type": m.group(2).strip(), "tag": int(m.group(4))}
        for m in _PROTO_FIELD.finditer(definition)
    }


def _fields(definition: str, data_format: str) -> dict[str, Any] | None:
    fmt = data_format.upper()
    if fmt == "PROTOBUF":
        return _proto_fields(definition)
    try:
        parsed = json.loads(definition)
    except (ValueError, TypeError):
        return None
    if fmt == "AVRO":
        return _avro_fields(parsed)
    if fmt == "JSON":
        return _json_fields(parsed)
    return None


def _describe_change(old: dict[str, Any], new: dict[str, Any]) -> str:
    parts = []
    for key in sorted(set(old) | set(new)):
        if old.get(key) != new.get(key):
            parts.append(f"{key}: {json.dumps(old.get(key))} → {json.dumps(new.get(key))}")
    return "; ".join(parts)


def diff_schemas(left: str, right: str, data_format: str, left_label: str, right_label: str) -> DiffResult:
    a, b = _normalize(left), _normalize(right)
    unified = "\n".join(
        difflib.unified_diff(a.splitlines(), b.splitlines(), fromfile=left_label, tofile=right_label, lineterm="")
    )
    if a == b:
        return DiffResult(changed=False, unified="")

    structural: list[tuple[str, str, str]] = []
    fa, fb = _fields(left, data_format), _fields(right, data_format)
    if fa is not None and fb is not None:
        for name in sorted(set(fb) - set(fa)):
            structural.append(("added", name, json.dumps(fb[name], sort_keys=True)))
        for name in sorted(set(fa) - set(fb)):
            structural.append(("removed", name, json.dumps(fa[name], sort_keys=True)))
        for name in sorted(set(fa) & set(fb)):
            if fa[name] != fb[name]:
                structural.append(("changed", name, _describe_change(fa[name], fb[name])))
    return DiffResult(changed=True, unified=unified, structural=structural)
