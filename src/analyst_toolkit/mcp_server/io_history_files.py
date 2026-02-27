"""History file parsing and atomic persistence helpers for MCP IO."""

import json
import os
from json import JSONDecodeError
from pathlib import Path
from typing import Any, cast

from analyst_toolkit.mcp_server.io_serialization import make_json_safe


def write_json_atomic(path: Path, payload: Any) -> None:
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, allow_nan=False)
    os.replace(tmp_path, path)


def read_history_file_safe(path: Path) -> tuple[list, dict[str, Any]]:
    meta: dict[str, Any] = {"parse_errors": [], "skipped_records": 0}
    parse_errors = cast(list[str], meta["parse_errors"])

    if not path.exists():
        return [], meta

    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return [], meta

    try:
        parsed = json.loads(raw)
    except JSONDecodeError as exc:
        parse_errors.append(f"{type(exc).__name__}: {exc}")
        recovered = _recover_history_entries(raw, meta)
        return recovered, meta

    return _coerce_history_entries(parsed, meta), meta


def _recover_history_entries(raw: str, meta: dict[str, Any]) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    recovered: list[dict[str, Any]] = []
    parse_errors = cast(list[str], meta["parse_errors"])
    idx = 0

    while idx < len(raw):
        while idx < len(raw) and raw[idx] in " \t\r\n[],":
            idx += 1
        if idx >= len(raw):
            break
        try:
            item, next_idx = decoder.raw_decode(raw, idx)
        except JSONDecodeError:
            meta["skipped_records"] += 1
            idx += 1
            continue
        if isinstance(item, dict):
            recovered.append(item)
        else:
            meta["skipped_records"] += 1
        idx = next_idx

    if not recovered:
        parse_errors.append("Unable to recover any valid history entries.")
    return [make_json_safe(entry) for entry in recovered if isinstance(entry, dict)]


def _coerce_history_entries(parsed: Any, meta: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(parsed, list):
        entries: list[dict[str, Any]] = []
        for item in parsed:
            if isinstance(item, dict):
                entries.append(make_json_safe(item))
            else:
                meta["skipped_records"] += 1
        return entries

    if isinstance(parsed, dict):
        return [make_json_safe(parsed)]

    meta["parse_errors"].append("History root is not a list/object.")
    meta["skipped_records"] += 1
    return []
