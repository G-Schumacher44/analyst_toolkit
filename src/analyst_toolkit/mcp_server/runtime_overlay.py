"""Runtime overlay validation and deep-merge helpers for MCP and CLI config resolution."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import yaml
from pydantic import ValidationError

from analyst_toolkit.mcp_server.config_models import RuntimeOverlayConfig


class RuntimeOverlayError(ValueError):
    """Raised when runtime overlay validation fails in strict mode."""


_RUNTIME_ALLOWED_KEYS: dict[str | None, set[str]] = {
    None: {"run", "artifacts", "destinations", "paths", "execution"},
    "run": {"run_id", "session_id", "input_path"},
    "artifacts": {
        "export_html",
        "export_xlsx",
        "export_data",
        "plotting",
        "artifact_mode",
        "collision_policy",
    },
    "destinations": {"local", "gcs", "drive"},
    "destinations.local": {"enabled", "root"},
    "destinations.gcs": {"enabled", "bucket_uri", "prefix"},
    "destinations.drive": {"enabled", "folder_id"},
    "paths": {"report_root", "plot_root", "checkpoint_root", "data_root"},
    "execution": {"allow_plot_generation", "upload_artifacts", "persist_history", "strict_config"},
}


def _runtime_key_path(parent: str | None, key: str) -> str:
    return key if parent is None else f"{parent}.{key}"


def _collect_unknown_runtime_keys(
    payload: dict[str, Any],
    *,
    parent: str | None = None,
) -> list[str]:
    allowed = _RUNTIME_ALLOWED_KEYS.get(parent, set())
    unknown: list[str] = []
    for key, value in payload.items():
        if key not in allowed:
            unknown.append(_runtime_key_path(parent, key))
            continue
        next_parent = _runtime_key_path(parent, key)
        if isinstance(value, dict) and next_parent in _RUNTIME_ALLOWED_KEYS:
            unknown.extend(_collect_unknown_runtime_keys(value, parent=next_parent))
    return unknown


def _coerce_runtime_overlay_input(runtime: Any) -> dict[str, Any]:
    if runtime is None:
        return {}
    if isinstance(runtime, str):
        loaded = yaml.safe_load(runtime)
        runtime = loaded if isinstance(loaded, dict) else {}
    if not isinstance(runtime, dict):
        raise RuntimeOverlayError("Runtime overlay must be a dict, YAML string, or None.")
    if "runtime" in runtime and isinstance(runtime.get("runtime"), dict):
        return deepcopy(runtime["runtime"])
    return deepcopy(runtime)


def _restore_explicit_nulls(
    normalized: dict[str, Any],
    raw_payload: dict[str, Any],
) -> dict[str, Any]:
    restored = deepcopy(normalized)
    for key, value in raw_payload.items():
        if value is None:
            restored[key] = None
            continue
        if isinstance(value, dict):
            normalized_child = restored.get(key, {})
            if not isinstance(normalized_child, dict):
                normalized_child = {}
            restored[key] = _restore_explicit_nulls(normalized_child, value)
    return restored


def normalize_runtime_overlay(
    runtime: Any,
    *,
    strict: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    payload = _coerce_runtime_overlay_input(runtime)
    if not payload:
        return {}, []

    unknown_keys = _collect_unknown_runtime_keys(payload)
    if unknown_keys and strict:
        raise RuntimeOverlayError("Unknown runtime keys: " + ", ".join(sorted(unknown_keys)))

    sanitized = deepcopy(payload)
    for dotted_key in unknown_keys:
        parts = dotted_key.split(".")
        cursor = sanitized
        for part in parts[:-1]:
            cursor = cursor.get(part, {})
        if isinstance(cursor, dict):
            cursor.pop(parts[-1], None)

    try:
        normalized = RuntimeOverlayConfig.model_validate(sanitized).model_dump(exclude_none=True)
    except ValidationError as exc:
        raise RuntimeOverlayError(str(exc)) from exc
    normalized = _restore_explicit_nulls(normalized, sanitized)

    warnings = [f"Ignored unknown runtime key: {key}" for key in sorted(unknown_keys)]
    return normalized, warnings


def deep_merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge dictionaries with replace semantics for lists and scalars."""
    merged = deepcopy(base)
    for key, override_value in override.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            merged[key] = deep_merge_dicts(base_value, override_value)
        else:
            merged[key] = deepcopy(override_value)
    return merged


def resolve_layered_config(
    *,
    base: dict[str, Any] | None = None,
    inferred: dict[str, Any] | None = None,
    provided: dict[str, Any] | None = None,
    runtime: Any = None,
    explicit: dict[str, Any] | None = None,
    strict_runtime: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Resolve a final config using deterministic precedence and runtime validation."""
    normalized_runtime, runtime_warnings = normalize_runtime_overlay(runtime, strict=strict_runtime)
    resolved: dict[str, Any] = {}
    for layer in (base or {}, inferred or {}, provided or {}, normalized_runtime, explicit or {}):
        if layer:
            resolved = deep_merge_dicts(resolved, layer)
    metadata = {
        "runtime_applied": bool(normalized_runtime),
        "runtime_warnings": runtime_warnings,
        "resolved_layers": {
            "base": bool(base),
            "inferred": bool(inferred),
            "provided": bool(provided),
            "runtime": bool(normalized_runtime),
            "explicit": bool(explicit),
        },
    }
    return resolved, metadata
