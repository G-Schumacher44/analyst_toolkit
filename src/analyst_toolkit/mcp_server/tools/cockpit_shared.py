"""Shared cockpit helper utilities."""

from __future__ import annotations

import os
import re

_SAFE_RUN_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _trusted_history_enabled() -> bool:
    return _env_bool(
        "ANALYST_MCP_ENABLE_TRUSTED_HISTORY_TOOL",
        _env_bool("ANALYST_MCP_STDIO", False),
    )


def _safe_run_id_for_path(run_id: str) -> str:
    normalized = _SAFE_RUN_ID_RE.sub("_", str(run_id).strip()).strip("._-")
    return normalized or "pipeline_run"


def _pipeline_dashboard_artifact_path(run_id: str, session_id: str | None = None) -> str:
    safe_run_id = _safe_run_id_for_path(run_id)
    if session_id:
        safe_session_id = _safe_run_id_for_path(session_id)
        return f"exports/reports/pipeline/{safe_run_id}_{safe_session_id}_pipeline_dashboard.html"
    return f"exports/reports/pipeline/{safe_run_id}_pipeline_dashboard.html"
