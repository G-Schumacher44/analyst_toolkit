"""Recent-history and cockpit dashboard report helpers."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from analyst_toolkit.mcp_server.local_artifact_server import (
    build_local_artifact_url,
    get_local_artifact_server_status,
)
from analyst_toolkit.mcp_server.tools.cockpit_runtime import build_data_health_report
from analyst_toolkit.mcp_server.tools.cockpit_shared import (
    _pipeline_dashboard_artifact_path,
    _safe_run_id_for_path,
    _trusted_history_enabled,
)
from analyst_toolkit.mcp_server.tools.cockpit_templates import (
    build_cockpit_launch_sequences,
    build_cockpit_launchpad,
    build_cockpit_operator_brief,
    build_cockpit_resource_groups,
    build_cockpit_resources,
    build_data_dictionary_lane,
)

MAX_ARTIFACT_ROWS = 24
_WORKSPACE_ROOT = Path.cwd().resolve(strict=False)


def _module_display_name(module: str) -> str:
    mapping = {
        "diagnostics": "Diagnostics",
        "validation": "Validation",
        "normalization": "Normalization",
        "duplicates": "Duplicates",
        "outliers": "Outliers",
        "imputation": "Imputation",
        "final_audit": "Final Audit",
        "auto_heal": "Auto Heal",
    }
    return mapping.get(module, module.replace("_", " ").title())


def _artifact_root_label(root: Any) -> str:
    text = str(root or "").strip()
    if not text:
        return ""
    path = Path(text).expanduser().resolve(strict=False)
    try:
        return path.relative_to(_WORKSPACE_ROOT).as_posix()
    except ValueError:
        return path.name or "artifacts"


def _read_history_entries(path: Path) -> list[dict[str, Any]]:
    if not _trusted_history_enabled():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return raw if isinstance(raw, list) else []


def _history_sort_value(path: Path) -> float:
    try:
        fallback = path.stat().st_mtime
    except OSError:
        return 0.0
    history = _read_history_entries(path)
    newest = ""
    for entry in history:
        timestamp = str(entry.get("timestamp", "") or "")
        if timestamp > newest:
            newest = timestamp
    if not newest:
        return fallback
    try:
        return datetime.fromisoformat(newest.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return fallback


def _iter_recent_history_files(limit: int) -> list[Path]:
    if not _trusted_history_enabled():
        return []
    history_root = Path("exports/reports/history")
    if not history_root.exists():
        return []
    return sorted(
        history_root.glob("**/*_history.json"),
        key=_history_sort_value,
        reverse=True,
    )[:limit]


def _dashboard_ref(entry: dict[str, Any]) -> str:
    dashboard_path = str(entry.get("dashboard_path") or "")
    return str(
        entry.get("dashboard_url") or build_local_artifact_url(dashboard_path) or dashboard_path
    )


def _discover_local_dashboard_ref(
    module: str,
    run_id: str,
    session_id: str | None = None,
) -> str:
    candidates: list[str] = []
    if module == "pipeline_dashboard":
        candidates.append(_pipeline_dashboard_artifact_path(run_id, session_id))
    elif module == "auto_heal":
        candidates.append(f"exports/reports/auto_heal/{run_id}_auto_heal_report.html")
    elif module == "final_audit":
        candidates.append(f"exports/reports/final_audit/{run_id}_final_audit_report.html")

    for candidate in candidates:
        if Path(candidate).exists():
            return build_local_artifact_url(candidate) or candidate
    return ""


def _export_ref(entry: dict[str, Any]) -> str:
    export_path = str(entry.get("export_path") or entry.get("xlsx_path") or "")
    return str(
        entry.get("export_url")
        or entry.get("artifact_url")
        or build_local_artifact_url(export_path)
        or export_path
    )


def _build_recent_run_cards(limit: int) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for history_file in _iter_recent_history_files(limit):
        history = _read_history_entries(history_file)
        if not history:
            continue
        last_entry = history[-1]
        run_id = str(last_entry.get("run_id") or history_file.stem.replace("_history", ""))
        session_id = str(last_entry.get("session_id") or "")
        history_meta = {"parse_errors": [], "skipped_records": 0}
        health = build_data_health_report(
            run_id=run_id,
            session_id=session_id or None,
            history=history,
            history_meta=history_meta,
        )
        latest_by_module: dict[str, dict[str, Any]] = {}
        for entry in history:
            module = str(entry.get("module", "")).strip()
            if module:
                latest_by_module[module] = entry

        final_audit = latest_by_module.get("final_audit", {})
        auto_heal = latest_by_module.get("auto_heal", {})
        pipeline_entry = latest_by_module.get("pipeline_dashboard", {})
        latest_non_synthetic = next(
            (
                entry
                for entry in reversed(history)
                if str(entry.get("module", "")).strip() != "pipeline_dashboard"
            ),
            last_entry,
        )
        pipeline_dashboard = _dashboard_ref(pipeline_entry)
        if not pipeline_dashboard:
            pipeline_dashboard = _discover_local_dashboard_ref(
                "pipeline_dashboard",
                run_id,
                session_id or None,
            )
        auto_heal_dashboard = _dashboard_ref(auto_heal)
        if not auto_heal_dashboard:
            auto_heal_dashboard = _discover_local_dashboard_ref("auto_heal", run_id)
        final_audit_dashboard = _dashboard_ref(final_audit)
        if not final_audit_dashboard:
            final_audit_dashboard = _discover_local_dashboard_ref("final_audit", run_id)
        cards.append(
            {
                "run_id": run_id,
                "session_id": session_id,
                "history_entries": len(history),
                "module_count": len(latest_by_module),
                "status": str(
                    final_audit.get("status")
                    or auto_heal.get("status")
                    or latest_non_synthetic.get("status")
                    or "unknown"
                ),
                "latest_module": str(latest_non_synthetic.get("module", "unknown")),
                "timestamp": str(latest_non_synthetic.get("timestamp", "")),
                "health_score": health.get("health_score", "N/A"),
                "health_status": health.get("health_status", "unknown"),
                "health_advisory": bool(health.get("health_advisory", False)),
                "certification_status": str(health.get("certification_status", "not_run")),
                "pipeline_dashboard": pipeline_dashboard,
                "auto_heal_dashboard": auto_heal_dashboard,
                "final_audit_dashboard": final_audit_dashboard,
                "best_dashboard": final_audit_dashboard
                or auto_heal_dashboard
                or _dashboard_ref(latest_non_synthetic),
                "best_export": _export_ref(final_audit)
                or _export_ref(auto_heal)
                or _export_ref(latest_non_synthetic),
                "warning_count": sum(len(entry.get("warnings", [])) for entry in history),
            }
        )
    return cards


def _latest_recent_module_entry(module_name: str, limit: int) -> dict[str, Any]:
    for history_file in _iter_recent_history_files(limit):
        history = _read_history_entries(history_file)
        if not history:
            continue
        for entry in reversed(history):
            if str(entry.get("module", "")).strip() == module_name:
                return entry
    return {}


def _build_recent_artifact_rows(limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for history_file in _iter_recent_history_files(limit):
        history = _read_history_entries(history_file)
        if not history:
            continue
        for entry in reversed(history):
            module = str(entry.get("module", "")).strip()
            if not module:
                continue
            dashboard_ref = _dashboard_ref(entry)
            export_ref = _export_ref(entry)
            artifact_path = str(entry.get("artifact_path", "") or entry.get("dashboard_path", ""))
            if not (dashboard_ref or export_ref or artifact_path):
                continue
            run_id = str(entry.get("run_id", ""))
            session_id = str(entry.get("session_id", ""))
            key = (run_id, session_id, module, dashboard_ref or export_ref or artifact_path)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "Run": run_id or "Unknown",
                    "Session": session_id or "",
                    "Module": _module_display_name(module),
                    "Status": str(entry.get("status", "unknown")).upper(),
                    "Dashboard": dashboard_ref,
                    "Export": export_ref,
                    "Artifact Path": artifact_path,
                }
            )
            if len(rows) >= MAX_ARTIFACT_ROWS:
                return rows
    return rows


def _build_cockpit_dashboard_report(limit: int) -> dict[str, Any]:
    recent_runs = _build_recent_run_cards(limit)
    warnings = sum(1 for run in recent_runs if str(run.get("status", "")).lower() in {"warn"})
    failures = sum(
        1 for run in recent_runs if str(run.get("status", "")).lower() in {"fail", "error"}
    )
    pipeline_count = sum(1 for run in recent_runs if run.get("pipeline_dashboard"))
    auto_heal_count = sum(1 for run in recent_runs if run.get("auto_heal_dashboard"))
    ready_count = sum(
        1 for run in recent_runs if str(run.get("status", "")).lower() in {"pass", "available"}
    )
    top_pipeline = next((run for run in recent_runs if run.get("pipeline_dashboard")), {})
    top_auto_heal = next((run for run in recent_runs if run.get("auto_heal_dashboard")), {})
    top_final_audit = next((run for run in recent_runs if run.get("final_audit_dashboard")), {})
    latest_dictionary = _latest_recent_module_entry("data_dictionary", limit)
    artifact_server_status = get_local_artifact_server_status()
    artifact_server = {
        **artifact_server_status,
        "root": _artifact_root_label(artifact_server_status.get("root", "")),
    }
    artifact_rows = _build_recent_artifact_rows(limit)
    blocker_runs = [
        {
            "run_id": str(run.get("run_id", "")),
            "status": str(run.get("status", "unknown")).upper(),
            "latest_module": str(run.get("latest_module", "unknown")),
            "warning_count": int(run.get("warning_count", 0) or 0),
        }
        for run in recent_runs
        if str(run.get("status", "")).lower() in {"warn", "fail", "error"}
    ][:3]
    recent_run_gaps: list[str] = []
    if not pipeline_count:
        recent_run_gaps.append("No recent pipeline dashboard artifacts were found.")
    if not auto_heal_count:
        recent_run_gaps.append("No recent auto-heal dashboards were found.")
    if not top_final_audit:
        recent_run_gaps.append("No recent final audit dashboard was recorded.")
    if failures:
        posture = {
            "label": "Blocked",
            "detail": "At least one recent run ended in fail/error and needs operator attention.",
        }
    elif warnings:
        posture = {
            "label": "Needs Review",
            "detail": "Recent runs exist, but warn-level outcomes still need a human read before trust.",
        }
    else:
        posture = {
            "label": "Healthy",
            "detail": "Recent runs are landing in pass-level states with no current blocking signal.",
        }
    resources = build_cockpit_resources()
    resource_groups = build_cockpit_resource_groups(resources)
    launchpad = build_cockpit_launchpad()
    launch_sequences = build_cockpit_launch_sequences()
    return {
        "recent_runs": recent_runs,
        "overview": {
            "recent_run_count": len(recent_runs),
            "warning_runs": warnings,
            "failed_runs": failures,
            "healthy_runs": ready_count,
            "pipeline_dashboards_available": pipeline_count,
            "auto_heal_dashboards_available": auto_heal_count,
        },
        "operator_brief": build_cockpit_operator_brief(),
        "operating_posture": posture,
        "best_surfaces": {
            "pipeline_dashboard": {
                "run_id": str(top_pipeline.get("run_id", "")),
                "reference": str(top_pipeline.get("pipeline_dashboard", "")),
            },
            "auto_heal_dashboard": {
                "run_id": str(top_auto_heal.get("run_id", "")),
                "reference": str(top_auto_heal.get("auto_heal_dashboard", "")),
            },
            "final_audit_dashboard": {
                "run_id": str(top_final_audit.get("run_id", "")),
                "reference": str(top_final_audit.get("final_audit_dashboard", "")),
            },
        },
        "blockers": blocker_runs,
        "recent_run_gaps": recent_run_gaps,
        "resources": resources,
        "resource_groups": resource_groups,
        "launchpad": launchpad,
        "launch_sequences": launch_sequences,
        "artifact_server": artifact_server,
        "artifacts": artifact_rows,
        "data_dictionary": build_data_dictionary_lane(latest_dictionary),
    }
