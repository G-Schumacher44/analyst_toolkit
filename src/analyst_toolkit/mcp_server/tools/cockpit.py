"""MCP tool: cockpit — user/agent guidance, capability catalog, history, and health scoring."""

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from analyst_toolkit.m00_utils.export_utils import export_html_report
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    compact_destination_metadata,
    deliver_artifact,
    empty_delivery_state,
    get_last_history_read_meta,
    get_run_history,
)
from analyst_toolkit.mcp_server.local_artifact_server import (
    build_local_artifact_url,
    ensure_local_artifact_server,
    get_local_artifact_server_status,
)
from analyst_toolkit.mcp_server.registry import register_tool
from analyst_toolkit.mcp_server.response_utils import (
    next_action,
    with_dashboard_artifact,
    with_next_actions,
)
from analyst_toolkit.mcp_server.templates import get_golden_configs
from analyst_toolkit.mcp_server.tools.cockpit_capabilities import (
    build_capability_catalog,
    filter_capability_catalog,
)
from analyst_toolkit.mcp_server.tools.cockpit_content import (
    agent_playbook_payload,
    user_quickstart_payload,
)
from analyst_toolkit.mcp_server.tools.cockpit_runtime import (
    build_data_health_report,
    build_run_history_result,
)
from analyst_toolkit.mcp_server.tools.cockpit_schemas import (
    ARTIFACT_SERVER_INPUT_SCHEMA,
    CAPABILITY_CATALOG_INPUT_SCHEMA,
    COCKPIT_DASHBOARD_INPUT_SCHEMA,
    DATA_HEALTH_REPORT_INPUT_SCHEMA,
    PIPELINE_DASHBOARD_INPUT_SCHEMA,
    RUN_HISTORY_INPUT_SCHEMA,
)

logger = logging.getLogger("analyst_toolkit.mcp_server.cockpit")
_SAFE_RUN_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


TEMPLATE_IO_TIMEOUT_SEC = _env_float("ANALYST_MCP_TEMPLATE_IO_TIMEOUT_SEC", 8.0)
RUN_HISTORY_DEFAULT_SUMMARY_ONLY = _env_bool("ANALYST_MCP_RUN_HISTORY_SUMMARY_ONLY_DEFAULT", True)
RUN_HISTORY_DEFAULT_LIMIT = _env_int("ANALYST_MCP_RUN_HISTORY_DEFAULT_LIMIT", 50)
TRUSTED_HISTORY_ENABLED = _env_bool(
    "ANALYST_MCP_ENABLE_TRUSTED_HISTORY_TOOL",
    _env_bool("ANALYST_MCP_STDIO", False),
)


def _artifact_server_control_enabled() -> bool:
    return _env_bool(
        "ANALYST_MCP_ENABLE_ARTIFACT_SERVER_TOOL",
        _env_bool("ANALYST_MCP_STDIO", False),
    )


def _build_capability_catalog() -> dict[str, Any]:
    return build_capability_catalog(golden_configs=get_golden_configs())


def _filter_capability_catalog(
    catalog: dict[str, Any],
    module: str | None = None,
    search: str | None = None,
    path_prefix: str | None = None,
    compact: bool = False,
) -> dict[str, Any]:
    return filter_capability_catalog(
        catalog,
        module=module,
        search=search,
        path_prefix=path_prefix,
        compact=compact,
    )


async def _toolkit_get_user_quickstart() -> dict:
    """Returns a concise, human-readable guide for operating the toolkit."""
    return user_quickstart_payload()


async def _toolkit_get_agent_playbook() -> dict:
    """Returns strict, step-by-step playbook data for client agents."""
    return agent_playbook_payload()


async def _toolkit_get_capability_catalog(
    module: str | None = None,
    search: str | None = None,
    path_prefix: str | None = None,
    compact: bool = False,
) -> dict:
    """Returns user-editable configuration capabilities by module/template."""
    try:
        catalog = await asyncio.wait_for(
            asyncio.to_thread(_build_capability_catalog),
            timeout=TEMPLATE_IO_TIMEOUT_SEC,
        )
        return _filter_capability_catalog(
            catalog,
            module=module,
            search=search,
            path_prefix=path_prefix,
            compact=compact,
        )
    except asyncio.TimeoutError:
        logger.error(
            "Capability catalog build timed out after %.1fs",
            TEMPLATE_IO_TIMEOUT_SEC,
        )
        return {
            "status": "error",
            "error": (
                "Capability catalog generation timed out. "
                f"Try increasing ANALYST_MCP_TEMPLATE_IO_TIMEOUT_SEC (current={TEMPLATE_IO_TIMEOUT_SEC}s)."
            ),
        }


async def _toolkit_get_golden_templates() -> dict:
    """Returns a library of 'Golden Config' templates."""
    try:
        templates = await asyncio.wait_for(
            asyncio.to_thread(get_golden_configs),
            timeout=TEMPLATE_IO_TIMEOUT_SEC,
        )
        return {"status": "pass", "templates": templates}
    except asyncio.TimeoutError:
        logger.error(
            "Golden templates read timed out after %.1fs",
            TEMPLATE_IO_TIMEOUT_SEC,
        )
        return {
            "status": "error",
            "error": (
                "Golden template loading timed out. "
                f"Try increasing ANALYST_MCP_TEMPLATE_IO_TIMEOUT_SEC (current={TEMPLATE_IO_TIMEOUT_SEC}s)."
            ),
        }


async def _toolkit_get_run_history(
    run_id: str,
    session_id: str | None = None,
    failures_only: bool = False,
    latest_errors: bool = False,
    latest_status_by_module: bool = False,
    limit: int | None = None,
    summary_only: bool | None = None,
) -> dict:
    """Returns the 'Prescription & Healing Ledger'."""
    history = get_run_history(run_id, session_id=session_id)
    history_meta = get_last_history_read_meta(run_id, session_id=session_id)
    return build_run_history_result(
        run_id=run_id,
        session_id=session_id,
        failures_only=failures_only,
        latest_errors=latest_errors,
        latest_status_by_module=latest_status_by_module,
        limit=limit,
        summary_only=summary_only,
        run_history_default_summary_only=RUN_HISTORY_DEFAULT_SUMMARY_ONLY,
        run_history_default_limit=RUN_HISTORY_DEFAULT_LIMIT,
        history=history,
        history_meta=history_meta,
    )


async def _toolkit_get_data_health_report(run_id: str, session_id: str | None = None) -> dict:
    """Calculates a Red/Yellow/Green Data Health Score (0-100)."""
    history = get_run_history(run_id, session_id=session_id)
    history_meta = get_last_history_read_meta(run_id, session_id=session_id)
    return build_data_health_report(
        run_id=run_id,
        session_id=session_id,
        history=history,
        history_meta=history_meta,
    )


def _module_display_name(module: str) -> str:
    mapping = {
        "diagnostics": "Diagnostics",
        "validation": "Validation",
        "normalization": "Normalization",
        "duplicates": "Duplicates",
        "outliers": "Outliers",
        "outlier_handling": "Outlier Handling",
        "imputation": "Imputation",
        "final_audit": "Final Audit",
        "auto_heal": "Auto Heal",
    }
    return mapping.get(module, module.replace("_", " ").title())


def _safe_run_id_for_path(run_id: str) -> str:
    normalized = _SAFE_RUN_ID_RE.sub("_", str(run_id).strip()).strip("._-")
    return normalized or "pipeline_run"


def _pipeline_dashboard_artifact_path(run_id: str, session_id: str | None = None) -> str:
    safe_run_id = _safe_run_id_for_path(run_id)
    if session_id:
        safe_session_id = _safe_run_id_for_path(session_id)
        return f"exports/reports/pipeline/{safe_run_id}_{safe_session_id}_pipeline_dashboard.html"
    return f"exports/reports/pipeline/{safe_run_id}_pipeline_dashboard.html"


def _cockpit_artifact_key(limit: int) -> str:
    return f"cockpit_dashboard_limit_{int(limit)}"


def _trusted_history_denial() -> dict[str, Any]:
    return {
        "status": "error",
        "code": "COCKPIT_HISTORY_DISABLED",
        "message": (
            "Cockpit history access is disabled. Enable ANALYST_MCP_ENABLE_TRUSTED_HISTORY_TOOL=1 "
            "or use trusted/local stdio mode."
        ),
        "warnings": [],
    }


def _history_sort_value(path: Path) -> float:
    try:
        fallback = path.stat().st_mtime
    except OSError:
        return 0.0
    try:
        history = _read_history_entries(path)
    except OSError:
        return fallback
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
    if not TRUSTED_HISTORY_ENABLED:
        return []
    history_root = Path("exports/reports/history")
    if not history_root.exists():
        return []
    return sorted(
        history_root.glob("**/*_history.json"),
        key=_history_sort_value,
        reverse=True,
    )[:limit]


def _read_history_entries(path: Path) -> list[dict[str, Any]]:
    if not TRUSTED_HISTORY_ENABLED:
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return raw if isinstance(raw, list) else []


def _dashboard_ref(entry: dict[str, Any]) -> str:
    dashboard_path = str(entry.get("dashboard_path") or "")
    return str(
        entry.get("dashboard_url") or build_local_artifact_url(dashboard_path) or dashboard_path
    )


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
        pipeline_path = _pipeline_dashboard_artifact_path(run_id, session_id or None)
        pipeline_dashboard = _dashboard_ref(pipeline_entry)
        if not pipeline_dashboard and Path(pipeline_path).exists():
            pipeline_dashboard = pipeline_path
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
                "pipeline_dashboard": pipeline_dashboard,
                "auto_heal_dashboard": _dashboard_ref(auto_heal),
                "final_audit_dashboard": _dashboard_ref(final_audit),
                "best_dashboard": _dashboard_ref(final_audit)
                or _dashboard_ref(auto_heal)
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
            if len(rows) >= 24:
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
    artifact_server = get_local_artifact_server_status()
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
    resources = [
        {
            "Title": "Quickstart",
            "Kind": "guide",
            "Reference": "analyst://docs/quickstart",
            "Detail": "Human-oriented operating guide for the toolkit.",
        },
        {
            "Title": "Agent Playbook",
            "Kind": "guide",
            "Reference": "analyst://docs/agent-playbook",
            "Detail": "Strict ordered workflow for client agents.",
        },
        {
            "Title": "Capability Catalog",
            "Kind": "catalog",
            "Reference": "analyst://catalog/capabilities",
            "Detail": "Editable config knobs, runtime overlays, and workflow templates.",
        },
        {
            "Title": "Runtime Template",
            "Kind": "template",
            "Reference": "analyst://templates/config/runtime_overlay_template.yaml",
            "Detail": "Cross-cutting run-time controls for input path, run_id, destinations, and artifacts.",
        },
        {
            "Title": "Auto Heal Template",
            "Kind": "template",
            "Reference": "analyst://templates/config/auto_heal_request_template.yaml",
            "Detail": "One-shot remediation request shape with dashboard output.",
        },
        {
            "Title": "Data Dictionary Template",
            "Kind": "template",
            "Reference": "analyst://templates/config/data_dictionary_request_template.yaml",
            "Detail": "Reserved prelaunch dictionary request shape seeded from infer_configs.",
        },
    ]
    resource_groups = [
        {
            "title": "Start Here",
            "intro": (
                "Open these first when you need orientation, a safe execution recipe, or a human-readable "
                "guide before touching module-specific configs."
            ),
            "items": [resources[0], resources[1], resources[3]],
        },
        {
            "title": "Templates And Contracts",
            "intro": (
                "These are the copyable request shapes for runtime overlays, auto-heal, and the "
                "data dictionary workflow."
            ),
            "items": [resources[3], resources[4], resources[5]],
        },
        {
            "title": "Capability Surfaces",
            "intro": (
                "Use these to inspect what the toolkit can do right now and which knobs are safe to edit "
                "without rewriting YAML by hand."
            ),
            "items": [resources[2]],
        },
    ]
    launchpad = [
        {
            "Action": "Ensure Artifact Server",
            "Tool": "ensure_artifact_server",
            "Why": "Start localhost artifact serving so dashboard links open as stable local URLs instead of raw paths.",
        },
        {
            "Action": "Infer Configs",
            "Tool": "infer_configs",
            "Why": "Seed config review and the data-dictionary prelaunch contract from inferred rules.",
        },
        {
            "Action": "Open Pipeline Dashboard",
            "Tool": "get_pipeline_dashboard",
            "Why": "Jump into the tabbed run-level review surface for a specific run.",
        },
        {
            "Action": "Run Auto Heal",
            "Tool": "auto_heal",
            "Why": "Start one-shot remediation when the user explicitly wants automation.",
        },
        {
            "Action": "Inspect Run History",
            "Tool": "get_run_history",
            "Why": "Read the prescription and healing ledger behind dashboard surfaces.",
        },
    ]
    launch_sequences = [
        {
            "title": "Raw Dataset To First Pass",
            "steps": [
                "Run infer_configs to derive a safe initial config shape and identify likely module needs.",
                "Use the runtime overlay template to set run-scoped inputs, paths, and artifact policy in one place.",
                "Open the pipeline dashboard once module outputs exist so review stays in one run-level surface.",
            ],
        },
        {
            "title": "Repair And Certify",
            "steps": [
                "Use auto_heal only when the user explicitly wants one-shot remediation.",
                "Review the auto-heal dashboard before trusting downstream artifacts.",
                "Finish in final_audit or the pipeline dashboard to confirm the healed output is certification-ready.",
            ],
        },
        {
            "title": "Prelaunch Dictionary Path",
            "steps": [
                "Start from infer_configs so the data dictionary inherits inferred types, rules, and high-signal column hints.",
                "Use the data_dictionary request template to keep the prelaunch contract consistent.",
                "Treat the prelaunch report as a cockpit-linked surface, not a disconnected export.",
            ],
        },
    ]
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
        "operator_brief": {
            "title": "Cockpit Briefing",
            "summary": (
                "This cockpit is the control tower for the toolkit. Use it to assess recent run health, "
                "open the strongest available artifact surface, and move into the right guide or tool without "
                "guessing where to start."
            ),
            "lanes": [
                {
                    "title": "Review",
                    "detail": "Start with recent runs and best-available surfaces to see what already exists for the current operating slice.",
                },
                {
                    "title": "Orient",
                    "detail": "Use the resource hub when you need human-readable guidance, templates, or capability references before editing config.",
                },
                {
                    "title": "Act",
                    "detail": "Use the launchpad when you are ready to move from review into execution for a specific tool or workflow.",
                },
            ],
        },
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
        "data_dictionary": {
            "status": str(latest_dictionary.get("status", "not_implemented") or "not_implemented"),
            "template_path": "config/data_dictionary_request_template.yaml",
            "implementation_plan": "local_plans/DATA_DICTIONARY_IMPLEMENTATION_WAVE_2026-03-14.md",
            "latest_run_id": str(latest_dictionary.get("run_id", "")),
            "latest_dashboard": str(
                latest_dictionary.get("dashboard_url")
                or latest_dictionary.get("dashboard_path")
                or latest_dictionary.get("artifact_url")
                or latest_dictionary.get("artifact_path")
                or ""
            ),
            "latest_export": str(
                latest_dictionary.get("xlsx_url")
                or latest_dictionary.get("xlsx_path")
                or latest_dictionary.get("export_url")
                or ""
            ),
            "direction": (
                "The data dictionary should be generated from infer_configs output and surfaced as a "
                "prelaunch report inside the cockpit so users can review structure expectations before "
                "running the rest of the pipeline."
            ),
            "cockpit_preview": latest_dictionary.get("cockpit_preview", {}),
        },
    }


async def _toolkit_get_pipeline_dashboard(run_id: str, session_id: str | None = None) -> dict:
    history = get_run_history(run_id, session_id=session_id)
    history_meta = get_last_history_read_meta(run_id, session_id=session_id)
    health = build_data_health_report(
        run_id=run_id,
        session_id=session_id,
        history=history,
        history_meta=history_meta,
    )

    latest_by_module: dict[str, dict[str, Any]] = {}
    for entry in history:
        module = str(entry.get("module", "")).strip()
        if module:
            latest_by_module[module] = entry

    module_order = [
        "diagnostics",
        "auto_heal",
        "normalization",
        "duplicates",
        "outliers",
        "outlier_handling",
        "imputation",
        "validation",
        "final_audit",
    ]

    modules: dict[str, dict[str, Any]] = {}
    ready_modules = warned_modules = failed_modules = not_run_modules = 0
    for module in module_order:
        entry = latest_by_module.get(module, {})
        if not entry:
            status = "not_run"
            not_run_modules += 1
        else:
            status = str(entry.get("status", "unknown")).lower()
            if status in {"pass", "available"}:
                ready_modules += 1
            elif status in {"fail", "error"}:
                failed_modules += 1
            else:
                warned_modules += 1
        modules[_module_display_name(module)] = {
            "status": "not_run" if not entry else entry.get("status", "unknown"),
            "summary": entry.get("summary", {}),
            "dashboard_path": entry.get("dashboard_path", ""),
            "dashboard_url": entry.get("dashboard_url", ""),
            "artifact_url": entry.get("artifact_url", ""),
            "export_url": entry.get("export_url", ""),
            "warnings": entry.get("warnings", []),
        }

    final_audit_entry = latest_by_module.get("final_audit", {})
    auto_heal_entry = latest_by_module.get("auto_heal", {})
    final_status = str(
        final_audit_entry.get("status") or auto_heal_entry.get("status") or "unknown"
    )
    effective_session_id = session_id or ""
    safe_run_id = _safe_run_id_for_path(run_id)

    report = {
        "run_id": run_id,
        "session_id": effective_session_id,
        "final_status": final_status,
        "health_score": health.get("health_score", "N/A"),
        "health_status": health.get("health_status", "unknown"),
        "ready_modules": ready_modules,
        "warned_modules": warned_modules,
        "failed_modules": failed_modules,
        "not_run_modules": not_run_modules,
        "module_order": list(modules.keys()),
        "modules": {
            name: {
                "status": payload.get("status", "unknown"),
                "summary": payload.get("summary", {}),
                "dashboard_url": payload.get("dashboard_url", ""),
                "dashboard_path": payload.get("dashboard_path", ""),
                "artifact_url": payload.get("artifact_url", ""),
                "export_url": payload.get("export_url", ""),
                "warnings": payload.get("warnings", []),
            }
            for name, payload in modules.items()
        },
        "final_dashboard_url": final_audit_entry.get("dashboard_url")
        or auto_heal_entry.get("dashboard_url", ""),
        "final_dashboard_path": final_audit_entry.get("dashboard_path")
        or auto_heal_entry.get("dashboard_path", ""),
        "final_export_url": final_audit_entry.get("export_url")
        or auto_heal_entry.get("export_url", ""),
    }

    artifact_path = _pipeline_dashboard_artifact_path(run_id, effective_session_id or None)
    artifact_delivery = empty_delivery_state()
    artifact_url = ""
    output_path = export_html_report(report, artifact_path, "Pipeline Dashboard", safe_run_id)
    artifact_delivery = deliver_artifact(
        output_path,
        run_id=run_id,
        module="pipeline_dashboard",
        config={},
        session_id=effective_session_id or None,
    )
    artifact_path = str(artifact_delivery.get("local_path", output_path))
    artifact_url = str(artifact_delivery.get("url", ""))

    res = {
        "status": "pass",
        "module": "pipeline_dashboard",
        "run_id": run_id,
        "session_id": effective_session_id,
        "summary": {
            "health_score": report["health_score"],
            "health_status": report["health_status"],
            "ready_modules": ready_modules,
            "warned_modules": warned_modules,
            "failed_modules": failed_modules,
            "not_run_modules": not_run_modules,
        },
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "destination_delivery": {
            "html_report": compact_destination_metadata(artifact_delivery["destinations"])
        },
        "warnings": list(artifact_delivery.get("warnings", [])),
    }
    res = with_dashboard_artifact(
        res, artifact_path=artifact_path, artifact_url=artifact_url, label="Pipeline dashboard"
    )
    append_to_run_history(run_id, res, session_id=effective_session_id or None)
    res = with_next_actions(
        res,
        [
            next_action(
                "get_run_history",
                "Review the underlying module ledger that powers this pipeline dashboard.",
                {"run_id": run_id, "session_id": report["session_id"]},
            )
        ],
    )
    return res


async def _toolkit_get_cockpit_dashboard(limit: int = 8) -> dict:
    if not TRUSTED_HISTORY_ENABLED:
        return _trusted_history_denial()
    try:
        limit = max(1, min(int(limit), 50))
        report = _build_cockpit_dashboard_report(limit)
        artifact_key = _cockpit_artifact_key(limit)
        artifact_path = f"exports/reports/cockpit/{artifact_key}.html"
        artifact_delivery = empty_delivery_state()
        output_path = export_html_report(report, artifact_path, "Cockpit Dashboard", artifact_key)
        artifact_delivery = deliver_artifact(
            output_path,
            run_id=artifact_key,
            module="cockpit_dashboard",
            config={"upload_artifacts": False},
            session_id=None,
        )
        local_path = str(artifact_delivery.get("local_path", output_path))
        artifact_url = str(artifact_delivery.get("url", ""))
        res = {
            "status": "pass",
            "module": "cockpit_dashboard",
            "summary": report["overview"],
            "artifact_path": local_path,
            "artifact_url": artifact_url,
            "destination_delivery": {
                "html_report": compact_destination_metadata(artifact_delivery["destinations"])
            },
            "warnings": list(artifact_delivery.get("warnings", [])),
        }
        res = with_dashboard_artifact(
            res,
            artifact_path=local_path,
            artifact_url=artifact_url,
            label="Cockpit dashboard",
        )
        return with_next_actions(
            res,
            [
                next_action(
                    "get_capability_catalog",
                    "Open the cockpit capability catalog to inspect editable knobs, templates, and workflow surfaces.",
                    {},
                ),
                next_action(
                    "get_user_quickstart",
                    "Open the quickstart guide for the operator/resource hub content surfaced in the cockpit dashboard.",
                    {},
                ),
                next_action(
                    "ensure_artifact_server",
                    "Start the local artifact server so cockpit and module dashboard links resolve as local URLs.",
                    {},
                ),
            ],
        )
    except Exception:
        logger.exception("Failed to build cockpit dashboard")
        failure = {
            "status": "fail",
            "module": "cockpit_dashboard",
            "summary": {"code": "COCKPIT_BUILD_FAILED"},
            "artifact_path": "",
            "artifact_url": "",
            "destination_delivery": {
                "html_report": compact_destination_metadata(empty_delivery_state()["destinations"])
            },
            "warnings": ["COCKPIT_BUILD_FAILED"],
        }
        failure = with_dashboard_artifact(
            failure,
            artifact_path="",
            artifact_url="",
            label="",
        )
        return with_next_actions(failure, [])


async def _toolkit_ensure_artifact_server() -> dict:
    if not _artifact_server_control_enabled():
        return {
            "status": "error",
            "module": "artifact_server",
            "code": "ARTIFACT_SERVER_CONTROL_DISABLED",
            "message": (
                "Artifact server control is disabled. Enable ANALYST_MCP_ENABLE_ARTIFACT_SERVER_TOOL=1 "
                "or use trusted/local stdio mode."
            ),
            "warnings": [],
        }

    result = ensure_local_artifact_server()
    status = "pass" if result.get("running") else "warn"
    return with_next_actions(
        {
            "status": status,
            "module": "artifact_server",
            "summary": {
                "running": bool(result.get("running")),
                "enabled": bool(result.get("enabled")),
                "already_running": bool(result.get("already_running")),
            },
            "base_url": str(result.get("base_url", "")),
            "root": str(result.get("root", "")),
            "warnings": ([] if result.get("running") else [str(result.get("message", ""))]),
        },
        [
            next_action(
                "get_cockpit_dashboard",
                "Open the cockpit after the artifact server is available so recent dashboard links use served local URLs.",
                {},
            )
        ],
    )


register_tool(
    name="get_agent_playbook",
    fn=_toolkit_get_agent_playbook,
    description="Returns structured, ordered execution guidance for client agents.",
    input_schema={"type": "object", "properties": {}},
)

register_tool(
    name="get_user_quickstart",
    fn=_toolkit_get_user_quickstart,
    description="Returns concise, human-readable usage guidance and config examples.",
    input_schema={"type": "object", "properties": {}},
)

register_tool(
    name="get_capability_catalog",
    fn=_toolkit_get_capability_catalog,
    description="Returns module capability knobs sourced from YAML templates, including defaults.",
    input_schema=CAPABILITY_CATALOG_INPUT_SCHEMA,
)

register_tool(
    name="ensure_artifact_server",
    fn=_toolkit_ensure_artifact_server,
    description="Ensure the opt-in localhost artifact web server is running for exported dashboard URLs.",
    input_schema=ARTIFACT_SERVER_INPUT_SCHEMA,
)

register_tool(
    name="get_golden_templates",
    fn=_toolkit_get_golden_templates,
    description="Returns a library of 'Golden Config' templates for common use cases.",
    input_schema={"type": "object", "properties": {}},
)

register_tool(
    name="get_run_history",
    fn=_toolkit_get_run_history,
    description="Returns the 'Prescription & Healing Ledger' for a run.",
    input_schema=RUN_HISTORY_INPUT_SCHEMA,
)

register_tool(
    name="get_data_health_report",
    fn=_toolkit_get_data_health_report,
    description="Returns a Visual Data Health Score (0-100) for a run.",
    input_schema=DATA_HEALTH_REPORT_INPUT_SCHEMA,
)

register_tool(
    name="get_cockpit_dashboard",
    fn=_toolkit_get_cockpit_dashboard,
    description="Builds an operator cockpit dashboard with recent runs, resources, and launch surfaces.",
    input_schema=COCKPIT_DASHBOARD_INPUT_SCHEMA,
)

register_tool(
    name="get_pipeline_dashboard",
    fn=_toolkit_get_pipeline_dashboard,
    description="Builds a tabbed pipeline dashboard artifact from run history and module outputs.",
    input_schema=PIPELINE_DASHBOARD_INPUT_SCHEMA,
)
