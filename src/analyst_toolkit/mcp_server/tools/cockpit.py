"""MCP tool: cockpit — user/agent guidance, capability catalog, history, and health scoring."""

import asyncio
import logging
import os
import re
import uuid
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
    new_trace_id,
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
from analyst_toolkit.mcp_server.tools.cockpit_history import (
    _artifact_root_label,
    _build_cockpit_dashboard_report,
    _build_recent_run_cards,
    _module_display_name,
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
from analyst_toolkit.mcp_server.tools.cockpit_templates import (
    build_cockpit_launch_sequences,
    build_cockpit_launchpad,
    build_cockpit_operator_brief,
    build_cockpit_resource_groups,
    build_cockpit_resources,
    build_data_dictionary_lane,
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


def _trusted_history_enabled() -> bool:
    return _env_bool(
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
        "trace_id": new_trace_id(),
        "message": (
            "Cockpit history access is disabled. Enable ANALYST_MCP_ENABLE_TRUSTED_HISTORY_TOOL=1 "
            "or use trusted/local stdio mode."
        ),
        "warnings": [],
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
        "health_advisory": bool(health.get("health_advisory", False)),
        "health_message": str(health.get("message", "")),
        "certification_status": str(health.get("certification_status", "not_run")),
        "certification_passed": health.get("certification_passed"),
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

    warnings = list(artifact_delivery.get("warnings", []))
    warnings.extend(str(item) for item in health.get("warnings", []) if str(item).strip())

    res = {
        "status": "pass",
        "module": "pipeline_dashboard",
        "run_id": run_id,
        "session_id": effective_session_id,
        "summary": {
            "health_score": report["health_score"],
            "health_status": report["health_status"],
            "health_advisory": report["health_advisory"],
            "certification_status": report["certification_status"],
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
        "warnings": warnings,
    }
    res = with_dashboard_artifact(
        res, artifact_path=artifact_path, artifact_url=artifact_url, label="Pipeline dashboard"
    )
    existing_pipeline_dashboard = latest_by_module.get("pipeline_dashboard", {})
    if not existing_pipeline_dashboard:
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
    if not _trusted_history_enabled():
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
                next_action(
                    "manage_session",
                    "List or fork sessions to start a new run context from existing data.",
                    {"action": "list"},
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

    try:
        result = ensure_local_artifact_server()
    except ValueError as exc:
        trace_id = str(uuid.uuid4())
        logger.exception(
            "Artifact server configuration invalid trace_id=%s",
            trace_id,
            exc_info=exc,
        )
        return {
            "status": "error",
            "module": "artifact_server",
            "code": "ARTIFACT_SERVER_CONFIG_INVALID",
            "trace_id": trace_id,
            "message": "Artifact server configuration invalid; see trace_id.",
            "warnings": [],
        }
    result_status = str(result.get("status", "")).lower()
    status = "error" if result_status == "error" else ("pass" if result.get("running") else "warn")
    message = str(result.get("message", "")).strip()
    payload = {
        "status": status,
        "module": "artifact_server",
        "summary": {
            "running": bool(result.get("running")),
            "enabled": bool(result.get("enabled")),
            "already_running": bool(result.get("already_running")),
        },
        "base_url": str(result.get("base_url", "")),
        "root": _artifact_root_label(result.get("root", "")),
        "warnings": [] if result.get("running") else ([message] if message else []),
    }
    if result.get("error_code"):
        payload["code"] = str(result["error_code"])
    return with_next_actions(
        payload,
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
