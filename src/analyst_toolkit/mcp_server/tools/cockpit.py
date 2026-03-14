"""MCP tool: cockpit — user/agent guidance, capability catalog, history, and health scoring."""

import asyncio
import logging
import os
from typing import Any

from analyst_toolkit.m00_utils.export_utils import export_html_report
from analyst_toolkit.mcp_server.io import (
    compact_destination_metadata,
    deliver_artifact,
    empty_delivery_state,
    get_last_history_read_meta,
    get_run_history,
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
    CAPABILITY_CATALOG_INPUT_SCHEMA,
    DATA_HEALTH_REPORT_INPUT_SCHEMA,
    PIPELINE_DASHBOARD_INPUT_SCHEMA,
    RUN_HISTORY_INPUT_SCHEMA,
)

logger = logging.getLogger("analyst_toolkit.mcp_server.cockpit")


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
        final_audit_entry.get("status")
        or auto_heal_entry.get("status")
        or (history[-1].get("status") if history else "unknown")
    )

    report = {
        "run_id": run_id,
        "session_id": session_id or str((history[-1].get("session_id") if history else "") or ""),
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

    artifact_path = f"exports/reports/pipeline/{run_id}_pipeline_dashboard.html"
    artifact_delivery = empty_delivery_state()
    artifact_url = ""
    output_path = export_html_report(report, artifact_path, "Pipeline Dashboard", run_id)
    artifact_delivery = deliver_artifact(
        output_path,
        run_id=run_id,
        module="pipeline_dashboard",
        config={},
        session_id=report["session_id"] or None,
    )
    artifact_path = str(artifact_delivery.get("local_path", output_path))
    artifact_url = str(artifact_delivery.get("url", ""))

    res = {
        "status": "pass",
        "module": "pipeline_dashboard",
        "run_id": run_id,
        "session_id": report["session_id"],
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
    name="get_pipeline_dashboard",
    fn=_toolkit_get_pipeline_dashboard,
    description="Builds a tabbed pipeline dashboard artifact from run history and module outputs.",
    input_schema=PIPELINE_DASHBOARD_INPUT_SCHEMA,
)
