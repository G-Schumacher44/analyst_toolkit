"""MCP tool: cockpit — user/agent guidance, capability catalog, history, and health scoring."""

import asyncio
import logging
import os
from typing import Any

from analyst_toolkit.mcp_server.io import get_last_history_read_meta, get_run_history
from analyst_toolkit.mcp_server.registry import register_tool
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
