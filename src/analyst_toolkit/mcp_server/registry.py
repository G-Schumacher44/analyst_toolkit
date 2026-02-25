"""
registry.py — Tool registry for the MCP server to avoid circular imports.
"""

import inspect
import logging
from typing import Any

from analyst_toolkit.mcp_server.response_utils import (
    attach_trace_id,
    build_error_envelope,
    new_trace_id,
)

logger = logging.getLogger("analyst_toolkit.mcp_server.registry")

# Tool registry: tool_name → {fn, description, inputSchema}
TOOL_REGISTRY: dict[str, dict[str, Any]] = {}


def register_tool(name: str, fn, description: str, input_schema: dict) -> None:
    """
    Register an async callable as an MCP tool.
    """
    async def _wrapped_fn(**kwargs):
        trace_id = new_trace_id()
        try:
            result = fn(**kwargs)
            if inspect.isawaitable(result):
                result = await result
            return attach_trace_id(result, trace_id)
        except Exception as exc:
            logger.exception("Tool '%s' failed (trace_id=%s)", name, trace_id)
            return {
                "status": "error",
                "module": name,
                "error": build_error_envelope(
                    category="internal",
                    code="tool_execution_failed",
                    message=f"{type(exc).__name__}: {str(exc)}",
                    remediation=(
                        "Verify tool arguments and environment prerequisites. "
                        "If the issue persists, inspect server logs using trace_id."
                    ),
                    retryable=False,
                    trace_id=trace_id,
                ),
                "trace_id": trace_id,
            }

    TOOL_REGISTRY[name] = {
        "fn": _wrapped_fn,
        "description": description,
        "inputSchema": input_schema,
    }
    logger.info(f"Registered tool: {name}")
