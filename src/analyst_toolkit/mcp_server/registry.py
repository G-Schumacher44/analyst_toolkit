"""
registry.py — Tool registry for the MCP server to avoid circular imports.
"""

import inspect
import logging
from typing import Any

from analyst_toolkit.mcp_server.input.errors import InputError, client_safe_input_error_code
from analyst_toolkit.mcp_server.response_utils import (
    attach_trace_id,
    build_error_envelope,
    new_trace_id,
)

logger = logging.getLogger("analyst_toolkit.mcp_server.registry")

# Tool registry: tool_name → {fn, description, inputSchema}
TOOL_REGISTRY: dict[str, dict[str, Any]] = {}


def _input_error_remediation(code: str) -> str:
    if code == "INPUT_PAYLOAD_TOO_LARGE":
        return (
            "Reduce input size, narrow the selected prefix, or raise the relevant "
            "ANALYST_MCP_MAX_INPUT_* limit if this workload is expected."
        )
    if code == "INPUT_NOT_SUPPORTED":
        return (
            "Use a supported source or file format: upload/server-visible .csv/.parquet, or gs://."
        )
    if code == "INPUT_PATH_DENIED":
        return (
            "Use a server-visible path, update ANALYST_MCP_ALLOWED_INPUT_ROOTS, "
            "upload the file, or switch to gs://."
        )
    if code == "INPUT_CONFLICT":
        return "Retry with a unique idempotency key or the same canonical source reference."
    if code == "INPUT_NOT_FOUND" or code == "INPUT_PATH_NOT_FOUND":
        return "Check the requested input_id or path and retry with an existing resource."
    return "Verify input arguments and retry."


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
        except InputError as exc:
            normalized_code = client_safe_input_error_code(exc.code)
            message = str(exc)
            logger.warning(
                "Tool '%s' rejected input at trust boundary (trace_id=%s, code=%s)",
                name,
                trace_id,
                normalized_code,
            )
            return {
                "status": "error",
                "module": name,
                "code": normalized_code,
                "message": message,
                "error": build_error_envelope(
                    category="io",
                    code=normalized_code.lower(),
                    message=message,
                    remediation=_input_error_remediation(normalized_code),
                    retryable=False,
                    trace_id=trace_id,
                ),
                "trace_id": trace_id,
            }
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
