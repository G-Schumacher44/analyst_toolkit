"""JSON-RPC method dispatch helpers for MCP HTTP transport."""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import mcp.types as types

from analyst_toolkit.mcp_server.response_utils import (
    attach_trace_id,
    build_error_envelope,
)


@dataclass(frozen=True)
class RpcDispatchResult:
    payload: dict[str, Any]
    ok: bool
    level: int = logging.INFO
    error_code: int | None = None
    run_id: str | None = None
    session_id: str | None = None


def rpc_error(req_id: Any, code: int, message: str, data: dict | None = None) -> dict:
    error_obj: dict[str, Any] = {"code": code, "message": message}
    if data:
        error_obj["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": error_obj}


def rpc_ok(req_id: Any, result: Any) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


async def dispatch_rpc_method(
    *,
    req_id: Any,
    method: str,
    params: dict[str, Any],
    server_info: dict[str, Any],
    tool_registry: dict[str, dict[str, Any]],
    advertise_resource_templates: bool,
    resource_io_timeout_sec: float,
    resource_models_with_timeout: Callable[[], Awaitable[list[types.Resource]]],
    resource_template_models: Callable[[], list[types.ResourceTemplate]],
    read_template_with_timeout: Callable[[str], Awaitable[str]],
    trace_id: str,
    logger: logging.Logger,
) -> RpcDispatchResult:
    if method == "initialize":
        return RpcDispatchResult(payload=rpc_ok(req_id, server_info), ok=True)

    if method == "tools/list":
        tools = [
            {
                "name": name,
                "description": meta["description"],
                "inputSchema": meta["inputSchema"],
            }
            for name, meta in tool_registry.items()
        ]
        return RpcDispatchResult(payload=rpc_ok(req_id, {"tools": tools}), ok=True)

    if method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        if not tool_name:
            return RpcDispatchResult(
                payload=rpc_error(req_id, -32602, "Missing 'name' in params"),
                ok=False,
                level=logging.WARNING,
                error_code=-32602,
            )

        if tool_name not in tool_registry:
            return RpcDispatchResult(
                payload=rpc_error(req_id, -32601, f"Tool not found: {tool_name}"),
                ok=False,
                level=logging.WARNING,
                error_code=-32601,
            )

        try:
            result = await tool_registry[tool_name]["fn"](**arguments)
            result = attach_trace_id(result, trace_id=trace_id)
            run_id = result.get("run_id") if isinstance(result, dict) else None
            session_id = result.get("session_id") if isinstance(result, dict) else None
            return RpcDispatchResult(
                payload=rpc_ok(req_id, result),
                ok=True,
                run_id=run_id,
                session_id=session_id,
            )
        except Exception as exc:
            envelope = build_error_envelope(
                category="internal",
                code="rpc_tools_call_internal_error",
                message=f"{type(exc).__name__}: {str(exc)}",
                remediation=(
                    "Retry once for transient failures. "
                    "If it continues, inspect server logs with trace_id."
                ),
                retryable=False,
                trace_id=trace_id,
            )
            logger.exception(f"Tool {tool_name} raised an error")
            return RpcDispatchResult(
                payload=rpc_error(
                    req_id,
                    -32603,
                    f"Internal error: {str(exc)} (trace_id={trace_id})",
                    data={"error": envelope},
                ),
                ok=False,
                level=logging.ERROR,
                error_code=-32603,
            )

    if method == "resources/list":
        try:
            model_list = await resource_models_with_timeout()
        except asyncio.TimeoutError:
            return RpcDispatchResult(
                payload=rpc_error(
                    req_id,
                    -32000,
                    (
                        "Resource listing timed out. "
                        f"Try increasing ANALYST_MCP_RESOURCE_TIMEOUT_SEC (current={resource_io_timeout_sec}s)."
                    ),
                    data={
                        "error": build_error_envelope(
                            category="transport",
                            code="resources_list_timeout",
                            message="Template resource listing exceeded configured timeout.",
                            remediation=(
                                "Increase ANALYST_MCP_RESOURCE_TIMEOUT_SEC and retry. "
                                "Check template storage I/O latency."
                            ),
                            retryable=True,
                            trace_id=trace_id,
                        )
                    },
                ),
                ok=False,
                level=logging.WARNING,
                error_code=-32000,
            )
        resources = [
            r.model_dump(mode="json", by_alias=True, exclude_none=True) for r in model_list
        ]
        return RpcDispatchResult(payload=rpc_ok(req_id, {"resources": resources}), ok=True)

    if method == "resources/templates/list":
        if not advertise_resource_templates:
            return RpcDispatchResult(payload=rpc_ok(req_id, {"resourceTemplates": []}), ok=True)
        templates = [
            t.model_dump(mode="json", by_alias=True, exclude_none=True)
            for t in resource_template_models()
        ]
        return RpcDispatchResult(payload=rpc_ok(req_id, {"resourceTemplates": templates}), ok=True)

    if method == "resources/read":
        uri = params.get("uri")
        if not isinstance(uri, str) or not uri:
            return RpcDispatchResult(
                payload=rpc_error(
                    req_id,
                    -32602,
                    "Missing or invalid 'uri' in params",
                    data={
                        "error": build_error_envelope(
                            category="config",
                            code="invalid_resource_uri",
                            message="resources/read requires a non-empty string URI.",
                            remediation="Pass a valid analyst://templates/... URI from resources/list.",
                            retryable=False,
                            trace_id=trace_id,
                        )
                    },
                ),
                ok=False,
                level=logging.WARNING,
                error_code=-32602,
            )
        try:
            text = await read_template_with_timeout(uri)
        except FileNotFoundError as exc:
            return RpcDispatchResult(
                payload=rpc_error(
                    req_id,
                    -32602,
                    f"Resource not found: {str(exc)}",
                    data={
                        "error": build_error_envelope(
                            category="io",
                            code="resource_not_found",
                            message=f"Template resource not found for URI: {uri}",
                            remediation="Refresh resources/list and retry with an existing URI.",
                            retryable=False,
                            trace_id=trace_id,
                        )
                    },
                ),
                ok=False,
                level=logging.WARNING,
                error_code=-32602,
            )
        except asyncio.TimeoutError:
            return RpcDispatchResult(
                payload=rpc_error(
                    req_id,
                    -32000,
                    (
                        "Resource read timed out. "
                        f"Try increasing ANALYST_MCP_RESOURCE_TIMEOUT_SEC (current={resource_io_timeout_sec}s)."
                    ),
                    data={
                        "error": build_error_envelope(
                            category="transport",
                            code="resource_read_timeout",
                            message=f"Template read timed out for URI: {uri}",
                            remediation=(
                                "Retry once. If repeated, increase ANALYST_MCP_RESOURCE_TIMEOUT_SEC "
                                "and validate storage responsiveness."
                            ),
                            retryable=True,
                            trace_id=trace_id,
                        )
                    },
                ),
                ok=False,
                level=logging.WARNING,
                error_code=-32000,
            )
        except Exception as exc:
            logger.exception("Resource read failed")
            return RpcDispatchResult(
                payload=rpc_error(
                    req_id,
                    -32603,
                    f"Internal error: {str(exc)} (trace_id={trace_id})",
                    data={
                        "error": build_error_envelope(
                            category="internal",
                            code="resource_read_internal_error",
                            message=f"{type(exc).__name__}: {str(exc)}",
                            remediation=(
                                "Retry once. If it persists, inspect server logs using trace_id."
                            ),
                            retryable=False,
                            trace_id=trace_id,
                        )
                    },
                ),
                ok=False,
                level=logging.ERROR,
                error_code=-32603,
            )

        return RpcDispatchResult(
            payload=rpc_ok(
                req_id,
                {
                    "contents": [
                        {
                            "uri": uri,
                            "mimeType": "application/x-yaml",
                            "text": text,
                        }
                    ]
                },
            ),
            ok=True,
        )

    return RpcDispatchResult(
        payload=rpc_error(req_id, -32601, f"Unknown method: {method}"),
        ok=False,
        level=logging.WARNING,
        error_code=-32601,
    )
