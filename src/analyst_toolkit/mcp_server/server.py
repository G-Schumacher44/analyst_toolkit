"""
server.py — analyst_toolkit MCP Server (Dual Transport)

Supports two modes:
1. HTTP (/rpc) — JSON-RPC 2.0 compatible with fridai-core HTTPRemoteClient.
2. Stdio         — Official MCP protocol for desktop hosts (Claude Desktop, etc.)

Tools self-register by calling register_tool() at import time.
The transport mode is selected via CLI flag --stdio or environment variable.

Start HTTP:
    python -m analyst_toolkit.mcp_server.server

Start Stdio:
    python -m analyst_toolkit.mcp_server.server --stdio
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import mcp.types as types
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.server import NotificationOptions, Server
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server

from analyst_toolkit.mcp_server.registry import TOOL_REGISTRY
from analyst_toolkit.mcp_server.response_utils import (
    attach_trace_id,
    build_error_envelope,
    new_trace_id,
)
from analyst_toolkit.mcp_server.templates import list_template_resources, read_template_resource

# Get package version dynamically
try:
    __version__ = version("analyst_toolkit")
except PackageNotFoundError:
    __version__ = "0.4.0"  # Fallback if not installed as package

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stderr,  # Log to stderr to avoid polluting stdout in stdio mode
)
logger = logging.getLogger("analyst_toolkit.mcp_server")


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
    return value.strip().lower() in {"1", "true", "yes", "on"}


RESOURCE_IO_TIMEOUT_SEC = _env_float("ANALYST_MCP_RESOURCE_TIMEOUT_SEC", 8.0)
ADVERTISE_RESOURCE_TEMPLATES = _env_bool("ANALYST_MCP_ADVERTISE_RESOURCE_TEMPLATES", False)

# Official MCP SDK server instance
mcp_server = Server("analyst-toolkit")

# FastAPI app for HTTP transport
app = FastAPI(title="analyst-toolkit MCP Server", version=__version__)

SERVER_INFO = {
    "protocolVersion": "2024-05-01",
    "serverInfo": {"name": "analyst-toolkit", "version": __version__},
    "capabilities": {"tools": {}, "resources": {"subscribe": False, "listChanged": False}},
}


@mcp_server.list_tools()
async def list_tools() -> list[types.Tool]:
    """Standard MCP tools/list handler."""
    return [
        types.Tool(
            name=name,
            description=meta["description"],
            inputSchema=meta["inputSchema"],
        )
        for name, meta in TOOL_REGISTRY.items()
    ]


@mcp_server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
    """Centralized tool dispatcher for the official MCP protocol (stdio)."""
    if name not in TOOL_REGISTRY:
        return [types.TextContent(type="text", text=f"Error: Unknown tool {name}")]

    try:
        fn = TOOL_REGISTRY[name]["fn"]
        res = await fn(**(arguments or {}))
        return [types.TextContent(type="text", text=json.dumps(res, indent=2))]
    except Exception as exc:
        logger.exception(f"Tool {name} failed")
        return [types.TextContent(type="text", text=f"Error: {str(exc)}")]


def _resource_models() -> list[types.Resource]:
    return [
        types.Resource(
            name=item["name"],
            uri=item["uri"],
            description=item["description"],
            mimeType=item["mimeType"],
        )
        for item in list_template_resources()
    ]


def _resource_template_models() -> list[types.ResourceTemplate]:
    return [
        types.ResourceTemplate(
            name="config_templates",
            uriTemplate="analyst://templates/config/{name}_template.yaml",
            description="Standard toolkit config templates (*_template.yaml).",
            mimeType="application/x-yaml",
        ),
        types.ResourceTemplate(
            name="golden_templates",
            uriTemplate="analyst://templates/golden/{name}.yaml",
            description="Golden templates for common use cases (fraud, migration, compliance).",
            mimeType="application/x-yaml",
        ),
    ]


async def _read_template_with_timeout(uri: str) -> str:
    return await asyncio.wait_for(
        asyncio.to_thread(read_template_resource, uri),
        timeout=RESOURCE_IO_TIMEOUT_SEC,
    )


async def _resource_models_with_timeout() -> list[types.Resource]:
    return await asyncio.wait_for(
        asyncio.to_thread(_resource_models),
        timeout=RESOURCE_IO_TIMEOUT_SEC,
    )


@mcp_server.list_resources()
async def list_resources() -> list[types.Resource]:
    """Standard MCP resources/list handler."""
    return await _resource_models_with_timeout()


@mcp_server.list_resource_templates()
async def list_resource_templates() -> list[types.ResourceTemplate]:
    """Standard MCP resources/templates/list handler."""
    if not ADVERTISE_RESOURCE_TEMPLATES:
        return []
    return _resource_template_models()


@mcp_server.read_resource()
async def read_resource(uri: Any) -> list[ReadResourceContents]:
    """Standard MCP resources/read handler."""
    uri_text = str(uri)
    text = await _read_template_with_timeout(uri_text)
    return [ReadResourceContents(content=text, mime_type="application/x-yaml")]


# --- HTTP /rpc JSON-RPC Handlers (FridAI Legacy/Native) ---


def _rpc_error(req_id: Any, code: int, message: str, data: dict | None = None) -> dict:
    error_obj: dict[str, Any] = {"code": code, "message": message}
    if data:
        error_obj["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": error_obj}


def _rpc_ok(req_id: Any, result: Any) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


@app.post("/rpc")
async def rpc_handler(request: Request) -> JSONResponse:
    """HTTP JSON-RPC 2.0 dispatcher for FridAI."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(_rpc_error(None, -32700, "Parse error"), status_code=200)

    req_id = body.get("id")
    method = body.get("method", "")
    params = body.get("params", {})

    logger.info(f"RPC request: method={method} id={req_id}")

    if method == "initialize":
        return JSONResponse(_rpc_ok(req_id, SERVER_INFO))

    if method == "tools/list":
        tools = [
            {
                "name": name,
                "description": meta["description"],
                "inputSchema": meta["inputSchema"],
            }
            for name, meta in TOOL_REGISTRY.items()
        ]
        return JSONResponse(_rpc_ok(req_id, {"tools": tools}))

    if method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        if not tool_name:
            return JSONResponse(_rpc_error(req_id, -32602, "Missing 'name' in params"))

        if tool_name not in TOOL_REGISTRY:
            return JSONResponse(_rpc_error(req_id, -32601, f"Tool not found: {tool_name}"))

        try:
            result = await TOOL_REGISTRY[tool_name]["fn"](**arguments)
            return JSONResponse(_rpc_ok(req_id, attach_trace_id(result)))
        except Exception as exc:
            trace_id = new_trace_id()
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
            # Return proper JSON-RPC 2.0 error
            return JSONResponse(
                _rpc_error(
                    req_id,
                    -32603,
                    f"Internal error: {str(exc)} (trace_id={trace_id})",
                    data={"error": envelope},
                )
            )

    if method == "resources/list":
        try:
            model_list = await _resource_models_with_timeout()
        except asyncio.TimeoutError:
            trace_id = new_trace_id()
            return JSONResponse(
                _rpc_error(
                    req_id,
                    -32000,
                    (
                        "Resource listing timed out. "
                        f"Try increasing ANALYST_MCP_RESOURCE_TIMEOUT_SEC (current={RESOURCE_IO_TIMEOUT_SEC}s)."
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
                )
            )
        resources = [
            r.model_dump(mode="json", by_alias=True, exclude_none=True) for r in model_list
        ]
        return JSONResponse(_rpc_ok(req_id, {"resources": resources}))

    if method == "resources/templates/list":
        if not ADVERTISE_RESOURCE_TEMPLATES:
            return JSONResponse(_rpc_ok(req_id, {"resourceTemplates": []}))
        templates = [
            t.model_dump(mode="json", by_alias=True, exclude_none=True)
            for t in _resource_template_models()
        ]
        return JSONResponse(_rpc_ok(req_id, {"resourceTemplates": templates}))

    if method == "resources/read":
        uri = params.get("uri")
        if not isinstance(uri, str) or not uri:
            trace_id = new_trace_id()
            return JSONResponse(
                _rpc_error(
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
                )
            )
        try:
            text = await _read_template_with_timeout(uri)
        except FileNotFoundError as exc:
            trace_id = new_trace_id()
            return JSONResponse(
                _rpc_error(
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
                )
            )
        except asyncio.TimeoutError:
            trace_id = new_trace_id()
            return JSONResponse(
                _rpc_error(
                    req_id,
                    -32000,
                    (
                        "Resource read timed out. "
                        f"Try increasing ANALYST_MCP_RESOURCE_TIMEOUT_SEC (current={RESOURCE_IO_TIMEOUT_SEC}s)."
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
                )
            )
        except Exception as exc:
            trace_id = new_trace_id()
            logger.exception("Resource read failed")
            return JSONResponse(
                _rpc_error(
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
                )
            )

        return JSONResponse(
            _rpc_ok(
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
            )
        )

    return JSONResponse(_rpc_error(req_id, -32601, f"Unknown method: {method}"))


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "tools": list(TOOL_REGISTRY.keys())}


# --- Tool Imports (Triggers Self-Registration) ---

from analyst_toolkit.mcp_server.tools import (  # noqa: F401, E402
    auto_heal,
    cockpit,
    config_schema,
    diagnostics,
    drift,
    duplicates,
    final_audit,
    imputation,
    infer_configs,
    jobs,
    normalization,
    outliers,
    preflight_config,
    validation,
)

# --- Entry point and transport selection ---


async def run_stdio():
    """Run the server using official stdio transport."""
    async with stdio_server() as (read_stream, write_stream):
        await mcp_server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="analyst-toolkit",
                server_version=__version__,
                capabilities=mcp_server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


def main():
    parser = argparse.ArgumentParser(description="Analyst Toolkit MCP Server")
    parser.add_argument("--stdio", action="store_true", help="Run in stdio mode for desktop hosts")
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("ANALYST_MCP_PORT", 8001)),
        help="HTTP port (default: 8001)",
    )
    args = parser.parse_args()

    if args.stdio or os.environ.get("ANALYST_MCP_STDIO", "").lower() == "true":
        logger.info("Starting Analyst Toolkit MCP Server in stdio mode")
        asyncio.run(run_stdio())
    else:
        logger.info(f"Starting Analyst Toolkit MCP Server in HTTP mode on port {args.port}")
        import uvicorn

        uvicorn.run(
            "analyst_toolkit.mcp_server.server:app",
            host="0.0.0.0",
            port=args.port,
            log_level="info",
        )


if __name__ == "__main__":
    main()
