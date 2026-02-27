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
import time
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import mcp.types as types
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.server import NotificationOptions, Server
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server

from analyst_toolkit.mcp_server.auth import is_authorized
from analyst_toolkit.mcp_server.observability import RuntimeMetrics, log_rpc_event
from analyst_toolkit.mcp_server.registry import TOOL_REGISTRY
from analyst_toolkit.mcp_server.response_utils import new_trace_id
from analyst_toolkit.mcp_server.rpc_dispatch import dispatch_rpc_method, rpc_error
from analyst_toolkit.mcp_server.templates import list_template_resources, read_template_resource

# Get package version dynamically
try:
    __version__ = version("analyst_toolkit")
except PackageNotFoundError:
    __version__ = os.environ.get("ANALYST_MCP_VERSION_FALLBACK", "0.0.0+local")

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
STRUCTURED_LOGS = _env_bool("ANALYST_MCP_STRUCTURED_LOGS", False)
AUTH_TOKEN = os.environ.get("ANALYST_MCP_AUTH_TOKEN", "").strip()
SERVER_STARTED_AT = time.time()

# Official MCP SDK server instance
mcp_server = Server("analyst-toolkit")

# FastAPI app for HTTP transport
app = FastAPI(title="analyst-toolkit MCP Server", version=__version__)

SERVER_INFO = {
    "protocolVersion": "2024-05-01",
    "serverInfo": {"name": "analyst-toolkit", "version": __version__},
    "capabilities": {"tools": {}, "resources": {"subscribe": False, "listChanged": False}},
}

METRICS = RuntimeMetrics(started_at=SERVER_STARTED_AT)


def _log_rpc_event(level: int, event: str, **fields: Any) -> None:
    log_rpc_event(
        logger=logger,
        structured_logs=STRUCTURED_LOGS,
        level=level,
        event=event,
        **fields,
    )


def _is_authorized(request: Request) -> bool:
    return is_authorized(request, AUTH_TOKEN)


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


@app.post("/rpc")
async def rpc_handler(request: Request) -> JSONResponse:
    """HTTP JSON-RPC 2.0 dispatcher for FridAI."""
    start = time.perf_counter()
    trace_id = new_trace_id()
    req_id: Any = None
    method = "unknown"
    tool_name: str | None = None

    def _respond(
        payload: dict[str, Any],
        *,
        ok: bool,
        level: int = logging.INFO,
        error_code: int | None = None,
        run_id: str | None = None,
        session_id: str | None = None,
    ) -> JSONResponse:
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        METRICS.record_rpc(method=method, duration_ms=duration_ms, ok=ok, tool_name=tool_name)
        _log_rpc_event(
            level,
            "rpc_request_completed",
            trace_id=trace_id,
            req_id=req_id,
            method=method,
            tool=tool_name,
            ok=ok,
            error_code=error_code,
            duration_ms=duration_ms,
            run_id=run_id,
            session_id=session_id,
        )
        return JSONResponse(payload, status_code=200)

    if not _is_authorized(request):
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        METRICS.record_rpc(
            method="tools/call_auth_rejected",
            duration_ms=duration_ms,
            ok=False,
            tool_name=None,
        )
        _log_rpc_event(
            logging.WARNING,
            "rpc_request_rejected_auth",
            trace_id=trace_id,
            req_id=req_id,
            method=method,
            duration_ms=duration_ms,
        )
        return JSONResponse(
            {"error": "Unauthorized", "trace_id": trace_id},
            status_code=401,
        )

    try:
        body = await request.json()
    except Exception:
        return _respond(rpc_error(None, -32700, "Parse error"), ok=False, level=logging.WARNING)

    req_id = body.get("id")
    method = body.get("method", "")
    params = body.get("params", {})
    if method == "tools/call":
        tool_name = params.get("name")

    _log_rpc_event(
        logging.INFO,
        "rpc_request_received",
        trace_id=trace_id,
        req_id=req_id,
        method=method,
        tool=tool_name,
    )

    outcome = await dispatch_rpc_method(
        req_id=req_id,
        method=method,
        params=params,
        server_info=SERVER_INFO,
        tool_registry=TOOL_REGISTRY,
        advertise_resource_templates=ADVERTISE_RESOURCE_TEMPLATES,
        resource_io_timeout_sec=RESOURCE_IO_TIMEOUT_SEC,
        resource_models_with_timeout=_resource_models_with_timeout,
        resource_template_models=_resource_template_models,
        read_template_with_timeout=_read_template_with_timeout,
        trace_id=trace_id,
        logger=logger,
    )
    return _respond(
        outcome.payload,
        ok=outcome.ok,
        level=outcome.level,
        error_code=outcome.error_code,
        run_id=outcome.run_id,
        session_id=outcome.session_id,
    )


@app.get("/health")
async def health(request: Request) -> Any:
    if not _is_authorized(request):
        return JSONResponse({"status": "unauthorized"}, status_code=401)
    metrics = METRICS.snapshot()
    return {
        "status": "ok",
        "version": __version__,
        "tools": list(TOOL_REGISTRY.keys()),
        "uptime_sec": metrics["uptime_sec"],
    }


@app.get("/ready")
async def ready(request: Request) -> Any:
    if not _is_authorized(request):
        return JSONResponse({"status": "unauthorized"}, status_code=401)
    return {"status": "ready"}


@app.get("/metrics")
async def metrics(request: Request) -> Any:
    if not _is_authorized(request):
        return JSONResponse({"status": "unauthorized"}, status_code=401)
    return METRICS.snapshot()


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
