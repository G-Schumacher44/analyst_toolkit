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
from typing import Any

import mcp.types as types
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server

from analyst_toolkit.mcp_server.registry import TOOL_REGISTRY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stderr,  # Log to stderr to avoid polluting stdout in stdio mode
)
logger = logging.getLogger("analyst_toolkit.mcp_server")

# Official MCP SDK server instance
mcp_server = Server("analyst-toolkit")

# FastAPI app for HTTP transport
app = FastAPI(title="analyst-toolkit MCP Server", version="0.1.0")

SERVER_INFO = {
    "protocolVersion": "2024-05-01",
    "serverInfo": {"name": "analyst-toolkit", "version": "0.1.0"},
    "capabilities": {"tools": {}},
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


# --- HTTP /rpc JSON-RPC Handlers (FridAI Legacy/Native) ---


def _rpc_error(req_id: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


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
            return JSONResponse(_rpc_ok(req_id, result))
        except Exception as exc:
            logger.exception(f"Tool {tool_name} raised an error")
            # Return proper JSON-RPC 2.0 error
            return JSONResponse(_rpc_error(req_id, -32603, f"Internal error: {str(exc)}"))

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
    imputation,
    infer_configs,
    normalization,
    outliers,
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
                server_version="0.1.0",
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
