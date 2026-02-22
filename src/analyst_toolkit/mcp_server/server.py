"""
server.py — analyst_toolkit MCP Server

FastAPI app exposing a JSON-RPC 2.0 /rpc endpoint compatible with
fridai-core's HTTPRemoteClient.

Supports JSON-RPC 2.0 methods:
  - initialize      → server info + capabilities
  - tools/list      → registered tool schemas
  - tools/call      → invoke a tool by name

Tools self-register by calling register_tool() at import time.

Start with:
    python -m analyst_toolkit.mcp_server.server
"""

import logging
import sys
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("analyst_toolkit.mcp_server")

app = FastAPI(title="analyst-toolkit MCP Server", version="0.1.0")

# Tool registry: tool_name → {fn, description, inputSchema}
TOOL_REGISTRY: dict[str, dict[str, Any]] = {}

SERVER_INFO = {
    "protocolVersion": "2024-05-01",
    "serverInfo": {"name": "analyst-toolkit", "version": "0.1.0"},
    "capabilities": {"tools": {}},
}


def register_tool(name: str, fn, description: str, input_schema: dict) -> None:
    """Register an async callable as an MCP tool. Called by tool modules at import time."""
    TOOL_REGISTRY[name] = {
        "fn": fn,
        "description": description,
        "inputSchema": input_schema,
    }
    logger.info(f"Registered tool: {name}")


def _rpc_error(req_id: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _rpc_ok(req_id: Any, result: Any) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


@app.post("/rpc")
async def rpc_handler(request: Request) -> JSONResponse:
    """JSON-RPC 2.0 dispatcher."""
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
            return JSONResponse(
                _rpc_ok(req_id, {
                    "status": "error",
                    "module": tool_name,
                    "error": str(exc),
                    "artifact_path": "",
                })
            )

    return JSONResponse(_rpc_error(req_id, -32601, f"Unknown method: {method}"))


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "tools": list(TOOL_REGISTRY.keys())}


# Import tool modules — each calls register_tool() as a side effect
from analyst_toolkit.mcp_server.tools import (  # noqa: E402
    diagnostics,
    validation,
    outliers,
    normalization,
    duplicates,
    imputation,
    infer_configs,
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
