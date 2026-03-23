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
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from mcp.server import NotificationOptions, Server
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from pydantic import BaseModel

from analyst_toolkit.mcp_server.auth import is_authorized
from analyst_toolkit.mcp_server.input.errors import (
    InputConflictError,
    InputError,
    InputNotSupportedError,
    InputPayloadTooLargeError,
    client_safe_input_error_code,
)
from analyst_toolkit.mcp_server.input.ingest import (
    get_input_descriptor,
    ingest_uploaded_bytes,
    register_input_source,
)
from analyst_toolkit.mcp_server.input.models import InputSourceType
from analyst_toolkit.mcp_server.observability import RuntimeMetrics, log_rpc_event
from analyst_toolkit.mcp_server.registry import TOOL_REGISTRY
from analyst_toolkit.mcp_server.resources import list_mcp_resources, read_mcp_resource
from analyst_toolkit.mcp_server.response_utils import new_trace_id
from analyst_toolkit.mcp_server.rpc_dispatch import dispatch_rpc_method, rpc_error

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
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


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


def _is_loopback_host(host: str) -> bool:
    normalized = host.strip().lower().strip("[]")
    return normalized in _LOOPBACK_HOSTS


def _log_http_auth_posture(host: str, auth_token: str) -> None:
    if auth_token:
        return
    if _is_loopback_host(host):
        logger.warning(
            "HTTP auth is disabled because ANALYST_MCP_AUTH_TOKEN is unset. "
            "The default bind host is loopback-only, but set a token before exposing the server."
        )
        return
    logger.warning(
        "HTTP auth is disabled because ANALYST_MCP_AUTH_TOKEN is unset and the bind host "
        "is non-loopback (%s). Set a token before exposing this server.",
        host,
    )


RESOURCE_IO_TIMEOUT_SEC = _env_float("ANALYST_MCP_RESOURCE_TIMEOUT_SEC", 8.0)
ADVERTISE_RESOURCE_TEMPLATES = _env_bool("ANALYST_MCP_ADVERTISE_RESOURCE_TEMPLATES", True)
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


class RegisterInputRequest(BaseModel):
    uri: str
    source_type: InputSourceType | None = None
    session_id: str | None = None
    run_id: str | None = None
    idempotency_key: str | None = None
    load_into_session: bool = True


def _input_error_http_status(exc: InputError) -> int:
    if isinstance(exc, InputPayloadTooLargeError):
        return 413
    if isinstance(exc, InputNotSupportedError):
        return 400
    if isinstance(exc, InputConflictError):
        return 409
    return 400


def _input_error_detail(exc: InputError, trace_id: str) -> dict[str, str]:
    return {
        "error": exc.message,
        "code": client_safe_input_error_code(exc.code),
        "trace_id": trace_id,
    }


def _require_http_auth(request: Request) -> str:
    trace_id = new_trace_id()
    if not _is_authorized(request):
        raise HTTPException(status_code=401, detail={"error": "Unauthorized", "trace_id": trace_id})
    return trace_id


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
        for item in list_mcp_resources()
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


async def _read_resource_with_timeout(uri: str) -> tuple[str, str]:
    return await asyncio.wait_for(
        asyncio.to_thread(read_mcp_resource, uri),
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
    text, mime_type = await _read_resource_with_timeout(uri_text)
    return [ReadResourceContents(content=text, mime_type=mime_type)]


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
        read_resource_with_timeout=_read_resource_with_timeout,
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


@app.post("/inputs/upload")
async def upload_input(
    request: Request,
    file: UploadFile = File(...),
    session_id: str | None = Form(default=None),
    run_id: str | None = Form(default=None),
    idempotency_key: str | None = Form(default=None),
    load_into_session: bool = Form(default=True),
) -> JSONResponse:
    trace_id = _require_http_auth(request)
    payload = await file.read()
    if not payload:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Empty upload payload.",
                "code": "INPUT_EMPTY_UPLOAD",
                "trace_id": trace_id,
            },
        )
    try:
        descriptor, df, effective_session_id = ingest_uploaded_bytes(
            filename=file.filename or "upload.csv",
            payload=payload,
            media_type=file.content_type,
            session_id=session_id,
            run_id=run_id,
            idempotency_key=idempotency_key,
            load_into_session=load_into_session,
        )
    except InputError as exc:
        logger.warning("upload_input failed (trace_id=%s, code=%s)", trace_id, exc.code)
        raise HTTPException(
            status_code=_input_error_http_status(exc),
            detail=_input_error_detail(exc, trace_id),
        ) from exc
    except Exception as exc:
        logger.exception("upload_input unexpected failure (trace_id=%s)", trace_id)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal server error.",
                "code": "INTERNAL_ERROR",
                "trace_id": trace_id,
            },
        ) from exc

    summary = {}
    if df is not None:
        summary = {"row_count": int(df.shape[0]), "column_count": int(df.shape[1])}
    return JSONResponse(
        {
            "status": "pass",
            "trace_id": trace_id,
            "input": descriptor.to_dict(),
            "session_id": effective_session_id or "",
            "summary": summary,
        }
    )


@app.post("/inputs/register")
async def register_input(request: Request, payload: RegisterInputRequest) -> JSONResponse:
    trace_id = _require_http_auth(request)
    try:
        descriptor, df, effective_session_id = register_input_source(
            reference=payload.uri,
            source_type=payload.source_type,
            session_id=payload.session_id,
            run_id=payload.run_id,
            idempotency_key=payload.idempotency_key,
            load_into_session=payload.load_into_session,
        )
    except InputError as exc:
        logger.warning("register_input failed (trace_id=%s, code=%s)", trace_id, exc.code)
        raise HTTPException(
            status_code=_input_error_http_status(exc),
            detail=_input_error_detail(exc, trace_id),
        ) from exc
    except Exception as exc:
        logger.exception("register_input unexpected failure (trace_id=%s)", trace_id)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal server error.",
                "code": "INTERNAL_ERROR",
                "trace_id": trace_id,
            },
        ) from exc

    summary = {}
    if df is not None:
        summary = {"row_count": int(df.shape[0]), "column_count": int(df.shape[1])}
    return JSONResponse(
        {
            "status": "pass",
            "trace_id": trace_id,
            "input": descriptor.to_dict(),
            "session_id": effective_session_id or "",
            "summary": summary,
        }
    )


@app.get("/inputs/{input_id}")
async def read_input_descriptor(input_id: str, request: Request) -> JSONResponse:
    trace_id = _require_http_auth(request)
    descriptor = get_input_descriptor(input_id)
    if descriptor is None:
        raise HTTPException(
            status_code=404, detail={"error": "Input not found.", "trace_id": trace_id}
        )
    return JSONResponse({"status": "pass", "trace_id": trace_id, "input": descriptor.to_dict()})


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
    data_dictionary,
    diagnostics,
    drift,
    duplicates,
    final_audit,
    imputation,
    infer_configs,
    input_ingest,
    jobs,
    normalization,
    outliers,
    preflight_config,
    read_artifact,
    session,
    validation,
)
from analyst_toolkit.mcp_server.tools import (
    upload_input as upload_input_tool,
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
    parser.add_argument(
        "--host",
        default=os.environ.get("ANALYST_MCP_HOST", "127.0.0.1"),
        help="HTTP bind host (default: 127.0.0.1)",
    )
    args = parser.parse_args()

    if args.stdio or os.environ.get("ANALYST_MCP_STDIO", "").lower() == "true":
        # Propagate stdio flag so downstream modules auto-enable local defaults
        # (trusted history, artifact server, CWD input roots, etc.)
        os.environ.setdefault("ANALYST_MCP_STDIO", "true")
        logger.info("Starting Analyst Toolkit MCP Server in stdio mode")
        asyncio.run(run_stdio())
    else:
        _log_http_auth_posture(args.host, AUTH_TOKEN)
        logger.info(
            "Starting Analyst Toolkit MCP Server in HTTP mode on %s:%s",
            args.host,
            args.port,
        )
        import uvicorn

        uvicorn.run(
            "analyst_toolkit.mcp_server.server:app",
            host=args.host,
            port=args.port,
            log_level="info",
        )


if __name__ == "__main__":
    main()
