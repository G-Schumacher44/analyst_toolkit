"""MCP tool: upload_input — accept base64-encoded file content through the MCP protocol."""

import asyncio
import base64
import logging
from functools import partial

from analyst_toolkit.mcp_server.input.errors import InputError
from analyst_toolkit.mcp_server.input.ingest import ingest_uploaded_bytes
from analyst_toolkit.mcp_server.registry import register_tool
from analyst_toolkit.mcp_server.response_utils import new_trace_id

logger = logging.getLogger("analyst_toolkit.mcp_server.upload_input")

_MAX_BASE64_LENGTH = 70 * 1024 * 1024  # ~52.5 MB decoded


async def _toolkit_upload_input(
    filename: str,
    content_base64: str,
    session_id: str | None = None,
    run_id: str | None = None,
    idempotency_key: str | None = None,
    load_into_session: bool = True,
) -> dict:
    """Upload a local file as base64-encoded content through the MCP protocol.

    This bridges the container isolation gap: agents that cannot reach the
    server filesystem or the HTTP /inputs/upload endpoint can push file
    bytes directly through MCP tool calls.
    """
    trace_id = new_trace_id()

    if not content_base64 or not content_base64.strip():
        return {
            "status": "error",
            "module": "upload_input",
            "code": "INPUT_EMPTY_UPLOAD",
            "message": "content_base64 is empty.",
            "trace_id": trace_id,
        }

    if len(content_base64) > _MAX_BASE64_LENGTH:
        return {
            "status": "error",
            "module": "upload_input",
            "code": "INPUT_PAYLOAD_TOO_LARGE",
            "message": f"Base64 payload exceeds maximum size ({_MAX_BASE64_LENGTH} bytes encoded).",
            "trace_id": trace_id,
        }

    try:
        payload = base64.b64decode(content_base64, validate=True)
    except Exception:
        received_len = len(content_base64)
        ends_cleanly = content_base64.rstrip().endswith("=") or (received_len % 4 == 0)
        hint = f" Received {received_len} chars" + (
            " which appears truncated (does not end with '=' padding)."
            " The MCP client or transport may have a message size limit."
            " Try splitting the file into smaller chunks or use the HTTP"
            " POST /inputs/upload endpoint instead."
            if not ends_cleanly
            else "."
        )
        return {
            "status": "error",
            "module": "upload_input",
            "code": "INPUT_INVALID_BASE64",
            "message": f"content_base64 is not valid base64.{hint}",
            "trace_id": trace_id,
            "next_actions": [
                {
                    "tool": "shell",
                    "why": (
                        "For files larger than ~100KB, upload via HTTP instead of MCP. "
                        "Run this curl command in your shell."
                    ),
                    "shell_command": (
                        "curl -sS -X POST"
                        " -F 'file=@<LOCAL_FILE_PATH>'"
                        " -F 'load_into_session=true'"
                        " http://127.0.0.1:8001/inputs/upload"
                    ),
                },
            ],
        }

    if not payload:
        return {
            "status": "error",
            "module": "upload_input",
            "code": "INPUT_EMPTY_UPLOAD",
            "message": "Decoded payload is empty.",
            "trace_id": trace_id,
        }

    try:
        loop = asyncio.get_running_loop()
        descriptor, df, effective_session_id = await loop.run_in_executor(
            None,
            partial(
                ingest_uploaded_bytes,
                filename=filename,
                payload=payload,
                media_type=None,
                session_id=session_id,
                run_id=run_id,
                idempotency_key=idempotency_key,
                load_into_session=load_into_session,
            ),
        )
    except InputError as exc:
        logger.warning("upload_input failed (trace_id=%s, code=%s)", trace_id, exc.code)
        return {
            "status": "error",
            "module": "upload_input",
            "code": exc.code,
            "message": exc.message,
            "trace_id": trace_id,
        }
    except Exception:
        logger.exception("upload_input unexpected failure (trace_id=%s)", trace_id)
        return {
            "status": "error",
            "module": "upload_input",
            "code": "INTERNAL_ERROR",
            "message": "Internal server error.",
            "trace_id": trace_id,
        }

    summary = {}
    if df is not None:
        summary = {"row_count": int(df.shape[0]), "column_count": int(df.shape[1])}
    return {
        "status": "pass",
        "module": "upload_input",
        "input": descriptor.to_dict(),
        "session_id": effective_session_id or "",
        "summary": summary,
        "trace_id": trace_id,
    }


register_tool(
    name="upload_input",
    fn=_toolkit_upload_input,
    description=(
        "Upload a local file as base64-encoded content through the MCP protocol. "
        "Use this when the file is not server-visible and cannot be registered "
        "via register_input or uploaded via /inputs/upload. The agent reads the "
        "local file, base64-encodes it, and sends it through this tool."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "filename": {
                "type": "string",
                "description": "Original filename (e.g. 'data.csv'). Used for display and format detection.",
            },
            "content_base64": {
                "type": "string",
                "description": "Base64-encoded file content.",
            },
            "session_id": {
                "type": "string",
                "description": "Optional session to bind the uploaded input to.",
            },
            "run_id": {
                "type": "string",
                "description": "Optional run identifier.",
            },
            "idempotency_key": {
                "type": "string",
                "description": "Optional stable key for idempotent uploads.",
            },
            "load_into_session": {
                "type": "boolean",
                "description": "If true, load the input into the session store. Default: true.",
                "default": True,
            },
        },
        "required": ["filename", "content_base64"],
    },
)
