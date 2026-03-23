"""MCP tools for registering and inspecting canonical input sources."""

import asyncio
import logging
from functools import partial

from analyst_toolkit.mcp_server.input.errors import InputError, client_safe_input_error_code
from analyst_toolkit.mcp_server.input.ingest import get_input_descriptor, register_input_source
from analyst_toolkit.mcp_server.input.models import InputSourceType
from analyst_toolkit.mcp_server.registry import register_tool
from analyst_toolkit.mcp_server.response_utils import new_trace_id
from analyst_toolkit.mcp_server.schemas import INPUT_ID_PROP

logger = logging.getLogger("analyst_toolkit.mcp_server.input_ingest")


async def _toolkit_register_input(
    uri: str,
    source_type: InputSourceType | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    idempotency_key: str | None = None,
    load_into_session: bool = True,
) -> dict:
    try:
        loop = asyncio.get_running_loop()
        descriptor, df, effective_session_id = await loop.run_in_executor(
            None,
            partial(
                register_input_source,
                reference=uri,
                source_type=source_type,
                session_id=session_id,
                run_id=run_id,
                idempotency_key=idempotency_key,
                load_into_session=load_into_session,
            ),
        )
    except InputError as exc:
        trace_id = new_trace_id()
        logger.exception("Input registration failed (trace_id=%s, code=%s)", trace_id, exc.code)
        result: dict = {
            "status": "error",
            "module": "register_input",
            "code": client_safe_input_error_code(exc.code),
            "message": exc.message,
            "trace_id": trace_id,
        }
        if client_safe_input_error_code(exc.code) == "INPUT_PATH_DENIED":
            safe_name = uri.rsplit("/", 1)[-1] if uri else "<filename>"
            result["next_actions"] = [
                {
                    "tool": "upload_input",
                    "why": (
                        "The path is not server-visible. Use upload_input to "
                        "base64-encode the file and push it through MCP. "
                        "Best for files under ~100KB."
                    ),
                    "arguments_hint": {"filename": safe_name},
                },
                {
                    "tool": "shell",
                    "why": (
                        "For larger files, upload via HTTP from your shell. "
                        "This avoids MCP transport size limits."
                    ),
                    "shell_command": (
                        f"curl -sS -X POST"
                        f" -F 'file=@{uri or '<LOCAL_FILE_PATH>'}'"
                        f" -F 'load_into_session=true'"
                        f" http://127.0.0.1:8001/inputs/upload"
                    ),
                },
            ]
        return result
    except Exception:
        trace_id = new_trace_id()
        logger.exception("Failed to register input (trace_id=%s)", trace_id)
        return {
            "status": "error",
            "module": "register_input",
            "code": "INPUT_REGISTER_FAILED",
            "message": "Failed to register input source.",
            "trace_id": trace_id,
        }

    summary = {}
    if df is not None:
        summary = {"row_count": int(df.shape[0]), "column_count": int(df.shape[1])}
    return {
        "status": "pass",
        "module": "register_input",
        "input": descriptor.to_dict(),
        "session_id": effective_session_id,
        "summary": summary,
    }


async def _toolkit_get_input_descriptor(input_id: str) -> dict:
    try:
        loop = asyncio.get_running_loop()
        descriptor = await loop.run_in_executor(None, get_input_descriptor, input_id)
        if descriptor is None:
            trace_id = new_trace_id()
            return {
                "status": "error",
                "module": "get_input_descriptor",
                "code": "INPUT_NOT_FOUND",
                "message": "Input descriptor not found.",
                "trace_id": trace_id,
            }
        descriptor_payload = descriptor.to_dict()
    except Exception:
        trace_id = new_trace_id()
        logger.exception("Failed to retrieve input descriptor (trace_id=%s)", trace_id)
        return {
            "status": "error",
            "module": "get_input_descriptor",
            "code": "INTERNAL_ERROR",
            "message": "Internal server error.",
            "trace_id": trace_id,
        }
    return {
        "status": "pass",
        "module": "get_input_descriptor",
        "input": descriptor_payload,
    }


register_tool(
    name="register_input",
    fn=_toolkit_register_input,
    description=(
        "Register a canonical input reference from a server-visible local path or gs:// URI "
        "and optionally bind it into a session. Local server_path access is restricted to "
        "server-configured allowlisted roots."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "uri": {"type": "string", "description": "Server-visible local path or gs:// URI."},
            "source_type": {
                "type": "string",
                "enum": ["server_path", "gcs", "gdrive"],
                "description": "Optional explicit source type. Omit to auto-detect.",
            },
            "session_id": {
                "type": "string",
                "description": "Optional session to bind the registered input to.",
            },
            "run_id": {
                "type": "string",
                "description": (
                    "Optional run identifier. Provide a stable run_id if clients need "
                    "idempotent retries; omitted run_id values may create distinct runs."
                ),
            },
            "idempotency_key": {
                "type": "string",
                "minLength": 1,
                "maxLength": 255,
                "pattern": "^.*\\S.*$",
                "description": (
                    "Optional stable idempotency key. Provide this to reuse the same "
                    "input_id across retries for the same logical source. Reusing the "
                    "same key for a different canonical source is rejected as a conflict."
                ),
            },
            "load_into_session": {
                "type": "boolean",
                "description": "If true, load the input and save it into the session store.",
                "default": True,
            },
        },
        "required": ["uri"],
    },
)

register_tool(
    name="get_input_descriptor",
    fn=_toolkit_get_input_descriptor,
    description="Fetch metadata for a canonical input reference by input_id.",
    input_schema={
        "type": "object",
        "properties": {**INPUT_ID_PROP},
        "required": ["input_id"],
    },
)
