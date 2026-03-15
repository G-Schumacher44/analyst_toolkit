"""MCP tools for registering and inspecting canonical input sources."""

from analyst_toolkit.mcp_server.input.ingest import get_input_descriptor, register_input_source
from analyst_toolkit.mcp_server.registry import register_tool


async def _toolkit_register_input(
    uri: str,
    source_type: str | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    load_into_session: bool = True,
) -> dict:
    try:
        descriptor, df, effective_session_id = register_input_source(
            reference=uri,
            source_type=source_type,  # type: ignore[arg-type]
            session_id=session_id,
            run_id=run_id,
            load_into_session=load_into_session,
        )
    except NotImplementedError as exc:
        return {
            "status": "error",
            "module": "register_input",
            "code": "INPUT_SOURCE_UNSUPPORTED",
            "message": str(exc),
        }
    except Exception as exc:
        return {
            "status": "error",
            "module": "register_input",
            "code": "INPUT_REGISTER_FAILED",
            "message": str(exc),
        }

    summary = {}
    if df is not None:
        summary = {"row_count": int(df.shape[0]), "column_count": int(df.shape[1])}
    return {
        "status": "pass",
        "module": "register_input",
        "input": descriptor.to_dict(),
        "session_id": effective_session_id or "",
        "summary": summary,
    }


async def _toolkit_get_input_descriptor(input_id: str) -> dict:
    descriptor = get_input_descriptor(input_id)
    if descriptor is None:
        return {
            "status": "error",
            "module": "get_input_descriptor",
            "code": "INPUT_NOT_FOUND",
            "message": f"Input not found: {input_id}",
        }
    return {
        "status": "pass",
        "module": "get_input_descriptor",
        "input": descriptor.to_dict(),
    }


register_tool(
    name="register_input",
    fn=_toolkit_register_input,
    description=(
        "Register a server-visible local path or gs:// URI as a canonical input reference "
        "and optionally bind it into a session."
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
            "run_id": {"type": "string", "description": "Optional run identifier."},
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
        "properties": {
            "input_id": {"type": "string", "description": "Canonical input reference identifier."}
        },
        "required": ["input_id"],
    },
)
