"""MCP tool: toolkit_manage_session — session lifecycle management."""

from datetime import datetime, timezone

from analyst_toolkit.mcp_server.response_utils import next_action, with_next_actions
from analyst_toolkit.mcp_server.state import StateStore


def _session_summary(session_id: str) -> dict:
    """Build a compact summary dict for a session."""
    metadata = StateStore.get_metadata(session_id) or {}
    return {
        "session_id": session_id,
        "run_id": StateStore.get_run_id(session_id),
        "started_at": StateStore.get_session_start(session_id),
        "row_count": metadata.get("row_count"),
        "col_count": metadata.get("col_count"),
        "updated_at": metadata.get("updated_at"),
        "stored_configs": sorted(StateStore.get_configs(session_id).keys()),
    }


async def _toolkit_manage_session(
    action: str,
    session_id: str | None = None,
    run_id: str | None = None,
    copy_configs: bool = True,
) -> dict:
    """
    Manage session lifecycle: list, inspect, fork, or rebind.

    Actions:
      list    — show all active sessions
      inspect — show details for a single session
      fork    — clone a session into a new session with a new run_id
      rebind  — change the run_id bound to a session
    """
    action = (action or "").strip().lower()

    if action == "list":
        sessions = StateStore.list_sessions()
        summaries = [_session_summary(sid) for sid in sessions]
        return {
            "status": "pass",
            "module": "manage_session",
            "action": "list",
            "sessions": summaries,
            "session_count": len(summaries),
        }

    if action == "inspect":
        if not session_id:
            return {
                "status": "error",
                "module": "manage_session",
                "action": "inspect",
                "error": "session_id is required for inspect.",
                "error_code": "MISSING_SESSION_ID",
            }
        if StateStore.get(session_id) is None:
            return {
                "status": "error",
                "module": "manage_session",
                "action": "inspect",
                "error": f"Session '{session_id}' not found or expired.",
                "error_code": "SESSION_NOT_FOUND",
            }
        summary = _session_summary(session_id)
        return with_next_actions(
            {
                "status": "pass",
                "module": "manage_session",
                "action": "inspect",
                "session": summary,
            },
            [
                next_action(
                    "manage_session",
                    "Fork this session to start a new run.",
                    {"action": "fork", "session_id": session_id},
                ),
            ],
        )

    if action == "fork":
        if not session_id:
            return {
                "status": "error",
                "module": "manage_session",
                "action": "fork",
                "error": "session_id is required for fork.",
                "error_code": "MISSING_SESSION_ID",
            }
        # Generate a fresh run_id if none provided
        if not run_id:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        new_session_id = StateStore.fork(
            session_id,
            run_id=run_id,
            copy_configs=copy_configs,
        )
        if new_session_id is None:
            return {
                "status": "error",
                "module": "manage_session",
                "action": "fork",
                "error": f"Source session '{session_id}' not found or expired.",
                "error_code": "SESSION_NOT_FOUND",
            }
        return with_next_actions(
            {
                "status": "pass",
                "module": "manage_session",
                "action": "fork",
                "source_session_id": session_id,
                "new_session_id": new_session_id,
                "run_id": run_id,
                "configs_copied": copy_configs,
                "session": _session_summary(new_session_id),
            },
            [
                next_action(
                    "infer_configs",
                    "Run config inference on the forked session.",
                    {"session_id": new_session_id, "run_id": run_id},
                ),
                next_action(
                    "final_audit",
                    "Run final audit on the forked session.",
                    {"session_id": new_session_id, "run_id": run_id},
                ),
            ],
        )

    if action == "rebind":
        if not session_id:
            return {
                "status": "error",
                "module": "manage_session",
                "action": "rebind",
                "error": "session_id is required for rebind.",
                "error_code": "MISSING_SESSION_ID",
            }
        if not run_id:
            return {
                "status": "error",
                "module": "manage_session",
                "action": "rebind",
                "error": "run_id is required for rebind.",
                "error_code": "MISSING_RUN_ID",
            }
        old_run_id = StateStore.get_run_id(session_id)
        ok = StateStore.rebind_run_id(session_id, run_id)
        if not ok:
            return {
                "status": "error",
                "module": "manage_session",
                "action": "rebind",
                "error": f"Session '{session_id}' not found or expired.",
                "error_code": "SESSION_NOT_FOUND",
            }
        return {
            "status": "pass",
            "module": "manage_session",
            "action": "rebind",
            "session_id": session_id,
            "previous_run_id": old_run_id,
            "new_run_id": run_id,
        }

    return {
        "status": "error",
        "module": "manage_session",
        "error": f"Unknown action '{action}'. Use list, inspect, fork, or rebind.",
        "error_code": "UNKNOWN_ACTION",
    }


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["list", "inspect", "fork", "rebind"],
            "description": (
                "Action to perform: "
                "list (show all sessions), "
                "inspect (show details for one session), "
                "fork (clone a session with a new run_id), "
                "rebind (change the run_id bound to a session)."
            ),
        },
        "session_id": {
            "type": "string",
            "description": "Target session. Required for inspect, fork, and rebind.",
        },
        "run_id": {
            "type": "string",
            "description": (
                "New run_id for fork or rebind. "
                "If omitted during fork, a timestamp-based run_id is generated."
            ),
        },
        "copy_configs": {
            "type": "boolean",
            "description": "Whether to copy inferred configs when forking. Defaults to true.",
            "default": True,
        },
    },
    "required": ["action"],
}

from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="manage_session",
    fn=_toolkit_manage_session,
    description=(
        "Manage session lifecycle: list active sessions, inspect a session, "
        "fork a session into a new run context, or rebind a session to a different run_id."
    ),
    input_schema=_INPUT_SCHEMA,
)
