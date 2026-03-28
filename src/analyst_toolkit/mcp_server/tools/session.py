"""MCP tool: toolkit_manage_session — session lifecycle management."""

import uuid
from datetime import datetime, timezone

from analyst_toolkit.mcp_server.response_utils import next_action, with_next_actions
from analyst_toolkit.mcp_server.state import StateStore


def _session_summary(session_id: str, *, include_configs: bool = False) -> dict:
    """Build a compact summary dict for a session."""
    metadata = StateStore.get_metadata(session_id) or {}
    expiry = StateStore.get_expiry_info(session_id)
    configs = StateStore.get_configs(session_id)
    config_names = sorted(configs.keys())
    return {
        "session_id": session_id,
        "run_id": StateStore.get_run_id(session_id),
        "started_at": StateStore.get_session_start(session_id),
        "row_count": metadata.get("row_count"),
        "col_count": metadata.get("col_count"),
        "updated_at": metadata.get("updated_at"),
        "last_accessed_at": expiry["last_accessed_at"],
        "expires_at": expiry["expires_at"],
        "expires_in_sec": expiry["expires_in_sec"],
        "stored_configs": config_names,
        "config_count": len(config_names),
        **(
            {
                "configs": configs,
                "config_bytes": sum(len(value.encode("utf-8")) for value in configs.values()),
            }
            if include_configs
            else {}
        ),
    }


async def _toolkit_manage_session(
    action: str,
    session_id: str | None = None,
    run_id: str | None = None,
    copy_configs: bool = True,
    include_configs: bool = False,
) -> dict:
    """
    Manage session lifecycle: list, inspect, fork, rebind, or clear.

    Actions:
      list    — show all active sessions
      inspect — show details for a single session
      fork    — clone a session into a new session with a new run_id
      rebind  — change the run_id bound to a session
      clear   — drop a session (or all sessions) to free memory
    """
    action = (action or "").strip().lower()

    if action == "list":
        sessions = StateStore.list_sessions()
        summaries = [_session_summary(sid) for sid in sessions]
        return {
            "status": "pass",
            "module": "manage_session",
            "action": "list",
            "session_policy": StateStore.policy(),
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
        summary = _session_summary(session_id, include_configs=include_configs)
        actions = []
        if not include_configs:
            actions.append(
                next_action(
                    "manage_session",
                    "Retrieve stored configs for this session.",
                    {"action": "inspect", "session_id": session_id, "include_configs": True},
                )
            )
        actions.extend(
            [
                next_action(
                    "manage_session",
                    "Fork this session to start a new run.",
                    {"action": "fork", "session_id": session_id},
                ),
                next_action(
                    "get_run_history",
                    "Inspect the run history associated with this session.",
                    {"run_id": summary["run_id"]},
                ),
            ]
        )
        return with_next_actions(
            {
                "status": "pass",
                "module": "manage_session",
                "action": "inspect",
                "session_policy": StateStore.policy(),
                "session": summary,
            },
            actions,
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
        # Generate a fresh run_id if none provided (include uuid suffix to avoid collisions)
        if not run_id:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            run_id = f"{ts}_{uuid.uuid4().hex[:6]}"
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
                "session_policy": StateStore.policy(),
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
            "session_policy": StateStore.policy(),
            "session_id": session_id,
            "previous_run_id": old_run_id,
            "new_run_id": run_id,
        }

    if action == "clear":
        if session_id:
            if StateStore.get(session_id) is None:
                return {
                    "status": "error",
                    "module": "manage_session",
                    "action": "clear",
                    "error": f"Session '{session_id}' not found or expired.",
                    "error_code": "SESSION_NOT_FOUND",
                }
            StateStore.clear(session_id)
            return {
                "status": "pass",
                "module": "manage_session",
                "action": "clear",
                "session_policy": StateStore.policy(),
                "cleared_session_id": session_id,
            }
        else:
            sessions = StateStore.list_sessions()
            count = len(sessions)
            StateStore.clear()
            return {
                "status": "pass",
                "module": "manage_session",
                "action": "clear",
                "session_policy": StateStore.policy(),
                "cleared_count": count,
                "message": f"Cleared all {count} sessions.",
            }

    return {
        "status": "error",
        "module": "manage_session",
        "error": f"Unknown action '{action}'. Use list, inspect, fork, rebind, or clear.",
        "error_code": "UNKNOWN_ACTION",
    }


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["list", "inspect", "fork", "rebind", "clear"],
            "description": (
                "Action to perform: "
                "list (show all sessions), "
                "inspect (show details for one session), "
                "fork (clone a session with a new run_id), "
                "rebind (change the run_id bound to a session), "
                "clear (drop a single session by session_id, or all sessions if omitted)."
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
        "include_configs": {
            "type": "boolean",
            "description": (
                "When action=inspect, include the stored inferred config YAML payloads in the response. "
                "Defaults to false to keep responses compact."
            ),
            "default": False,
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
        "fork a session into a new run context, rebind a session to a different run_id, "
        "or clear sessions to free memory."
    ),
    input_schema=_INPUT_SCHEMA,
)
