"""Input schemas for cockpit MCP tools."""

from analyst_toolkit.mcp_server.schemas import _GCS_PATH_PROP, _INPUT_ID_PROP, _RUN_ID_PROP

CAPABILITY_CATALOG_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "module": {
            "type": "string",
            "description": "Optional tool/module name filter (e.g., 'normalization').",
        },
        "search": {
            "type": "string",
            "description": "Optional case-insensitive text filter over knob paths/descriptions.",
        },
        "path_prefix": {
            "type": "string",
            "description": "Optional path prefix filter for knob paths.",
        },
        "compact": {
            "type": "boolean",
            "description": "If true, return a compact payload with minimal module fields.",
            "default": False,
        },
    },
}

ARTIFACT_SERVER_INPUT_SCHEMA = {
    "type": "object",
    "properties": {},
}

RUN_HISTORY_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "run_id": {"type": "string"},
        "session_id": {
            "type": "string",
            "description": "Optional session scope. Recommended when reusing run_id values.",
        },
        "failures_only": {
            "type": "boolean",
            "description": "If true, only include failed/error history entries in ledger.",
            "default": False,
        },
        "latest_errors": {
            "type": "boolean",
            "description": "If true, include up to 5 most recent failed/error entries.",
            "default": False,
        },
        "latest_status_by_module": {
            "type": "boolean",
            "description": "If true, include the most recent status per module.",
            "default": False,
        },
        "limit": {
            "type": "integer",
            "description": "Optional max number of most recent ledger entries to return.",
            "minimum": 1,
        },
        "summary_only": {
            "type": "boolean",
            "description": "If true, return a compact ledger (module/status/timestamp/summary).",
            "default": True,
        },
    },
    "required": ["run_id"],
}

DATA_HEALTH_REPORT_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "run_id": {"type": "string"},
        "session_id": {
            "type": "string",
            "description": "Optional session scope. Recommended when reusing run_id values.",
        },
    },
    "required": ["run_id"],
}

DATA_DICTIONARY_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        **_GCS_PATH_PROP,
        "session_id": {
            "type": "string",
            "description": (
                "Optional in-memory session identifier to use as the primary input source "
                "when building from an existing run context."
            ),
        },
        **_INPUT_ID_PROP,
        "run_id": {
            "type": "string",
            "description": (
                "Optional run identifier used for output paths and artifact naming. "
                "This does not resolve the input source by itself; provide gcs_path, "
                "session_id, or input_id."
            ),
            "default": "mcp_run",
        },
        "runtime": {
            "type": "object",
            "description": "Optional runtime overlay for cross-cutting execution settings.",
        },
        "profile_depth": {
            "type": "string",
            "enum": ["light", "standard", "deep"],
            "default": "standard",
        },
        "include_examples": {
            "type": "boolean",
            "default": True,
        },
        "prelaunch_report": {
            "type": "boolean",
            "default": True,
            "description": "Reserve a prelaunch dictionary/readiness surface seeded from infer_configs.",
        },
    },
    "anyOf": [
        {"required": ["gcs_path"]},
        {"required": ["session_id"]},
        {"required": ["input_id"]},
    ],
}

COCKPIT_DASHBOARD_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "limit": {
            "type": "integer",
            "description": "How many recent runs to include in the cockpit dashboard. Maximum 50.",
            "minimum": 1,
            "maximum": 50,
            "default": 8,
        }
    },
}

PIPELINE_DASHBOARD_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "run_id": {"type": "string"},
        "session_id": {
            "type": "string",
            "description": "Optional session scope. Recommended when reusing run_id values.",
        },
    },
    "required": ["run_id"],
}
