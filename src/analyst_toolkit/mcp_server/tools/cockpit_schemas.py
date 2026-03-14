"""Input schemas for cockpit MCP tools."""

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
        "gcs_path": {
            "type": "string",
            "description": "Optional input dataset path when no session_id exists yet.",
        },
        "session_id": {
            "type": "string",
            "description": "Optional session scope when building from an existing run context.",
        },
        "run_id": {
            "type": "string",
            "description": "Optional run identifier used for future artifact names.",
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
