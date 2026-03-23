"""
schemas.py — TypedDicts and JSON Schema definitions for MCP tool I/O.

All MCP tools return a dict matching ToolResponse (plus module-specific keys).
The inputSchema dicts here are passed to tools/list so fridai-core can validate
and document tool inputs.
"""

from typing import TypedDict

from analyst_toolkit.mcp_server.input.models import INPUT_ID_HEX_LENGTH, INPUT_ID_PATTERN


class ToolResponse(TypedDict):
    status: str  # "pass" | "warn" | "fail" | "error"
    module: str
    run_id: str
    summary: dict
    artifact_path: str  # absolute path to HTML report, or "" if not generated


# ------------------------------------------------------------------
# Reusable input schema fragments
# ------------------------------------------------------------------

_GCS_PATH_PROP = {
    "gcs_path": {
        "type": "string",
        "description": "Local file path (.csv / .parquet) or GCS URI (gs://bucket/path) to load data from. Optional if session_id is used.",
    }
}

_SESSION_ID_PROP = {
    "session_id": {
        "type": "string",
        "description": "Optional: In-memory session identifier from a previous tool run. If provided, gcs_path is ignored.",
    }
}

INPUT_ID_PROP = {
    "input_id": {
        "type": "string",
        "pattern": INPUT_ID_PATTERN,
        "description": (
            "Optional: Canonical server-managed input reference returned by input "
            "ingest/register flows. Uses a stable 16-hex suffix collision budget. "
            "If provided, gcs_path and session_id are ignored."
        ),
    }
}

_CONFIG_PROP = {
    "config": {
        "type": "object",
        "description": "Module config dict (matches the relevant YAML block). Merged with defaults.",
        "default": {},
    }
}

_RUNTIME_PROP = {
    "runtime": {
        "type": ["object", "string"],
        "description": (
            "Optional runtime overlay dict or YAML string for run-scoped execution settings "
            "such as run_id, input_path, export_html, plotting, and destination overrides."
        ),
        "default": {},
    }
}

_RUN_ID_PROP = {
    "run_id": {
        "type": "string",
        "description": "Run identifier used for output paths and artifact naming.",
        "default": "mcp_run",
    }
}

_OUTPUT_CONTROL_PROPS = {
    "output_bucket": {
        "type": "string",
        "description": "Optional: Override the GCS bucket for report uploads (e.g., 'gs://my-custom-bucket').",
    },
    "output_prefix": {
        "type": "string",
        "description": "Optional: Override the GCS prefix for report uploads (default: 'analyst_toolkit/reports').",
    },
    "export_path": {
        "type": "string",
        "description": "Optional: A specific GCS or local path to save the resulting dataset (e.g., 'gs://bucket/cleaned.parquet' or 'outputs/final.csv').",
    },
}


def base_input_schema(extra_props: dict | None = None) -> dict:
    """Return a JSON Schema object for standard toolkit tool inputs."""
    props = {
        **_GCS_PATH_PROP,
        **_SESSION_ID_PROP,
        **INPUT_ID_PROP,
        **_CONFIG_PROP,
        **_RUNTIME_PROP,
        **_RUN_ID_PROP,
        **_OUTPUT_CONTROL_PROPS,
    }
    if extra_props:
        props.update(extra_props)
    return {
        "type": "object",
        "properties": props,
        "anyOf": [
            {"required": ["gcs_path"]},
            {"required": ["session_id"]},
            {"required": ["input_id"]},
        ],
    }


# ------------------------------------------------------------------
# Module-specific response TypedDicts (extend ToolResponse)
# ------------------------------------------------------------------


class DiagnosticsResponse(ToolResponse):
    profile_shape: list  # [rows, cols]
    null_rate: float
    column_count: int


class ValidationResponse(ToolResponse):
    passed: bool
    failed_rules: list[str]
    issue_count: int


class OutliersResponse(ToolResponse):
    flagged_columns: list[str]
    outlier_count: int


class NormalizationResponse(ToolResponse):
    changes_made: int


class DuplicatesResponse(ToolResponse):
    duplicate_count: int
    mode: str


class ImputationResponse(ToolResponse):
    columns_imputed: list[str]
    nulls_filled: int


class InferConfigsResponse(TypedDict):
    configs: dict  # module_name → YAML string
    modules_generated: list[str]
