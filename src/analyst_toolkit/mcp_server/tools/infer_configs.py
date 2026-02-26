"""MCP tool: toolkit_infer_configs — config generation via analyst_toolkit_deploy."""

import os
import tempfile

from analyst_toolkit.mcp_server.io import load_input, save_to_session
from analyst_toolkit.mcp_server.response_utils import next_action, with_next_actions


async def _toolkit_infer_configs(
    gcs_path: str | None = None,
    session_id: str | None = None,
    options: dict | None = None,
    modules: list[str] | None = None,
    sample_rows: int | None = None,
) -> dict:
    """
    Generate YAML config strings for toolkit modules by inspecting the dataset at gcs_path or session_id.

    Returns a dict with the generated YAML config string.
    """
    options = options or {}
    df = load_input(gcs_path, session_id=session_id)

    # If it came from a path and we don't have a session, start one
    if not session_id:
        session_id = save_to_session(df)

    input_path = gcs_path
    temp_file = None

    if session_id and not gcs_path:
        # We need a physical file for the inference engine
        temp_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        df.to_csv(temp_file.name, index=False)
        input_path = temp_file.name

    try:
        try:
            from analyst_toolkit_deploy.infer_configs import infer_configs
        except ImportError:
            from analyst_toolkit_deployment_utility.infer_configs import infer_configs
    except ImportError as exc:
        if temp_file:
            os.unlink(temp_file.name)
        return {
            "status": "error",
            "error": (
                f"Deployment utility not found ({str(exc)}). "
                "Ensure analyst-toolkit-deploy is in requirements-mcp.txt and rebuild."
            ),
            "config_yaml": "",
        }

    try:
        configs = infer_configs(
            root=options.get("root", "."),
            input_path=input_path,
            modules=modules or options.get("modules"),
            outdir=options.get("outdir"),
            sample_rows=sample_rows or options.get("sample_rows"),
            max_unique=options.get("max_unique", 30),
            exclude_patterns=options.get("exclude_patterns", "id|uuid|tag"),
            detect_datetimes=options.get("detect_datetimes", True),
            datetime_hints=options.get("datetime_hints"),
        )
    finally:
        if temp_file:
            os.unlink(temp_file.name)

    module_order = [
        "normalization",
        "duplicates",
        "outliers",
        "imputation",
        "validation",
        "final_audit",
    ]
    apply_actions = [
        next_action(
            module,
            f"Apply inferred config for {module}.",
            {
                "session_id": session_id,
                "run_id": "<set_run_id>",
                "config": f"<configs.{module}>",
            },
        )
        for module in module_order
        if module in configs
    ]

    capability_action = next_action(
        "get_capability_catalog",
        "Cross-check inferred YAML paths against supported capability knobs.",
        {},
    )

    if not apply_actions:
        apply_actions = [
            next_action(
                "get_capability_catalog",
                "No executable module configs were inferred; review available knobs manually.",
                {},
            )
        ]
        next_steps = apply_actions
    else:
        next_steps = apply_actions + [capability_action]

    return with_next_actions(
        {
            "status": "pass",
            "module": "infer_configs",
            "session_id": session_id,
            "configs": configs,
        },
        next_steps,
    )


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "gcs_path": {
            "type": "string",
            "description": "Path to the dataset (local CSV/parquet or gs:// URI). Optional if session_id is used.",
        },
        "session_id": {
            "type": "string",
            "description": "Optional: In-memory session identifier from a previous tool run.",
        },
        "modules": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Module names to generate configs for. Defaults to all. "
                "Valid: validation, certification, outliers, diagnostics, "
                "normalization, duplicates, handling, imputation, final_audit."
            ),
        },
        "sample_rows": {
            "type": "integer",
            "description": "Read only the first N rows for speed. Defaults to all rows.",
        },
        "options": {
            "type": "object",
            "description": (
                "Advanced overrides: max_unique, exclude_patterns, detect_datetimes, "
                "datetime_hints, outdir."
            ),
            "default": {},
        },
    },
    "anyOf": [
        {"required": ["gcs_path"]},
        {"required": ["session_id"]},
    ],
}

from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="infer_configs",
    fn=_toolkit_infer_configs,
    description=(
        "Inspect a dataset and generate YAML config strings for toolkit modules. "
        "Returns dict of module_name → YAML string."
    ),
    input_schema=_INPUT_SCHEMA,
)
