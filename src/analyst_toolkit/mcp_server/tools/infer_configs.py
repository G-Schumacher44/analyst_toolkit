"""MCP tool: toolkit_infer_configs — config generation via analyst_toolkit_deploy."""

import inspect
import os
import tempfile

from analyst_toolkit.mcp_server.io import load_input, resolve_run_context, save_to_session
from analyst_toolkit.mcp_server.response_utils import next_action, with_next_actions
from analyst_toolkit.mcp_server.runtime_overlay import (
    normalize_runtime_overlay,
    runtime_to_tool_overrides,
)
from analyst_toolkit.mcp_server.schemas import INPUT_ID_PROP


def _call_external_infer_configs(
    infer_configs_fn,
    *,
    input_path: str,
    options: dict,
    modules: list[str] | None,
    sample_rows: int | None,
) -> tuple[dict, list[str]]:
    """Call the external infer_configs helper with signature compatibility."""
    kwargs = {
        "root": options.get("root", "."),
        "input_path": input_path,
        "modules": modules or options.get("modules"),
        "outdir": options.get("outdir"),
        "sample_rows": sample_rows or options.get("sample_rows"),
        "max_unique": options.get("max_unique", 30),
        "exclude_patterns": options.get("exclude_patterns", "id|uuid|tag"),
        "detect_datetimes": options.get("detect_datetimes", True),
        "datetime_hints": options.get("datetime_hints"),
    }

    signature = inspect.signature(infer_configs_fn)
    accepts_var_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    if accepts_var_kwargs:
        return infer_configs_fn(**kwargs), []

    supported = set(signature.parameters)
    filtered_kwargs = {key: value for key, value in kwargs.items() if key in supported}
    dropped = sorted(key for key in kwargs if key not in supported and kwargs[key] is not None)
    warnings = []
    if dropped:
        warnings.append(
            "External infer_configs helper does not support the following arguments and they "
            f"were ignored: {', '.join(dropped)}."
        )
    return infer_configs_fn(**filtered_kwargs), warnings


async def _toolkit_infer_configs(
    gcs_path: str | None = None,
    session_id: str | None = None,
    input_id: str | None = None,
    runtime: dict | str | None = None,
    options: dict | None = None,
    modules: list[str] | None = None,
    sample_rows: int | None = None,
    run_id: str | None = None,
) -> dict:
    """
    Generate YAML config strings for toolkit modules by inspecting the dataset at gcs_path or session_id.

    Returns a dict with the generated YAML config string.
    """
    runtime_cfg, runtime_warnings = normalize_runtime_overlay(runtime)
    runtime_overrides = runtime_to_tool_overrides(runtime_cfg)
    runtime_applied = bool(runtime_cfg)
    gcs_path = gcs_path or runtime_overrides.get("gcs_path")
    session_id = session_id or runtime_overrides.get("session_id")
    input_id = input_id or runtime_overrides.get("input_id")
    run_id = run_id or runtime_overrides.get("run_id")
    run_id, _lifecycle = resolve_run_context(run_id, session_id)
    options = options or {}
    provided_inputs = [gcs_path is not None, session_id is not None, input_id is not None]
    if sum(provided_inputs) > 1:
        return {
            "status": "error",
            "module": "infer_configs",
            "error": "Provide exactly one of gcs_path, session_id, or input_id.",
            "error_code": "AMBIGUOUS_INPUT_SOURCE",
            "config_yaml": "",
        }
    df = load_input(gcs_path, session_id=session_id, input_id=input_id)

    # If it came from a path and we don't have a session, start one
    if not session_id:
        session_id = save_to_session(df)

    # Always materialize an input snapshot locally for inference.
    # This avoids path-construction drift between modules and ensures deterministic reads.
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
                "Ensure analyst_toolkit_deploy is installed from requirements-mcp.txt and rebuild."
            ),
            "config_yaml": "",
        }

    external_warnings: list[str] = []
    try:
        configs, external_warnings = _call_external_infer_configs(
            infer_configs,
            input_path=input_path,
            options=options,
            modules=modules,
            sample_rows=sample_rows,
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
            "run_id": run_id,
            "session_id": session_id,
            "configs": configs,
            "runtime_applied": runtime_applied,
            "warnings": runtime_warnings + external_warnings,
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
        **INPUT_ID_PROP,
        "runtime": {
            "type": ["object", "string"],
            "description": (
                "Optional runtime overlay dict or YAML string for run-scoped settings "
                "such as run_id and input_path."
            ),
            "default": {},
        },
        "run_id": {
            "type": "string",
            "description": "Optional run identifier propagated through runtime-aware workflows.",
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
        {"required": ["input_id"]},
        {"required": ["runtime"]},
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
