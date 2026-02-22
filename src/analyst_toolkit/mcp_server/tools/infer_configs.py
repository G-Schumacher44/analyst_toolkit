"""MCP tool: toolkit_infer_configs — config generation via analyst_toolkit_deploy."""


async def _toolkit_infer_configs(
    gcs_path: str,
    options: dict | None = None,
) -> dict:
    """
    Generate YAML config strings for toolkit modules by inspecting the dataset.

    Returns a dict with the generated YAML config string.
    """
    options = options or {}
    try:
        try:
            from analyst_toolkit_deploy.infer_configs import infer_configs
        except ImportError:
            from analyst_toolkit_deployment_utility.infer_configs import infer_configs
    except ImportError as exc:
        return {
            "status": "error",
            "error": (
                f"Deployment utility not found ({str(exc)}). "
                "Ensure analyst-toolkit-deploy is in requirements-mcp.txt and rebuild."
            ),
            "config_yaml": "",
        }

    config_yaml = infer_configs(
        root=options.get("root", "."),
        input_path=gcs_path,
        outdir=options.get("outdir"),
        sample_rows=options.get("sample_rows"),
        max_unique=options.get("max_unique", 30),
        exclude_patterns=options.get("exclude_patterns", "id|uuid|tag"),
        detect_datetimes=options.get("detect_datetimes", True),
        datetime_hints=options.get("datetime_hints"),
    )

    return {
        "status": "pass",
        "module": "infer_configs",
        "config_yaml": config_yaml,
    }


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "gcs_path": {
            "type": "string",
            "description": "Path to the dataset (local CSV/parquet or gs:// URI).",
        },
        "options": {
            "type": "object",
            "description": "Optional overrides: sample_rows, max_unique, exclude_patterns, detect_datetimes, datetime_hints, outdir.",
            "default": {},
        },
    },
    "required": ["gcs_path"],
}

from analyst_toolkit.mcp_server.server import register_tool  # noqa: E402

register_tool(
    name="toolkit_infer_configs",
    fn=_toolkit_infer_configs,
    description=(
        "Inspect a dataset and generate YAML config strings for toolkit modules. "
        "Returns dict of module_name → YAML string."
    ),
    input_schema=_INPUT_SCHEMA,
)
