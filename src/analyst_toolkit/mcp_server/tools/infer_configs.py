"""MCP tool: toolkit_infer_configs — config generation via analyst_toolkit_deploy."""


async def _toolkit_infer_configs(
    gcs_path: str,
    modules: list | None = None,
    options: dict | None = None,
) -> dict:
    """
    Generate YAML config strings for the specified toolkit modules by inspecting
    the dataset at gcs_path.

    Returns a dict of module_name → YAML string ready for direct use or editing.
    The deploy utility's infer_configs() API is called with input_path=gcs_path
    so it handles local, parquet, and GCS inputs identically.
    """
    modules = modules or []
    options = options or {}
    try:
        from analyst_toolkit_deploy.infer_configs import infer_configs
    except ImportError:
        return {
            "status": "error",
            "error": (
                "analyst_toolkit_deploy is not installed. "
                "Add it to requirements-mcp.txt and rebuild the container."
            ),
            "configs": {},
            "modules_generated": [],
        }

    configs = infer_configs(
        root=None,
        input_path=gcs_path,
        modules=modules or None,
        outdir=options.get("outdir"),
        sample_rows=options.get("sample_rows"),
        max_unique=options.get("max_unique", 30),
        exclude_patterns=options.get("exclude_patterns", "id|uuid|tag"),
        detect_datetimes=options.get("detect_datetimes", True),
        datetime_hints=options.get("datetime_hints"),
    )

    return {
        "configs": configs,
        "modules_generated": list(configs.keys()),
    }


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "gcs_path": {
            "type": "string",
            "description": "Path to the dataset (local CSV/parquet or gs:// URI).",
        },
        "modules": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Module names to generate configs for "
                "(e.g. ['validation', 'outliers', 'normalization']). "
                "Empty list = all inferrable."
            ),
            "default": [],
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
