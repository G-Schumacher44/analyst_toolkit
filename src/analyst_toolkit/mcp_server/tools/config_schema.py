"""MCP tool: toolkit_get_config_schema — returns JSON Schema for module configs."""

from analyst_toolkit.mcp_server.config_models import CONFIG_MODELS


async def _toolkit_get_config_schema(module_name: str) -> dict:
    """
    Return the JSON Schema for a specific module's configuration.
    Valid modules are derived from CONFIG_MODELS.
    """
    if module_name not in CONFIG_MODELS:
        return {
            "status": "error",
            "message": f"Unknown module: {module_name}. Available: {list(CONFIG_MODELS.keys())}",
        }

    model = CONFIG_MODELS[module_name]
    schema = model.model_json_schema()

    return {
        "status": "pass",
        "module": module_name,
        "schema": schema,
    }


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "module_name": {
            "type": "string",
            "description": "Name of the module (e.g., 'normalization', 'imputation').",
            "enum": list(CONFIG_MODELS.keys()),
        }
    },
    "required": ["module_name"],
}

from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="get_config_schema",
    fn=_toolkit_get_config_schema,
    description="Returns the JSON Schema for a specific module's configuration to help guide config creation.",
    input_schema=_INPUT_SCHEMA,
)
