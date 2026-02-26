"""MCP tool: toolkit_preflight_config — normalize and preview effective module config."""

from typing import Any

from analyst_toolkit.mcp_server.config_models import CONFIG_MODELS
from analyst_toolkit.mcp_server.config_normalizers import normalize_module_config
from analyst_toolkit.mcp_server.io import coerce_config
from analyst_toolkit.mcp_server.registry import register_tool


def _rules_path_hint(module_name: str) -> str:
    mapping = {
        "validation": "validation.schema_validation.rules.*",
        "final_audit": "final_audit.certification.schema_validation.rules.*",
        "outliers": "outlier_detection.detection_specs.<column>.*",
        "normalization": "normalization.rules.*",
        "imputation": "imputation.rules.*",
        "duplicates": "duplicates.(subset_columns|mode)",
        "diagnostics": "diagnostics.<module-specific>",
    }
    return mapping.get(module_name, "<module-specific>")


def _shape_warnings(module_name: str, config: dict[str, Any]) -> list[str]:
    warnings: list[str] = []

    if module_name == "validation":
        rules = config.get("rules")
        if isinstance(rules, dict) and "schema_validation" in rules:
            warnings.append(
                "Found nested 'rules.schema_validation'. Use top-level 'rules.*' shorthand "
                "or canonical 'validation.schema_validation.rules.*'."
            )

    if module_name == "final_audit":
        rules = config.get("rules")
        if isinstance(rules, dict) and "schema_validation" in rules:
            warnings.append(
                "Found nested 'rules.schema_validation'. For final_audit use top-level "
                "'rules.*' shorthand or canonical 'final_audit.certification.schema_validation.rules.*'."
            )

    if module_name == "outliers":
        has_shorthand = any(k in config for k in ("method", "columns", "iqr_multiplier"))
        if has_shorthand and "outlier_detection" in config:
            warnings.append(
                "Found mixed shorthand and canonical keys; shorthand is normalized into "
                "'outlier_detection.detection_specs'."
            )

    return warnings


async def _toolkit_preflight_config(module_name: str, config: dict | None = None) -> dict:
    """Normalize config input into the effective module config shape before execution."""
    if module_name not in CONFIG_MODELS:
        return {
            "status": "error",
            "module": module_name,
            "message": f"Unknown module: {module_name}. Available: {list(CONFIG_MODELS.keys())}",
        }

    raw_config = config or {}
    coerce_key = "outlier_detection" if module_name == "outliers" else module_name
    coerced = coerce_config(raw_config, coerce_key)
    normalized = normalize_module_config(module_name, coerced)
    root_key = "outlier_detection" if module_name == "outliers" else module_name

    warnings = _shape_warnings(module_name, raw_config)
    changed = coerced != raw_config or normalized != coerced

    return {
        "status": "pass",
        "module": module_name,
        "summary": {
            "normalized": True,
            "input_changed": changed,
            "effective_rules_path": _rules_path_hint(module_name),
        },
        "warnings": warnings,
        "effective_config": normalized,
        "canonical_config": {root_key: normalized},
    }


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "module_name": {
            "type": "string",
            "description": "Target module name to normalize config for.",
            "enum": list(CONFIG_MODELS.keys()),
        },
        "config": {
            "type": "object",
            "description": "Raw config candidate to normalize into the effective module shape.",
            "default": {},
        },
    },
    "required": ["module_name"],
}

register_tool(
    name="preflight_config",
    fn=_toolkit_preflight_config,
    description="Normalize a candidate config and preview the effective module config shape before execution.",
    input_schema=_INPUT_SCHEMA,
)
