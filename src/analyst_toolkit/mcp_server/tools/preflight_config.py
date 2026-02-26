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


def _allowed_keys(module_name: str) -> set[str]:
    common = {"run", "logging", "input_path", "settings", "export_html"}
    if module_name == "validation":
        return common | {
            "validation",
            "rules",
            "schema_validation",
            "fail_on_error",
        }
    if module_name == "final_audit":
        return common | {
            "final_audit",
            "raw_data_path",
            "final_edits",
            "certification",
            "rules",
            "disallowed_null_columns",
            "fail_on_error",
        }
    if module_name == "outliers":
        return common | {
            "outlier_detection",
            "detection_specs",
            "exclude_columns",
            "append_flags",
            "plotting",
            "export",
            "checkpoint",
            "method",
            "columns",
            "iqr_multiplier",
            "zscore_threshold",
        }
    if module_name == "normalization":
        return common | {"normalization", "rules"}
    if module_name == "imputation":
        return common | {"imputation", "rules"}
    if module_name == "duplicates":
        return common | {"duplicates", "subset_columns", "mode"}
    if module_name == "diagnostics":
        return common | {"diagnostics", "plotting", "max_plots", "profile"}
    return set()


def _unknown_keys(module_name: str, config: dict[str, Any]) -> list[str]:
    allowed = _allowed_keys(module_name)
    unknown: set[str] = set()
    if allowed:
        unknown.update(k for k in config.keys() if k not in allowed)
    return sorted(unknown)


def _unknown_effective_keys(module_name: str, config: dict[str, Any]) -> list[str]:
    unknown: set[str] = set()

    def _add_unknown(prefix: str, values: dict[str, Any], allowed: set[str]) -> None:
        for key in values.keys():
            if key not in allowed:
                unknown.add(f"{prefix}.{key}" if prefix else key)

    if module_name == "normalization":
        top_allowed = {"run", "logging", "input_path", "settings", "rules", "export_html"}
        _add_unknown("", config, top_allowed)
        rules = config.get("rules")
        if isinstance(rules, dict):
            rules_allowed = {
                "rename_columns",
                "standardize_text_columns",
                "value_mappings",
                "fuzzy_matching",
                "parse_datetimes",
                "coerce_dtypes",
            }
            _add_unknown("rules", rules, rules_allowed)
        return sorted(unknown)

    if module_name == "validation":
        top_allowed = {
            "run",
            "logging",
            "input_path",
            "settings",
            "schema_validation",
            "rules",
            "fail_on_error",
            "export_html",
        }
        _add_unknown("", config, top_allowed)
        schema_cfg = config.get("schema_validation")
        if isinstance(schema_cfg, dict):
            _add_unknown("schema_validation", schema_cfg, {"run", "fail_on_error", "rules"})
        return sorted(unknown)

    if module_name == "final_audit":
        top_allowed = {
            "run",
            "logging",
            "input_path",
            "settings",
            "raw_data_path",
            "final_edits",
            "certification",
            "rules",
            "disallowed_null_columns",
            "fail_on_error",
            "export_html",
        }
        _add_unknown("", config, top_allowed)
        cert_cfg = config.get("certification")
        if isinstance(cert_cfg, dict):
            _add_unknown("certification", cert_cfg, {"run", "schema_validation"})
            schema_cfg = cert_cfg.get("schema_validation")
            if isinstance(schema_cfg, dict):
                _add_unknown(
                    "certification.schema_validation", schema_cfg, {"run", "fail_on_error", "rules"}
                )
        return sorted(unknown)

    if module_name == "imputation":
        top_allowed = {"run", "logging", "input_path", "settings", "rules", "export_html"}
        _add_unknown("", config, top_allowed)
        return sorted(unknown)

    if module_name == "duplicates":
        top_allowed = {
            "run",
            "logging",
            "input_path",
            "settings",
            "subset_columns",
            "mode",
            "keep",
            "export_html",
        }
        _add_unknown("", config, top_allowed)
        return sorted(unknown)

    if module_name == "outliers":
        top_allowed = {
            "run",
            "logging",
            "input_path",
            "settings",
            "detection_specs",
            "exclude_columns",
            "append_flags",
            "plotting",
            "export",
            "checkpoint",
            "method",
            "columns",
            "iqr_multiplier",
            "zscore_threshold",
            "export_html",
        }
        _add_unknown("", config, top_allowed)
        return sorted(unknown)

    if module_name == "diagnostics":
        top_allowed = {
            "run",
            "logging",
            "input_path",
            "settings",
            "profile",
            "plotting",
            "max_plots",
            "export_html",
        }
        _add_unknown("", config, top_allowed)
        return sorted(unknown)

    return []


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


async def _toolkit_preflight_config(
    module_name: str,
    config: dict | None = None,
    strict: bool = False,
) -> dict:
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
    raw_unknown_keys = _unknown_keys(module_name, raw_config)
    effective_unknown_keys = _unknown_effective_keys(module_name, normalized)
    unknown_keys = sorted(set(raw_unknown_keys + effective_unknown_keys))
    if unknown_keys:
        warnings.append(
            "Unknown top-level keys detected and ignored by runtime: " + ", ".join(unknown_keys)
        )
    changed = coerced != raw_config or normalized != coerced

    if strict and (warnings or unknown_keys):
        return {
            "status": "error",
            "module": module_name,
            "summary": {
                "normalized": True,
                "input_changed": changed,
                "effective_rules_path": _rules_path_hint(module_name),
                "strict": True,
                "unknown_key_count": len(unknown_keys),
            },
            "warnings": warnings,
            "unknown_keys": unknown_keys,
            "message": "Strict preflight failed due to config warnings or unknown keys.",
            "effective_config": normalized,
            "canonical_config": {root_key: normalized},
        }

    return {
        "status": "pass",
        "module": module_name,
        "summary": {
            "normalized": True,
            "input_changed": changed,
            "effective_rules_path": _rules_path_hint(module_name),
            "strict": bool(strict),
            "unknown_key_count": len(unknown_keys),
        },
        "warnings": warnings,
        "unknown_keys": unknown_keys,
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
        "strict": {
            "type": "boolean",
            "description": "If true, fail when warnings or unknown top-level keys are detected.",
            "default": False,
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
