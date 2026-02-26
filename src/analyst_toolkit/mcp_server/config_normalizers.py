"""
config_normalizers.py — Shared config normalization helpers for MCP tools.
"""

from copy import deepcopy
from typing import Any


def normalize_validation_config(config: dict[str, Any]) -> dict[str, Any]:
    """
    Accept both full module config and MCP shorthand config.

    MCP config schema exposes top-level `rules`, while M02 expects:
      validation.schema_validation.rules
    """
    if "validation" in config and isinstance(config.get("validation"), dict):
        base_cfg = deepcopy(config["validation"])
    else:
        base_cfg = deepcopy(config)

    if not isinstance(base_cfg, dict):
        base_cfg = {}

    schema_cfg = base_cfg.get("schema_validation", {})
    if not isinstance(schema_cfg, dict):
        schema_cfg = {}
    else:
        schema_cfg = deepcopy(schema_cfg)

    nested_rules = schema_cfg.get("rules", {})
    if not isinstance(nested_rules, dict):
        nested_rules = {}

    shorthand_rules = base_cfg.get("rules", {})
    if isinstance(shorthand_rules, dict) and shorthand_rules:
        nested_rules = {**nested_rules, **shorthand_rules}

    schema_cfg["rules"] = nested_rules
    schema_cfg.setdefault("run", True)
    if "fail_on_error" in base_cfg and "fail_on_error" not in schema_cfg:
        schema_cfg["fail_on_error"] = bool(base_cfg["fail_on_error"])
    schema_cfg.setdefault("fail_on_error", False)

    base_cfg["schema_validation"] = schema_cfg
    base_cfg.pop("rules", None)
    return base_cfg


def normalize_final_audit_config(config: dict[str, Any]) -> dict[str, Any]:
    """
    Accept both full module config and MCP shorthand config.

    MCP callers often pass top-level `rules`, while M10 expects:
      final_audit.certification.schema_validation.rules
    """
    if "final_audit" in config and isinstance(config.get("final_audit"), dict):
        base_cfg = deepcopy(config["final_audit"])
    else:
        base_cfg = deepcopy(config)

    if not isinstance(base_cfg, dict):
        base_cfg = {}

    cert_cfg = base_cfg.get("certification", {})
    if not isinstance(cert_cfg, dict):
        cert_cfg = {}
    else:
        cert_cfg = deepcopy(cert_cfg)

    schema_cfg = cert_cfg.get("schema_validation", {})
    if not isinstance(schema_cfg, dict):
        schema_cfg = {}
    else:
        schema_cfg = deepcopy(schema_cfg)

    nested_rules = schema_cfg.get("rules", {})
    if not isinstance(nested_rules, dict):
        nested_rules = {}

    shorthand_rules = base_cfg.get("rules", {})
    if isinstance(shorthand_rules, dict) and shorthand_rules:
        nested_rules = {**nested_rules, **shorthand_rules}

    if "disallowed_null_columns" in base_cfg and isinstance(
        base_cfg.get("disallowed_null_columns"), list
    ):
        nested_rules["disallowed_null_columns"] = base_cfg["disallowed_null_columns"]

    schema_cfg["rules"] = nested_rules
    schema_cfg.setdefault("run", True)
    if "fail_on_error" in base_cfg and "fail_on_error" not in schema_cfg:
        schema_cfg["fail_on_error"] = bool(base_cfg["fail_on_error"])
    schema_cfg.setdefault("fail_on_error", True)

    cert_cfg.setdefault("run", True)
    cert_cfg["schema_validation"] = schema_cfg

    base_cfg["certification"] = cert_cfg
    base_cfg.pop("rules", None)
    base_cfg.pop("schema_validation", None)
    base_cfg.pop("disallowed_null_columns", None)
    base_cfg.pop("fail_on_error", None)
    return base_cfg


def normalize_outliers_config(base_cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Accept canonical M05 config plus shorthand used by golden templates.

    Shorthand example:
      {"method": "iqr", "iqr_multiplier": 1.1, "columns": ["a", "b"]}
    """
    if not isinstance(base_cfg, dict):
        return {}

    normalized = dict(base_cfg)
    detection_specs = normalized.get("detection_specs", {})
    if not isinstance(detection_specs, dict):
        detection_specs = {}
    else:
        detection_specs = dict(detection_specs)

    method = normalized.get("method")
    columns = normalized.get("columns")

    if isinstance(method, str) and method in {"iqr", "zscore"}:
        spec: dict[str, object] = {"method": method}
        if method == "iqr" and isinstance(normalized.get("iqr_multiplier"), (int, float)):
            spec["iqr_multiplier"] = float(normalized["iqr_multiplier"])
        if method == "zscore" and isinstance(normalized.get("zscore_threshold"), (int, float)):
            spec["zscore_threshold"] = float(normalized["zscore_threshold"])

        if isinstance(columns, list) and columns:
            for col in columns:
                if isinstance(col, str) and col.strip():
                    col_name = col.strip()
                    current = detection_specs.get(col_name, {})
                    if not isinstance(current, dict):
                        current = {}
                    detection_specs[col_name] = {**spec, **current}
        elif "__default__" not in detection_specs:
            detection_specs["__default__"] = spec

    normalized["detection_specs"] = detection_specs
    normalized.pop("method", None)
    normalized.pop("columns", None)
    normalized.pop("iqr_multiplier", None)
    normalized.pop("zscore_threshold", None)
    return normalized


def normalize_module_config(module_name: str, config: dict[str, Any]) -> dict[str, Any]:
    """Return normalized module-level config for a known module."""
    if module_name == "validation":
        return normalize_validation_config(config)
    if module_name == "final_audit":
        return normalize_final_audit_config(config)
    if module_name == "outliers":
        if "outlier_detection" in config and isinstance(config.get("outlier_detection"), dict):
            return normalize_outliers_config(config["outlier_detection"])
        return normalize_outliers_config(config)

    if module_name in config and isinstance(config.get(module_name), dict):
        return deepcopy(config[module_name])

    return deepcopy(config)
