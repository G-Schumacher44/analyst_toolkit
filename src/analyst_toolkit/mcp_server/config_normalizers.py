"""
config_normalizers.py — Shared config normalization helpers for MCP tools.
"""

from copy import deepcopy
from typing import Any

INFER_CONFIG_REQUIRED_WARNING = (
    "No inferred or explicit config found. Run infer_configs first for meaningful results."
)

_GENERATED_FLAG_SUFFIXES = ("_iqr_outlier", "_zscore_outlier")
_NUMERIC_TYPE_MARKERS = ("int", "float", "double", "decimal", "number")
_TEMPORAL_TYPE_MARKERS = ("datetime", "timestamp", "date")


def _is_non_text_expected_type(expected_type: Any) -> bool:
    if not isinstance(expected_type, str):
        return False
    normalized = expected_type.strip().lower()
    return any(marker in normalized for marker in (*_NUMERIC_TYPE_MARKERS, *_TEMPORAL_TYPE_MARKERS))


def _is_non_text_observed_dtype(dtype: Any) -> bool:
    if dtype is None:
        return False
    return _is_non_text_expected_type(str(dtype))


def _looks_like_lowercased_text_values(values: list[Any]) -> bool:
    text_values = [value for value in values if isinstance(value, str)]
    if not text_values:
        return False
    return all(value == value.strip().lower() for value in text_values)


def _normalize_allowed_values(allowed: Any) -> Any:
    if not isinstance(allowed, list):
        return allowed
    normalized: list[Any] = []
    seen: set[str] = set()
    for value in allowed:
        candidate: Any = value
        if isinstance(value, str):
            candidate = value.strip().lower()
            if candidate in seen:
                continue
            seen.add(candidate)
        normalized.append(candidate)
    return normalized


def _normalize_rule_contract(
    rules: dict[str, Any],
    *,
    expected_types: dict[str, Any],
    observed_df: Any | None = None,
) -> dict[str, Any]:
    normalized_rules = deepcopy(rules) if isinstance(rules, dict) else {}
    normalized_expected_types = deepcopy(expected_types) if isinstance(expected_types, dict) else {}

    if observed_df is not None and normalized_expected_types:
        for column in getattr(observed_df, "columns", []):
            if column not in normalized_expected_types:
                continue
            observed_dtype = getattr(observed_df[column], "dtype", None)
            if observed_dtype is None:
                continue
            observed_dtype_str = str(observed_dtype)
            expected_type = normalized_expected_types.get(column)
            if observed_dtype_str == expected_type:
                continue
            if _is_non_text_expected_type(expected_type) or _is_non_text_observed_dtype(
                observed_dtype
            ):
                normalized_expected_types[column] = observed_dtype_str

    if normalized_expected_types:
        normalized_rules["expected_types"] = normalized_expected_types

    expected_columns = normalized_rules.get("expected_columns", [])
    if isinstance(expected_columns, list) and observed_df is not None:
        for column in getattr(observed_df, "columns", []):
            if (
                isinstance(column, str)
                and column.endswith(_GENERATED_FLAG_SUFFIXES)
                and column not in expected_columns
            ):
                expected_columns.append(column)
        normalized_rules["expected_columns"] = expected_columns

    categorical_values = normalized_rules.get("categorical_values", {})
    if not isinstance(categorical_values, dict):
        return normalized_rules

    cleaned_categorical: dict[str, Any] = {}
    for column, allowed in categorical_values.items():
        observed_dtype = None
        if observed_df is not None and column in getattr(observed_df, "columns", []):
            observed_dtype = getattr(observed_df[column], "dtype", None)
        if _is_non_text_expected_type(
            normalized_expected_types.get(column)
        ) or _is_non_text_observed_dtype(observed_dtype):
            continue
        normalized_allowed = allowed
        if observed_df is not None and column in getattr(observed_df, "columns", []):
            observed_values = observed_df[column].dropna().astype(object).head(100).tolist()
            if _looks_like_lowercased_text_values(observed_values):
                normalized_allowed = _normalize_allowed_values(allowed)
        cleaned_categorical[column] = normalized_allowed

    normalized_rules["categorical_values"] = cleaned_categorical
    return normalized_rules


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

    # Lift certification.rules into schema_validation.rules (common agent shorthand)
    cert_shorthand_rules = cert_cfg.get("rules", {})
    if isinstance(cert_shorthand_rules, dict) and cert_shorthand_rules:
        nested_rules = {**nested_rules, **cert_shorthand_rules}

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
    cert_cfg.pop("rules", None)

    base_cfg["certification"] = cert_cfg
    base_cfg.pop("rules", None)
    base_cfg.pop("schema_validation", None)
    base_cfg.pop("disallowed_null_columns", None)
    base_cfg.pop("fail_on_error", None)
    return base_cfg


def sanitize_inferred_validation_config(config: dict[str, Any]) -> dict[str, Any]:
    """Remove categorical rules that do not make sense for numeric/datetime fields."""
    base_cfg = normalize_validation_config(config)
    schema_cfg = deepcopy(base_cfg.get("schema_validation", {}))
    rules = deepcopy(schema_cfg.get("rules", {}))
    expected_types = rules.get("expected_types", {})
    if not isinstance(expected_types, dict):
        expected_types = {}
    schema_cfg["rules"] = _normalize_rule_contract(rules, expected_types=expected_types)
    base_cfg["schema_validation"] = schema_cfg
    return {"validation": base_cfg}


def sanitize_inferred_final_audit_config(config: dict[str, Any]) -> dict[str, Any]:
    """Remove categorical rules that do not make sense for numeric/datetime fields."""
    base_cfg = normalize_final_audit_config(config)
    cert_cfg = deepcopy(base_cfg.get("certification", {}))
    schema_cfg = deepcopy(cert_cfg.get("schema_validation", {}))
    rules = deepcopy(schema_cfg.get("rules", {}))
    expected_types = rules.get("expected_types", {})
    if not isinstance(expected_types, dict):
        expected_types = {}
    schema_cfg["rules"] = _normalize_rule_contract(rules, expected_types=expected_types)
    cert_cfg["schema_validation"] = schema_cfg
    base_cfg["certification"] = cert_cfg
    return {"final_audit": base_cfg}


def adapt_validation_config_to_dataframe(config: dict[str, Any], df: Any) -> dict[str, Any]:
    """Align inferred validation rules to the transformed session dataframe."""
    base_cfg = normalize_validation_config(config)
    schema_cfg = deepcopy(base_cfg.get("schema_validation", {}))
    rules = deepcopy(schema_cfg.get("rules", {}))
    expected_types = rules.get("expected_types", {})
    if not isinstance(expected_types, dict):
        expected_types = {}
    schema_cfg["rules"] = _normalize_rule_contract(
        rules,
        expected_types=expected_types,
        observed_df=df,
    )
    base_cfg["schema_validation"] = schema_cfg
    return base_cfg


def adapt_final_audit_config_to_dataframe(config: dict[str, Any], df: Any) -> dict[str, Any]:
    """Align inferred certification rules to the transformed session dataframe."""
    base_cfg = normalize_final_audit_config(config)
    cert_cfg = deepcopy(base_cfg.get("certification", {}))
    schema_cfg = deepcopy(cert_cfg.get("schema_validation", {}))
    rules = deepcopy(schema_cfg.get("rules", {}))
    expected_types = rules.get("expected_types", {})
    if not isinstance(expected_types, dict):
        expected_types = {}
    schema_cfg["rules"] = _normalize_rule_contract(
        rules,
        expected_types=expected_types,
        observed_df=df,
    )
    cert_cfg["schema_validation"] = schema_cfg
    base_cfg["certification"] = cert_cfg
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


def has_actionable_validation_config(config: dict[str, Any]) -> bool:
    """Return True when validation rules contain at least one meaningful constraint."""
    normalized = normalize_validation_config(config)
    schema_cfg = normalized.get("schema_validation", {})
    if not isinstance(schema_cfg, dict):
        return False
    rules = schema_cfg.get("rules", {})
    if not isinstance(rules, dict):
        return False
    return any(
        bool(rules.get(key))
        for key in (
            "expected_columns",
            "expected_types",
            "categorical_values",
            "numeric_ranges",
            "disallowed_null_columns",
        )
    )


def has_actionable_normalization_config(config: dict[str, Any]) -> bool:
    """Return True when normalization contains at least one transformation rule."""
    if "normalization" in config and isinstance(config.get("normalization"), dict):
        normalized = deepcopy(config["normalization"])
    else:
        normalized = deepcopy(config)
    if not isinstance(normalized, dict):
        return False
    rules = normalized.get("rules", {})
    return isinstance(rules, dict) and any(bool(value) for value in rules.values())


def has_actionable_imputation_config(config: dict[str, Any]) -> bool:
    """Return True when imputation contains at least one executable strategy or rule."""
    if "imputation" in config and isinstance(config.get("imputation"), dict):
        normalized = deepcopy(config["imputation"])
    else:
        normalized = deepcopy(config)
    if not isinstance(normalized, dict):
        return False
    rules = normalized.get("rules", {})
    if not isinstance(rules, dict):
        return False
    strategies = rules.get("strategies", {})
    if isinstance(strategies, dict) and any(bool(value) for value in strategies.values()):
        return True
    return any(bool(value) for key, value in rules.items() if key != "strategies")


def has_actionable_outliers_config(config: dict[str, Any]) -> bool:
    """Return True when outlier detection contains at least one detection spec."""
    if "outlier_detection" in config and isinstance(config.get("outlier_detection"), dict):
        normalized = normalize_outliers_config(config["outlier_detection"])
    else:
        normalized = normalize_outliers_config(config)
    detection_specs = normalized.get("detection_specs", {})
    return isinstance(detection_specs, dict) and any(
        bool(spec) for spec in detection_specs.values()
    )
