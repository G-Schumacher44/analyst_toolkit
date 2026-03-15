"""Pure data-dictionary payload builders."""

from __future__ import annotations

from typing import Any

import pandas as pd
import yaml

from analyst_toolkit.m01_diagnostics.data_diag import generate_data_profile

_PROFILE_DEPTH_SETTINGS = {
    "light": {"max_rows": 3, "high_cardinality_threshold": 20, "example_limit": 3},
    "standard": {"max_rows": 5, "high_cardinality_threshold": 12, "example_limit": 5},
    "deep": {"max_rows": 10, "high_cardinality_threshold": 8, "example_limit": 8},
}


def _profile_settings(profile_depth: str) -> dict[str, int]:
    return _PROFILE_DEPTH_SETTINGS.get(profile_depth, _PROFILE_DEPTH_SETTINGS["standard"])


def _parse_inferred_configs(configs: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    parsed: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    for module_name, payload in configs.items():
        if isinstance(payload, dict):
            root = payload.get(module_name, payload)
            parsed[module_name] = root if isinstance(root, dict) else {}
            continue
        if not isinstance(payload, str):
            warnings.append(f"Inferred config for {module_name} was not YAML text.")
            parsed[module_name] = {}
            continue
        try:
            loaded = yaml.safe_load(payload) or {}
        except yaml.YAMLError as exc:
            warnings.append(f"Failed to parse inferred {module_name} config: {exc}")
            parsed[module_name] = {}
            continue
        if not isinstance(loaded, dict):
            warnings.append(f"Inferred config for {module_name} did not parse to a dict.")
            parsed[module_name] = {}
            continue
        root = loaded.get(module_name, loaded)
        parsed[module_name] = root if isinstance(root, dict) else {}
    return parsed, warnings


def _semantic_type_for(series: pd.Series) -> str:
    name = str(series.name or "").strip().lower()
    dtype = str(series.dtype)
    if name.endswith("_id") or name == "id":
        return "identifier"
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "numeric"
    non_null = series.dropna()
    if non_null.empty:
        return "unknown"
    # Bound the categorical heuristic so tiny samples are not overfit while
    # very high-cardinality text columns do not get mislabeled as categorical.
    if non_null.nunique() <= min(20, max(5, len(non_null) // 2)):
        return "categorical"
    if "date" in name or "time" in name:
        return "datetime_like_text"
    if dtype == "object":
        return "text"
    return dtype


def _example_values(series: pd.Series, limit: int, include_examples: bool) -> str:
    if not include_examples:
        return "Omitted"
    values: list[str] = []
    seen: set[str] = set()
    for value in series.dropna():
        text = str(value)
        if text in seen:
            continue
        seen.add(text)
        values.append(text)
        if len(values) >= limit:
            break
    return ", ".join(values) if values else "None"


def _format_numeric_rule(bounds: Any) -> str:
    if not isinstance(bounds, dict):
        return ""
    lower = bounds.get("min")
    upper = bounds.get("max")
    if lower is None and upper is None:
        return ""
    return (
        f"min={lower if lower is not None else '-inf'}, max={upper if upper is not None else 'inf'}"
    )


def build_data_dictionary_report(
    df: pd.DataFrame,
    *,
    inferred_configs: dict[str, Any] | None = None,
    profile_depth: str = "standard",
    include_examples: bool = True,
    prelaunch_report: bool = True,
) -> dict[str, Any]:
    settings = _profile_settings(profile_depth)
    profile = generate_data_profile(
        df,
        high_cardinality_threshold=settings["high_cardinality_threshold"],
        max_rows=settings["max_rows"],
    )["for_display"]
    schema_df = profile.get("schema", pd.DataFrame())
    parsed_configs, parse_warnings = _parse_inferred_configs(inferred_configs or {})

    normalization_cfg = parsed_configs.get("normalization", {})
    duplicates_cfg = parsed_configs.get("duplicates", {})
    outliers_cfg = parsed_configs.get("outliers", {})
    imputation_cfg = parsed_configs.get("imputation", {})
    validation_cfg = parsed_configs.get("validation", {})

    normalization_rules = normalization_cfg.get("rules", {})
    validation_rules = validation_cfg.get("schema_validation", {}).get("rules", {})
    outlier_specs = outliers_cfg.get("detection_specs", {})
    imputation_strategies = imputation_cfg.get("rules", {}).get("strategies", {})
    duplicate_subset = duplicates_cfg.get("subset_columns") or []
    expected_columns = validation_rules.get("expected_columns") or []
    categorical_rules = validation_rules.get("categorical_values", {})
    numeric_rules = validation_rules.get("numeric_ranges", {})
    dtype_rules = normalization_rules.get("coerce_dtypes", {})

    schema_lookup = {}
    if (
        isinstance(schema_df, pd.DataFrame)
        and not schema_df.empty
        and "Column" in schema_df.columns
    ):
        if schema_df["Column"].duplicated().any():
            parse_warnings.append(
                "Profile schema contained duplicate Column entries; later rows were kept when building the dictionary lookup."
            )
            schema_df = schema_df.drop_duplicates(subset=["Column"], keep="last")
        schema_lookup = schema_df.set_index("Column").to_dict(orient="index")

    column_rows: list[dict[str, Any]] = []
    readiness_rows: list[dict[str, Any]] = []
    expected_rows: list[dict[str, Any]] = []
    missing_expected = [column for column in expected_columns if column not in df.columns]

    for column in expected_columns:
        expected_rows.append(
            {
                "Column": column,
                "Observed": "Yes" if column in df.columns else "No",
                "Contract Source": "infer_configs validation",
                "Expected Dtype": dtype_rules.get(column, ""),
                "Allowed Values Preview": ", ".join(
                    map(str, categorical_rules.get(column, [])[:5])
                ),
                "Numeric Rule": _format_numeric_rule(numeric_rules.get(column)),
            }
        )

    if not expected_rows:
        for column in df.columns:
            series = df[column]
            expected_rows.append(
                {
                    "Column": column,
                    "Observed": "Yes",
                    "Contract Source": "observed profile baseline",
                    "Expected Dtype": str(dtype_rules.get(column, series.dtype)),
                    "Allowed Values Preview": "",
                    "Numeric Rule": "",
                }
            )

    for column in df.columns:
        series = df[column]
        schema_entry = schema_lookup.get(column, {})
        distinct_count = int(series.nunique(dropna=True))
        null_count = int(series.isna().sum())
        null_pct = round((null_count / len(df)) * 100, 2) if len(df) else 0.0
        duplicate_key = column in duplicate_subset
        outlier_spec = outlier_specs.get(column) or outlier_specs.get("__default__", {})
        outlier_method = (
            str(outlier_spec.get("method", "")) if isinstance(outlier_spec, dict) else ""
        )
        imputation_strategy = imputation_strategies.get(column)
        transformation_notes = []
        if column in dtype_rules:
            transformation_notes.append(f"coerce_dtypes -> {dtype_rules[column]}")
        if duplicate_key:
            transformation_notes.append("duplicate identity column")
        if imputation_strategy:
            transformation_notes.append(f"imputation -> {imputation_strategy}")
        if outlier_method:
            transformation_notes.append(f"outlier method -> {outlier_method}")
        allowed_values = categorical_rules.get(column, [])
        quality_notes = str(schema_entry.get("Audit Remarks", "") or "").replace("<br>", "; ")

        column_rows.append(
            {
                "Column": column,
                "Observed Dtype": str(series.dtype),
                "Expected Dtype": str(dtype_rules.get(column, "")),
                "Semantic Type": _semantic_type_for(series),
                "Expected In Schema": "Yes" if column in expected_columns else "Inferred Only",
                "Nullable": "Yes" if null_count > 0 else "No",
                "Unique": "Yes" if distinct_count == len(series) and len(series) > 0 else "No",
                "Distinct Count": distinct_count,
                "Null Count": null_count,
                "Null %": null_pct,
                "Example Values": _example_values(
                    series, settings["example_limit"], include_examples
                ),
                "Allowed Values Preview": ", ".join(map(str, allowed_values[:5])) or "",
                "Numeric Rule": _format_numeric_rule(numeric_rules.get(column)),
                "Transformation Notes": "; ".join(transformation_notes) or "",
                "Quality Notes": quality_notes,
            }
        )

        if column not in expected_columns and expected_columns:
            readiness_rows.append(
                {
                    "Severity": "warn",
                    "Type": "unexpected_column",
                    "Column": column,
                    "Detail": "Observed in dataset but not listed in inferred validation schema.",
                }
            )
        if (expected_columns or dtype_rules) and not dtype_rules.get(column):
            readiness_rows.append(
                {
                    "Severity": "info",
                    "Type": "missing_dtype_hint",
                    "Column": column,
                    "Detail": "No inferred dtype coercion hint was available for this column.",
                }
            )

    for column in missing_expected:
        readiness_rows.append(
            {
                "Severity": "fail",
                "Type": "missing_expected_column",
                "Column": column,
                "Detail": "Present in inferred validation schema but missing from the current dataset.",
            }
        )

    if not expected_columns:
        readiness_rows.append(
            {
                "Severity": "warn",
                "Type": "no_expected_schema",
                "Column": "",
                "Detail": "infer_configs did not produce an expected_columns validation contract.",
            }
        )
    if not parsed_configs.get("validation"):
        readiness_rows.append(
            {
                "Severity": "warn",
                "Type": "no_validation_contract",
                "Column": "",
                "Detail": "Validation config was unavailable, so schema/rule expectations are partial.",
            }
        )
    if not (expected_columns or dtype_rules or categorical_rules or numeric_rules):
        readiness_rows.append(
            {
                "Severity": "info",
                "Type": "business_metadata_needed",
                "Column": "",
                "Detail": "Only a profile-derived baseline contract is available. Add infer_configs or authored metadata to turn this into a stronger prelaunch contract.",
            }
        )
    if parse_warnings:
        for warning in parse_warnings:
            readiness_rows.append(
                {
                    "Severity": "warn",
                    "Type": "infer_parse_warning",
                    "Column": "",
                    "Detail": warning,
                }
            )

    status = "pass"
    if any(row["Severity"] == "fail" for row in readiness_rows):
        status = "fail"
    elif readiness_rows:
        status = "warn"

    overview_df = pd.DataFrame(
        [
            {
                "Rows": len(df),
                "Columns": len(df.columns),
                "Expected Columns": len(expected_columns),
                "Missing Expected Columns": len(missing_expected),
                "Metadata Gaps": len(readiness_rows),
                "Profile Depth": profile_depth,
                "Examples Included": include_examples,
                "Prelaunch Report": prelaunch_report,
                "Inference Seeded": bool(parsed_configs),
            }
        ]
    )
    return {
        "overview": overview_df,
        "expected_schema": pd.DataFrame(expected_rows),
        "column_dictionary": pd.DataFrame(column_rows),
        "prelaunch_readiness": pd.DataFrame(readiness_rows),
        "profile_snapshot": schema_df if isinstance(schema_df, pd.DataFrame) else pd.DataFrame(),
        "__dashboard_meta__": {
            "status": status,
            "profile_depth": profile_depth,
            "include_examples": include_examples,
            "prelaunch_report": prelaunch_report,
            "warnings": parse_warnings,
            "template_path": "config/data_dictionary_request_template.yaml",
        },
    }
