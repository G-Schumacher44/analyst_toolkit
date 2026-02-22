"""
🧪 Module: validate_data.py

Rule-based validation engine for tabular datasets in the Analyst Toolkit.

This module evaluates data against schema, dtype, categorical, and numeric
range rules defined in a config dictionary. It is non-destructive and produces
a structured summary of pass/fail results suitable for QA dashboards.

Checks include:
- Schema conformity
- Dtype enforcement
- Categorical value validation
- Numeric range enforcement

Returns structured output for downstream display or export.
"""

import pandas as pd


def validate_categorical_values(df: pd.DataFrame, validation_plan: dict) -> dict:
    """
    Checks for values not included in the allowed category list and summarizes them.
    """
    invalid_details = {}
    for col, allowed in validation_plan.items():
        if col in df.columns:
            allowed_set = set(allowed)
            # Find rows with invalid categorical values (excluding nulls)
            violating_rows = df[~df[col].isin(allowed_set) & df[col].notna()]

            if not violating_rows.empty:
                # Create a summary of the unique invalid values and their counts
                invalid_summary_df = violating_rows[col].value_counts().reset_index()
                invalid_summary_df.columns = ["Invalid Value", "Count"]

                invalid_details[col] = {
                    "allowed_values": allowed,
                    "violating_rows": violating_rows,
                    "invalid_value_summary": invalid_summary_df,
                }
    return invalid_details


def run_validation_suite(df: pd.DataFrame, config: dict) -> dict:
    """
    Runs a suite of validation checks and returns a structured, auditable results dictionary.
    """
    schema_validation_cfg = config.get("schema_validation", {})
    rules = schema_validation_cfg.get("rules", {})
    results = {}

    # --- Schema Conformity ---
    expected_cols = set(rules.get("expected_columns", []))
    actual_cols = set(df.columns)
    results["schema_conformity"] = {
        "rule_description": "Verify column names match the expected schema.",
        "passed": actual_cols == expected_cols,
        "details": {
            "missing_columns": list(expected_cols - actual_cols),
            "unexpected_columns": list(actual_cols - expected_cols),
        },
    }

    # --- Dtype Enforcement ---
    expected_types = rules.get("expected_types", {})
    mismatches = {}
    for col, expected in expected_types.items():
        if col in df.columns and str(df[col].dtype) != expected:
            mismatches[col] = {"expected": expected, "actual": str(df[col].dtype)}
    results["dtype_enforcement"] = {
        "rule_description": "Verify column data types match expectations.",
        "passed": not mismatches,
        "details": mismatches,
    }

    # --- Categorical Value Validation ---
    allowed_values = rules.get("categorical_values", {})
    cat_violations = validate_categorical_values(df, allowed_values)
    results["categorical_values"] = {
        "rule_description": "Verify values in categorical columns are within an allowed set.",
        "passed": not cat_violations,
        "details": cat_violations,
    }

    # --- Numeric Range Validation ---
    numeric_ranges = rules.get("numeric_ranges", {})
    range_violations = {}
    for col, bounds in numeric_ranges.items():
        if col in df.columns and "min" in bounds and "max" in bounds:
            min_val, max_val = bounds["min"], bounds["max"]
            violating_rows = df[~df[col].between(min_val, max_val) & df[col].notna()]
            if not violating_rows.empty:
                range_violations[col] = {
                    "enforced_range": f"[{min_val}, {max_val}]",
                    "violating_rows": violating_rows,
                }
    results["numeric_ranges"] = {
        "rule_description": "Verify values in numeric columns are within a defined range.",
        "passed": not range_violations,
        "details": range_violations,
    }

    # --- Row-Level Coverage Calculation ---
    all_failing_indices = set()
    for col, violation_info in range_violations.items():
        all_failing_indices.update(violation_info["violating_rows"].index)
    for col, violation_info in cat_violations.items():
        all_failing_indices.update(violation_info["violating_rows"].index)

    total_rows = len(df)
    failing_rows_count = len(all_failing_indices)
    coverage_pct = ((total_rows - failing_rows_count) / total_rows * 100) if total_rows > 0 else 100
    results["summary"] = {"row_coverage_percent": round(coverage_pct, 2)}

    return results
