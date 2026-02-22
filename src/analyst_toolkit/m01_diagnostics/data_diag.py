"""
📊 Module: data_diag.py

Config-driven data profiling for pandas DataFrames.

Generates a structured summary of a dataset's characteristics including:
- Schema overview with audit remarks
- Missing value statistics
- Duplicate row counts and previews
- High cardinality detection
- Memory usage and shape
- Descriptive statistics and sample previews

Designed for use in diagnostics modules or interactive QA notebooks.
Supports export-ready output and optional validation metadata.
"""

import pandas as pd


def generate_data_profile(
    df: pd.DataFrame,
    high_cardinality_threshold: int = 10,
    max_rows: int = 5,
    quality_checks: dict = None,
    **kwargs,
):
    """
    Generates a structured profile of a DataFrame with optional audit metadata.

    Args:
        df (pd.DataFrame): The DataFrame to profile.
        high_cardinality_threshold (int): Threshold for flagging high-cardinality object columns.
        max_rows (int): Number of rows to include in preview tables.
        quality_checks (dict, optional): Includes skew threshold and expected dtypes.
        **kwargs: Additional parameters passed through (currently unused).

    Returns:
        dict: A dictionary with 'for_display' and 'for_export' keys containing DataFrames.
    """
    if quality_checks is None:
        quality_checks = {}
    skew_threshold = quality_checks.get("skew_threshold", 2.0)
    expected_dtypes = quality_checks.get("expected_dtypes", {})

    numeric_cols = df.select_dtypes(include="number")
    skews = numeric_cols.skew().abs()  # type: ignore

    # Schema summary: dtype and uniqueness
    schema_df = pd.DataFrame(
        {"Column": df.columns, "Dtype": df.dtypes.astype(str), "Unique Values": df.nunique()}
    )

    # Audit remarks: unexpected types and skew
    audit_remarks = []
    for i, row in schema_df.iterrows():
        col = row["Column"]
        remarks = []
        # Check 1: Unexpected Dtype
        if col in expected_dtypes and row["Dtype"] != expected_dtypes[col]:
            remarks.append(f"⚠️ Unexpected Type (Expected: {expected_dtypes[col]})")

        # Check 2: High Skew
        if col in skews and skews[col] > skew_threshold:
            remarks.append(f"⚠️ High Skew ({skews[col]:.2f})")

        # If no remarks were raised, the status is OK.
        if not remarks:
            audit_remarks.append("✅ OK")
        else:
            # Join multiple remarks with a line break for display
            audit_remarks.append("<br>".join(remarks))

    # Renamed column for clarity
    schema_df["Audit Remarks"] = audit_remarks

    # Merge missing value stats
    missing_counts = df.isnull().sum().reset_index(name="Missing Count")
    missing_counts.columns = ["Column", "Missing Count"]  # type: ignore[assignment]
    missing_counts["Missing %"] = ((missing_counts["Missing Count"] / len(df)) * 100).round(2)
    schema_df = pd.merge(schema_df, missing_counts, on="Column", how="left")
    schema_df["Missing Count"] = schema_df["Missing Count"].fillna(0).astype(int)

    # identify high-cardinality object columns
    high_card_df = (
        schema_df[
            (schema_df["Dtype"] == "object")
            & (schema_df["Unique Values"] > high_cardinality_threshold)
        ][["Column", "Unique Values"]]
        .sort_values("Unique Values", ascending=False)
        .reset_index(drop=True)
    )

    dup_count = df.duplicated().sum()
    dup_summary_df = pd.DataFrame(
        [
            {
                "Duplicate Rows": dup_count,
                "Duplicate %": (dup_count / len(df) * 100).round(2) if len(df) > 0 else 0,
            }
        ]
    )
    duplicated_rows_df = df[df.duplicated(keep=False)].head(max_rows)
    if numeric_cols.empty:
        describe_df = pd.DataFrame(columns=["Metric"])
    else:
        describe_df = numeric_cols.describe().T  # type: ignore
        describe_df["skew"] = numeric_cols.skew()
        describe_df["kurtosis"] = numeric_cols.kurt()
        describe_df = describe_df.reset_index().rename(columns={"index": "Metric"})
    sample_head_df = df.head(max_rows)
    shape_df = pd.DataFrame([{"Rows": df.shape[0], "Columns": df.shape[1]}])
    mem_mb = df.memory_usage(deep=True).sum() / (1024**2)
    mem_df = pd.DataFrame([{"Memory Usage": f"{mem_mb:.2f} MB"}])

    profile_for_display = {
        "schema": schema_df,
        "high_cardinality": high_card_df,
        "shape": shape_df,
        "memory_usage": mem_df,
        "duplicates_summary": dup_summary_df,
        "duplicated_rows": duplicated_rows_df,
        "describe": describe_df,
        "sample_head": sample_head_df,
    }

    profile_for_export = {
        k: v.copy() for k, v in profile_for_display.items() if isinstance(v, pd.DataFrame)
    }

    return {"for_display": profile_for_display, "for_export": profile_for_export}


def run_data_profile(df: pd.DataFrame, config: dict = {}, **kwargs):
    """
    Orchestrates data profiling using config-driven settings.

    Args:
        df (pd.DataFrame): Input DataFrame to profile.
        config (dict): YAML-style config with nested profile > settings structure.
        **kwargs: Additional settings forwarded to the generator.

    Returns:
        dict: Structured profile output containing display and export blocks.
    """
    profile_cfg = config.get("profile", {})
    return generate_data_profile(df, **profile_cfg.get("settings", {}))
