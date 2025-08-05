"""
✅ Module: final_audit_producer.py

This is the core producer logic for the M10 Final Audit module in the Analyst Toolkit.

Responsibilities:
- Applies final column-level transformations (drops, renames, dtype coercion)
- Executes strict validation checks using the existing validation suite
- Performs a dedicated null audit on required non-null fields

Outputs:
- Certified and cleaned DataFrame
- Dictionary of certification and audit results used in final report generation

This module is called by the M10 pipeline runner and does not export files directly.
"""
import pandas as pd
import logging
from analyst_toolkit.m02_validation.validate_data import run_validation_suite

def _apply_final_edits(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Applies final data cleaning and returns the transformed df and a changelog."""
    df_out = df.copy()
    changelog = []
    
    drop_cols = config.get("drop_columns", [])
    if drop_cols:
        existing_cols = [col for col in drop_cols if col in df_out.columns]
        if existing_cols:
            df_out = df_out.drop(columns=existing_cols)
            changelog.append({"Action": "drop_columns", "Details": f"Removed: {existing_cols}"})

    rename_map = config.get("rename_columns", {})
    if rename_map:
        df_out.rename(columns=rename_map, inplace=True)
        changelog.append({"Action": "rename_columns", "Details": f"Renamed {len(rename_map)} columns"})

    dtype_map = config.get("coerce_dtypes", {})
    if dtype_map:
        df_out = df_out.astype(dtype_map)
        changelog.append({"Action": "coerce_dtypes", "Details": f"Changed types for {len(dtype_map)} columns"})

    return df_out, pd.DataFrame(changelog)

def _run_null_audit(df: pd.DataFrame, disallowed_columns: list) -> dict:
    """Performs a final check for nulls in critical columns."""
    failures = {}
    for col in disallowed_columns:
        if col in df.columns and df[col].isnull().any():
            null_count = int(df[col].isnull().sum())
            failures[col] = {"null_count": null_count}
    
    return {
        "passed": not bool(failures),
        "details": pd.DataFrame.from_dict(failures, orient='index').rename_axis("Column").reset_index()
    }

def run_final_audit_producer(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, dict]:
    """
    Main producer function for the final audit module.

    Args:
        df (pd.DataFrame): The DataFrame to certify.
        config (dict): Final audit configuration block.

    Returns:
        tuple:
            - pd.DataFrame: The cleaned and certified DataFrame.
            - dict: A dictionary containing:
                - 'final_edits_log': Summary of final cleaning actions.
                - 'certification_results': Output from validation suite.
                - 'null_audit_results': Audit of disallowed nulls.
    """
    results = {}

    # Step 1: Apply final edits
    df_edited, edits_log = _apply_final_edits(df, config.get("final_edits", {}))
    results["final_edits_log"] = edits_log

    # Step 2: Run the final, strict validation checks
    cert_config = config.get("certification", {})
    results["certification_results"] = run_validation_suite(df_edited, cert_config)
    
    # Step 3: Run the dedicated null audit
    disallowed_nulls = cert_config.get("rules", {}).get("disallowed_null_columns", [])
    results["null_audit_results"] = _run_null_audit(df_edited, disallowed_nulls)
    
    return df_edited, results