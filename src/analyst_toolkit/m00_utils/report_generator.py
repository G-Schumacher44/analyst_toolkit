"""
📦 Module: report_generator.py

Advanced report generation utilities for transformation and QA modules.

This module is used by destructive or stateful pipeline steps (e.g., normalization, outliers, imputation)
to produce comprehensive multi-sheet reports including row diffs, column summaries, validation status,
changelogs, and post-hoc analysis like categorical shift.

Designed to complement the export_utils module, with deeper introspection and side-effect visibility.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

def generate_transformation_report(
    df_original: pd.DataFrame,
    df_transformed: pd.DataFrame,
    changelog: dict,
    module_name: str,
    run_id: str,
    export_config: dict,
) -> dict:
    """
    Generates comparison tables, summary stats, and changelog views from a transformation module.

    Returns:
        dict[str, pd.DataFrame]: dictionary of DataFrames for export
    """
    report_tables = {}

    # 1. Detect row-level changes (align columns first)
    shared_cols = df_original.columns.intersection(df_transformed.columns)
    df_o_aligned = df_original[shared_cols].copy()
    df_t_aligned = df_transformed[shared_cols].copy()
    changed_rows = df_o_aligned.compare(df_t_aligned, keep_shape=False, keep_equal=False)
    report_tables["changed_rows"] = changed_rows

    # Optional: Preview sample of changed rows (head)
    # Useful for dashboards or UI display
    report_tables["changed_rows_preview"] = changed_rows.head(20)

    # Row-level change flags (boolean mask where any value changed)
    change_mask = (df_o_aligned != df_t_aligned) & ~(df_o_aligned.isna() & df_t_aligned.isna())
    row_change_flags = change_mask.any(axis=1)
    report_tables["row_change_flags"] = pd.DataFrame({
        "index": df_original.index,
        "row_changed": row_change_flags
    }).set_index("index")

    # Row delta summary
    rows_total = len(df_original)
    rows_changed = row_change_flags.sum()
    rows_changed_percent = (rows_changed / rows_total) * 100
    row_change_summary = pd.DataFrame([{
        "rows_total": rows_total,
        "rows_changed": rows_changed,
        "rows_unchanged": rows_total - rows_changed,
        "rows_changed_percent": round(rows_changed_percent, 2)
    }])
    report_tables["row_change_summary"] = row_change_summary

    # 2. Long-form diff table
    diff_table = []
    shared_cols = df_original.columns.intersection(df_transformed.columns)
    for idx in df_original.index:
        for col in shared_cols:
            orig_val = df_original.at[idx, col]
            new_val = df_transformed.at[idx, col]
            if pd.notna(orig_val) or pd.notna(new_val):
                if orig_val != new_val:
                    diff_table.append({
                        "index": idx,
                        "column": col,
                        "original": orig_val,
                        "transformed": new_val
                    })
    report_tables["diff_table"] = pd.DataFrame(diff_table)

    # 3. Column-wise change summary
    col_change_summary = report_tables["diff_table"].groupby("column").size().reset_index(name="change_count")
    report_tables["column_changes_summary"] = col_change_summary

    # 4. Add changelog as a flat table
    changelog_flat = []
    for key, val in changelog.items():
        changelog_flat.append({"step": key, "details": str(val)})
    report_tables["changelog"] = pd.DataFrame(changelog_flat)

    # 5. Optional metadata
    meta_info = {
        "module": module_name,
        "run_id": run_id,
        "timestamp": datetime.utcnow().isoformat(),
        "original_shape": df_original.shape,
        "transformed_shape": df_transformed.shape
    }
    report_tables["meta_info"] = pd.DataFrame([meta_info])

    return report_tables


def generate_duplicates_report(df_original: pd.DataFrame, df_processed: pd.DataFrame, detection_results: dict, mode: str, df_flagged: pd.DataFrame = None) -> dict:
    """
    Compares original and deduplicated DataFrames to generate a detailed report,
    using pre-computed detection_results for consistency.

    Args:
        df_original (pd.DataFrame): The DataFrame before processing.
        df_processed (pd.DataFrame): The DataFrame after processing.
        detection_results (dict): The results from the detection step.
        mode (str): The handling mode ('remove' or 'flag').
        df_flagged (pd.DataFrame, optional): The full DataFrame with 'is_duplicate' flag.

    Returns:
        dict: A dictionary containing report DataFrames.
    """
    report = {}
    duplicate_count = detection_results.get("duplicate_count", 0)
    clusters = detection_results.get("duplicate_clusters")

    if duplicate_count == 0:
        report['summary'] = pd.DataFrame([{"Metric": "Status", "Value": "No duplicates found."}])
        return report

    if mode == 'remove':
        rows_removed = len(df_original) - len(df_processed)
        summary_data = {
            "Metric": ["Original Row Count", "Deduplicated Row Count", "Rows Removed"],
            "Value": [len(df_original), len(df_processed), rows_removed]
        }
        report['summary'] = pd.DataFrame(summary_data)

        dropped_indices = df_original.index.difference(df_processed.index)
        if not dropped_indices.empty:
            report['dropped_rows'] = df_original.loc[dropped_indices].copy()
    else:  # 'flag' mode
        summary_data = {
            "Metric": ["Total Row Count", "Duplicate Rows Flagged"],
            "Value": [len(df_original), duplicate_count],
        }
        report['summary'] = pd.DataFrame(summary_data)
        
        if df_flagged is not None:
            report['flagged_dataset'] = df_flagged

        if clusters is not None and not clusters.empty:
            report['duplicate_clusters'] = clusters

    if clusters is not None and not clusters.empty:
        report['all_duplicate_instances'] = clusters
            
    return report


def generate_outlier_report(detection_results: dict) -> dict:
    """
    Standardizes the results from the outlier detection producer for export,
    now including the full outlier rows.
    """
    report = {}
    
    outlier_log_df = detection_results.get("outlier_log")
    if outlier_log_df is not None and not outlier_log_df.empty:
        report['outlier_detection_log'] = outlier_log_df

    outlier_rows_df = detection_results.get("outlier_rows")
    if outlier_rows_df is not None and not outlier_rows_df.empty:
        report['outlier_rows_details'] = outlier_rows_df
            
    return report



def generate_outlier_handling_report(df_original: pd.DataFrame, df_handled: pd.DataFrame, handling_log: pd.DataFrame) -> dict:
    """
    Compares original and handled DataFrames to generate a detailed report on outlier handling.
    """
    report = {}
    report['handling_summary_log'] = handling_log

    if handling_log is None or handling_log.empty:
        return report

    # --- Dropped Rows Log ---
    if 'global_drop' in handling_log['strategy'].unique():
        dropped_indices = df_original.index.difference(df_handled.index)
        if not dropped_indices.empty:
            report['removed_outlier_rows'] = df_original.loc[dropped_indices].copy()

    # --- Capped Values Log ---
    if 'clip' in handling_log['strategy'].unique():
        capped_cols = handling_log[handling_log['strategy'] == 'clip']['column'].tolist()
        if capped_cols:
            shared_indices = df_original.index.intersection(df_handled.index)
            original_subset = df_original.loc[shared_indices, capped_cols]
            handled_subset = df_handled.loc[shared_indices, capped_cols]
            
            changed_mask = original_subset.ne(handled_subset) & original_subset.notna()
            capped_changes = []
            for col in capped_cols:
                col_mask = changed_mask[col]
                if col_mask.any():
                    diff_df = pd.DataFrame({
                        'Column': col, 'Row_Index': col_mask[col_mask].index,
                        'Original_Value': original_subset.loc[col_mask, col],
                        'Capped_Value': handled_subset.loc[col_mask, col]
                    })
                    capped_changes.append(diff_df)
            if capped_changes:
                report['capped_values_log'] = pd.concat(capped_changes, ignore_index=True)

    return report


def generate_imputation_report(df_original: pd.DataFrame, df_imputed: pd.DataFrame, detailed_changelog: pd.DataFrame) -> dict:
    """
    Receives a detailed changelog and builds the final multi-sheet report dictionary,
    including categorical shift analysis.
    """
    report = {}
    
    # Pass the detailed changelog through for the actions log table
    report['imputation_actions_log'] = detailed_changelog
    
    imputed_cols = detailed_changelog['Column'].unique().tolist()

    # --- Null Value Audit: Before vs. After ---
    if imputed_cols:
        nulls_before = df_original[imputed_cols].isnull().sum()
        nulls_after = df_imputed[imputed_cols].isnull().sum()
        null_audit_df = pd.DataFrame({'Nulls Before': nulls_before, 'Nulls After': nulls_after})
        null_audit_df['Nulls Filled'] = null_audit_df['Nulls Before'] - null_audit_df['Nulls After']
        report['null_value_audit'] = null_audit_df[null_audit_df['Nulls Filled'] > 0].reset_index().rename(columns={'index': 'Column'})
        
    # --- Categorical Shift Analysis ---
    categorical_shift = {}
    categorical_cols = df_original.select_dtypes(include=['object', 'category']).columns
    
    for col in imputed_cols:
        if col in categorical_cols:
            vc_before = df_original[col].value_counts(dropna=False)
            vc_after = df_imputed[col].value_counts(dropna=False)
            
            # Only proceed if there was a change in value counts
            if not vc_before.equals(vc_after):
                all_values = pd.Index(vc_before.index).union(vc_after.index)
                audit_df = pd.DataFrame({
                    "Value": all_values,
                    "Original Count": [vc_before.get(val, 0) for val in all_values],
                    "Imputed Count": [vc_after.get(val, 0) for val in all_values]
                }).sort_values(by=["Original Count", "Imputed Count"], ascending=False)
                audit_df["Change"] = audit_df["Imputed Count"] - audit_df["Original Count"]
                categorical_shift[col] = audit_df
    
    if categorical_shift:
        report['categorical_shift'] = categorical_shift
        
    # --- Remaining Nulls Check ---
    final_nulls = df_imputed.isnull().sum()
    remaining_nulls_df = final_nulls[final_nulls > 0].reset_index(name="Remaining Nulls")
    if not remaining_nulls_df.empty:
        report['remaining_nulls'] = remaining_nulls_df.rename(columns={'index': 'Column'})

    return report


def generate_final_audit_report(df_raw: pd.DataFrame, df_final: pd.DataFrame, validation_results: dict, final_edits_log: pd.DataFrame) -> dict:
    """
    Generates a capstone report with corrected pass/fail logic.
    """
    report = {}
    
    # 1. Correctly Determine Final Pass/Fail Status
    val_checks = {k: v for k, v in validation_results.items() if isinstance(v, dict) and 'passed' in v}
    failed_checks = {name for name, check in val_checks.items() if not check.get('passed')}
    
    status = "✅ PASSED" if not failed_checks else "❌ FAILED"
    details = "All quality gates passed." if not failed_checks else f"Failed {len(failed_checks)} validation rule(s): {', '.join(failed_checks)}"
    
    report['pipeline_summary'] = pd.DataFrame([
        {"Metric": "Final Pipeline Status", "Value": status},
        {"Metric": "Details", "Value": details}
    ])

    # 2. Add other report components
    report['data_lifecycle_summary'] = pd.DataFrame({
        "Metric": ["Initial Rows", "Final Rows", "Rows Dropped", "Initial Columns", "Final Columns"],
        "Value": [len(df_raw), len(df_final), len(df_raw) - len(df_final), len(df_raw.columns), len(df_final.columns)]
    })
    
    if not final_edits_log.empty:
        report['final_edits_log'] = final_edits_log

    val_summary_data = [{"Rule": name.replace('_', ' ').title(), "Status": "Pass" if check['passed'] else "Fail"} for name, check in val_checks.items()]
    report['final_validation_summary'] = pd.DataFrame(val_summary_data)
    
    # 3. Add drill-down details for failures
    for name in failed_checks:
        check = val_checks[name]
        details = check.get('details', {})
        if not details: continue
        # Robustly handle different failure detail structures
        if isinstance(details, dict):
             report[f"failure_details_{name}"] = pd.DataFrame.from_dict(details, orient='index')
        else: # Handle lists
             report[f"failure_details_{name}"] = pd.DataFrame(details)

    return report