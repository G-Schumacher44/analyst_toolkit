"""
📦 Module: report_generator.py

Advanced report generation utilities for transformation and QA modules.

This module is used by destructive or stateful pipeline steps (e.g., normalization, outliers, imputation)
to produce comprehensive multi-sheet reports including row diffs, column summaries, validation status,
changelogs, and post-hoc analysis like categorical shift.

Designed to complement the export_utils module, with deeper introspection and side-effect visibility.
"""

import base64
import pandas as pd
from pathlib import Path
from datetime import datetime

_HTML_CSS = """
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f8f9fa; color: #333; }
  .page-wrap { max-width: 1100px; margin: 2em auto; padding: 0 1.5em 3em; }
  h1 { color: #111; font-size: 1.6em; margin-bottom: 0.2em; }
  .meta { color: #888; font-size: 0.82em; margin-bottom: 2em; border-bottom: 2px solid #e0e0e0; padding-bottom: 1em; }
  h2 { color: #1a1a2e; font-size: 1.05em; font-weight: 600; margin: 0 0 0.6em;
       border-left: 4px solid #4a7fcb; padding-left: 0.6em; }
  h3 { color: #444; font-size: 0.92em; margin: 1em 0 0.3em; }
  .section { background: #fff; border: 1px solid #e8e8e8; border-radius: 6px;
             padding: 1em 1.2em; margin-bottom: 1em; box-shadow: 0 1px 3px rgba(0,0,0,0.04); overflow-x: auto; }
  table { border-collapse: collapse; width: 100%; font-size: 0.84em; }
  th, td { border: 1px solid #e0e0e0; padding: 5px 9px; text-align: left; white-space: nowrap; }
  th { background: #f0f4ff; font-weight: 600; color: #1a1a2e; }
  tr:nth-child(even) td { background: #fafbff; }
  .truncated { color: #999; font-size: 0.78em; margin-top: 0.5em; font-style: italic; }
  .plot-container { margin: 0.5em 0; }
  img { max-width: 100%; height: auto; display: block; border-radius: 4px; }
  p.empty { color: #bbb; font-style: italic; margin: 0.3em 0; font-size: 0.88em; }
  .toc { background: #fff; border: 1px solid #e8e8e8; border-radius: 6px;
         padding: 0.8em 1.2em; margin-bottom: 1.5em; font-size: 0.86em; }
  .toc a { color: #4a7fcb; text-decoration: none; margin-right: 1em; }
  .toc a:hover { text-decoration: underline; }
</style>
"""

_MAX_PREVIEW_ROWS = 50  # cap for large DataFrames in HTML output


def _render_df(df: pd.DataFrame) -> tuple[str, str]:
    """Render a DataFrame as an HTML table, capped at _MAX_PREVIEW_ROWS.
    Returns (table_html, truncation_notice_html)."""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = ["__".join(str(c) for c in col).strip("_") for col in df.columns]
    total = len(df)
    preview = df.head(_MAX_PREVIEW_ROWS)
    table_html = preview.to_html(classes="", escape=False, index=False, border=0)
    notice = ""
    if total > _MAX_PREVIEW_ROWS:
        notice = f"<p class='truncated'>Showing {_MAX_PREVIEW_ROWS} of {total:,} rows.</p>"
    return table_html, notice


def generate_html_report(
    report_tables: dict,
    module_name: str,
    run_id: str,
    plot_paths: dict | None = None,
) -> str:
    """
    Build a single-page self-contained HTML report from a dict of DataFrames.

    Each key in report_tables becomes a section card. Large DataFrames are capped
    at _MAX_PREVIEW_ROWS with a truncation notice. Plots are embedded as base64 PNG.

    Args:
        report_tables: dict[str, pd.DataFrame] — keyed by section name.
        module_name: Display name for the module (used in title/header).
        run_id: Pipeline run identifier.
        plot_paths: Optional dict[str, str] of plot name → local file path.

    Returns:
        str: Self-contained HTML string (does NOT write to disk).
    """
    from datetime import datetime, timezone
    title = f"{module_name} Report"
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Collect section keys for TOC (skip empty/non-renderable)
    renderable = [
        k for k, v in report_tables.items()
        if (isinstance(v, pd.DataFrame) and not v.empty)
        or (isinstance(v, dict) and any(isinstance(sv, pd.DataFrame) and not sv.empty for sv in v.values()))
    ]
    if plot_paths:
        renderable.append("plots")

    toc_links = "".join(
        f"<a href='#{k}'>{k.replace('_', ' ').title()}</a>"
        for k in renderable
    )

    html_parts = [
        "<html><head>",
        f"<title>{title} — {run_id}</title>",
        _HTML_CSS,
        "</head><body><div class='page-wrap'>",
        f"<h1>{title}</h1>",
        f"<div class='meta'>Run ID: <strong>{run_id}</strong> &nbsp;|&nbsp; Generated: {generated_at}</div>",
    ]

    if not report_tables:
        html_parts.append("<div class='section'><p class='empty'>No report data was produced for this run.</p></div>")
        html_parts.append("</div></body></html>")
        return "\n".join(html_parts)

    if toc_links:
        html_parts.append(f"<div class='toc'><strong>Sections:</strong> {toc_links}</div>")

    for section_name, value in report_tables.items():
        anchor = section_name
        heading = section_name.replace("_", " ").title()
        html_parts.append(f"<div class='section' id='{anchor}'>")
        html_parts.append(f"<h2>{heading}</h2>")

        if not isinstance(value, pd.DataFrame):
            if isinstance(value, dict):
                for sub_key, sub_df in value.items():
                    if isinstance(sub_df, pd.DataFrame) and not sub_df.empty:
                        sub_heading = sub_key.replace("_", " ").title()
                        html_parts.append(f"<h3>{sub_heading}</h3>")
                        table_html, notice = _render_df(sub_df)
                        html_parts.append(table_html)
                        if notice:
                            html_parts.append(notice)
            else:
                html_parts.append("<p class='empty'>No data available.</p>")
            html_parts.append("</div>")
            continue

        if value.empty:
            html_parts.append("<p class='empty'>No data available.</p>")
        else:
            table_html, notice = _render_df(value)
            html_parts.append(table_html)
            if notice:
                html_parts.append(notice)

        html_parts.append("</div>")

    # Plots section
    if plot_paths:
        html_parts.append("<div class='section' id='plots'><h2>Plots</h2>")
        for plot_name, path_str in plot_paths.items():
            if not path_str:
                continue
            path = Path(path_str)
            if path.exists():
                with open(path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode("utf-8")
                label = plot_name.replace("_", " ").title()
                html_parts.append(
                    f"<div class='plot-container'><h3>{label}</h3>"
                    f"<img src='data:image/png;base64,{encoded}'></div>"
                )
        html_parts.append("</div>")

    html_parts.append("</div></body></html>")
    return "\n".join(html_parts)

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
        mode (str): The operational mode ('remove' or 'flag').
        df_flagged (pd.DataFrame, optional): The full DataFrame with the 'is_duplicate'
                                            flag, used for reporting in 'flag' mode.

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