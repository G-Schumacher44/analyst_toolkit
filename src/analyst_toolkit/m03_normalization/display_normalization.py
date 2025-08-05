"""
🖥️ Module: display_normalization.py

Notebook-facing display script for the M03 Normalization module.

This module renders interactive summaries of data cleaning and standardization
actions applied during normalization. It shows transformation logs, value mapping
audits, and column-level before/after comparisons in a styled, collapsible layout.

Designed to assist users in understanding exactly how data was modified.
"""

import pandas as pd
from IPython.display import display, HTML
from analyst_toolkit.m00_utils.rendering_utils import to_html_table

def display_normalization_summary(changelog: dict, df_original: pd.DataFrame, df_normalized: pd.DataFrame, rules: dict):
    """Renders the standard dashboard, using the config to drive before/after previews."""
    total_actions = sum(len(df) for df in changelog.values())
    banner_html = f"""<div style="border: 1px solid #d0d7de; background-color: #eef2f7; color: #24292e; padding: 12px; border-radius: 6px; margin-bottom: 20px;">
        <strong>Stage:</strong> M03 Data Normalization ✅ | <strong>Action Types:</strong> {len(changelog)} | <strong>Total Transformations:</strong> {total_actions}</div>"""
    display(HTML(banner_html))
    
    action_titles = {
        "renamed_columns": "✏️ Columns Renamed",
        "types_coerced": "🔢 Types Coerced",
        "strings_cleaned": "🧹 Strings Cleaned",
        "values_mapped": "🧩 Values Mapped",
        "datetimes_parsed": "📅 Datetimes Parsed",
        "fuzzy_matches": "🤖 Fuzzy Matches"
    }

    # Define rows by grouping keys
    top_row = ["renamed_columns", "strings_cleaned", "datetimes_parsed"]
    bottom_row = ["values_mapped", "fuzzy_matches", "types_coerced"]

    def build_action_row(row_keys):
        return "".join([
            f'<div style="flex: 1; min-width: 300px;"><h4>{action_titles.get(action, action.title())} ({len(changelog[action])})</h4>{to_html_table(changelog[action], full_preview=True)}</div>'
            for action in row_keys if action in changelog and not changelog[action].empty
        ])

    top_row_html = f'<div style="display: flex; flex-wrap: wrap; gap: 20px;">{build_action_row(top_row)}</div>'
    bottom_row_html = f'<div style="display: flex; flex-wrap: wrap; gap: 20px;">{build_action_row(bottom_row)}</div>'

    action_html = f'''
    <details open>
      <summary><strong>⚙️ Normalization Actions (Transform Log)</strong></summary>
      <div style="margin-top: 1em; max-height: 500px; overflow-y: auto; padding-right: 10px;">
        {top_row_html}
        {bottom_row_html}
      </div>
    </details>
    '''

    display(HTML(action_html))

    # --- REFACTORED PREVIEW SECTION ---
    preview_cols = rules.get("preview_columns", [])
    column_analysis_html = ""
    
    for col in preview_cols:
        original_col_name = _get_original_col_name(changelog, col)
        
        if original_col_name in df_original.columns and col in df_normalized.columns:
            # Normalized values table
            vc_after = df_normalized[col].value_counts(dropna=False)
            norm_vals_df = pd.DataFrame({"Value": vc_after.index, "Count": vc_after.values}).sort_values(by="Count", ascending=False)
            norm_vals_html = to_html_table(norm_vals_df, max_rows=20)
            
            # Merged audit table
            vc_before = df_original[original_col_name].value_counts(dropna=False)
            all_values = pd.Index(vc_before.index).union(vc_after.index)
            audit_df = pd.DataFrame({
                "Value": all_values,
                "Original Count": [vc_before.get(val, 0) for val in all_values],
                "Normalized Count": [vc_after.get(val, 0) for val in all_values]
            }).sort_values(by=["Original Count", "Normalized Count"], ascending=False)
            audit_html = to_html_table(audit_df, max_rows=20)
            
            # Section for each column with side-by-side tables
            column_html = f"""
            <div style="margin-bottom: 24px; border-top: 1px solid #d0d7de; padding-top: 16px;">
                <h5 style="margin-top:0; margin-bottom: 12px;">Column: <code>{col}</code></h5>
                <div style="display: flex; gap: 20px; align-items: flex-start;">
                    <div style="flex: 1; min-width: 0;">
                        <strong>Normalized Values</strong>
                        {norm_vals_html}
                    </div>
                    <div style="flex: 1; min-width: 0;">
                        <strong>Value Audit</strong>
                        {audit_html}
                    </div>
                </div>
            </div>
            """
            column_analysis_html += column_html

    if column_analysis_html:
        # New container matches the vertical, scrollable drill-down from the validation display
        container_html = f"""
        <details>
            <summary><strong>📊 Column Value Analysis: Before & After(click to scroll)</strong></summary>
            <div style="max-height: 400px; overflow-y: auto; margin-top: 1em; padding: 10px 20px;">
                {column_analysis_html.replace('<div style="margin-bottom: 24px; border-top: 1px solid #d0d7de; padding-top: 16px;">', '<div style="margin-bottom: 24px; padding-top: 4px;">', 1)}
            </div>
        </details>
        """
        display(HTML(container_html))

def _get_original_col_name(changelog: dict, normalized_col: str) -> str:
    """Helper to find the original name of a renamed column for diffing."""
    if 'renamed_columns' in changelog:
        rename_df = changelog['renamed_columns'].set_index('New Name')
        if normalized_col in rename_df.index:
            return rename_df.loc[normalized_col, 'Original Name']
    return normalized_col