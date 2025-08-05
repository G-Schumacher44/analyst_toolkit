"""
📊 Module: display_imputation.py

Notebook-facing dashboard renderer for the M07 Imputation module.

Displays a full summary of imputation actions including:
- Strategy logs and number of nulls filled
- Before/after categorical shift analysis
- Final null audit status
- Optional rendering of imputation-related plots

This module is intended for notebook use and designed to align with all other
stage-based display modules in the Analyst Toolkit. It uses HTML and widgets
for collapsible insight blocks and side-by-side comparison tables.
"""

from IPython.display import display, HTML
import pandas as pd
import ipywidgets as widgets
from analyst_toolkit.m00_utils.rendering_utils import to_html_table
from analyst_toolkit.m00_utils.plot_viewer_comparison import PlotViewer_Outliers
import os
import logging

def display_imputation_summary(report: dict, plot_paths: dict = None):
    """
    Renders the final, polished dashboard for imputation results, displaying
    the calculated fill values and the detailed categorical shift analysis.
    """
    # The primary log now contains the calculated fill values
    actions_log_df = report.get("imputation_actions_log")
    null_audit_df = report.get("null_value_audit")
    
    if actions_log_df is None or actions_log_df.empty:
        display(HTML("<h4>💉 Imputation Summary</h4><p><em>No imputation actions were performed.</em></p>"))
        return

    # --- 1. Banner ---
    total_filled = int(actions_log_df['Nulls Filled'].sum()) if 'Nulls Filled' in actions_log_df.columns else 0
    cols_affected = len(actions_log_df)
    status_emoji = "✅" if total_filled > 0 else "ℹ️"
    
    banner_html = f"""
    <div style="border: 1px solid #d0d7de; background-color: #eef2f7; color: #24292e; padding: 12px; border-radius: 6px; margin-bottom: 20px;">
        <strong>Stage:</strong> M07 Data Imputation {status_emoji} | 
        <strong>Total Values Filled:</strong> {total_filled} |
        <strong>Columns Affected:</strong> {cols_affected}
    </div>"""
    display(HTML(banner_html))

    # --- 2. Consolidated Summary Block ---
    summary_table_html = f'<div style="flex: 1;"><h4>📋 Imputation Actions Log</h4>{to_html_table(actions_log_df, full_preview=True)}</div>'
    
    audit_table_html = ""
    if null_audit_df is not None and not null_audit_df.empty:
        audit_table_html = f'<div style="flex: 1;"><h4>🔍 Null Value Audit</h4>{to_html_table(null_audit_df, full_preview=True)}</div>'
    
    summary_block = f"""
    <details open><summary><strong>📈 Imputation Summary & Null Audit</strong></summary>
        <div style="display: flex; gap: 30px; margin-top: 1em; align-items: flex-start;">
            {summary_table_html}{audit_table_html}
        </div>
    </details>"""
    display(HTML(summary_block))

    # --- 3. Categorical Shift Analysis (Your working layout) ---
    categorical_shift_report = report.get("categorical_shift", {})
    if categorical_shift_report:
        column_analysis_html = ""
        for i, (col, audit_df) in enumerate(categorical_shift_report.items()):
            vc_after = audit_df.set_index('Value')['Imputed Count']
            norm_vals_df = pd.DataFrame({"Value": vc_after.index, "Count": vc_after.values}).sort_values(by="Count", ascending=False)
            norm_vals_html = to_html_table(norm_vals_df[norm_vals_df['Count'] > 0], max_rows=10)
            audit_html = to_html_table(audit_df, max_rows=10)
            
            border_style = "border-top: 1px solid #d0d7de; padding-top: 16px;" if i > 0 else ""
            column_html = f"""
            <div style="margin-bottom: 24px; {border_style}">
                <h5 style="margin-top:0; margin-bottom: 12px;">Column: <code>{col}</code></h5>
                <div style="display: flex; gap: 30px; align-items: flex-start;">
                    <div style="flex: 1; min-width: 0;"><strong>Normalized Values</strong>{norm_vals_html}</div>
                    <div style="flex: 1; min-width: 0;"><strong>Value Audit (Before vs. After)</strong>{audit_html}</div>
                </div>
            </div>"""
            column_analysis_html += column_html

        container_html = f"""
        <details>
            <summary><strong>📊 Categorical Shift Analysis (click to expand & scroll)</strong></summary>
            <div style="max-height: 500px; overflow-y: auto; margin-top: 1em; padding: 10px;">{column_analysis_html}</div>
        </details>"""
        display(HTML(container_html))
        
    # --- 4. Remaining Nulls & Plot Viewer ---
    remaining_nulls_df = report.get("remaining_nulls")
    if remaining_nulls_df is not None and not remaining_nulls_df.empty:
        html_block = f"""
        <details open style="border: 1px solid #d9534f; border-radius: 6px; padding: 10px; margin-top: 15px;">
            <summary><strong>⚠️ Remaining Nulls Found</strong></summary>
            <div style='margin-top: 1em;'><p>The following columns still contain null values after imputation:</p>{to_html_table(remaining_nulls_df, full_preview=True)}</div>
        </details>"""
        display(HTML(html_block))

    if plot_paths:
        first_plot_path = next(iter(plot_paths.values()), [None])[0]
        if first_plot_path and os.path.exists(first_plot_path):
            from pathlib import Path
            plot_save_dir = str(Path(first_plot_path).parent)
            viewer = PlotViewer_Outliers(image_dir=plot_save_dir, title="Imputation Visualizations")
            accordion = widgets.Accordion(children=[viewer.widget_box])
            accordion.set_title(0, '🖼️ Visual Analysis: Before & After Imputation')
            accordion.selected_index = None
            display(accordion)