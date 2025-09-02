"""
♻️ Module: dup_display.py

Notebook-facing display renderer for the M04 Duplicates module.

Renders a visual summary of deduplication actions, including shape change stats,
duplicate cluster previews, and optional interactive plots. Used during notebook-based
pipeline review to support validation and interpretability of duplicate handling logic.
"""

from IPython.display import display, HTML
import pandas as pd
from analyst_toolkit.m00_utils.rendering_utils import to_html_table
from analyst_toolkit.m00_utils.plot_viewer import PlotViewer
import ipywidgets as widgets

def display_dupes_summary(report: dict, subset_cols: list, plot_paths: dict = None):
    """
    Renders a single, consolidated dashboard for the deduplication results.
    """
    summary_df = report.get("summary")
    if summary_df is None or summary_df.empty:
        display(HTML("<h4>♻️ Deduplication Summary</h4><p><em>No duplicate handling was performed or no changes were made.</em></p>"))
        return

    # --- 1. Banner ---
    # Determine mode from the report content to build the correct banner
    is_remove_mode = 'Rows Removed' in summary_df['Metric'].values
    
    if is_remove_mode:
        rows_changed = summary_df.loc[summary_df['Metric'] == 'Rows Removed', 'Value'].iloc[0]
        banner_metric = f"<strong>Rows Removed:</strong> {rows_changed}"
    else:  # flag mode
        rows_changed = summary_df.loc[summary_df['Metric'] == 'Duplicate Rows Flagged', 'Value'].iloc[0]
        banner_metric = f"<strong>Rows Flagged:</strong> {rows_changed}"

    status_emoji = "✅" if rows_changed == 0 else "⚠️"
    criteria_str = f"`{'`, `'.join(subset_cols)}`" if subset_cols else 'all columns'
    
    banner_html = f"""
    <div style="border: 1px solid #d0d7de; background-color: #eef2f7; color: #24292e; padding: 12px; border-radius: 6px; margin-bottom: 20px;">
        <strong>Stage:</strong> M04 Deduplication {status_emoji} | 
        {banner_metric} |
        <strong>Criteria:</strong> Based on {criteria_str}
    </div>"""

    # --- 2. Summary Table ---
    summary_table_html = to_html_table(summary_df, full_preview=True)
    summary_block = f"<details open><summary><strong>📈 Summary of Changes</strong></summary><div style='margin-top: 1em;'>{summary_table_html}</div></details>"

    # --- 3. Duplicate Details (Flagged or Dropped) ---
    details_df = None
    details_block = ""
    clusters_block = ""

    if is_remove_mode:
        # In remove mode, show what was dropped and provide full clusters for context.
        dropped_df = report.get("dropped_rows")
        if dropped_df is not None and not dropped_df.empty:
            details_html = to_html_table(dropped_df, max_rows=20)
            details_block = f"""
            <details open>
                <summary><strong>🔍 Dropped Duplicate Rows (click to scroll)</strong></summary>
                <div style="max-height: 400px; overflow-y: auto; margin-top: 1em; padding-top: 4px;">
                    {details_html}
                </div>
            </details>
            """
        
        all_duplicates_df = report.get("all_duplicate_instances")
        if all_duplicates_df is not None and not all_duplicates_df.empty:
            clusters_html = to_html_table(all_duplicates_df, max_rows=20)
            clusters_block = f"""
            <details>
                <summary><strong>🔬 All Duplicate Instances Found (for context, click to expand)</strong></summary>
                <div style="max-height: 400px; overflow-y: auto; margin-top: 1em; padding-top: 4px;">
                    {clusters_html}
                </div>
            </details>
            """
    else: # flag mode
        # In flag mode, the main detail to show is the clusters of duplicates found.
        clusters_df = report.get("duplicate_clusters")
        if clusters_df is not None and not clusters_df.empty:
            clusters_html = to_html_table(clusters_df, max_rows=20)
            details_block = f"""
            <details open>
                <summary><strong>🔍 Duplicate Clusters Found (click to scroll)</strong></summary>
                <div style="max-height: 400px; overflow-y: auto; margin-top: 1em; padding-top: 4px;">
                    {clusters_html}
                </div>
            </details>
            """
    
    # --- 4. Assemble and Display all HTML content at once ---
    final_html = f"<div>{banner_html}{summary_block}{details_block}{clusters_block}</div>"
    display(HTML(final_html))

    # --- 5. Display the Interactive Widget Separately ---
    if plot_paths:
        viewer = PlotViewer(plot_paths, title="Visual Summary")
        accordion = widgets.Accordion(children=[viewer.widget_box])
        accordion.set_title(0, '🖼️ Duplication Visual Summary')
        accordion.selected_index = 0 # Start open
        display(accordion)