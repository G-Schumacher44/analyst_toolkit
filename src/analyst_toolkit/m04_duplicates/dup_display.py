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
    rows_removed = summary_df.loc[summary_df['Metric'] == 'Rows Removed', 'Value'].iloc[0]
    status_emoji = "✅" if rows_removed == 0 else "⚠️"
    criteria_str = 'all columns' if subset_cols == list(report.get('duplicate_clusters', pd.DataFrame()).columns) else f"`{'`, `'.join(subset_cols)}`"
    
    banner_html = f"""
    <div style="border: 1px solid #d0d7de; background-color: #eef2f7; color: #24292e; padding: 12px; border-radius: 6px; margin-bottom: 20px;">
        <strong>Stage:</strong> M04 Deduplication {status_emoji} | 
        <strong>Rows Removed:</strong> {rows_removed} |
        <strong>Criteria:</strong> Based on {criteria_str}
    </div>"""

    # --- 2. Summary Table ---
    summary_table_html = to_html_table(summary_df, full_preview=True)
    summary_block = f"<details open><summary><strong>📈 Summary of Changes</strong></summary><div style='margin-top: 1em;'>{summary_table_html}</div></details>"

    # --- 3. Duplicate Clusters ---
    duplicate_clusters_df = report.get("duplicate_clusters")
    clusters_block = ""
    if duplicate_clusters_df is not None and not duplicate_clusters_df.empty:
        clusters_html = to_html_table(duplicate_clusters_df, max_rows=20)
        clusters_block = f"""
        <details>
            <summary><strong>🔍 Duplicate Clusters Found (click to scroll)</strong></summary>
            <div style="max-height: 400px; overflow-y: auto; margin-top: 1em; padding-top: 4px;">
                {clusters_html}
            </div>
        </details>
        """
    
    # --- 4. Assemble and Display all HTML content at once ---
    final_html = f"<div>{banner_html}{summary_block}{clusters_block}</div>"
    display(HTML(final_html))

    # --- 5. Display the Interactive Widget Separately ---
    if plot_paths:
        viewer = PlotViewer(plot_paths, title="Visual Summary")
        accordion = widgets.Accordion(children=[viewer.widget_box])
        accordion.set_title(0, '🖼️ Duplication Visual Summary')
        accordion.selected_index = 0 # Start open
        display(accordion)