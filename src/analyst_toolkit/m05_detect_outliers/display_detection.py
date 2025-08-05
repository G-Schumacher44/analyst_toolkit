"""
🖥️ Module: display_detection.py

Notebook-facing display renderer for the M05 Outlier Detection module.

Generates a collapsible HTML dashboard summarizing outlier detection results.
Displays detection logs, affected rows, and optionally visualizes plots using
the PlotViewer widget. Intended for inline review and QA in notebook workflows.
"""

from IPython.display import display, HTML
import pandas as pd
import os
import logging
import ipywidgets as widgets
from analyst_toolkit.m00_utils.rendering_utils import to_html_table
from analyst_toolkit.m00_utils.plot_viewer_comparison import PlotViewer_Outliers

def display_detection_summary(results: dict, plot_save_dir: str = None):
    """
    Renders a comprehensive dashboard and invokes the advanced PlotViewer widget
    inside a collapsible accordion.
    """
    outlier_log_df = results.get("outlier_log")
    outlier_rows_df = results.get("outlier_rows")

    if outlier_log_df is None or outlier_log_df.empty:
        # ... (no outliers banner remains the same)
        return

    # --- Banner and Tabular Reports ---
    total_outliers = int(outlier_log_df['outlier_count'].sum())
    cols_with_outliers = len(outlier_log_df)
    banner_html = f"""<div style="border: 1px solid #d0d7de; background-color: #eef2f7; color: #24292e; padding: 12px; border-radius: 6px; margin-bottom: 20px;">
        <strong>Stage:</strong> M05 Outlier Detection ⚠️ | <strong>Total Outliers Found:</strong> {total_outliers} | <strong>Columns Affected:</strong> {cols_with_outliers}</div>"""
    display(HTML(banner_html))
    display(HTML(f"<details open><summary><strong>📋 Outlier Detection Log</strong></summary><div style='margin-top: 1em;'>{to_html_table(outlier_log_df, full_preview=True)}</div></details>"))
    if outlier_rows_df is not None and not outlier_rows_df.empty:
        rows_html = to_html_table(outlier_rows_df, max_rows=20)
        display(HTML(f"""<details><summary><strong>🔍 Preview of Rows Containing Outliers</strong></summary><div style="max-height: 400px; overflow-y: auto; margin-top: 1em;">{rows_html}</div></details>"""))

    # --- Invoke Advanced Plot Viewer Widget within a Collapsible Accordion ---
    if plot_save_dir and os.path.exists(plot_save_dir) and any(f.endswith(('.png', '.jpg')) for f in os.listdir(plot_save_dir)):
        viewer = PlotViewer_Outliers(image_dir=plot_save_dir, title="Outlier Visualizations")
        
        # This now works because the viewer object has a .widget_box attribute
        accordion = widgets.Accordion(children=[viewer.widget_box])
        accordion.set_title(0, '🖼️ Visual Profile: Outlier Plots')
        accordion.selected_index = None # Start collapsed by default
        
        display(accordion)
    else:
        logging.info("No plots available to display in the viewer.")