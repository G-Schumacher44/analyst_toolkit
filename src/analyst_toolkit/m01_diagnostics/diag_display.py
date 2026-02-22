"""
📑 Module: diag_display.py

Notebook-facing display logic for the M01 Diagnostics module.

This script assembles and renders a multi-block HTML dashboard using
profile results from the data_diag.py module. It summarizes nulls,
duplicates, cardinality, memory usage, and audit results in a styled
interactive format. Also includes optional widget-based plot browsing.

Used in notebook pipelines to present diagnostics outputs to users.
"""

import pandas as pd

from analyst_toolkit.m00_utils.plot_viewer import PlotViewer
from analyst_toolkit.m00_utils.rendering_utils import to_html_table


def display_profile_summary(profile: dict, plot_paths: dict = None, settings: dict = None):
    """
    Renders the final, multi-block dashboard for the data profile with custom styling.
    """
    try:
        import ipywidgets as widgets
        from IPython.display import HTML, display
    except ImportError:
        return

    if not profile:
        display(
            HTML(
                "<h4>📊 Data Diagnostics</h4><p><em>Profiling did not run or returned no results.</em></p>"
            )
        )
        return

    # --- 1. Enhanced Banner with Custom Styling ---
    dup_df = profile.get("duplicates_summary", pd.DataFrame())
    dup_count = dup_df.iloc[0]["Duplicate Rows"] if not dup_df.empty else 0
    schema_df = profile.get("schema", pd.DataFrame())
    missing_cols_count = len(schema_df[schema_df["Missing Count"] > 0])
    shape_df = profile.get("shape", pd.DataFrame())
    rows, cols = (
        (shape_df.iloc[0]["Rows"], shape_df.iloc[0]["Columns"]) if not shape_df.empty else (0, 0)
    )

    banner_html = f"""
    <div style="border: 1px solid #d0d7de; background-color: #eef2f7; color: #24292e; padding: 12px; border-radius: 6px; margin-bottom: 20px;">
        <strong>Stage:</strong> M01 Data Diagnostics ✅ |
        <strong>Columns with Nulls:</strong> {missing_cols_count} |
        <strong>Duplicate Rows Found:</strong> {dup_count} |
        <strong>Shape:</strong> {rows} Rows, {cols} Columns
    </div>"""
    display(HTML(banner_html))

    # --- 2. Data Preparation ---
    if settings is None:
        settings = {}
    max_rows = settings.get("max_rows", 5)

    high_card_df = profile.get("high_cardinality", pd.DataFrame())
    mem_df = profile.get("memory_usage", pd.DataFrame())
    main_profile_df = profile.get("schema", pd.DataFrame())
    sample_head_df = profile.get("sample_head", pd.DataFrame())
    duplicates_summary_df = profile.get("duplicates_summary", pd.DataFrame())
    duplicated_rows_df = profile.get("duplicated_rows", pd.DataFrame())
    describe_df = profile.get("describe", pd.DataFrame())

    # --- 3. Render Dashboard Blocks ---

    # Block 1: Key Metrics
    tier1_html = f"""
    <details open><summary><strong>📈 Key Metrics</strong></summary>
        <div style="display: flex; gap: 20px; margin-top: 1em; align-items: flex-start;">
            <div style="flex: 1;">
                <h4>🔷 Shape</h4>
                {to_html_table(shape_df, full_preview=True)}
            </div>
            <div style="flex: 1;">
                <h4>🧠 Memory Usage</h4>
                {to_html_table(mem_df, full_preview=True)}
            </div>
            <div style="flex: 1;">
                <h4>♻️ Duplicate Summary</h4>
                {to_html_table(duplicates_summary_df, full_preview=True)}
            </div>
        </div>
    </details>"""
    display(HTML(tier1_html))

    # Block 2: Full Profile & Cardinality
    # --- Audit Remarks Key with Custom Styling ---
    audit_key_html = """
    <div style="margin-top: 15px; padding: 10px; border: 1px solid #d0d7de; border-radius: 6px; font-size: 0.9em; background-color: #eef2f7; color: #24292e;">
        <strong style="display: block; margin-bottom: 5px;">Audit Remarks Key:</strong>
        <ul style="margin: 0 0 0 20px; padding: 0;">
            <li><strong>✅ OK:</strong> Passed all configured quality checks.</li>
            <li><strong>⚠️ High Skew:</strong> Skewness exceeds the configured threshold.</li>
            <li><strong>⚠️ Unexpected Type:</strong> Data type does not match the expected type.</li>
        </ul>
    </div>
    """

    tier2_html = f"""
    <details open><summary><strong>📝 Full Profile & Cardinality</strong></summary>
        <div style="display: flex; gap: 20px; margin-top: 1em;">
            <div style="flex: 1;">
                <h4>🔢 High Cardinality</h4>
                {to_html_table(high_card_df, full_preview=True)}
                {audit_key_html}
            </div>
            <div style="flex: 3;">
                <h4>📚 Full Data Profile</h4>
                {to_html_table(main_profile_df, full_preview=True)}
            </div>
        </div>
    </details>"""
    display(HTML(tier2_html))

    # (Other blocks remain the same)
    display(
        HTML(
            f"<details><summary><strong>🔬 Quantitative Summary</strong></summary><div style='margin-top: 1em;'><h4>🔢 Descriptive Statistics</h4>{to_html_table(describe_df, full_preview=True)}</div></details>"
        )
    )
    display(
        HTML(
            f"<details><summary><strong>📄 Preview of Duplicated Rows</strong></summary><div style='margin-top: 1em;'>{to_html_table(duplicated_rows_df, max_rows=max_rows)}</div></details>"
        )
    )
    display(
        HTML(
            f"<details><summary><strong>🔍 First Rows Preview</strong></summary><div style='margin-top: 1em;'><h4>📋 First {max_rows} Rows (.head)</h4>{to_html_table(sample_head_df, max_rows=max_rows)}</div></details>"
        )
    )

    # Block 6: Visual Profile Widget
    if plot_paths:
        viewer = PlotViewer(plot_paths, title="Visual Profile")
        accordion = widgets.Accordion(children=[viewer.widget_box])
        accordion.set_title(0, "🖼️ Visual Profile: Distributions")
        accordion.selected_index = None
        display(accordion)
