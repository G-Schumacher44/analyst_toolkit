"""
🖥️ Module: display_handling.py

Notebook-facing display renderer for the M06 Outlier Handling module.

Renders a collapsible HTML dashboard summarizing all outlier handling actions,
including strategy usage, total outliers affected, capped value drilldowns, and
optional row removal previews. Designed for inline interpretability in notebooks.
"""

from IPython.display import HTML, display

from analyst_toolkit.m00_utils.rendering_utils import to_html_table


def display_handling_summary(report: dict):
    """
    Renders a comprehensive dashboard for the outlier handling results.
    """
    summary_df = report.get("handling_summary_log")
    if summary_df is None or summary_df.empty:
        display(
            HTML(
                "<h4>🔪 Outlier Handling Summary</h4><p><em>No outlier handling was performed.</em></p>"
            )
        )
        return

    # --- Banner (with robust calculation) ---
    total_handled = summary_df["outliers_handled"].sum()
    strategies_used = ", ".join(summary_df["strategy"].unique())
    status_emoji = "⚠️" if total_handled > 0 else "✅"

    banner_html = f"""
    <div style="border: 1px solid #d0d7de; background-color: #eef2f7; color: #24292e; padding: 12px; border-radius: 6px; margin-bottom: 20px;">
        <strong>Stage:</strong> M06 Outlier Handling {status_emoji} |
        <strong>Strategies Used:</strong> {strategies_used} |
        <strong>Total Outliers Handled:</strong> {total_handled}
    </div>"""
    display(HTML(banner_html))

    # --- Summary Table ---
    display(
        HTML(
            f"<details open><summary><strong>📋 Handling Actions Summary</strong></summary><div style='margin-top: 1em;'>{to_html_table(summary_df, full_preview=True)}</div></details>"
        )
    )

    # --- Drill-Down Sections ---
    capped_log_df = report.get("capped_values_log")
    if capped_log_df is not None and not capped_log_df.empty:
        display(
            HTML(
                f"""<details><summary><strong>🔍 Details: Capped Values</strong></summary><div style="max-height: 400px; overflow-y: auto; margin-top: 1em;">{to_html_table(capped_log_df, max_rows=20)}</div></details>"""
            )
        )

    removed_log_df = report.get("removed_outlier_rows")
    if removed_log_df is not None and not removed_log_df.empty:
        display(
            HTML(
                f"""<details><summary><strong>🗑️ Details: Removed Rows</strong></summary><div style="max-height: 400px; overflow-y: auto; margin-top: 1em;">{to_html_table(removed_log_df, max_rows=20)}</div></details>"""
            )
        )
