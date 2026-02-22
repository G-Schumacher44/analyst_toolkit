"""
🧾 Module: display_final_audit.py

Notebook-facing dashboard renderer for the M10 Final Audit module.

This module formats and displays the complete audit report, including:
- Pipeline pass/fail status banner
- Certification failure breakdown (if any)
- Pipeline summary with final edits
- Final data profile, stats, and preview

Each block uses structured HTML and collapsible sections for readability
in notebook environments.
"""

import json

import pandas as pd
from IPython.display import HTML, display

from analyst_toolkit.m00_utils.rendering_utils import to_html_table


def _json_default_serializer(obj):
    """Safely serializes unsupported types (e.g., DataFrames) for JSON dumps."""
    if isinstance(obj, pd.DataFrame):
        return obj.head(5).to_dict(orient="records")
    return str(obj)


def display_final_audit_summary(report: dict):
    """Renders the full final audit report in an interactive notebook-friendly layout."""

    summary_df = report.get("Pipeline_Summary")
    if summary_df is None:
        display(HTML("<h4>Final Audit Report</h4><p><em>Report data is missing.</em></p>"))
        return

    # 1. --- BANNER ---
    status_row = summary_df[summary_df["Metric"] == "Final Pipeline Status"]
    status = status_row["Value"].iloc[0] if not status_row.empty else "STATUS UNKNOWN"
    bg_color = "#e6ffed" if "✅" in status else "#ffeded"
    border_color = "#b7ebc9" if "✅" in status else "#f5b1b0"

    banner_html = f"""
    <div style="border: 1px solid {border_color}; background-color: {bg_color}; padding: 16px; border-radius: 6px; margin-bottom: 20px;">
        <strong style="font-size: 1.2em;">{status}</strong>
    </div>"""
    display(HTML(banner_html))

    # 2. --- FAILURE DETAILS (CONDITIONAL) ---
    if "❌" in status:
        failure_html = ""
        for key, value in report.items():
            if key.startswith("FAILURES_") or key == "Null_Check_Failures":
                clean_title = key.replace("_", " ").title()

                # Special handling for schema conformity failures to render a table.
                if key.lower() == "failures_schema_conformity" and isinstance(value, dict):
                    rows = []
                    missing = value.get("missing_columns", [])
                    unexpected = value.get("unexpected_columns", [])
                    if missing:
                        rows.append({"Issue Type": "Missing", "Columns": ", ".join(missing)})
                    if unexpected:
                        rows.append({"Issue Type": "Unexpected", "Columns": ", ".join(unexpected)})

                    if rows:
                        df_details = pd.DataFrame(rows)
                        failure_html += f"<h4>🚦 {clean_title}</h4>{to_html_table(df_details, full_preview=True)}"
                elif isinstance(value, pd.DataFrame):
                    failure_html += (
                        f"<h4>🚦 {clean_title}</h4>{to_html_table(value, full_preview=True)}"
                    )
                else:
                    # Fallback for other non-DataFrame failure details
                    pretty_dict = json.dumps(value, indent=2, default=_json_default_serializer)
                    failure_html += f"<h4>🚦 {clean_title}</h4><pre>{pretty_dict}</pre>"

        if failure_html:
            display(
                HTML(f"""
            <details open style="border: 1px solid {border_color}; border-radius: 6px; padding: 10px; margin-bottom: 15px;">
                <summary><strong>⚠️ Failure Details</strong></summary>
                <div style='margin-top: 1em; padding: 5px;'>{failure_html}</div>
            </details>""")
            )

    # 3. --- PIPELINE SUMMARY ---
    pipeline_status_df = report.get("Pipeline_Summary")
    data_lifecycle_df = report.get("Data_Lifecycle")
    final_edits_df = report.get("Final_Edits_Log")

    status_table_html = f"<div style='flex: 1;'><h4>📊 Pipeline Status</h4>{to_html_table(pipeline_status_df, full_preview=True)}</div>"
    edits_table_html = f"<div style='flex: 1;'><h4>🛠️ Final Edits Log</h4>{to_html_table(final_edits_df, full_preview=True)}</div>"

    summary_block = f"""
    <details open><summary><strong>📈 Pipeline Summary</strong></summary>
        <div style="display: flex; gap: 20px; margin-top: 1em; align-items: flex-start;">
            {status_table_html}{edits_table_html}
        </div>
    </details>"""
    display(HTML(summary_block))

    # 4. --- FINAL DATA PROFILE & STATS (RESTRUCTURED) ---
    profile_df = report.get("Final_Data_Profile")
    stats_df = report.get("Final_Descriptive_Stats")
    preview_df = report.get("Final_Data_Preview")

    if profile_df is not None:
        lifecycle_table_html = f"<div style='margin-bottom: 1em;'><h4>🧬 Data Lifecycle</h4>{to_html_table(data_lifecycle_df, full_preview=True)}</div>"
        profile_key_html = """<div style="margin-top: 15px; padding: 10px; border: 1px solid #d0d7de; border-radius: 6px; font-size: 0.9em; background-color: #f6f8fa;"><strong style="display: block; margin-bottom: 5px;">Audit Remarks Key:</strong><ul style="margin: 0 0 0 20px; padding: 0;"><li><strong>✅ OK:</strong> Passed all configured quality checks.</li><li><strong>⚠️ High Skew:</strong> Skewness exceeds threshold.</li><li><strong>⚠️ Unexpected Type:</strong> Data type mismatch.</li></ul></div>"""
        profile_html = f"<div style='flex: 3;'><h4>📚 Data Dictionary / Schema</h4>{to_html_table(profile_df, full_preview=True)}</div>"
        profile_block = f"""
<div style='display: flex; gap: 20px; margin-top: 1em;'>
    <div style='flex: 1; display: flex; flex-direction: column; gap: 20px;'>
        {lifecycle_table_html}
        {profile_key_html}
    </div>
    {profile_html}
</div>
"""
        display(
            HTML(
                f"<details><summary><strong>🔬 Final Data Profile</strong></summary>{profile_block}</details>"
            )
        )

    if stats_df is not None:
        stats_html = to_html_table(stats_df, full_preview=True)
        display(
            HTML(
                f"<details><summary><strong>🔢 Descriptive Statistics</strong></summary><div style='margin-top: 1em;'>{stats_html}</div></details>"
            )
        )

    if preview_df is not None:
        preview_html = to_html_table(preview_df, max_rows=5)
        display(
            HTML(
                f"<details><summary><strong>📄 Data Preview (.head)</strong></summary><div style='margin-top: 1em;'>{preview_html}</div></details>"
            )
        )
