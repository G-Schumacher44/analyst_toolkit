"""
📑 Module: validation_display.py

Notebook-facing dashboard renderer for the M02 Validation module.

This script processes the structured results dictionary returned by `validate_data.py`
and renders it as a multi-section HTML summary. It includes a header banner, rule-by-rule
pass/fail summary, and expandable drill-down blocks with contextual details.

Used in notebooks to visually audit schema, dtype, range, and categorical checks.
"""

import pandas as pd
from IPython.display import HTML, display

from analyst_toolkit.m00_utils.rendering_utils import to_html_table


def display_validation_summary(results: dict, notebook: bool = True):
    """
    Renders the final, standardized dashboard for a validation report,
    with corrected drill-down logic for all failure types.
    """
    # --- 1. Banner Calculation & Rendering ---
    checks = {k: v for k, v in results.items() if isinstance(v, dict) and "passed" in v}
    total_checks, passed_checks = len(checks), sum(1 for c in checks.values() if c["passed"])
    status_emoji, coverage_pct = (
        ("✅", results.get("summary", {}).get("row_coverage_percent", "N/A"))
        if passed_checks == total_checks
        else ("⚠️", results.get("summary", {}).get("row_coverage_percent", "N/A"))
    )

    header_html = f"""
    <div style="border: 1px solid #d0d7de; background-color: #eef2f7; color: #24292e; padding: 12px; border-radius: 6px; margin-bottom: 20px;">
        <strong>Stage:</strong> M02 Data Validation {status_emoji} |
        <strong>Checks Passed:</strong> {passed_checks}/{total_checks} |
        <strong>Row Coverage:</strong> {coverage_pct}%
    </div>"""
    display(HTML(header_html))

    # --- 2. Rules Summary Table & Key ---
    summary_data = [
        {
            "Validation Rule": name.replace("_", " ").title(),
            "Description": check["rule_description"],
            "Status": "✅ Pass" if check["passed"] else f"⚠️ Fail ({len(check['details'])} issues)",
        }
        for name, check in checks.items()
    ]
    df_summary = pd.DataFrame(summary_data)
    summary_table_html = to_html_table(df_summary, full_preview=True)
    status_key_html = """
    <div style="margin-top: 15px; padding: 10px; border: 1px solid #d0d7de; border-radius: 6px; font-size: 0.9em; background-color: #eef2f7; color: #24292e;"><strong style="display: block; margin-bottom: 5px;">Status Key:</strong>
        <ul style="margin: 0 0 0 20px; padding: 0;">
            <li><strong>✅ Pass:</strong> The data conforms to this rule.</li>
            <li><strong>⚠️ Fail:</strong> One or more issues were found. See drill-down for details.</li>
        </ul>
    </div>"""
    summary_block = f'<details open><summary><strong>🔎 Validation Rules Summary</strong></summary><div style="margin-top: 1em;">{summary_table_html}{status_key_html}</div></details>'
    display(HTML(summary_block))

    # --- 3. Drill-Down Section Logic ---
    drill_down_blocks = []
    for name, check in checks.items():
        if not check["passed"]:
            title = f"⚠️ Drill-Down: {name.replace('_', ' ').title()}"
            content = ""
            details = check["details"]

            if name == "categorical_values":
                for col, violation_info in details.items():
                    context_html = f"""
                    <div style="flex: 1;">
                        <p><strong>Rule Violated:</strong></p>
                        <p style="font-size:0.9em;">Values for column <code>{col}</code> must be in the allowed set.</p>
                        <p style="font-size:0.9em;"><strong>Allowed Values:</strong></p>
                        <code style="font-size:0.8em; word-wrap:break-word;">{violation_info["allowed_values"]}</code>
                    </div>"""
                    summary_table = to_html_table(
                        violation_info["invalid_value_summary"], full_preview=True
                    )
                    scrolling_table_html = f"""
                    <div style="flex: 2;">
                         <p><strong>Invalid Values Found:</strong></p>
                         <div style="max-height: 300px; overflow-y: auto;">{summary_table}</div>
                    </div>"""
                    content += f"""
                    <div style="display: flex; gap: 20px; align-items: flex-start;">{context_html}{scrolling_table_html}</div>
                    <hr style="border-top: 1px solid #d0d7de; margin: 18px 0;">"""
            else:
                if name == "schema_conformity":
                    df_details = pd.DataFrame(
                        [
                            {
                                "Issue Type": "Missing",
                                "Columns": ", ".join(details.get("missing_columns", [])) or "None",
                            },
                            {
                                "Issue Type": "Unexpected",
                                "Columns": ", ".join(details.get("unexpected_columns", []))
                                or "None",
                            },
                        ]
                    )
                    content = to_html_table(df_details)
                elif name == "dtype_enforcement":
                    # This block is now corrected
                    df_details = pd.DataFrame.from_dict(details, orient="index")
                    df_details.index.name = "Column"
                    df_details = df_details.rename(
                        columns={"expected": "Expected Type", "actual": "Actual Type"}
                    )
                    content = to_html_table(df_details.reset_index(), full_preview=True)
                elif name == "numeric_ranges":
                    for col, violation_info in details.items():
                        context_html = f"<p><strong>Rule:</strong> Values must be in the range {violation_info['enforced_range']}</p>"
                        content += f"<h5>Column: <code>{col}</code></h5>{context_html}"
                        content += to_html_table(violation_info["violating_rows"], max_rows=5)

            drill_down_blocks.append(f"""
            <details><summary><strong>{title}(click to expand & scroll)</strong></summary>
                <div style="max-height: 400px; overflow-y: auto; margin-top: 1em; padding: 10px; border-top: 1px solid #eee;">
                    {content}
                </div>
            </details>""")

    drill_down_html = "".join(drill_down_blocks)
    if drill_down_html:
        display(
            HTML(f'<div><h3 style="margin-top: 20px;">Failure Details</h3>{drill_down_html}</div>')
        )
