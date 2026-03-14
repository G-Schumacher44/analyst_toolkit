"""Validation and final-audit dashboard renderers."""

from __future__ import annotations

import html
from typing import Any

import pandas as pd

from analyst_toolkit.m00_utils.dashboard_core import _assemble_page
from analyst_toolkit.m00_utils.dashboard_shared import (
    _display_name,
    _metric_value,
    _render_section,
    _status_tone_class,
)
from analyst_toolkit.m00_utils.dashboard_tables import _render_df


def _build_validation_summary_df(results: dict[str, Any]) -> pd.DataFrame:
    checks = {k: v for k, v in results.items() if isinstance(v, dict) and "passed" in v}
    rows = []
    for name, check in checks.items():
        details = check.get("details")
        issue_count = len(details) if hasattr(details, "__len__") else 0
        status_label = "Pass" if check.get("passed") else f"Fail ({issue_count} issues)"
        status_class = "pass" if check.get("passed") else "fail"
        rows.append(
            {
                "Validation Rule": _display_name(name),
                "Description": check.get("rule_description", ""),
                "Status": f"<span class='status-pill {status_class}'>{html.escape(status_label)}</span>",
            }
        )
    return pd.DataFrame(rows)


def _count_validation_issue_units(details: Any) -> int:
    if isinstance(details, dict):
        return sum(
            len(value) if hasattr(value, "__len__") else 1 for value in details.values()
        ) or len(details)
    if hasattr(details, "__len__"):
        return len(details)
    return 0


def _render_validation_drilldowns(results: dict[str, Any]) -> str:
    checks = {k: v for k, v in results.items() if isinstance(v, dict) and "passed" in v}
    blocks = []
    for name, check in checks.items():
        if check.get("passed"):
            continue

        details = check.get("details", {})
        title = f"Failure Detail: {_display_name(name)}"
        issue_units = _count_validation_issue_units(details)
        parts = [
            "<div class='failure-grid'>"
            "<div class='card'>"
            f"<h3>{html.escape(_display_name(name))}</h3>"
            f"<p class='subtle'>Issue units detected</p>{_metric_value(issue_units)}"
            f"<p class='subtle'>{html.escape(check.get('rule_description', 'Validation rule'))}</p>"
            "</div>"
            "<div class='card wide'>"
            "<h3>Why This Failed</h3>"
            f"<p>{html.escape(check.get('rule_description', 'Validation rule'))}</p>"
            "<div class='key'><strong>Operator note</strong><ul>"
            "<li>The tables below show the exact evidence exported for this failed rule.</li>"
            "<li>Use these details to adjust the dataset or the rule contract before rerunning validation.</li>"
            "</ul></div></div>"
            "</div>"
        ]

        if name == "schema_conformity":
            df = pd.DataFrame(
                [
                    {
                        "Issue Type": "Missing",
                        "Columns": ", ".join(details.get("missing_columns", [])) or "None",
                    },
                    {
                        "Issue Type": "Unexpected",
                        "Columns": ", ".join(details.get("unexpected_columns", [])) or "None",
                    },
                ]
            )
            parts.append(
                "<div class='card'><h3>Schema Mismatches</h3>"
                f"{_render_df(df, full_preview=True)}</div>"
            )
        elif name == "dtype_enforcement":
            df = pd.DataFrame.from_dict(details, orient="index")
            df.index.name = "Column"
            parts.append(
                "<div class='card'><h3>Dtype Drift</h3>"
                f"{_render_df(df.reset_index(), full_preview=True)}</div>"
            )
        elif name == "categorical_values":
            for column, violation_info in details.items():
                parts.append(
                    "<div class='card drilldown'>"
                    f"<h4>{html.escape(column)}</h4>"
                    f"<p class='subtle'><strong>Allowed values:</strong> {html.escape(str(violation_info.get('allowed_values', [])))}</p>"
                    f"{_render_df(violation_info.get('invalid_value_summary', pd.DataFrame()), full_preview=True)}"
                    "</div>"
                )
                violating_rows = violation_info.get("violating_rows", pd.DataFrame())
                if isinstance(violating_rows, pd.DataFrame) and not violating_rows.empty:
                    parts.append(
                        "<div class='card'>"
                        f"<h3>{html.escape(column)} Row Samples</h3>"
                        f"{_render_df(violating_rows, max_rows=5)}</div>"
                    )
        elif name == "numeric_ranges":
            for column, violation_info in details.items():
                parts.append(
                    "<div class='card drilldown'>"
                    f"<h4>{html.escape(column)}</h4>"
                    f"<p class='subtle'><strong>Allowed range:</strong> {html.escape(str(violation_info.get('enforced_range', '')))}</p>"
                    f"{_render_df(violation_info.get('violating_rows', pd.DataFrame()), max_rows=5)}"
                    "</div>"
                )
        else:
            parts.append(f"<div class='card'><pre>{html.escape(str(details))}</pre></div>")

        blocks.append(_render_section(title, "<div class='stack'>" + "".join(parts) + "</div>"))

    return "".join(blocks) or "<p class='empty'>No failures were recorded.</p>"


def render_validation_dashboard(results: dict[str, Any], run_id: str) -> str:
    checks = {k: v for k, v in results.items() if isinstance(v, dict) and "passed" in v}
    total_checks = len(checks)
    passed_checks = sum(1 for check in checks.values() if check.get("passed"))
    failed_checks = total_checks - passed_checks
    coverage_pct = results.get("summary", {}).get("row_coverage_percent", "N/A")
    decision_class = "pass" if failed_checks == 0 else "fail"
    failed_rule_pills = (
        "".join(
            f"<span class='pill warn'>{html.escape(_display_name(name))}</span>"
            for name, check in checks.items()
            if not check.get("passed")
        )
        or "<p class='empty'>No validation failures were recorded.</p>"
    )
    failed_issue_units = sum(
        _count_validation_issue_units(check.get("details", {}))
        for check in checks.values()
        if not check.get("passed")
    )
    banner = (
        f"<div class='cert-hero {decision_class}'>"
        "<div class='cert-kicker'>M02 Validation Gate</div>"
        f"<h2 class='cert-title'>{'Validation Passed' if failed_checks == 0 else 'Validation Requires Attention'}</h2>"
        f"<p class='cert-copy'>Checks passed: {passed_checks}/{total_checks}. Row coverage: {coverage_pct}%. "
        + (
            "The current rule contract is satisfied."
            if failed_checks == 0
            else "Review the failed rule set and drill-down evidence below before continuing to certification."
        )
        + "</p>"
        "</div>"
    )

    summary_df = _build_validation_summary_df(results)
    sections = [
        _render_section(
            "Failure Overview",
            (
                "<div class='cert-grid'>"
                f"<div class='cert-stat-card {'pass' if failed_checks == 0 else 'fail'}'>"
                "<h3>Validation Health</h3>"
                f"<p class='subtle'>Rules failed</p>{_metric_value(failed_checks)}"
                f"<p class='subtle'>Out of {total_checks} configured checks.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Issue Units</h3>"
                f"{_metric_value(failed_issue_units)}"
                "<p class='subtle'>Aggregate issue payloads across failed rules.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Coverage</h3>"
                f"{_metric_value(f'{coverage_pct}%')}"
                "<p class='subtle'>Rows evaluated under the current rule set.</p>"
                "</div>"
                f"<div class='cert-stat-card {'pass' if failed_checks == 0 else 'warn'}'>"
                "<h3>Next Step</h3>"
                f"{_metric_value('Proceed' if failed_checks == 0 else 'Repair')}"
                "<p class='subtle'>Promotion guidance for the next pipeline step.</p>"
                "</div>"
                "</div>"
                "<div class='card'>"
                "<h3>Rules Requiring Review</h3>"
                f"<div class='pill-list'>{failed_rule_pills}</div>"
                "<div class='key'><strong>Review note</strong><ul>"
                "<li>Failure drill-downs stay expanded below for the rules that need attention.</li>"
                "<li>Use the rule list to compare what failed against the exported details.</li>"
                "</ul></div></div>"
            ),
            open_by_default=failed_checks > 0,
        ),
        _render_section(
            "Validation Rules Summary",
            (
                "<div class='card'>"
                f"{_render_df(summary_df, full_preview=True, allow_html_cols={'Status'})}"
                "<div class='key'><strong>Status Key</strong><ul>"
                "<li><strong>Pass:</strong> The data conformed to the rule.</li>"
                "<li><strong>Fail:</strong> One or more issues were found.</li>"
                "</ul></div></div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Failure Details", _render_validation_drilldowns(results), open_by_default=True
        ),
    ]
    toc = [
        ("Failure Overview", "Failure Overview"),
        ("Validation Rules Summary", "Validation Rules Summary"),
        ("Failure Details", "Failure Details"),
    ]
    return _assemble_page(
        module_name="Validation",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def _render_final_audit_failures(report: dict[str, Any]) -> str:
    blocks = []
    for key, value in report.items():
        if not (key.startswith("FAILURES_") or key == "Null_Check_Failures"):
            continue
        title = _display_name(key)
        if key.lower() == "failures_schema_conformity" and isinstance(value, dict):
            rows = []
            missing = value.get("missing_columns", [])
            unexpected = value.get("unexpected_columns", [])
            if missing:
                rows.append({"Issue Type": "Missing", "Columns": ", ".join(missing)})
            if unexpected:
                rows.append({"Issue Type": "Unexpected", "Columns": ", ".join(unexpected)})
            blocks.append(_render_section(title, _render_df(pd.DataFrame(rows), full_preview=True)))
        elif isinstance(value, pd.DataFrame):
            blocks.append(_render_section(title, _render_df(value, full_preview=True)))
        else:
            blocks.append(_render_section(title, f"<pre>{html.escape(str(value))}</pre>"))
    return "".join(blocks) or "<p class='empty'>No failures were recorded.</p>"


def _safe_summary_flag(summary_df: pd.DataFrame, metric_name: str) -> str:
    if summary_df.empty:
        return "Unknown"
    row = summary_df[summary_df["Metric"] == metric_name]
    if row.empty:
        return "Unknown"
    value = row["Value"].iloc[0]
    if isinstance(value, bool):
        return "Passed" if value else "Failed"
    return str(value)


def _safe_metric_value(summary_df: pd.DataFrame, metric_name: str) -> int:
    try:
        series = summary_df.loc[summary_df["Metric"] == metric_name, "Value"]
        return int(series.iloc[0]) if not series.empty else 0
    except Exception:
        return 0


def render_final_audit_dashboard(report: dict[str, Any], run_id: str) -> str:
    summary_df = report.get("Pipeline_Summary", pd.DataFrame())
    status_row = (
        summary_df[summary_df["Metric"] == "Final Pipeline Status"]
        if not summary_df.empty
        else pd.DataFrame()
    )
    status = str(status_row["Value"].iloc[0]) if not status_row.empty else "STATUS UNKNOWN"
    ok = "CERTIFIED" in status and "❌" not in status
    cert_class = "pass" if ok else "fail"
    cert_title = "Healing Certificate Issued" if ok else "Certification Failed"
    cert_copy = (
        "The dataset cleared the final certification gate. This export captures the terminal status, lifecycle footprint, and schema evidence for delivery."
        if ok
        else "The dataset reached the final audit gate but did not satisfy every certification rule. Use the failure ledger below to identify the blocking checks before delivery."
    )
    lifecycle_df = report.get("Data_Lifecycle", pd.DataFrame())
    edits_df = report.get("Final_Edits_Log", pd.DataFrame())
    profile_df = report.get("Final_Data_Profile", pd.DataFrame())
    stats_df = report.get("Final_Descriptive_Stats", pd.DataFrame())
    preview_df = report.get("Final_Data_Preview", pd.DataFrame())
    failed_sections = [
        key for key in report if key.startswith("FAILURES_") or key == "Null_Check_Failures"
    ]
    initial_rows = _safe_metric_value(lifecycle_df, "Initial Rows")
    final_rows = _safe_metric_value(lifecycle_df, "Final Rows")
    row_delta = initial_rows - final_rows
    banner = (
        f"<div class='cert-hero {cert_class}'>"
        "<div class='cert-kicker'>M10 Final Audit & Certification</div>"
        f"<h2 class='cert-title'>{html.escape(cert_title)}</h2>"
        f"<p class='cert-copy'>{html.escape(cert_copy)}</p>"
        "</div>"
    )

    sections = [
        _render_section(
            "Certificate Summary",
            (
                "<div class='cert-grid'>"
                f"<div class='cert-stat-card {'pass' if ok else 'fail'}'>"
                "<h3>Certificate Status</h3>"
                f"{_metric_value('Pass' if ok else 'Fail')}"
                f"<p class='subtle'>{html.escape(status)}</p>"
                "</div>"
                f"<div class='cert-stat-card {_status_tone_class(_safe_summary_flag(summary_df, 'Certification Rules Passed'))}'>"
                "<h3>Certification Rules</h3>"
                f"{_metric_value(_safe_summary_flag(summary_df, 'Certification Rules Passed'))}"
                "<p class='subtle'>Result of the strict validation contract.</p>"
                "</div>"
                f"<div class='cert-stat-card {_status_tone_class(_safe_summary_flag(summary_df, 'Null Value Audit Passed'))}'>"
                "<h3>Null Audit</h3>"
                f"{_metric_value(_safe_summary_flag(summary_df, 'Null Value Audit Passed'))}"
                "<p class='subtle'>Required non-null columns checked at the final gate.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Rows Changed</h3>"
                f"{_metric_value(f'{row_delta:,}')}"
                f"<p class='subtle'>{final_rows:,} rows remain from {initial_rows:,} raw rows.</p>"
                "</div>"
                "</div>"
                "<div class='cert-ledger'>"
                f"<div class='card'><h3>Certification Ledger</h3>{_render_df(summary_df, full_preview=True)}</div>"
                f"<div class='card'><h3>Lifecycle Snapshot</h3>{_render_df(lifecycle_df, full_preview=True)}</div>"
                "</div>"
            ),
            open_by_default=True,
        )
    ]
    toc = [("Certificate Summary", "Certificate Summary")]
    if not ok:
        sections.append(
            _render_section(
                "Failure Ledger",
                (
                    "<div class='card'>"
                    "<h3>Certification Blocks</h3>"
                    "<div class='pill-list'>"
                    + "".join(
                        f"<span class='pill warn'>{html.escape(_display_name(name))}</span>"
                        for name in failed_sections
                    )
                    + "</div>"
                    "<div class='key'><strong>Resolution note</strong><ul>"
                    "<li>Each failed check is broken out below so the release blocker is explicit.</li>"
                    "<li>Use this section as the operator handoff for the final repair pass.</li>"
                    "</ul></div></div>"
                    + _render_final_audit_failures(report)
                ),
                open_by_default=True,
            )
        )
        toc.append(("Failure Ledger", "Failure Ledger"))

    sections.extend(
        [
            _render_section(
                "Pipeline Evidence",
                (
                    "<div class='cert-ledger'>"
                    f"<div class='card'><h3>Final Edits Log</h3>{_render_df(edits_df, full_preview=True)}</div>"
                    f"<div class='card'><h3>Data Lifecycle</h3>{_render_df(lifecycle_df, full_preview=True)}</div>"
                    "</div>"
                ),
                open_by_default=True,
            ),
            _render_section(
                "Final Data Profile",
                (
                    "<div class='section-grid'>"
                    "<div class='card'><h3>Data Lifecycle</h3>"
                    f"{_render_df(lifecycle_df, full_preview=True)}"
                    "<div class='key'><strong>Audit Remarks Key</strong><ul>"
                    "<li><strong>OK:</strong> Passed all configured quality checks.</li>"
                    "<li><strong>High Skew:</strong> Skewness exceeded threshold.</li>"
                    "<li><strong>Unexpected Type:</strong> Data type mismatch.</li>"
                    "</ul></div></div>"
                    f"<div class='card wide'><h3>Data Dictionary / Schema</h3>{_render_df(profile_df, full_preview=True)}</div>"
                    "</div>"
                ),
            ),
            _render_section(
                "Descriptive Statistics",
                f"<div class='card'><h3>Final Descriptive Statistics</h3>{_render_df(stats_df, full_preview=True)}</div>",
            ),
            _render_section(
                "Data Preview",
                f"<div class='card'><h3>Final Data Preview</h3>{_render_df(preview_df, max_rows=5)}</div>",
            ),
        ]
    )
    toc.extend(
        [
            ("Pipeline Evidence", "Pipeline Evidence"),
            ("Final Data Profile", "Final Data Profile"),
            ("Descriptive Statistics", "Descriptive Statistics"),
            ("Data Preview", "Data Preview"),
        ]
    )
    return _assemble_page(
        module_name="Final Audit",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )
