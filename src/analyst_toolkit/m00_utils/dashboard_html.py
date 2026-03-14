"""Standalone dashboard HTML renderer for module exports."""

from __future__ import annotations

import base64
import html
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from analyst_toolkit.m00_utils.dashboard_core import _assemble_page
from analyst_toolkit.m00_utils.dashboard_shared import (
    _display_name,
    _embed_reference_src,
    _metric_value,
    _module_badge,
    _normalize_reference_text,
    _render_reference_value,
    _render_section,
    _slugify,
    _status_chip,
    _status_tone_class,
    _tab_status_label,
)

_MAX_PREVIEW_ROWS = 50
_SIZE_WARNING_THRESHOLD_MB = 25


def _normalize_text(value: str) -> str:
    replacements = {
        "âœ… OK": "OK",
        "✅ OK": "OK",
        "⚠️ High Skew": "High Skew",
        "⚠️ Unexpected Type": "Unexpected Type",
    }
    normalized = value
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    return normalized


def _normalize_df_for_display(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in normalized.columns:
        normalized[column] = normalized[column].map(
            lambda value: _normalize_text(value) if isinstance(value, str) else value
        )
    return normalized


def _render_df(
    df: pd.DataFrame,
    *,
    max_rows: int = _MAX_PREVIEW_ROWS,
    full_preview: bool = False,
    allow_html_cols: set[str] | None = None,
) -> str:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return "<p class='empty'>No data available.</p>"

    total_rows = len(df)
    if full_preview:
        working = _normalize_df_for_display(df)
        if isinstance(working.columns, pd.MultiIndex):
            working.columns = [
                "__".join(str(part) for part in column if str(part)).strip("_")
                for column in working.columns
            ]
        preview = working.copy()
    else:
        preview = _normalize_df_for_display(df.head(max_rows))
        if isinstance(preview.columns, pd.MultiIndex):
            preview.columns = [
                "__".join(str(part) for part in column if str(part)).strip("_")
                for column in preview.columns
            ]
        preview = preview.copy()
    safe_html_cols = allow_html_cols or set()
    for column in preview.columns:
        if str(column) in safe_html_cols:
            continue
        preview[column] = preview[column].map(
            lambda value: html.escape(value) if isinstance(value, str) else value
        )
    table_html = preview.to_html(index=False, escape=False, border=0)
    wrapped_table = f"<div class='table-wrap'>{table_html}</div>"
    if full_preview or total_rows <= max_rows:
        return wrapped_table
    return f"{wrapped_table}<p class='subtle'>Showing {len(preview):,} of {total_rows:,} rows.</p>"


def _flatten_plot_paths(plot_paths: dict[str, Any] | None) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    if not plot_paths:
        return items

    for name, value in plot_paths.items():
        if isinstance(value, list):
            for index, item in enumerate(value, start=1):
                if item:
                    label = f"{name} {index}" if len(value) > 1 else name
                    items.append((label, str(item)))
        elif value:
            items.append((name, str(value)))
    return items


def _render_plot_grid(plot_paths: dict[str, Any] | None) -> str:
    cards = []
    total_bytes = 0
    for name, path_str in _flatten_plot_paths(plot_paths):
        path = Path(path_str)
        if not path.exists():
            continue
        file_bytes = path.read_bytes()
        total_bytes += len(file_bytes)
        encoded = base64.b64encode(file_bytes).decode("utf-8")
        image_src = f"data:image/png;base64,{encoded}"
        escaped_title = html.escape(_display_name(name))
        escaped_name = html.escape(name)
        cards.append(
            "<div class='card plot-card'>"
            f"<h3>{escaped_title}</h3>"
            f"<button class='plot-trigger' type='button' onclick='window.atkDashboard.openPlot(this)' data-plot-title='{escaped_title}'>"
            f"<img src='{image_src}' alt='{escaped_name}'>"
            "</button>"
            "<p class='plot-caption'>Click to expand</p>"
            "</div>"
        )
    if not cards:
        return "<p class='empty'>No plots were generated for this run.</p>"
    if total_bytes > _SIZE_WARNING_THRESHOLD_MB * 1024 * 1024:
        logging.warning(
            "Embedded plot data exceeds %s MB. Consider reducing plot count or resolution.",
            _SIZE_WARNING_THRESHOLD_MB,
        )
    return (
        "<p class='plot-intro'>The standalone export keeps the visuals in the same file so the report travels without sidecar assets.</p>"
        "<div class='plot-grid'>" + "".join(cards) + "</div>"
    )


def _render_generic_dashboard(
    report_tables: dict[str, Any],
    module_name: str,
    run_id: str,
    plot_paths: dict[str, Any] | None = None,
) -> str:
    sections = []
    toc = []
    for section_name, value in report_tables.items():
        body = "<div class='stack'>"
        if isinstance(value, pd.DataFrame):
            body += _render_df(value)
        elif isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, pd.DataFrame) and not sub_value.empty:
                    body += f"<div><h3>{html.escape(_display_name(sub_key))}</h3>{_render_df(sub_value)}</div>"
        else:
            body += "<p class='empty'>No data available.</p>"
        body += "</div>"
        sections.append(_render_section(_display_name(section_name), body, open_by_default=True))
        toc.append((section_name, _display_name(section_name)))

    if plot_paths:
        sections.append(
            _render_section("Plots", _render_plot_grid(plot_paths), open_by_default=True)
        )
        toc.append(("plots", "Plots"))

    return _assemble_page(
        module_name=module_name,
        run_id=run_id,
        banner_html="",
        toc_items=toc,
        sections=sections,
    )


def _render_diagnostics_dashboard(
    report_tables: dict[str, Any], run_id: str, plot_paths: dict[str, Any] | None
) -> str:
    schema_df = report_tables.get("schema", pd.DataFrame())
    dup_df = report_tables.get("duplicates_summary", pd.DataFrame())
    shape_df = report_tables.get("shape", pd.DataFrame())
    mem_df = report_tables.get("memory_usage", pd.DataFrame())
    high_card_df = report_tables.get("high_cardinality", pd.DataFrame())
    describe_df = report_tables.get("describe", pd.DataFrame())
    duplicated_rows_df = report_tables.get("duplicated_rows", pd.DataFrame())
    sample_head_df = report_tables.get("sample_head", pd.DataFrame())

    dup_count = int(dup_df.iloc[0]["Duplicate Rows"]) if not dup_df.empty else 0
    missing_cols = (
        int((schema_df["Missing Count"] > 0).sum()) if "Missing Count" in schema_df.columns else 0
    )
    rows = int(shape_df.iloc[0]["Rows"]) if not shape_df.empty else 0
    cols = int(shape_df.iloc[0]["Columns"]) if not shape_df.empty else 0

    banner = (
        "<div class='banner'>"
        "<div class='banner-item'><strong>Stage:</strong> M01 Data Diagnostics</div>"
        f"<div class='banner-item'><strong>Columns with Nulls:</strong> {missing_cols}</div>"
        f"<div class='banner-item'><strong>Duplicate Rows:</strong> {dup_count}</div>"
        f"<div class='banner-item'><strong>Shape:</strong> {rows} rows x {cols} columns</div>"
        "</div>"
    )

    sections = [
        _render_section(
            "Key Metrics",
            (
                "<div class='section-grid'>"
                f"<div class='card'><h3>Shape</h3>{_render_df(shape_df, full_preview=True)}</div>"
                f"<div class='card'><h3>Memory Usage</h3>{_render_df(mem_df, full_preview=True)}</div>"
                f"<div class='card'><h3>Duplicate Summary</h3>{_render_df(dup_df, full_preview=True)}</div>"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Full Profile And Cardinality",
            (
                "<div class='section-grid'>"
                f"<div class='card'><h3>High Cardinality</h3>{_render_df(high_card_df, full_preview=True)}"
                "<div class='key'><strong>Audit Remarks Key</strong><ul>"
                "<li><strong>OK:</strong> Passed all configured quality checks.</li>"
                "<li><strong>High Skew:</strong> Skewness exceeded the configured threshold.</li>"
                "<li><strong>Unexpected Type:</strong> Data type did not match the expected type.</li>"
                "</ul></div></div>"
                f"<div class='card wide'><h3>Full Data Profile</h3>{_render_df(schema_df, full_preview=True)}</div>"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Quantitative Summary",
            f"<div class='card'><h3>Descriptive Statistics</h3>{_render_df(describe_df, full_preview=True)}</div>",
            open_by_default=True,
        ),
        _render_section(
            "Preview Of Duplicated Rows",
            f"<div class='card'><h3>Duplicated Rows</h3>{_render_df(duplicated_rows_df, max_rows=5)}</div>",
        ),
        _render_section(
            "First Rows Preview",
            f"<div class='card'><h3>Head</h3>{_render_df(sample_head_df, max_rows=5)}</div>",
            open_by_default=True,
        ),
    ]
    toc = [
        ("Key Metrics", "Key Metrics"),
        ("Full Profile And Cardinality", "Full Profile & Cardinality"),
        ("Quantitative Summary", "Quantitative Summary"),
        ("Preview Of Duplicated Rows", "Preview Of Duplicated Rows"),
        ("First Rows Preview", "First Rows Preview"),
    ]
    if plot_paths:
        sections.append(
            _render_section("Plots", _render_plot_grid(plot_paths), open_by_default=True)
        )
        toc.append(("Plots", "Plots"))

    return _assemble_page(
        module_name="Diagnostics",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


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


def _render_validation_dashboard(results: dict[str, Any], run_id: str) -> str:
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


def _render_final_audit_dashboard(report: dict[str, Any], run_id: str) -> str:
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
                    "</ul></div></div>" + _render_final_audit_failures(report)
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


def _render_duplicates_key_clusters(
    clusters_df: pd.DataFrame, subset_cols: list[str]
) -> tuple[str, str]:
    if clusters_df.empty:
        empty = "<p class='empty'>No duplicate clusters were recorded for this run.</p>"
        return empty, empty

    if subset_cols:
        key_counts = clusters_df.groupby(subset_cols).size().reset_index(name="Duplicate Count")
        key_counts = key_counts[key_counts["Duplicate Count"] >= 2].sort_values(
            by=["Duplicate Count", *subset_cols],
            ascending=[False, *([True] * len(subset_cols))],
        )
        subset_only = clusters_df[subset_cols].sort_values(by=subset_cols).reset_index(drop=True)
        return (
            _render_df(key_counts, full_preview=True),
            _render_df(subset_only, max_rows=20),
        )

    return (
        "<p class='empty'>Subset criteria were not specified, so there is no separate duplicate-key view.</p>",
        _render_df(clusters_df, max_rows=20),
    )


def _render_duplicates_dashboard(
    report: dict[str, Any], run_id: str, plot_paths: dict[str, Any] | None
) -> str:
    summary_df = report.get("summary", pd.DataFrame())
    flagged_df = report.get("flagged_dataset", pd.DataFrame())
    duplicate_clusters_df = report.get("duplicate_clusters", pd.DataFrame())
    all_duplicate_instances_df = report.get("all_duplicate_instances", pd.DataFrame())
    dropped_rows_df = report.get("dropped_rows", pd.DataFrame())
    metadata = report.get("__dashboard_meta__", {}) if isinstance(report, dict) else {}
    subset_cols = metadata.get("subset_columns") or []

    is_remove_mode = (
        isinstance(summary_df, pd.DataFrame)
        and not summary_df.empty
        and "Rows Removed" in summary_df.get("Metric", pd.Series(dtype=object)).values
    )
    mode_label = "Remove" if is_remove_mode else "Flag"
    rows_changed = (
        _safe_metric_value(summary_df, "Rows Removed")
        if is_remove_mode
        else _safe_metric_value(summary_df, "Duplicate Rows Flagged")
    )
    criteria_label = ", ".join(subset_cols) if subset_cols else "All columns"
    banner_class = "ok" if rows_changed == 0 else "warn"
    action_label = "Rows Removed" if is_remove_mode else "Rows Flagged"
    source_rows = (
        _safe_metric_value(summary_df, "Original Row Count")
        if is_remove_mode
        else _safe_metric_value(summary_df, "Total Row Count")
    )
    duplicate_instance_count = len(all_duplicate_instances_df)
    top_cluster_size = 0
    if not all_duplicate_instances_df.empty and subset_cols:
        top_cluster_size = int(
            all_duplicate_instances_df.groupby(subset_cols)
            .size()
            .sort_values(ascending=False)
            .iloc[0]
        )
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> M04 Deduplication</div>"
        f"<div class='banner-item'><strong>Mode:</strong> {html.escape(mode_label)}</div>"
        f"<div class='banner-item'><strong>{action_label}:</strong> {rows_changed}</div>"
        f"<div class='banner-item'><strong>Criteria:</strong> {html.escape(criteria_label)}</div>"
        "</div>"
    )

    key_table_html, subset_rows_html = _render_duplicates_key_clusters(
        all_duplicate_instances_df
        if not all_duplicate_instances_df.empty
        else duplicate_clusters_df,
        subset_cols,
    )
    detail_cards = [
        f"<div class='card'><h3>Summary Of Changes</h3>{_render_df(summary_df, full_preview=True)}</div>"
    ]
    if is_remove_mode and not dropped_rows_df.empty:
        detail_cards.append(
            f"<div class='card wide'><h3>Dropped Duplicate Rows</h3>{_render_df(dropped_rows_df, max_rows=20)}</div>"
        )
    elif not duplicate_clusters_df.empty:
        detail_cards.append(
            f"<div class='card wide'><h3>Duplicate Clusters</h3>{_render_df(duplicate_clusters_df, max_rows=20)}</div>"
        )
    elif not flagged_df.empty:
        detail_cards.append(
            f"<div class='card wide'><h3>Flagged Dataset Preview</h3>{_render_df(flagged_df, max_rows=20)}</div>"
        )

    sections = [
        _render_section(
            "Deduplication Overview",
            (
                "<div class='cert-grid'>"
                "<div class='cert-stat-card'>"
                "<h3>Processing Mode</h3>"
                f"{_metric_value(mode_label)}"
                "<p class='subtle'>Whether duplicates were flagged or removed.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                f"<h3>{html.escape(action_label)}</h3>"
                f"{_metric_value(rows_changed)}"
                "<p class='subtle'>Rows affected by the deduplication decision.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Source Rows</h3>"
                f"{_metric_value(source_rows)}"
                "<p class='subtle'>Rows evaluated under the selected duplicate criteria.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Largest Cluster</h3>"
                f"{_metric_value(top_cluster_size if top_cluster_size else duplicate_instance_count)}"
                "<p class='subtle'>Largest duplicate group found in this run.</p>"
                "</div>"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Deduplication Summary",
            (
                "<div class='section-grid'>"
                + "".join(detail_cards)
                + "<div class='card'><h3>Criteria & Operator Note</h3>"
                f"<p><strong>Criteria:</strong> {html.escape(criteria_label)}</p>"
                f"<p><strong>Duplicate instances captured:</strong> {duplicate_instance_count}</p>"
                "<div class='key'><strong>Review note</strong><ul>"
                "<li>Use the duplicate-key view to understand repeated groups before inspecting raw rows.</li>"
                "<li>The row-level sections below keep the evidence portable inside the export.</li>"
                "</ul></div></div>"
                "</div>"
            ),
            open_by_default=True,
        ),
    ]
    toc = [
        ("Deduplication Overview", "Deduplication Overview"),
        ("Deduplication Summary", "Deduplication Summary"),
    ]

    if rows_changed > 0:
        sections.append(
            _render_section(
                "Duplicate Keys And Evidence",
                (
                    "<div class='section-grid'>"
                    f"<div class='card'><h3>Duplicate Keys</h3>{key_table_html}"
                    "<div class='key'><strong>What this shows</strong><ul>"
                    "<li>Each row is a duplicated key group under the chosen subset.</li>"
                    "<li>Start here before dropping into the raw row evidence.</li>"
                    "</ul></div></div>"
                    f"<div class='card wide'><h3>Subset View</h3>{subset_rows_html}</div>"
                    "</div>"
                ),
                open_by_default=True,
            )
        )
        toc.append(("Duplicate Keys And Evidence", "Duplicate Keys & Evidence"))

    if not all_duplicate_instances_df.empty:
        sections.append(
            _render_section(
                "All Duplicate Instances",
                f"<div class='card'><h3>All Duplicate Instances</h3>{_render_df(all_duplicate_instances_df, max_rows=20)}</div>",
            )
        )
        toc.append(("All Duplicate Instances", "All Duplicate Instances"))

    if plot_paths:
        sections.append(
            _render_section("Plots", _render_plot_grid(plot_paths), open_by_default=True)
        )
        toc.append(("Plots", "Plots"))

    return _assemble_page(
        module_name="Duplicates",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def _render_normalization_changelog(changelog: dict[str, Any]) -> str:
    if not isinstance(changelog, dict) or not changelog:
        return "<p class='empty'>No normalization changelog entries were recorded.</p>"

    title_map = {
        "renamed_columns": "Columns Renamed",
        "types_coerced": "Types Coerced",
        "strings_cleaned": "Strings Cleaned",
        "values_mapped": "Values Mapped",
        "datetimes_parsed": "Datetimes Parsed",
        "fuzzy_matches": "Fuzzy Matches",
    }
    ordered_keys = [
        "renamed_columns",
        "strings_cleaned",
        "values_mapped",
        "fuzzy_matches",
        "datetimes_parsed",
        "types_coerced",
    ]

    cards = []
    for key in ordered_keys:
        value = changelog.get(key)
        if isinstance(value, pd.DataFrame) and not value.empty:
            cards.append(
                "<div class='card'>"
                f"<h3>{html.escape(title_map.get(key, _display_name(key)))}</h3>"
                f"{_render_df(value, full_preview=True)}"
                "</div>"
            )
    return (
        "".join(cards) or "<p class='empty'>No normalization changelog entries were recorded.</p>"
    )


def _render_normalization_dashboard(report: dict[str, Any], run_id: str) -> str:
    row_summary_df = report.get("row_change_summary", pd.DataFrame())
    column_changes_df = report.get("column_changes_summary", pd.DataFrame())
    changed_preview_df = report.get("changed_rows_preview", pd.DataFrame())
    diff_table_df = report.get("diff_table", pd.DataFrame())
    changelog = report.get("changelog", {})
    changelog_summary_df = report.get("changelog_summary", pd.DataFrame())
    meta_df = report.get("meta_info", pd.DataFrame())

    rows_total = int(row_summary_df.iloc[0]["rows_total"]) if not row_summary_df.empty else 0
    rows_changed = int(row_summary_df.iloc[0]["rows_changed"]) if not row_summary_df.empty else 0
    rows_changed_pct = (
        float(row_summary_df.iloc[0]["rows_changed_percent"]) if not row_summary_df.empty else 0.0
    )
    columns_changed = (
        int(column_changes_df["column"].nunique())
        if not column_changes_df.empty and "column" in column_changes_df.columns
        else 0
    )
    top_change = "None"
    if not column_changes_df.empty and "change_count" in column_changes_df.columns:
        top_row = column_changes_df.sort_values("change_count", ascending=False).iloc[0]
        top_change = f"{top_row['column']} ({int(top_row['change_count'])})"

    action_count = 0
    if isinstance(changelog, dict):
        for value in changelog.values():
            if isinstance(value, pd.DataFrame):
                action_count += len(value)

    banner_class = "warn" if rows_changed > 0 else "ok"
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> M03 Data Normalization</div>"
        f"<div class='banner-item'><strong>Rows Changed:</strong> {rows_changed} / {rows_total}</div>"
        f"<div class='banner-item'><strong>Columns Changed:</strong> {columns_changed}</div>"
        f"<div class='banner-item'><strong>Action Entries:</strong> {action_count}</div>"
        "</div>"
    )

    sections = [
        _render_section(
            "Normalization Overview",
            (
                "<div class='cert-grid'>"
                "<div class='cert-stat-card'>"
                "<h3>Rows Changed</h3>"
                f"{_metric_value(rows_changed)}"
                "<p class='subtle'>Rows with at least one value changed by normalization.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Change Coverage</h3>"
                f"{_metric_value(f'{rows_changed_pct:.2f}%')}"
                "<p class='subtle'>Share of rows affected by the configured transformation rules.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Columns Changed</h3>"
                f"{_metric_value(columns_changed)}"
                "<p class='subtle'>Distinct columns with at least one transformed value.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Primary Change</h3>"
                f"{_metric_value(top_change)}"
                "<p class='subtle'>Column with the largest number of value-level changes.</p>"
                "</div>"
                "</div>"
                "<div class='section-grid'>"
                f"<div class='card'><h3>Run Metadata</h3>{_render_df(meta_df, full_preview=True)}</div>"
                f"<div class='card'><h3>Row Change Summary</h3>{_render_df(row_summary_df, full_preview=True)}</div>"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Transformation Log",
            _render_normalization_changelog(changelog),
            open_by_default=True,
        ),
    ]
    toc = [
        ("Normalization Overview", "Normalization Overview"),
        ("Transformation Log", "Transformation Log"),
    ]

    if isinstance(changelog_summary_df, pd.DataFrame) and not changelog_summary_df.empty:
        sections.append(
            _render_section(
                "Scalar Changelog Notes",
                f"<div class='card'><h3>Pipeline Notes</h3>{_render_df(changelog_summary_df, full_preview=True)}</div>",
            )
        )
        toc.append(("Scalar Changelog Notes", "Scalar Changelog Notes"))

    if isinstance(column_changes_df, pd.DataFrame) and not column_changes_df.empty:
        sections.append(
            _render_section(
                "Column Change Impact",
                (
                    "<div class='section-grid'>"
                    f"<div class='card'><h3>Column Change Summary</h3>{_render_df(column_changes_df, full_preview=True)}</div>"
                    f"<div class='card'><h3>Changed Rows Preview</h3>{_render_df(changed_preview_df, max_rows=20)}</div>"
                    "</div>"
                ),
                open_by_default=True,
            )
        )
        toc.append(("Column Change Impact", "Column Change Impact"))

    if isinstance(diff_table_df, pd.DataFrame) and not diff_table_df.empty:
        sections.append(
            _render_section(
                "Value-Level Differences",
                (
                    "<div class='card'>"
                    "<h3>Normalized Value Diff Table</h3>"
                    "<p class='subtle'>Row-level evidence showing original versus transformed values.</p>"
                    f"{_render_df(diff_table_df, max_rows=50)}</div>"
                ),
            )
        )
        toc.append(("Value-Level Differences", "Value-Level Differences"))

    return _assemble_page(
        module_name="Normalization",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def _render_outlier_detection_dashboard(
    report: dict[str, Any], run_id: str, plot_paths: dict[str, Any] | None
) -> str:
    log_df = report.get("outlier_detection_log", pd.DataFrame())
    rows_df = report.get("outlier_rows_details", pd.DataFrame())
    total_outliers = int(log_df["outlier_count"].sum()) if not log_df.empty else 0
    columns_affected = int(log_df["column"].nunique()) if not log_df.empty else 0
    methods = (
        ", ".join(sorted(str(method) for method in log_df["method"].dropna().unique()))
        if not log_df.empty and "method" in log_df.columns
        else "None"
    )
    hottest_column = "None"
    if not log_df.empty and "outlier_count" in log_df.columns:
        top_row = log_df.sort_values("outlier_count", ascending=False).iloc[0]
        hottest_column = f"{top_row['column']} ({int(top_row['outlier_count'])})"

    banner_class = "warn" if total_outliers > 0 else "ok"
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> M05 Outlier Detection</div>"
        f"<div class='banner-item'><strong>Total Outliers:</strong> {total_outliers}</div>"
        f"<div class='banner-item'><strong>Columns Affected:</strong> {columns_affected}</div>"
        f"<div class='banner-item'><strong>Methods:</strong> {html.escape(methods)}</div>"
        "</div>"
    )

    sections = [
        _render_section(
            "Detection Overview",
            (
                "<div class='cert-grid'>"
                "<div class='cert-stat-card'>"
                "<h3>Total Outliers</h3>"
                f"{_metric_value(total_outliers)}"
                "<p class='subtle'>Flagged across all configured numeric checks.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Columns Affected</h3>"
                f"{_metric_value(columns_affected)}"
                "<p class='subtle'>Numeric columns with at least one detected outlier.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Primary Hotspot</h3>"
                f"{_metric_value(hottest_column)}"
                "<p class='subtle'>Column with the heaviest outlier concentration.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Detection Methods</h3>"
                f"{_metric_value(methods)}"
                "<p class='subtle'>Configured statistical rules used in this run.</p>"
                "</div>"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Outlier Detection Log",
            (
                "<div class='card'>"
                f"<h3>Detection Ledger</h3>{_render_df(log_df, full_preview=True)}"
                "<div class='key'><strong>Interpretation</strong><ul>"
                "<li><strong>outlier_count:</strong> number of rows flagged for the column.</li>"
                "<li><strong>lower_bound / upper_bound:</strong> numeric thresholds used by the detector.</li>"
                "<li><strong>outlier_examples:</strong> sample values that breached the threshold.</li>"
                "</ul></div></div>"
            ),
            open_by_default=True,
        ),
    ]
    toc = [
        ("Detection Overview", "Detection Overview"),
        ("Outlier Detection Log", "Outlier Detection Log"),
    ]

    if not rows_df.empty:
        sections.append(
            _render_section(
                "Outlier Rows Details",
                f"<div class='card'><h3>Affected Row Samples</h3>{_render_df(rows_df, max_rows=50)}</div>",
            )
        )
        toc.append(("Outlier Rows Details", "Outlier Rows Details"))

    if plot_paths:
        sections.append(
            _render_section("Plots", _render_plot_grid(plot_paths), open_by_default=True)
        )
        toc.append(("Plots", "Plots"))

    return _assemble_page(
        module_name="Outlier Detection",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def _render_outlier_handling_dashboard(report: dict[str, Any], run_id: str) -> str:
    summary_df = report.get("handling_summary_log", pd.DataFrame())
    capped_df = report.get("capped_values_log", pd.DataFrame())
    removed_df = report.get("removed_outlier_rows", pd.DataFrame())

    total_handled = (
        int(summary_df["outliers_handled"].sum())
        if not summary_df.empty and "outliers_handled" in summary_df.columns
        else 0
    )
    columns_affected = (
        int(summary_df.loc[summary_df["column"] != "ALL", "column"].nunique())
        if not summary_df.empty and "column" in summary_df.columns
        else 0
    )
    strategies = (
        sorted(str(value) for value in summary_df["strategy"].dropna().unique())
        if not summary_df.empty and "strategy" in summary_df.columns
        else []
    )
    primary_action = "None"
    if not summary_df.empty and "outliers_handled" in summary_df.columns:
        top_row = summary_df.sort_values("outliers_handled", ascending=False).iloc[0]
        primary_action = (
            f"{top_row.get('column', 'ALL')} · "
            f"{str(top_row.get('strategy', 'none')).replace('_', ' ')} "
            f"({int(top_row.get('outliers_handled', 0))})"
        )

    banner_class = "warn" if total_handled > 0 else "ok"
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> M06 Outlier Handling</div>"
        f"<div class='banner-item'><strong>Total Values Handled:</strong> {total_handled}</div>"
        f"<div class='banner-item'><strong>Columns Affected:</strong> {columns_affected}</div>"
        f"<div class='banner-item'><strong>Strategies:</strong> {html.escape(', '.join(strategies) or 'None')}</div>"
        "</div>"
    )

    strategy_pills = (
        "".join(
            f"<span class='pill warn'>{html.escape(strategy.replace('_', ' '))}</span>"
            for strategy in strategies
        )
        or "<p class='empty'>No handling strategies were applied.</p>"
    )
    touched_columns = (
        "".join(
            f"<span class='pill'>{html.escape(str(column))}</span>"
            for column in sorted(
                summary_df.loc[summary_df["column"] != "ALL", "column"]
                .dropna()
                .astype(str)
                .unique()
            )
        )
        if not summary_df.empty and "column" in summary_df.columns
        else ""
    )
    if not touched_columns:
        touched_columns = "<p class='empty'>Only global row-level handling was recorded.</p>"

    sections = [
        _render_section(
            "Handling Overview",
            (
                "<div class='cert-grid'>"
                "<div class='cert-stat-card'>"
                "<h3>Values Handled</h3>"
                f"{_metric_value(total_handled)}"
                "<p class='subtle'>Outlier values or rows changed by the configured remediation rules.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Columns Affected</h3>"
                f"{_metric_value(columns_affected)}"
                "<p class='subtle'>Distinct columns touched by column-level handling strategies.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Strategies Used</h3>"
                f"{_metric_value(len(strategies))}"
                "<p class='subtle'>Unique handling strategies applied in this run.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Primary Action</h3>"
                f"{_metric_value(primary_action)}"
                "<p class='subtle'>Largest single remediation action recorded in the ledger.</p>"
                "</div>"
                "</div>"
                "<div class='section-grid'>"
                f"<div class='card'><h3>Strategy Mix</h3><div class='pill-list'>{strategy_pills}</div></div>"
                f"<div class='card'><h3>Affected Columns</h3><div class='pill-list'>{touched_columns}</div></div>"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Handling Ledger",
            (
                "<div class='card'>"
                f"<h3>Handling Actions Log</h3>{_render_df(summary_df, full_preview=True)}"
                "<div class='key'><strong>Interpretation</strong><ul>"
                "<li><strong>strategy:</strong> remediation applied to the flagged values or rows.</li>"
                "<li><strong>column:</strong> target column, or <strong>ALL</strong> for global row drops.</li>"
                "<li><strong>outliers_handled:</strong> count of values or rows changed by that action.</li>"
                "<li><strong>details:</strong> human-readable evidence describing what was changed.</li>"
                "</ul></div></div>"
            ),
            open_by_default=True,
        ),
    ]
    toc = [
        ("Handling Overview", "Handling Overview"),
        ("Handling Ledger", "Handling Ledger"),
    ]

    if isinstance(capped_df, pd.DataFrame) and not capped_df.empty:
        sections.append(
            _render_section(
                "Capped Values",
                (
                    "<div class='card'>"
                    "<h3>Capped Value Evidence</h3>"
                    "<p class='subtle'>Original and post-cap values for rows handled with the <strong>clip</strong> strategy.</p>"
                    f"{_render_df(capped_df, full_preview=True)}</div>"
                ),
                open_by_default=True,
            )
        )
        toc.append(("Capped Values", "Capped Values"))

    if isinstance(removed_df, pd.DataFrame) and not removed_df.empty:
        sections.append(
            _render_section(
                "Removed Rows",
                (
                    "<div class='card'>"
                    "<h3>Removed Outlier Rows</h3>"
                    "<p class='subtle'>Rows removed when global drop handling was configured.</p>"
                    f"{_render_df(removed_df, full_preview=True)}</div>"
                ),
                open_by_default=True,
            )
        )
        toc.append(("Removed Rows", "Removed Rows"))

    return _assemble_page(
        module_name="Outlier Handling",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def _render_imputation_categorical_shift(shift_report: dict[str, Any]) -> str:
    if not shift_report:
        return "<p class='empty'>No categorical distribution shifts were recorded.</p>"

    blocks = []
    for column, audit_df in shift_report.items():
        if not isinstance(audit_df, pd.DataFrame) or audit_df.empty:
            continue
        normalized_values = audit_df[audit_df["Imputed Count"] > 0][["Value", "Imputed Count"]]
        blocks.append(
            "<div class='card drilldown'>"
            f"<h4>{html.escape(str(column))}</h4>"
            "<div class='cert-ledger'>"
            f"<div><h3>Normalized Values</h3>{_render_df(normalized_values, max_rows=10)}</div>"
            f"<div><h3>Value Audit</h3>{_render_df(audit_df, max_rows=10)}</div>"
            "</div></div>"
        )
    return (
        "".join(blocks) or "<p class='empty'>No categorical distribution shifts were recorded.</p>"
    )


def _render_imputation_dashboard(
    report: dict[str, Any], run_id: str, plot_paths: dict[str, Any] | None
) -> str:
    actions_df = report.get("imputation_actions_log", pd.DataFrame())
    null_audit_df = report.get("null_value_audit", pd.DataFrame())
    categorical_shift = report.get("categorical_shift", {})
    remaining_nulls_df = report.get("remaining_nulls", pd.DataFrame())

    total_filled = (
        int(actions_df["Nulls Filled"].sum())
        if not actions_df.empty and "Nulls Filled" in actions_df.columns
        else 0
    )
    columns_affected = len(actions_df) if isinstance(actions_df, pd.DataFrame) else 0
    remaining_null_columns = (
        len(remaining_nulls_df) if isinstance(remaining_nulls_df, pd.DataFrame) else 0
    )
    top_fill_target = "None"
    if not actions_df.empty and "Nulls Filled" in actions_df.columns:
        top_row = actions_df.sort_values("Nulls Filled", ascending=False).iloc[0]
        top_fill_target = f"{top_row['Column']} ({int(top_row['Nulls Filled'])})"

    banner_class = "warn" if remaining_null_columns > 0 else "ok"
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> M07 Data Imputation</div>"
        f"<div class='banner-item'><strong>Total Values Filled:</strong> {total_filled}</div>"
        f"<div class='banner-item'><strong>Columns Affected:</strong> {columns_affected}</div>"
        f"<div class='banner-item'><strong>Remaining Null Columns:</strong> {remaining_null_columns}</div>"
        "</div>"
    )

    sections = [
        _render_section(
            "Imputation Overview",
            (
                "<div class='cert-grid'>"
                "<div class='cert-stat-card'>"
                "<h3>Total Values Filled</h3>"
                f"{_metric_value(total_filled)}"
                "<p class='subtle'>Null values replaced during this run.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Columns Affected</h3>"
                f"{_metric_value(columns_affected)}"
                "<p class='subtle'>Columns with an imputation action applied.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Top Fill Target</h3>"
                f"{_metric_value(top_fill_target)}"
                "<p class='subtle'>Column with the highest number of filled nulls.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Remaining Null Risk</h3>"
                f"{_metric_value(remaining_null_columns)}"
                "<p class='subtle'>Columns still containing nulls after imputation.</p>"
                "</div>"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Imputation Summary & Null Audit",
            (
                "<div class='cert-ledger'>"
                f"<div class='card'><h3>Imputation Actions Log</h3>{_render_df(actions_df, full_preview=True)}</div>"
                f"<div class='card'><h3>Null Value Audit</h3>{_render_df(null_audit_df, full_preview=True)}</div>"
                "</div>"
            ),
            open_by_default=True,
        ),
    ]
    toc = [
        ("Imputation Overview", "Imputation Overview"),
        ("Imputation Summary & Null Audit", "Imputation Summary & Null Audit"),
    ]

    if categorical_shift:
        sections.append(
            _render_section(
                "Categorical Shift Analysis",
                _render_imputation_categorical_shift(categorical_shift),
            )
        )
        toc.append(("Categorical Shift Analysis", "Categorical Shift Analysis"))

    if isinstance(remaining_nulls_df, pd.DataFrame) and not remaining_nulls_df.empty:
        sections.append(
            _render_section(
                "Remaining Nulls",
                (
                    "<div class='card'>"
                    "<h3>Remaining Null Risk</h3>"
                    "<p class='subtle'>These columns still contain null values after the configured fill strategies ran.</p>"
                    f"{_render_df(remaining_nulls_df, full_preview=True)}</div>"
                ),
                open_by_default=True,
            )
        )
        toc.append(("Remaining Nulls", "Remaining Nulls"))

    if plot_paths:
        sections.append(
            _render_section("Plots", _render_plot_grid(plot_paths), open_by_default=True)
        )
        toc.append(("Plots", "Plots"))

    return _assemble_page(
        module_name="Imputation",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def _render_auto_heal_summary_table(summary: Any) -> str:
    if not isinstance(summary, dict) or not summary:
        return "<p class='empty'>No step summary available.</p>"
    rows = [
        {"Field": _display_name(str(key)), "Value": str(value)} for key, value in summary.items()
    ]
    return _render_df(pd.DataFrame(rows), full_preview=True)


def _render_auto_heal_step_cards(step_results: dict[str, Any]) -> str:
    cards: list[str] = []
    for step_name in ("normalization", "imputation"):
        step = step_results.get(step_name, {})
        if isinstance(step, dict):
            summary = step.get("summary", {})
            status = str(step.get("status", "skipped")).upper()
            artifact = step.get("artifact_url") or step.get("artifact_path") or "No dashboard"
            export_ref = step.get("export_url") or "No export"
        else:
            summary = {}
            status = "SKIPPED"
            artifact = "No dashboard"
            export_ref = "No export"
        tone = _status_tone_class(status)
        cards.append(
            f"<div class='cert-stat-card {tone}'>"
            f"<h3>{html.escape(step_name.title())}</h3>"
            f"{_metric_value(status)}"
            f"<p class='subtle'><strong>Dashboard:</strong> {html.escape(_normalize_reference_text(artifact))}</p>"
            f"<p class='subtle'><strong>Export:</strong> {html.escape(_normalize_reference_text(export_ref))}</p>"
            f"<p class='subtle'><strong>Summary Keys:</strong> {html.escape(', '.join(summary.keys()) if isinstance(summary, dict) and summary else 'None')}</p>"
            "</div>"
        )
    return "<div class='cert-grid'>" + "".join(cards) + "</div>"


def _render_auto_heal_step_drilldowns(step_results: dict[str, Any]) -> str:
    blocks: list[str] = []
    for step_name in ("normalization", "imputation"):
        step = step_results.get(step_name, {})
        if isinstance(step, dict):
            status = str(step.get("status", "skipped")).lower()
            summary = step.get("summary", {})
            artifact_ref = step.get("artifact_url") or step.get("artifact_path")
            export_ref = step.get("export_url")
        else:
            status = "skipped"
            summary = {}
            artifact_ref = None
            export_ref = None
        blocks.append(
            "<div class='card'>"
            f"<h3>{html.escape(step_name.title())}</h3>"
            "<div class='cert-grid'>"
            f"<div class='cert-stat-card {_status_tone_class(status)}'>"
            "<h3>Status</h3>"
            f"{_metric_value(status.upper())}"
            "<p class='subtle'>Outcome recorded for this repair stage.</p>"
            "</div>"
            "<div class='cert-stat-card'>"
            "<h3>Dashboard Reference</h3>"
            f"{_render_reference_value(artifact_ref, empty_label='No dashboard generated.')}"
            "</div>"
            "<div class='cert-stat-card'>"
            "<h3>Data Export</h3>"
            f"{_render_reference_value(export_ref, empty_label='No export recorded.')}"
            "</div>"
            "</div>"
            "<h4>Step Evidence</h4>"
            f"{_render_auto_heal_summary_table(summary)}"
            "</div>"
        )
    return "<div class='stack'>" + "".join(blocks) + "</div>"


def _render_auto_heal_dashboard(report: dict[str, Any], run_id: str) -> str:
    steps = report.get("steps", {})
    failed_steps = report.get("failed_steps", [])
    row_count = report.get("row_count", 0)
    final_session_id = report.get("final_session_id", "")
    final_export = report.get("final_export_url") or "Unavailable"
    final_dashboard = (
        report.get("final_dashboard_url") or report.get("final_dashboard_path") or "Unavailable"
    )
    inferred_modules = report.get("inferred_modules", [])
    message = str(report.get("message", ""))

    status = str(report.get("status", "warn")).lower()
    banner_class = "ok" if status == "pass" and not failed_steps else "warn"
    readiness = (
        "Ready For Final Audit"
        if status == "pass" and not failed_steps
        else "Needs Operator Review"
    )
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> MCP Auto Heal</div>"
        f"<div class='banner-item'><strong>Status:</strong> {html.escape(status.upper())}</div>"
        f"<div class='banner-item'><strong>Readiness:</strong> {html.escape(readiness)}</div>"
        f"<div class='banner-item'><strong>Final Session:</strong> {html.escape(final_session_id or 'Unavailable')}</div>"
        f"<div class='banner-item'><strong>Failed Steps:</strong> {len(failed_steps)}</div>"
        "</div>"
    )

    failed_df = pd.DataFrame({"failed_step": failed_steps}) if failed_steps else pd.DataFrame()
    inferred_df = pd.DataFrame({"module": inferred_modules}) if inferred_modules else pd.DataFrame()
    outcome_df = pd.DataFrame(
        [
            {"Field": "Run Status", "Value": status.upper()},
            {"Field": "Readiness", "Value": readiness},
            {"Field": "Operator Message", "Value": message or "No operator message recorded."},
            {"Field": "Final Session", "Value": final_session_id or "Unavailable"},
        ]
    )

    sections = [
        _render_section(
            "Outcome Summary",
            (
                "<div class='cert-grid'>"
                "<div class='cert-stat-card'>"
                "<h3>Final Row Count</h3>"
                f"{_metric_value(row_count)}"
                "<p class='subtle'>Rows in the healed session after automation.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Inferred Modules</h3>"
                f"{_metric_value(len(inferred_modules))}"
                "<p class='subtle'>Modules inferred and considered for execution.</p>"
                "</div>"
                f"<div class='cert-stat-card {_status_tone_class(readiness)}'>"
                "<h3>Readiness</h3>"
                f"{_metric_value(readiness)}"
                "<p class='subtle'>Whether the healed result is ready for final certification.</p>"
                "</div>"
                f"<div class='cert-stat-card {'pass' if len(failed_steps) == 0 else 'warn'}'>"
                "<h3>Failed Steps</h3>"
                f"{_metric_value(len(failed_steps))}"
                "<p class='subtle'>Repair stages that still require intervention.</p>"
                "</div>"
                "</div>"
                "<div class='cert-ledger'>"
                f"<div class='card'><h3>Outcome Ledger</h3>{_render_df(outcome_df, full_preview=True)}</div>"
                "<div class='card'>"
                "<h3>Terminal References</h3>"
                "<p class='subtle'><strong>Final Export</strong></p>"
                f"{_render_reference_value(final_export, empty_label='No final export recorded.')}"
                "<p class='subtle'><strong>Final Dashboard</strong></p>"
                f"{_render_reference_value(final_dashboard, empty_label='No child dashboard recorded.')}"
                "</div>"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Step Outcomes",
            _render_auto_heal_step_cards(steps),
            open_by_default=True,
        ),
        _render_section(
            "Step Drilldowns",
            (
                "<div class='stack'>"
                f"<div class='card'><h3>Inferred Modules</h3>{_render_df(inferred_df, full_preview=True)}</div>"
                f"{_render_auto_heal_step_drilldowns(steps)}"
                "</div>"
            ),
            open_by_default=False,
        ),
    ]
    toc = [
        ("Outcome Summary", "Outcome Summary"),
        ("Step Outcomes", "Step Outcomes"),
        ("Step Drilldowns", "Step Drilldowns"),
    ]

    if not failed_df.empty:
        sections.append(
            _render_section(
                "Failures",
                (
                    "<div class='card'>"
                    "<h3>Failed Steps</h3>"
                    "<p class='subtle'>These steps ended in fail/error and need operator review.</p>"
                    f"{_render_df(failed_df, full_preview=True)}</div>"
                ),
                open_by_default=True,
            )
        )
        toc.append(("Failures", "Failures"))

    return _assemble_page(
        module_name="Auto Heal",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def _render_terminal_references(
    *,
    final_dashboard: Any,
    final_export: Any,
    final_status: str,
    failed_modules: int,
    modules: dict[str, Any],
    module_order: list[str],
) -> str:
    expected_terminal = modules.get("Final Audit") or {}
    expected_status = _tab_status_label(expected_terminal.get("status", "not_run"))
    fallback_module = None
    fallback_dashboard = ""
    fallback_export = ""
    for module_name in reversed(module_order):
        payload = modules.get(module_name) or {}
        if not isinstance(payload, dict):
            continue
        candidate_dashboard = payload.get("dashboard_url") or payload.get("dashboard_path") or ""
        candidate_export = payload.get("export_url") or payload.get("artifact_url") or ""
        if candidate_dashboard or candidate_export:
            fallback_module = module_name
            fallback_dashboard = candidate_dashboard
            fallback_export = candidate_export
            break
    terminal_ready = bool(final_dashboard or final_export)
    fallback_detail = (
        (
            "<p class='subtle'><strong>Fallback Dashboard</strong></p>"
            + _render_reference_value(
                fallback_dashboard, empty_label="No fallback dashboard recorded."
            )
            + "<p class='subtle'><strong>Fallback Export</strong></p>"
            + _render_reference_value(fallback_export, empty_label="No fallback export recorded.")
        )
        if fallback_module
        else ""
    )
    summary_line = (
        "Terminal artifacts are available for direct review."
        if terminal_ready
        else (
            "Terminal artifacts have not been recorded yet for this pipeline view. "
            "Final Audit is the expected terminal source."
        )
    )
    empty_state = (
        ""
        if terminal_ready
        else (
            "<div class='terminal-art'>"
            "<h3>Awaiting Terminal Artifacts</h3>"
            f"<p class='subtle'><strong>Expected Terminal Module:</strong> Final Audit ({html.escape(expected_status)})</p>"
            f"<p class='subtle'><strong>Best Available Fallback:</strong> {html.escape(fallback_module or 'None recorded')}</p>"
            "<p class='subtle'>No final dashboard or final export was attached to this run. Review the fallback surface below until the terminal certification artifacts land.</p>"
            f"{fallback_detail}"
            "</div>"
        )
    )
    return (
        "<div class='terminal-card'>"
        f"<p class='subtle'>{html.escape(summary_line)}</p>"
        "<div class='terminal-grid'>"
        "<div class='terminal-slot'>"
        "<h4>Final Dashboard</h4>"
        f"{_render_reference_value(final_dashboard, empty_label='No final dashboard recorded.')}"
        "</div>"
        "<div class='terminal-slot'>"
        "<h4>Final Export</h4>"
        f"{_render_reference_value(final_export, empty_label='No final export recorded.')}"
        "</div>"
        "<div class='terminal-slot'>"
        "<h4>Pipeline End State</h4>"
        f"<p class='subtle'><strong>Status:</strong> {html.escape(final_status.upper())}</p>"
        f"<p class='subtle'><strong>Blocking Modules:</strong> {failed_modules}</p>"
        f"<p class='subtle'><strong>Expected Terminal Module:</strong> Final Audit ({html.escape(expected_status)})</p>"
        "</div>"
        "</div>"
        f"{empty_state}"
        "</div>"
    )


def _render_pipeline_module_panel(module_name: str, payload: dict[str, Any]) -> str:
    effective_payload = payload if isinstance(payload, dict) and payload else {"status": "not_run"}
    status = str(effective_payload.get("status", "not_run")).lower()
    summary = effective_payload.get("summary", {})
    dashboard_url = effective_payload.get("dashboard_url")
    dashboard_path = effective_payload.get("dashboard_path")
    dashboard_ref = dashboard_url or dashboard_path
    embed_src = _embed_reference_src(dashboard_path, dashboard_url)
    export_ref = effective_payload.get("export_url") or effective_payload.get("artifact_url")
    warnings = effective_payload.get("warnings", [])
    summary_table = _render_auto_heal_summary_table(summary)
    warning_table = (
        _render_df(pd.DataFrame({"Warning": [str(item) for item in warnings]}), full_preview=True)
        if warnings
        else "<p class='empty'>No module warnings recorded.</p>"
    )
    warning_count = len(warnings)
    summary_keys = len(summary) if isinstance(summary, dict) else 0
    dashboard_card = (
        "<div class='card'>"
        f"<h3>{html.escape(module_name)} Report</h3>"
        f"<iframe class='tab-embed' src='{html.escape(embed_src, quote=True)}' title='{html.escape(module_name)} report'></iframe>"
        "<div class='module-callout'>"
        "<p class='subtle'>If the embedded report does not load in your environment, open the standalone module dashboard directly.</p>"
        f"<a class='action-link' href='{html.escape(embed_src, quote=True)}' target='_blank' rel='noopener noreferrer'>Open Report Directly</a>"
        "</div>"
        "</div>"
        if embed_src
        else (
            "<div class='card'>"
            f"<h3>{html.escape(module_name)} Report</h3>"
            "<div class='module-callout'>"
            "<p class='subtle'>No embeddable dashboard reference was recorded for this module in the current pipeline run.</p>"
            "<span class='action-link secondary'>Report Unavailable</span>"
            "</div>"
            "</div>"
        )
    )
    if status == "not_run":
        not_run_note = (
            "<div class='card'>"
            f"<h3>{html.escape(module_name)} Not Run</h3>"
            "<p class='subtle'>This module was not observed in the current pipeline execution. That may be expected if prerequisites were skipped or the flow terminated early.</p>"
            "</div>"
        )
    else:
        not_run_note = ""
    return (
        "<div class='module-shell'>"
        "<div class='module-mini-grid'>"
        f"<div class='module-mini-card {_status_tone_class(status)}'>"
        "<h3>Module Status</h3>"
        f"{_metric_value(status.upper())}"
        f"<p class='subtle'>{html.escape(module_name)} latest recorded state.</p>"
        "</div>"
        f"<div class='module-mini-card {'pass' if warning_count == 0 else 'warn'}'>"
        "<h3>Warnings</h3>"
        f"{_metric_value(warning_count)}"
        "<p class='subtle'>Review warnings and operator notes before treating the module as complete.</p>"
        "</div>"
        "<div class='module-mini-card'>"
        "<h3>Summary Fields</h3>"
        f"{_metric_value(summary_keys)}"
        "<p class='subtle'>Top-level summary values recorded for this stage.</p>"
        "</div>"
        "</div>"
        "<div class='cert-ledger'>"
        "<div class='card'>"
        "<h3>Artifact References</h3>"
        "<p class='subtle'><strong>Dashboard</strong></p>"
        f"{_render_reference_value(dashboard_ref, empty_label='No dashboard artifact recorded.')}"
        "<p class='subtle'><strong>Data Export</strong></p>"
        f"{_render_reference_value(export_ref, empty_label='No data artifact recorded.')}"
        "</div>"
        f"<div class='card'><h3>{html.escape(module_name)} Summary</h3>{summary_table}{warning_table if warning_count else ''}</div>"
        "</div>"
        f"{not_run_note}"
        f"{dashboard_card}"
        "</div>"
    )


def _render_pipeline_dashboard(report: dict[str, Any], run_id: str) -> str:
    final_status = str(report.get("final_status", "unknown"))
    session_id = str(report.get("session_id", ""))
    health_score = report.get("health_score", "N/A")
    health_status = str(report.get("health_status", "unknown")).upper()
    ready_modules = int(report.get("ready_modules", 0))
    warned_modules = int(report.get("warned_modules", 0))
    failed_modules = int(report.get("failed_modules", 0))
    not_run_modules = int(report.get("not_run_modules", 0))
    module_order = report.get("module_order", [])
    modules = report.get("modules", {})
    final_dashboard = report.get("final_dashboard_url") or report.get("final_dashboard_path")
    final_export = report.get("final_export_url")

    banner_class = "ok" if failed_modules == 0 else "warn"
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> Pipeline Review Shell</div>"
        f"<div class='banner-item'><strong>Final Status:</strong> {html.escape(final_status.upper())}</div>"
        f"<div class='banner-item'><strong>Health:</strong> {html.escape(str(health_score))} ({html.escape(health_status)})</div>"
        f"<div class='banner-item'><strong>Session:</strong> {html.escape(session_id or 'Unavailable')}</div>"
        f"<div class='banner-item'><strong>Modules:</strong> {len(module_order)}</div>"
        "</div>"
    )

    executive = (
        "<div class='stack'>"
        "<div class='cert-grid'>"
        "<div class='cert-stat-card'><h3>Healthy Modules</h3>"
        f"{_metric_value(ready_modules)}"
        "<p class='subtle'>Modules with pass-level outcomes.</p></div>"
        "<div class='cert-stat-card'><h3>Warned Modules</h3>"
        f"{_metric_value(warned_modules)}"
        "<p class='subtle'>Modules that need review but did not fail outright.</p></div>"
        "<div class='cert-stat-card'><h3>Failed Modules</h3>"
        f"{_metric_value(failed_modules)}"
        "<p class='subtle'>Blocking modules or missing end-state evidence.</p></div>"
        "<div class='cert-stat-card'><h3>Not Run</h3>"
        f"{_metric_value(not_run_modules)}"
        "<p class='subtle'>Pipeline stages not observed in the current run history.</p></div>"
        "</div>"
        "<div class='cert-ledger'>"
        "<div class='card'><h3>Final References</h3>"
        f"{_render_terminal_references(final_dashboard=final_dashboard, final_export=final_export, final_status=final_status, failed_modules=failed_modules, modules=modules, module_order=module_order)}"
        "</div>"
        f"<div class='card'><h3>Module Status Ledger</h3>{_render_df(pd.DataFrame([{'Module': name, 'Status': _tab_status_label((modules.get(name) or {}).get('status', 'unknown')), 'Badge': _module_badge(_tab_status_label((modules.get(name) or {}).get('status', 'unknown')))} for name in module_order]), full_preview=True, allow_html_cols={'Badge'})}</div>"
        "</div>"
        "</div>"
    )

    tab_buttons = [
        "<button class='tab-button active' type='button' data-tab-target='pipeline-overview' onclick='window.atkDashboard.openTab(this)'>Executive Summary</button>"
    ]
    tab_panels = [f"<div class='tab-panel active' id='pipeline-overview'>{executive}</div>"]
    for module_name in module_order:
        panel_id = f"pipeline-{_slugify(module_name)}"
        status = _tab_status_label((modules.get(module_name) or {}).get("status", "unknown"))
        tab_buttons.append(
            f"<button class='tab-button' type='button' data-tab-target='{panel_id}' onclick='window.atkDashboard.openTab(this)'>{html.escape(module_name)}<span class='tab-status'>{html.escape(status)}</span></button>"
        )
        tab_panels.append(
            f"<div class='tab-panel' id='{panel_id}'>{_render_pipeline_module_panel(module_name, modules.get(module_name, {}))}</div>"
        )

    sections = [
        "<div class='tab-shell' data-tab-shell='pipeline'>"
        f"<div class='tab-nav'>{''.join(tab_buttons)}</div>"
        f"{''.join(tab_panels)}"
        "</div>"
    ]
    toc: list[tuple[str, str]] = []
    return _assemble_page(
        module_name="Pipeline Dashboard",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def _render_cockpit_dashboard(report: dict[str, Any], run_id: str) -> str:
    overview = report.get("overview", {})
    operating_posture = report.get("operating_posture", {})
    operator_brief = report.get("operator_brief", {})
    best_surfaces = report.get("best_surfaces", {})
    blockers = report.get("blockers", [])
    recent_run_gaps = report.get("recent_run_gaps", [])
    recent_runs = report.get("recent_runs", [])
    resources = report.get("resources", [])
    resource_groups = report.get("resource_groups", [])
    launchpad = report.get("launchpad", [])
    launch_sequences = report.get("launch_sequences", [])
    data_dictionary = report.get("data_dictionary", {})

    posture_label = str(operating_posture.get("label", "Healthy"))
    banner_class = (
        "fail"
        if posture_label.lower() == "blocked"
        else "warn"
        if posture_label.lower() in {"needs review", "warn", "warning"}
        else "ok"
    )
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> Cockpit Operator Hub</div>"
        f"<div class='banner-item'><strong>Posture:</strong> {html.escape(posture_label)}</div>"
        f"<div class='banner-item'><strong>Recent Runs:</strong> {html.escape(str(overview.get('recent_run_count', 0)))}</div>"
        f"<div class='banner-item'><strong>Warning Runs:</strong> {html.escape(str(overview.get('warning_runs', 0)))}</div>"
        f"<div class='banner-item'><strong>Failed Runs:</strong> {html.escape(str(overview.get('failed_runs', 0)))}</div>"
        "</div>"
    )

    brief_lanes = "".join(
        "<div class='brief-lane'>"
        f"<h4>{html.escape(str(item.get('title', 'Lane')))}</h4>"
        f"<p>{html.escape(str(item.get('detail', '')))}</p>"
        "</div>"
        for item in operator_brief.get("lanes", [])
        if isinstance(item, dict)
    )
    surface_items = []
    for label, payload in (
        ("Latest Pipeline Dashboard", best_surfaces.get("pipeline_dashboard", {})),
        ("Latest Auto-Heal Dashboard", best_surfaces.get("auto_heal_dashboard", {})),
        ("Latest Final Audit Dashboard", best_surfaces.get("final_audit_dashboard", {})),
    ):
        surface_items.append(
            "<div class='surface-item'>"
            f"<h4>{html.escape(label)}</h4>"
            f"<p class='subtle'><strong>Run:</strong> {html.escape(str((payload or {}).get('run_id') or 'Unavailable'))}</p>"
            f"{_render_reference_value((payload or {}).get('reference', ''), empty_label='No artifact recorded.')}"
            "</div>"
        )
    blocker_items = []
    for item in blockers:
        blocker_items.append(
            "<div class='surface-item'>"
            f"<h4>{html.escape(str(item.get('run_id', 'unknown')))} · {html.escape(str(item.get('status', 'UNKNOWN')))}</h4>"
            f"<p class='subtle'><strong>Latest module:</strong> {html.escape(str(item.get('latest_module', 'unknown')))}</p>"
            f"<p class='subtle'><strong>Warnings recorded:</strong> {html.escape(str(item.get('warning_count', 0)))}</p>"
            "</div>"
        )
    blocker_fallback_html = "<p class='empty'>No warn/fail runs in the current cockpit slice.</p>"
    gap_items = "".join(
        "<div class='surface-item'>"
        "<h4>Missing Dashboard Or Artifact</h4>"
        f"<p class='subtle'>{html.escape(str(item))}</p>"
        "</div>"
        for item in recent_run_gaps
        if str(item).strip()
    )
    gaps_panel = (
        "<div class='readme-section'>"
        "<h3>Missing Dashboards Or Artifacts</h3>"
        "<p class='subtle'>These are recent runs where the cockpit could not find an expected dashboard or artifact reference.</p>"
        "<div class='missing-list'>"
        f"{gap_items}"
        "</div>"
        "</div>"
        if gap_items
        else ""
    )
    overview_section = (
        "<div class='hub-stack'>"
        "<div class='brief-card'>"
        f"<p class='hub-kicker'>{html.escape(str(operator_brief.get('title', 'Cockpit Briefing')))}</p>"
        "<h3>What This Cockpit Helps You Review</h3>"
        f"<p>{html.escape(str(operator_brief.get('summary', '')))}</p>"
        "<p><strong>This page is organized into three simple lanes:</strong></p>"
        f"<div class='brief-lanes'>{brief_lanes}</div>"
        "</div>"
        "<div class='hub-grid'>"
        "<div class='hub-card'><p class='hub-kicker'>Ops Snapshot</p><h3>Recent Runs</h3>"
        f"{_metric_value(overview.get('recent_run_count', 0))}"
        "<p class='subtle'>History-backed runs discovered from local cockpit data.</p></div>"
        "<div class='hub-card'><p class='hub-kicker'>Attention</p><h3>Warning Runs</h3>"
        f"{_metric_value(overview.get('warning_runs', 0))}"
        "<p class='subtle'>Runs that ended in warn-level states and likely need review.</p></div>"
        "<div class='hub-card'><p class='hub-kicker'>Blocking</p><h3>Failed Runs</h3>"
        f"{_metric_value(overview.get('failed_runs', 0))}"
        "<p class='subtle'>Runs with fail/error end states in the recent cockpit slice.</p></div>"
        "<div class='hub-card'><p class='hub-kicker'>Stable</p><h3>Healthy Runs</h3>"
        f"{_metric_value(overview.get('healthy_runs', 0))}"
        "<p class='subtle'>Recent runs that currently look safe to treat as pass-level outcomes.</p></div>"
        "<div class='hub-card'><p class='hub-kicker'>Coverage</p><h3>Pipeline Dashboards</h3>"
        f"{_metric_value(overview.get('pipeline_dashboards_available', 0))}"
        "<p class='subtle'>Recent runs that already have a pipeline dashboard artifact available.</p></div>"
        "<div class='hub-card'><p class='hub-kicker'>Coverage</p><h3>Auto-Heal Dashboards</h3>"
        f"{_metric_value(overview.get('auto_heal_dashboards_available', 0))}"
        "<p class='subtle'>Recent runs with an operator-facing remediation dashboard already attached.</p></div>"
        "</div>"
        "<div class='readme-section'>"
        "<h3>Current Alerts And Blockers</h3>"
        "<div class='alert-list'>"
        f"{''.join(blocker_items) if blocker_items else blocker_fallback_html}"
        "</div>"
        "</div>"
        "<div class='overview-split'>"
        "<div class='overview-column'>"
        "<div class='readme-section'>"
        "<h3>Recent Run Dashboards</h3>"
        "<div class='surface-list'>"
        f"{''.join(surface_items)}"
        "</div>"
        "</div>"
        "</div>"
        f"{gaps_panel}"
        "</div>"
        "</div>"
    )

    recent_run_cards: list[str] = []
    for run in recent_runs:
        dashboard_ref = run.get("pipeline_dashboard") or run.get("best_dashboard")
        export_ref = run.get("best_export")
        recent_run_cards.append(
            "<div class='resource-card'>"
            f"<p class='resource-meta'>{html.escape(str(run.get('timestamp') or 'Recent run'))}</p>"
            f"<h3>{html.escape(str(run.get('run_id', 'unknown')))}</h3>"
            f"{_status_chip(str(run.get('status', 'unknown')))}"
            "<div class='module-mini-grid'>"
            "<div class='module-mini-card'><h3>Status</h3>"
            f"{_metric_value(_tab_status_label(run.get('status', 'unknown')))}"
            f"<p class='subtle'>Latest module: {html.escape(str(run.get('latest_module', 'unknown')))}</p></div>"
            "<div class='module-mini-card'><h3>Health</h3>"
            f"{_metric_value(run.get('health_score', 'N/A'))}"
            f"<p class='subtle'>{html.escape(str(run.get('health_status', 'unknown')).upper())}</p></div>"
            "<div class='module-mini-card'><h3>Warnings</h3>"
            f"{_metric_value(run.get('warning_count', 0))}"
            f"<p class='subtle'>Modules observed: {html.escape(str(run.get('module_count', 0)))}</p></div>"
            "</div>"
            "<p class='subtle'><strong>Session:</strong> "
            f"{html.escape(str(run.get('session_id') or 'Unavailable'))}</p>"
            "<p class='subtle'><strong>Best Dashboard</strong></p>"
            f"{_render_reference_value(dashboard_ref, empty_label='No dashboard recorded.')}"
            "<p class='subtle'><strong>Best Export</strong></p>"
            f"{_render_reference_value(export_ref, empty_label='No export recorded.')}"
            "</div>"
        )
    recent_runs_section = (
        "<div class='hub-stack'>" + "".join(recent_run_cards) + "</div>"
        if recent_run_cards
        else "<p class='empty'>No recent runs were discovered in local history.</p>"
    )

    grouped_resources: list[str] = []
    template_items = [item for item in resources if str(item.get("Kind", "")).lower() == "template"]
    reference_items = [
        item for item in resources if str(item.get("Kind", "")).lower() != "template"
    ]
    for group in resource_groups:
        group_items = group.get("items", [])
        if str(group.get("title", "")).lower() == "templates and contracts":
            group_items = template_items
        items_html = []
        for item in group_items:
            items_html.append(
                "<div class='resource-inline-item'>"
                f"<p class='resource-meta'>{html.escape(str(item.get('Kind', 'resource')).replace('_', ' ').title())}</p>"
                f"<h4>{html.escape(str(item.get('Title', 'Untitled')))}</h4>"
                f"<p class='subtle'>{html.escape(str(item.get('Detail', '')))}</p>"
                "<p class='subtle'><strong>Open With</strong></p>"
                f"{_render_reference_value(item.get('Reference', ''), empty_label='No reference recorded.')}"
                "</div>"
            )
        grouped_resources.append(
            "<div class='readme-section'>"
            f"<h3>{html.escape(str(group.get('title', 'Resources')))}</h3>"
            f"<p class='subtle'>{html.escape(str(group.get('intro', '')))}</p>"
            "<div class='resource-inline-list scroll-pane'>"
            f"{''.join(items_html)}"
            "</div>"
            "</div>"
        )
    all_resource_refs = "".join(
        "<div class='resource-inline-item'>"
        f"<p class='resource-meta'>{html.escape(str(item.get('Kind', 'resource')).replace('_', ' ').title())}</p>"
        f"<h4>{html.escape(str(item.get('Title', 'Untitled')))}</h4>"
        f"{_render_reference_value(item.get('Reference', ''), empty_label='No reference recorded.')}"
        "</div>"
        for item in reference_items
    )
    resources_panel = (
        "<div class='readme-grid'>"
        "<div class='readme-section'>"
        "<h3>Resources For Reading, Planning, And Setup</h3>"
        "<p class='subtle'>Use this tab when you want context before you click deeper. The guides explain the toolkit in plain language, the templates give you safe starting points, and the catalog helps when you need the exact setting behind a visible behavior.</p>"
        "</div>"
        "<div class='resource-group-grid'>"
        f"{''.join(grouped_resources)}"
        "</div>"
        "<div class='readme-section'>"
        "<h3>All References</h3>"
        "<p class='subtle'>This is the compact shelf of guides, catalogs, and other non-template references linked from the cockpit.</p>"
        "<div class='resource-inline-list scroll-pane'>"
        f"{all_resource_refs}"
        "</div>"
        "</div>"
        "</div>"
    )
    sequence_cards = []
    for sequence in launch_sequences:
        steps_html = "".join(
            f"<li>{html.escape(str(step))}</li>"
            for step in sequence.get("steps", [])
            if str(step).strip()
        )
        sequence_cards.append(
            "<div class='sequence-card'>"
            f"<h3>{html.escape(str(sequence.get('title', 'Workflow')))}</h3>"
            f"<ol class='sequence-list'>{steps_html}</ol>"
            "</div>"
        )
    launch_cards = []
    for item in launchpad:
        launch_cards.append(
            "<div class='launch-item'>"
            "<p class='resource-meta'>Open This Next</p>"
            f"<h3>{html.escape(str(item.get('Action', 'Action')))}</h3>"
            f"<p class='subtle'>{html.escape(str(item.get('Why', '')))}</p>"
            f"<p class='subtle'><strong>Tool surface:</strong> {html.escape(str(item.get('Tool', 'tool')))}</p>"
            "</div>"
        )
    launchpad_panel = (
        "<div class='readme-grid'>"
        "<div class='readme-section'>"
        "<h3>Launchpad For Moving From Review To Action</h3>"
        "<p class='subtle'>Use this tab when you are ready to move from understanding the run to doing something about it. The workflow cards show the common paths people take through the toolkit, and the cards below point to the specific surfaces that support those tasks.</p>"
        "</div>"
        "<div class='sequence-grid'>"
        f"{''.join(sequence_cards)}"
        "</div>"
        "<div class='readme-section'>"
        "<h3>Things You Can Open Next</h3>"
        "<div class='launch-list'>"
        f"{''.join(launch_cards)}"
        "</div>"
        "</div>"
        "</div>"
    )
    dictionary_tab = (
        "<div class='readme-grid'>"
        "<div class='brief-card'>"
        "<p class='hub-kicker'>Planned Surface</p>"
        "<h3>Data Dictionary Lane</h3>"
        f"<p>{html.escape(str(data_dictionary.get('direction', '')))}</p>"
        "</div>"
        "<div class='hub-grid'>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Status</p>"
        "<h3>Data Dictionary</h3>"
        f"{_status_chip(str(data_dictionary.get('status', 'not_implemented')))}"
        "<p class='subtle'>Reserve the future prelaunch dictionary and schema-expectation surface in the cockpit.</p>"
        "<p class='subtle'><strong>Template</strong></p>"
        f"{_render_reference_value(data_dictionary.get('template_path', ''), empty_label='No template recorded.')}"
        "<p class='subtle'><strong>Plan</strong></p>"
        f"{_render_reference_value(data_dictionary.get('implementation_plan', ''), empty_label='No plan recorded.')}"
        "</div>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Why It Matters</p>"
        "<h3>Prelaunch Report Space</h3>"
        "<p class='subtle'>This tab exists so the eventual dictionary does not feel bolted on later. It should sit beside the rest of the operator surfaces, inherit infer_configs evidence, and give users a readable preflight view before the heavier pipeline stages run.</p>"
        "</div>"
        "</div>"
        "</div>"
    )

    tab_buttons = [
        "<button class='tab-button active' type='button' data-tab-target='cockpit-overview' onclick='window.atkDashboard.openTab(this)'>Overview</button>",
        "<button class='tab-button' type='button' data-tab-target='cockpit-runs' onclick='window.atkDashboard.openTab(this)'>Recent Runs</button>",
        "<button class='tab-button' type='button' data-tab-target='cockpit-resources' onclick='window.atkDashboard.openTab(this)'>Resources</button>",
        "<button class='tab-button' type='button' data-tab-target='cockpit-launchpad' onclick='window.atkDashboard.openTab(this)'>Launchpad</button>",
        "<button class='tab-button' type='button' data-tab-target='cockpit-dictionary' onclick='window.atkDashboard.openTab(this)'>Data Dictionary</button>",
    ]
    tab_panels = [
        f"<div class='tab-panel active' id='cockpit-overview'>{overview_section}</div>",
        f"<div class='tab-panel' id='cockpit-runs'>{recent_runs_section}</div>",
        f"<div class='tab-panel' id='cockpit-resources'>{resources_panel}</div>",
        f"<div class='tab-panel' id='cockpit-launchpad'>{launchpad_panel}</div>",
        f"<div class='tab-panel' id='cockpit-dictionary'>{dictionary_tab}</div>",
    ]
    sections = [
        "<div class='tab-shell' data-tab-shell='cockpit'>"
        f"<div class='tab-nav'>{''.join(tab_buttons)}</div>"
        f"{''.join(tab_panels)}"
        "</div>"
    ]
    toc: list[tuple[str, str]] = []

    return _assemble_page(
        module_name="Cockpit Dashboard",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def generate_dashboard_html(
    report_tables: dict[str, Any],
    module_name: str,
    run_id: str,
    plot_paths: dict[str, Any] | None = None,
) -> str:
    """Render a standalone HTML dashboard for the provided module payload."""
    normalized = module_name.strip().lower().replace("_", " ")
    if normalized == "diagnostics":
        return _render_diagnostics_dashboard(report_tables, run_id, plot_paths)
    if normalized == "validation":
        return _render_validation_dashboard(report_tables, run_id)
    if normalized == "final audit":
        return _render_final_audit_dashboard(report_tables, run_id)
    if normalized == "normalization":
        return _render_normalization_dashboard(report_tables, run_id)
    if normalized == "duplicates":
        return _render_duplicates_dashboard(report_tables, run_id, plot_paths)
    if normalized == "outlier detection":
        return _render_outlier_detection_dashboard(report_tables, run_id, plot_paths)
    if normalized == "outlier handling":
        return _render_outlier_handling_dashboard(report_tables, run_id)
    if normalized == "imputation":
        return _render_imputation_dashboard(report_tables, run_id, plot_paths)
    if normalized == "auto heal":
        return _render_auto_heal_dashboard(report_tables, run_id)
    if normalized == "cockpit dashboard":
        return _render_cockpit_dashboard(report_tables, run_id)
    if normalized == "pipeline dashboard":
        return _render_pipeline_dashboard(report_tables, run_id)
    return _render_generic_dashboard(report_tables, module_name, run_id, plot_paths)
