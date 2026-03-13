"""Standalone dashboard HTML renderer for module exports."""

from __future__ import annotations

import base64
import html
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

_MAX_PREVIEW_ROWS = 50

_DASHBOARD_CSS = """
<style>
  :root {
    --bg: #f4f1ea;
    --paper: #fffdf8;
    --ink: #1f2933;
    --muted: #667085;
    --line: #d9d3c7;
    --accent: #1f4b4a;
    --accent-soft: #dceae7;
    --warn: #9a3412;
    --warn-soft: #fef0e8;
    --ok: #14532d;
    --ok-soft: #e8f5ec;
    --shadow: 0 14px 30px rgba(31, 41, 51, 0.08);
    --radius: 18px;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: Georgia, "Times New Roman", serif;
    background:
      radial-gradient(circle at top left, rgba(31, 75, 74, 0.10), transparent 28%),
      linear-gradient(180deg, #f7f2ea 0%, #f1ece2 100%);
    color: var(--ink);
  }
  .page {
    max-width: 1180px;
    margin: 0 auto;
    padding: 40px 20px 64px;
  }
  .hero {
    background: linear-gradient(135deg, rgba(31, 75, 74, 0.98), rgba(50, 82, 117, 0.92));
    color: #f9f6ef;
    border-radius: 28px;
    padding: 28px;
    box-shadow: var(--shadow);
    margin-bottom: 24px;
  }
  .hero-kicker {
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-size: 0.76rem;
    opacity: 0.82;
    margin-bottom: 10px;
  }
  .hero h1 {
    margin: 0 0 10px;
    font-size: 2.2rem;
    line-height: 1.1;
  }
  .hero-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 10px 16px;
    color: rgba(249, 246, 239, 0.88);
    font-size: 0.95rem;
  }
  .banner {
    background: var(--accent-soft);
    border: 1px solid rgba(31, 75, 74, 0.15);
    border-radius: var(--radius);
    padding: 16px 18px;
    margin-bottom: 20px;
    display: flex;
    flex-wrap: wrap;
    gap: 10px 18px;
    box-shadow: var(--shadow);
  }
  .banner.warn {
    background: var(--warn-soft);
    border-color: rgba(154, 52, 18, 0.18);
  }
  .banner.ok {
    background: var(--ok-soft);
    border-color: rgba(20, 83, 45, 0.18);
  }
  .banner-item {
    font-size: 0.96rem;
  }
  .toc {
    background: rgba(255, 253, 248, 0.72);
    backdrop-filter: blur(6px);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 14px 18px;
    margin-bottom: 18px;
  }
  .toc a {
    color: var(--accent);
    text-decoration: none;
    margin-right: 14px;
    font-weight: 600;
  }
  details.section {
    background: var(--paper);
    border: 1px solid rgba(31, 41, 51, 0.08);
    border-radius: 22px;
    padding: 0 18px 18px;
    margin-bottom: 18px;
    box-shadow: var(--shadow);
  }
  details.section[open] {
    animation: fade-in 160ms ease-out;
  }
  summary {
    cursor: pointer;
    list-style: none;
    padding: 18px 0 14px;
    font-weight: 700;
    font-size: 1.04rem;
  }
  summary::-webkit-details-marker { display: none; }
  .section-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px;
  }
  .card {
    background: #fcfaf5;
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: 14px;
    min-width: 0;
    overflow: hidden;
  }
  .card.wide {
    grid-column: span 2;
  }
  .card h3 {
    margin: 0 0 10px;
    font-size: 0.98rem;
  }
  .key {
    margin-top: 12px;
    border-top: 1px solid var(--line);
    padding-top: 12px;
    color: var(--muted);
    font-size: 0.9rem;
  }
  .key ul {
    margin: 8px 0 0 18px;
    padding: 0;
  }
  .stack > * + * {
    margin-top: 14px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.88rem;
    background: #fff;
    border-radius: 12px;
    min-width: max-content;
  }
  th, td {
    padding: 8px 10px;
    border: 1px solid #e7dfd1;
    text-align: left;
    vertical-align: top;
    white-space: nowrap;
  }
  th {
    background: #f1ece2;
    color: #23303b;
    font-weight: 700;
  }
  tr:nth-child(even) td {
    background: #fdfaf3;
  }
  .subtle {
    color: var(--muted);
    font-size: 0.88rem;
  }
  .table-wrap {
    width: 100%;
    max-width: 100%;
    overflow-x: auto;
    overflow-y: hidden;
    border-radius: 12px;
  }
  .empty {
    color: var(--muted);
    font-style: italic;
    margin: 0;
  }
  .plot-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px;
  }
  .plot-card img {
    width: 100%;
    height: auto;
    display: block;
    border-radius: 14px;
    border: 1px solid var(--line);
    background: #fff;
  }
  .plot-card h3 {
    margin: 0 0 10px;
  }
  .drilldown {
    margin-top: 12px;
    padding-top: 12px;
    border-top: 1px solid var(--line);
  }
  .drilldown h4 {
    margin: 0 0 10px;
    font-size: 0.95rem;
  }
  pre {
    white-space: pre-wrap;
    word-break: break-word;
    font-size: 0.84rem;
    background: #fbf7ef;
    border: 1px solid var(--line);
    border-radius: 14px;
    padding: 12px;
    margin: 0;
  }
  @media (max-width: 720px) {
    .page { padding: 20px 12px 44px; }
    .hero { padding: 22px 18px; }
    .hero h1 { font-size: 1.8rem; }
    .card.wide { grid-column: auto; }
  }
  @keyframes fade-in {
    from { opacity: 0; transform: translateY(2px); }
    to { opacity: 1; transform: translateY(0); }
  }
</style>
"""


def _slugify(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")


def _display_name(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").title()


def _render_df(
    df: pd.DataFrame, *, max_rows: int = _MAX_PREVIEW_ROWS, full_preview: bool = False
) -> str:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return "<p class='empty'>No data available.</p>"

    working = df.copy()
    if isinstance(working.columns, pd.MultiIndex):
        working.columns = [
            "__".join(str(part) for part in column if str(part)).strip("_")
            for column in working.columns
        ]

    preview = working if full_preview else working.head(max_rows)
    table_html = preview.to_html(index=False, escape=False, border=0)
    wrapped_table = f"<div class='table-wrap'>{table_html}</div>"
    if full_preview or len(working) <= max_rows:
        return wrapped_table
    return (
        f"{wrapped_table}<p class='subtle'>Showing {len(preview):,} of {len(working):,} rows.</p>"
    )


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
    for name, path_str in _flatten_plot_paths(plot_paths):
        path = Path(path_str)
        if not path.exists():
            continue
        encoded = base64.b64encode(path.read_bytes()).decode("utf-8")
        cards.append(
            "<div class='card plot-card'>"
            f"<h3>{html.escape(_display_name(name))}</h3>"
            f"<img src='data:image/png;base64,{encoded}' alt='{html.escape(name)}'>"
            "</div>"
        )
    if not cards:
        return "<p class='empty'>No plots were generated for this run.</p>"
    return "<div class='plot-grid'>" + "".join(cards) + "</div>"


def _render_section(title: str, body: str, *, open_by_default: bool = False) -> str:
    open_attr = " open" if open_by_default else ""
    return (
        f"<details class='section'{open_attr} id='{_slugify(title)}'>"
        f"<summary>{html.escape(title)}</summary>"
        f"{body}</details>"
    )


def _assemble_page(
    *,
    module_name: str,
    run_id: str,
    banner_html: str,
    toc_items: list[tuple[str, str]],
    sections: list[str],
) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    toc_html = ""
    if toc_items:
        toc_links = "".join(
            f"<a href='#{_slugify(anchor)}'>{html.escape(label)}</a>" for anchor, label in toc_items
        )
        toc_html = f"<div class='toc'><strong>Sections:</strong> {toc_links}</div>"

    body = "".join(sections) or "<p class='empty'>No report data was produced for this run.</p>"
    return (
        "<html><head>"
        f"<title>{html.escape(module_name)} Dashboard - {html.escape(run_id)}</title>"
        f"{_DASHBOARD_CSS}</head><body><div class='page'>"
        "<div class='hero'>"
        "<div class='hero-kicker'>Analyst Toolkit Export</div>"
        f"<h1>{html.escape(module_name)} Dashboard</h1>"
        "<div class='hero-meta'>"
        f"<span><strong>Run ID:</strong> {html.escape(run_id)}</span>"
        f"<span><strong>Generated:</strong> {generated_at}</span>"
        "</div></div>"
        f"{banner_html}{toc_html}{body}</div></body></html>"
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
        ),
        _render_section(
            "Preview Of Duplicated Rows",
            f"<div class='card'><h3>Duplicated Rows</h3>{_render_df(duplicated_rows_df, max_rows=5)}</div>",
        ),
        _render_section(
            "First Rows Preview",
            f"<div class='card'><h3>Head</h3>{_render_df(sample_head_df, max_rows=5)}</div>",
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
        sections.append(_render_section("Plots", _render_plot_grid(plot_paths)))
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
        rows.append(
            {
                "Validation Rule": _display_name(name),
                "Description": check.get("rule_description", ""),
                "Status": "Pass" if check.get("passed") else f"Fail ({issue_count} issues)",
            }
        )
    return pd.DataFrame(rows)


def _render_validation_drilldowns(results: dict[str, Any]) -> str:
    checks = {k: v for k, v in results.items() if isinstance(v, dict) and "passed" in v}
    blocks = []
    for name, check in checks.items():
        if check.get("passed"):
            continue

        details = check.get("details", {})
        title = f"Failure Detail: {_display_name(name)}"
        parts = []

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
            parts.append(_render_df(df, full_preview=True))
        elif name == "dtype_enforcement":
            df = pd.DataFrame.from_dict(details, orient="index")
            df.index.name = "Column"
            parts.append(_render_df(df.reset_index(), full_preview=True))
        elif name == "categorical_values":
            for column, violation_info in details.items():
                parts.append(
                    "<div class='drilldown'>"
                    f"<h4>{html.escape(column)}</h4>"
                    f"<p class='subtle'><strong>Allowed values:</strong> {html.escape(str(violation_info.get('allowed_values', [])))}</p>"
                    f"{_render_df(violation_info.get('invalid_value_summary', pd.DataFrame()), full_preview=True)}"
                    "</div>"
                )
        elif name == "numeric_ranges":
            for column, violation_info in details.items():
                parts.append(
                    "<div class='drilldown'>"
                    f"<h4>{html.escape(column)}</h4>"
                    f"<p class='subtle'><strong>Allowed range:</strong> {html.escape(str(violation_info.get('enforced_range', '')))}</p>"
                    f"{_render_df(violation_info.get('violating_rows', pd.DataFrame()), max_rows=5)}"
                    "</div>"
                )
        else:
            parts.append(f"<pre>{html.escape(str(details))}</pre>")

        blocks.append(_render_section(title, "<div class='stack'>" + "".join(parts) + "</div>"))

    return "".join(blocks) or "<p class='empty'>No failures were recorded.</p>"


def _render_validation_dashboard(results: dict[str, Any], run_id: str) -> str:
    checks = {k: v for k, v in results.items() if isinstance(v, dict) and "passed" in v}
    total_checks = len(checks)
    passed_checks = sum(1 for check in checks.values() if check.get("passed"))
    coverage_pct = results.get("summary", {}).get("row_coverage_percent", "N/A")
    banner_class = "ok" if passed_checks == total_checks else "warn"
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> M02 Data Validation</div>"
        f"<div class='banner-item'><strong>Checks Passed:</strong> {passed_checks}/{total_checks}</div>"
        f"<div class='banner-item'><strong>Row Coverage:</strong> {coverage_pct}%</div>"
        "</div>"
    )

    summary_df = _build_validation_summary_df(results)
    sections = [
        _render_section(
            "Validation Rules Summary",
            (
                "<div class='card'>"
                f"{_render_df(summary_df, full_preview=True)}"
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


def _render_final_audit_dashboard(report: dict[str, Any], run_id: str) -> str:
    summary_df = report.get("Pipeline_Summary", pd.DataFrame())
    status_row = (
        summary_df[summary_df["Metric"] == "Final Pipeline Status"]
        if not summary_df.empty
        else pd.DataFrame()
    )
    status = str(status_row["Value"].iloc[0]) if not status_row.empty else "STATUS UNKNOWN"
    ok = "CERTIFIED" in status and "❌" not in status
    banner_class = "ok" if ok else "warn"
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> M10 Final Audit</div>"
        f"<div class='banner-item'><strong>Status:</strong> {html.escape(status)}</div>"
        "</div>"
    )

    lifecycle_df = report.get("Data_Lifecycle", pd.DataFrame())
    edits_df = report.get("Final_Edits_Log", pd.DataFrame())
    profile_df = report.get("Final_Data_Profile", pd.DataFrame())
    stats_df = report.get("Final_Descriptive_Stats", pd.DataFrame())
    preview_df = report.get("Final_Data_Preview", pd.DataFrame())

    sections = []
    toc = []
    if not ok:
        sections.append(
            _render_section(
                "Failure Details", _render_final_audit_failures(report), open_by_default=True
            )
        )
        toc.append(("Failure Details", "Failure Details"))

    sections.extend(
        [
            _render_section(
                "Pipeline Summary",
                (
                    "<div class='section-grid'>"
                    f"<div class='card'><h3>Pipeline Status</h3>{_render_df(summary_df, full_preview=True)}</div>"
                    f"<div class='card'><h3>Final Edits Log</h3>{_render_df(edits_df, full_preview=True)}</div>"
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
            ("Pipeline Summary", "Pipeline Summary"),
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
    return _render_generic_dashboard(report_tables, module_name, run_id, plot_paths)
