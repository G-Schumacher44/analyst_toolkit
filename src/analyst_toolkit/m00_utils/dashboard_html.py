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
    --muted: #52606d;
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
    background: #fbe4df;
    border-color: rgba(153, 27, 27, 0.22);
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
    padding: 16px 18px;
    margin-bottom: 18px;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px 14px;
  }
  .toc strong { color: #334155; }
  .toc a {
    color: var(--accent);
    text-decoration: none;
    font-weight: 600;
    line-height: 1.3;
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
    font-size: 1.12rem;
    color: #22303a;
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
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .card.wide {
    grid-column: span 2;
  }
  .card h3 {
    margin: 0 0 10px;
    font-size: 1rem;
    color: #22303a;
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
    font-size: 0.92rem;
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
  td {
    color: #334155;
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
    overflow-y: auto;
    max-height: min(360px, 68vh);
    border-radius: 12px;
    border: 1px solid #e7dfd1;
    background: #fff;
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
  .plot-intro {
    margin: 0 0 14px;
    color: var(--muted);
    font-size: 0.95rem;
  }
  .plot-card img {
    width: 100%;
    height: auto;
    display: block;
    border-radius: 14px;
    border: 1px solid var(--line);
    background: #fff;
    cursor: zoom-in;
  }
  .plot-card h3 {
    margin: 0 0 10px;
  }
  .plot-trigger {
    appearance: none;
    border: 0;
    padding: 0;
    margin: 0;
    background: transparent;
    width: 100%;
    text-align: left;
    cursor: zoom-in;
  }
  .plot-trigger:focus-visible {
    outline: 3px solid rgba(31, 75, 74, 0.35);
    outline-offset: 6px;
    border-radius: 16px;
  }
  .plot-caption {
    margin: 10px 0 0;
    color: var(--muted);
    font-size: 0.88rem;
  }
  .plot-modal {
    border: 0;
    padding: 0;
    width: min(92vw, 1280px);
    max-height: 92vh;
    background: transparent;
  }
  .plot-modal::backdrop {
    background: rgba(17, 24, 39, 0.74);
    backdrop-filter: blur(4px);
  }
  .plot-modal-card {
    background: var(--paper);
    border: 1px solid rgba(255, 255, 255, 0.22);
    border-radius: 24px;
    overflow: hidden;
    box-shadow: 0 22px 48px rgba(15, 23, 42, 0.28);
  }
  .plot-modal-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding: 18px 20px 0;
  }
  .plot-modal-header h3 {
    margin: 0;
    font-size: 1.08rem;
    color: #22303a;
  }
  .plot-modal-close {
    appearance: none;
    border: 1px solid var(--line);
    background: #fff;
    color: #22303a;
    border-radius: 999px;
    width: 36px;
    height: 36px;
    font-size: 1.1rem;
    line-height: 1;
    cursor: pointer;
  }
  .plot-modal-body {
    padding: 14px 20px 20px;
    overflow: auto;
    max-height: calc(92vh - 72px);
  }
  .plot-modal-body img {
    width: 100%;
    height: auto;
    display: block;
    border-radius: 18px;
    border: 1px solid var(--line);
    background: #fff;
  }
  .badge-ok {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 2px 8px;
    border-radius: 999px;
    background: var(--ok-soft);
    color: var(--ok);
    font-weight: 700;
    font-size: 0.82rem;
  }
  .badge-warn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 2px 8px;
    border-radius: 999px;
    background: var(--warn-soft);
    color: var(--warn);
    font-weight: 700;
    font-size: 0.82rem;
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
  .failure-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
  }
  .metric-stat {
    font-size: clamp(1.15rem, 2.1vw, 2rem);
    line-height: 1.08;
    font-weight: 700;
    color: #22303a;
    margin: 4px 0 0;
    overflow-wrap: anywhere;
    word-break: break-word;
    text-wrap: balance;
  }
  .metric-stat.compact {
    font-size: clamp(1rem, 1.65vw, 1.42rem);
    line-height: 1.15;
  }
  .pill-list {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }
  .pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 6px 10px;
    border-radius: 999px;
    background: #f3ede2;
    color: #334155;
    font-size: 0.88rem;
    font-weight: 600;
  }
  .pill.warn {
    background: #fbe4df;
    color: #9f1239;
  }
  .status-pill {
    display: inline-flex;
    align-items: center;
    padding: 4px 10px;
    border-radius: 999px;
    font-size: 0.84rem;
    font-weight: 700;
    line-height: 1.2;
  }
  .status-pill.pass {
    background: var(--ok-soft);
    color: var(--ok);
  }
  .status-pill.fail {
    background: #fbe4df;
    color: #9f1239;
  }
  .cert-hero {
    border-radius: 28px;
    padding: 24px 26px;
    margin-bottom: 22px;
    color: #f8fafc;
    box-shadow: var(--shadow);
  }
  .cert-hero.pass {
    background: linear-gradient(135deg, #123c2b, #1f6f54);
  }
  .cert-hero.fail {
    background: linear-gradient(135deg, #5f1726, #9f1239);
  }
  .cert-kicker {
    text-transform: uppercase;
    letter-spacing: 0.14em;
    font-size: 0.74rem;
    opacity: 0.82;
    margin-bottom: 10px;
  }
  .cert-title {
    margin: 0 0 10px;
    font-size: 2rem;
    line-height: 1.08;
  }
  .cert-copy {
    margin: 0;
    max-width: 760px;
    color: rgba(248, 250, 252, 0.92);
    font-size: 0.98rem;
    line-height: 1.55;
  }
  .cert-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 16px;
    margin-bottom: 18px;
  }
  .cert-stat-card {
    background: #fcfaf5;
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: 16px;
    box-shadow: var(--shadow);
  }
  .cert-stat-card h3 {
    margin: 0 0 6px;
    font-size: 0.96rem;
    color: #22303a;
  }
  .cert-stat-card .metric-stat {
    margin-bottom: 6px;
  }
  .cert-ledger {
    display: grid;
    grid-template-columns: 1.2fr 0.8fr;
    gap: 16px;
  }
  @media (max-width: 920px) {
    .cert-ledger {
      grid-template-columns: 1fr;
    }
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

_DASHBOARD_SCRIPT = """
<script>
  window.atkDashboard = {
    openPlot(button) {
      const modal = document.getElementById("plot-modal");
      const image = document.getElementById("plot-modal-image");
      const title = document.getElementById("plot-modal-title");
      if (!modal || !image || !title) return;
      image.src = button.dataset.plotSrc || "";
      image.alt = button.dataset.plotTitle || "Expanded plot";
      title.textContent = button.dataset.plotTitle || "Plot";
      if (typeof modal.showModal === "function") {
        modal.showModal();
      }
    },
    closePlot() {
      const modal = document.getElementById("plot-modal");
      const image = document.getElementById("plot-modal-image");
      if (!modal) return;
      modal.close();
      if (image) image.src = "";
    }
  };

  document.addEventListener("click", (event) => {
    const modal = document.getElementById("plot-modal");
    if (!modal || event.target !== modal) return;
    window.atkDashboard.closePlot();
  });
</script>
"""


def _slugify(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")


def _display_name(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").title()


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
    df: pd.DataFrame, *, max_rows: int = _MAX_PREVIEW_ROWS, full_preview: bool = False
) -> str:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return "<p class='empty'>No data available.</p>"

    working = _normalize_df_for_display(df)
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
        image_src = f"data:image/png;base64,{encoded}"
        escaped_title = html.escape(_display_name(name))
        escaped_name = html.escape(name)
        cards.append(
            "<div class='card plot-card'>"
            f"<h3>{escaped_title}</h3>"
            f"<button class='plot-trigger' type='button' onclick='window.atkDashboard.openPlot(this)' data-plot-src='{image_src}' data-plot-title='{escaped_title}'>"
            f"<img src='{image_src}' alt='{escaped_name}'>"
            "</button>"
            "<p class='plot-caption'>Click to expand</p>"
            "</div>"
        )
    if not cards:
        return "<p class='empty'>No plots were generated for this run.</p>"
    return (
        "<p class='plot-intro'>The standalone export keeps the visuals in the same file so the report travels without sidecar assets.</p>"
        "<div class='plot-grid'>" + "".join(cards) + "</div>"
    )


def _metric_value(value: Any) -> str:
    rendered = html.escape(str(value))
    compact_class = " compact" if len(str(value)) > 18 else ""
    return f"<p class='metric-stat{compact_class}'>{rendered}</p>"


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
        f"{_DASHBOARD_CSS}{_DASHBOARD_SCRIPT}</head><body><div class='page'>"
        "<div class='hero'>"
        "<div class='hero-kicker'>Analyst Toolkit Export</div>"
        f"<h1>{html.escape(module_name)} Dashboard</h1>"
        "<div class='hero-meta'>"
        f"<span><strong>Run ID:</strong> {html.escape(run_id)}</span>"
        f"<span><strong>Generated:</strong> {generated_at}</span>"
        "</div></div>"
        f"{banner_html}{toc_html}{body}"
        "<dialog class='plot-modal' id='plot-modal'>"
        "<div class='plot-modal-card'>"
        "<div class='plot-modal-header'>"
        "<h3 id='plot-modal-title'>Plot</h3>"
        "<button class='plot-modal-close' type='button' onclick='window.atkDashboard.closePlot()' aria-label='Close expanded plot'>&times;</button>"
        "</div>"
        "<div class='plot-modal-body'>"
        "<img id='plot-modal-image' src='' alt='Expanded plot'>"
        "</div></div></dialog>"
        "</div></body></html>"
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
                "<div class='cert-stat-card'>"
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
                "<div class='cert-stat-card'>"
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
    row_delta = initial_rows - final_rows if initial_rows and final_rows else 0
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
                "<div class='cert-stat-card'>"
                "<h3>Certificate Status</h3>"
                f"{_metric_value('Pass' if ok else 'Fail')}"
                f"<p class='subtle'>{html.escape(status)}</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Certification Rules</h3>"
                f"{_metric_value(_safe_summary_flag(summary_df, 'Certification Rules Passed'))}"
                "<p class='subtle'>Result of the strict validation contract.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
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


def _safe_metric_value(summary_df: pd.DataFrame, metric_name: str) -> int:
    try:
        series = summary_df.loc[summary_df["Metric"] == metric_name, "Value"]
        return int(series.iloc[0]) if not series.empty else 0
    except Exception:
        return 0


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
    return _render_generic_dashboard(report_tables, module_name, run_id, plot_paths)
