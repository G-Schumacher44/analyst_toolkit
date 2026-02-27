"""HTML report rendering helpers."""

import base64
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

_HTML_CSS = """
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f8f9fa; color: #333; }
  .page-wrap { max-width: 1100px; margin: 2em auto; padding: 0 1.5em 3em; }
  h1 { color: #111; font-size: 1.6em; margin-bottom: 0.2em; }
  .meta { color: #888; font-size: 0.82em; margin-bottom: 2em; border-bottom: 2px solid #e0e0e0; padding-bottom: 1em; }
  h2 { color: #1a1a2e; font-size: 1.05em; font-weight: 600; margin: 0 0 0.6em;
       border-left: 4px solid #4a7fcb; padding-left: 0.6em; }
  h3 { color: #444; font-size: 0.92em; margin: 1em 0 0.3em; }
  .section { background: #fff; border: 1px solid #e8e8e8; border-radius: 6px;
             padding: 1em 1.2em; margin-bottom: 1em; box-shadow: 0 1px 3px rgba(0,0,0,0.04); overflow-x: auto; }
  table { border-collapse: collapse; width: 100%; font-size: 0.84em; }
  th, td { border: 1px solid #e0e0e0; padding: 5px 9px; text-align: left; white-space: nowrap; }
  th { background: #f0f4ff; font-weight: 600; color: #1a1a2e; }
  tr:nth-child(even) td { background: #fafbff; }
  .truncated { color: #999; font-size: 0.78em; margin-top: 0.5em; font-style: italic; }
  .plot-container { margin: 0.5em 0; }
  img { max-width: 100%; height: auto; display: block; border-radius: 4px; }
  p.empty { color: #bbb; font-style: italic; margin: 0.3em 0; font-size: 0.88em; }
  .toc { background: #fff; border: 1px solid #e8e8e8; border-radius: 6px;
         padding: 0.8em 1.2em; margin-bottom: 1.5em; font-size: 0.86em; }
  .toc a { color: #4a7fcb; text-decoration: none; margin-right: 1em; }
  .toc a:hover { text-decoration: underline; }
</style>
"""

_MAX_PREVIEW_ROWS = 50  # cap for large DataFrames in HTML output


def _render_df(df: pd.DataFrame) -> tuple[str, str]:
    """Render a DataFrame as an HTML table, capped at _MAX_PREVIEW_ROWS.
    Returns (table_html, truncation_notice_html)."""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = ["__".join(str(c) for c in col).strip("_") for col in df.columns]  # type: ignore
    total = len(df)
    preview = df.head(_MAX_PREVIEW_ROWS)
    table_html = preview.to_html(classes="", escape=False, index=False, border=0)
    notice = ""
    if total > _MAX_PREVIEW_ROWS:
        notice = f"<p class='truncated'>Showing {_MAX_PREVIEW_ROWS} of {total:,} rows.</p>"
    return table_html, notice


def generate_html_report(
    report_tables: dict,
    module_name: str,
    run_id: str,
    plot_paths: dict | None = None,
) -> str:
    """Build a single-page self-contained HTML report from a dict of DataFrames."""
    title = f"{module_name} Report"
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Collect section keys for TOC (skip empty/non-renderable)
    renderable = [
        k
        for k, v in report_tables.items()
        if (isinstance(v, pd.DataFrame) and not v.empty)
        or (
            isinstance(v, dict)
            and any(isinstance(sv, pd.DataFrame) and not sv.empty for sv in v.values())
        )
    ]
    if plot_paths:
        renderable.append("plots")

    toc_links = "".join(f"<a href='#{k}'>{k.replace('_', ' ').title()}</a>" for k in renderable)

    html_parts = [
        "<html><head>",
        f"<title>{title} — {run_id}</title>",
        _HTML_CSS,
        "</head><body><div class='page-wrap'>",
        f"<h1>{title}</h1>",
        f"<div class='meta'>Run ID: <strong>{run_id}</strong> &nbsp;|&nbsp; Generated: {generated_at}</div>",
    ]

    if not report_tables:
        html_parts.append(
            "<div class='section'><p class='empty'>No report data was produced for this run.</p></div>"
        )
        html_parts.append("</div></body></html>")
        return "\n".join(html_parts)

    if toc_links:
        html_parts.append(f"<div class='toc'><strong>Sections:</strong> {toc_links}</div>")

    for section_name, value in report_tables.items():
        anchor = section_name
        heading = section_name.replace("_", " ").title()
        html_parts.append(f"<div class='section' id='{anchor}'>")
        html_parts.append(f"<h2>{heading}</h2>")

        if not isinstance(value, pd.DataFrame):
            if isinstance(value, dict):
                for sub_key, sub_df in value.items():
                    if isinstance(sub_df, pd.DataFrame) and not sub_df.empty:
                        sub_heading = sub_key.replace("_", " ").title()
                        html_parts.append(f"<h3>{sub_heading}</h3>")
                        table_html, notice = _render_df(sub_df)
                        html_parts.append(table_html)
                        if notice:
                            html_parts.append(notice)
            else:
                html_parts.append("<p class='empty'>No data available.</p>")
            html_parts.append("</div>")
            continue

        if value.empty:
            html_parts.append("<p class='empty'>No data available.</p>")
        else:
            table_html, notice = _render_df(value)
            html_parts.append(table_html)
            if notice:
                html_parts.append(notice)

        html_parts.append("</div>")

    # Plots section
    if plot_paths:
        html_parts.append("<div class='section' id='plots'><h2>Plots</h2>")
        for plot_name, path_str in plot_paths.items():
            if not path_str:
                continue
            path = Path(path_str)
            if path.exists():
                with open(path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode("utf-8")
                label = plot_name.replace("_", " ").title()
                html_parts.append(
                    f"<div class='plot-container'><h3>{label}</h3>"
                    f"<img src='data:image/png;base64,{encoded}'></div>"
                )
        html_parts.append("</div>")

    html_parts.append("</div></body></html>")
    return "\n".join(html_parts)
