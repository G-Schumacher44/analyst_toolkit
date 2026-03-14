"""Diagnostics dashboard renderer."""

from __future__ import annotations

from typing import Any

import pandas as pd

from analyst_toolkit.m00_utils.dashboard_core import _assemble_page
from analyst_toolkit.m00_utils.dashboard_plots import render_plot_grid
from analyst_toolkit.m00_utils.dashboard_shared import _render_section
from analyst_toolkit.m00_utils.dashboard_tables import _render_df


def render_diagnostics_dashboard(
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
            _render_section("Plots", render_plot_grid(plot_paths), open_by_default=True)
        )
        toc.append(("Plots", "Plots"))

    return _assemble_page(
        module_name="Diagnostics",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )
