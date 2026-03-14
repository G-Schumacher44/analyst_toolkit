"""Data dictionary dashboard renderer."""

from __future__ import annotations

from typing import Any

import pandas as pd

from analyst_toolkit.m00_utils.dashboard_core import _assemble_page
from analyst_toolkit.m00_utils.dashboard_shared import _metric_value, _render_section
from analyst_toolkit.m00_utils.dashboard_tables import _render_df


def render_data_dictionary_dashboard(report: dict[str, Any], run_id: str) -> str:
    overview_df = report.get("overview", pd.DataFrame())
    expected_df = report.get("expected_schema", pd.DataFrame())
    dictionary_df = report.get("column_dictionary", pd.DataFrame())
    readiness_df = report.get("prelaunch_readiness", pd.DataFrame())
    profile_df = report.get("profile_snapshot", pd.DataFrame())
    meta = report.get("__dashboard_meta__", {})

    overview_row = (
        overview_df.iloc[0]
        if isinstance(overview_df, pd.DataFrame) and not overview_df.empty
        else {}
    )
    status = str(meta.get("status", "warn")).lower()
    banner_class = "ok" if status == "pass" else "warn" if status == "warn" else "fail"
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> Data Dictionary</div>"
        f"<div class='banner-item'><strong>Columns:</strong> {int(overview_row.get('Columns', 0) or 0)}</div>"
        f"<div class='banner-item'><strong>Expected Columns:</strong> {int(overview_row.get('Expected Columns', 0) or 0)}</div>"
        f"<div class='banner-item'><strong>Metadata Gaps:</strong> {int(overview_row.get('Metadata Gaps', 0) or 0)}</div>"
        f"<div class='banner-item'><strong>Profile Depth:</strong> {overview_row.get('Profile Depth', '')}</div>"
        "</div>"
    )

    sections = [
        _render_section(
            "Dictionary Overview",
            (
                "<div class='cert-grid'>"
                "<div class='cert-stat-card'>"
                "<h3>Rows</h3>"
                f"{_metric_value(int(overview_row.get('Rows', 0) or 0))}"
                "<p class='subtle'>Observed rows used to seed the dictionary snapshot.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Observed Columns</h3>"
                f"{_metric_value(int(overview_row.get('Columns', 0) or 0))}"
                "<p class='subtle'>Columns present in the current dataset.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Expected Columns</h3>"
                f"{_metric_value(int(overview_row.get('Expected Columns', 0) or 0))}"
                "<p class='subtle'>Columns inferred from the validation contract.</p>"
                "</div>"
                "<div class='cert-stat-card'>"
                "<h3>Readiness Gaps</h3>"
                f"{_metric_value(int(overview_row.get('Metadata Gaps', 0) or 0))}"
                "<p class='subtle'>Items that still need human review before downstream execution.</p>"
                "</div>"
                "</div>"
                f"<div class='card'><h3>Overview Ledger</h3>{_render_df(overview_df, full_preview=True)}</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Expected Schema And Contract",
            (
                "<div class='section-grid'>"
                f"<div class='card wide'><h3>Expected Schema</h3>{_render_df(expected_df, full_preview=True)}</div>"
                f"<div class='card'><h3>Profile Snapshot</h3>{_render_df(profile_df, full_preview=True)}</div>"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Column Dictionary",
            (
                "<div class='card wide'>"
                "<h3>Per-Column Definitions</h3>"
                "<p class='subtle'>This table combines observed profile data with inferred config hints so operators can review field expectations before heavier pipeline work.</p>"
                f"{_render_df(dictionary_df, full_preview=True)}"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Prelaunch Readiness",
            (
                "<div class='card'>"
                "<h3>Gaps And Open Questions</h3>"
                f"{_render_df(readiness_df, full_preview=True)}"
                "</div>"
            ),
            open_by_default=True,
        ),
    ]
    toc = [
        ("Dictionary Overview", "Dictionary Overview"),
        ("Expected Schema And Contract", "Expected Schema And Contract"),
        ("Column Dictionary", "Column Dictionary"),
        ("Prelaunch Readiness", "Prelaunch Readiness"),
    ]
    return _assemble_page(
        module_name="Data Dictionary",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )
