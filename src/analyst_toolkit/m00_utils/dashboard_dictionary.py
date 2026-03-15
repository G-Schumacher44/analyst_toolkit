"""Data dictionary dashboard renderer."""

from __future__ import annotations

from typing import Any

import pandas as pd

from analyst_toolkit.m00_utils.dashboard_core import _assemble_page
from analyst_toolkit.m00_utils.dashboard_shared import _metric_value, _render_section
from analyst_toolkit.m00_utils.dashboard_tables import _render_df

STATUS_TO_CLASS = {"pass": "ok", "warn": "warn", "fail": "fail"}


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
    banner_class = STATUS_TO_CLASS.get(status, "fail")
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> Data Dictionary</div>"
        f"<div class='banner-item'><strong>Columns:</strong> {int(overview_row.get('Columns', 0) or 0)}</div>"
        f"<div class='banner-item'><strong>Expected Columns:</strong> {int(overview_row.get('Expected Columns', 0) or 0)}</div>"
        f"<div class='banner-item'><strong>Metadata Gaps:</strong> {int(overview_row.get('Metadata Gaps', 0) or 0)}</div>"
        f"<div class='banner-item'><strong>Profile Depth:</strong> {overview_row.get('Profile Depth', '')}</div>"
        "</div>"
    )
    readiness_count = len(readiness_df) if isinstance(readiness_df, pd.DataFrame) else 0
    readiness_explainer = (
        "This is a generated prelaunch gap ledger built from the observed dataset profile plus "
        "whatever infer_configs could turn into validation, dtype, duplicate, outlier, and "
        "imputation hints. It is not hand-authored business metadata yet."
    )
    if (
        isinstance(readiness_df, pd.DataFrame)
        and not readiness_df.empty
        and "Type" in readiness_df.columns
    ):
        contract_gap_types = {"missing_expected_column", "unexpected_column", "no_expected_schema"}
        inference_warning_types = {"infer_parse_warning", "no_validation_contract"}
        contract_gaps_df = readiness_df[readiness_df["Type"].isin(contract_gap_types)].reset_index(
            drop=True
        )
        inference_warnings_df = readiness_df[
            readiness_df["Type"].isin(inference_warning_types)
        ].reset_index(drop=True)
        metadata_needed_df = readiness_df[
            ~readiness_df["Type"].isin(contract_gap_types | inference_warning_types)
        ].reset_index(drop=True)
    else:
        contract_gaps_df = readiness_df
        inference_warnings_df = pd.DataFrame()
        metadata_needed_df = pd.DataFrame()

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
                "<div class='card wide'>"
                "<h3>Expected Schema</h3>"
                "<p class='subtle'>This is the best current contract surface. When infer_configs is available it shows inferred expectations; otherwise it falls back to an observed baseline so the section never collapses into an empty tile.</p>"
                f"{_render_df(expected_df, full_preview=True, wide_layout=True)}"
                "</div>"
                "<div class='card wide'>"
                "<h3>Profile Snapshot</h3>"
                "<p class='subtle'>Observed profile facts from the current dataset snapshot. This sits below the expected contract so the comparison reads top to bottom instead of fighting for horizontal space.</p>"
                f"{_render_df(profile_df, full_preview=True, wide_layout=True)}"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Column Dictionary",
            (
                "<div class='card wide'>"
                "<h3>Per-Column Definitions</h3>"
                "<p class='subtle'>This table combines observed profile data with inferred config hints so operators can review field expectations before heavier pipeline work. It is intentionally wide because the useful version of a dictionary is not a narrow summary.</p>"
                f"{_render_df(dictionary_df, full_preview=True, wide_layout=True)}"
                "</div>"
            ),
            open_by_default=True,
        ),
        _render_section(
            "Prelaunch Readiness",
            (
                "<div class='card wide'>"
                "<h3>Prelaunch Readiness Map</h3>"
                f"<p class='subtle'>{readiness_explainer}</p>"
                f"<p class='subtle'><strong>Current gaps:</strong> {readiness_count}</p>"
                "<div class='section-grid'>"
                "<div class='card wide'>"
                "<h3>What The Toolkit Still Can&apos;t Confirm</h3>"
                "<p class='subtle'>These items mean the current dataset and the best available inferred contract do not line up cleanly yet. Review them before treating this as an execution-ready prelaunch surface.</p>"
                f"{_render_df(contract_gaps_df, full_preview=True)}"
                "</div>"
                "<div class='card wide'>"
                "<h3>Where The Inference Is Still Thin</h3>"
                "<p class='subtle'>These items explain where infer_configs or config parsing produced only partial guidance, so the dictionary is still leaning on observed profile evidence.</p>"
                f"{_render_df(inference_warnings_df, full_preview=True)}"
                "</div>"
                "<div class='card wide'>"
                "<h3>What Still Needs Human Input</h3>"
                "<p class='subtle'>These are the places where the system has structural evidence, but a human still needs to provide business meaning, policy context, or stronger authored metadata.</p>"
                f"{_render_df(metadata_needed_df, full_preview=True)}"
                "</div>"
                "</div>"
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
