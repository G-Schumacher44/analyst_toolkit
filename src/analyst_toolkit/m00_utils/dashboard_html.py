"""Standalone dashboard HTML renderer facade for module exports."""

from __future__ import annotations

from typing import Any

import pandas as pd

from analyst_toolkit.m00_utils.dashboard_auto_heal import render_auto_heal_dashboard
from analyst_toolkit.m00_utils.dashboard_certification import (
    render_final_audit_dashboard,
    render_validation_dashboard,
)
from analyst_toolkit.m00_utils.dashboard_core import _assemble_page
from analyst_toolkit.m00_utils.dashboard_data_prep import (
    render_duplicates_dashboard,
    render_imputation_dashboard,
    render_normalization_dashboard,
    render_outlier_detection_dashboard,
    render_outlier_handling_dashboard,
)
from analyst_toolkit.m00_utils.dashboard_diagnostics import (
    render_diagnostics_dashboard,
)
from analyst_toolkit.m00_utils.dashboard_dictionary import (
    render_data_dictionary_dashboard,
)
from analyst_toolkit.m00_utils.dashboard_plots import render_plot_grid
from analyst_toolkit.m00_utils.dashboard_shared import _display_name, _render_section
from analyst_toolkit.m00_utils.dashboard_tables import _render_df
from analyst_toolkit.m00_utils.dashboard_views import (
    render_cockpit_dashboard,
    render_pipeline_dashboard,
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
                    body += f"<div><h3>{_display_name(sub_key)}</h3>{_render_df(sub_value)}</div>"
        else:
            body += "<p class='empty'>No data available.</p>"
        body += "</div>"
        sections.append(_render_section(_display_name(section_name), body, open_by_default=True))
        toc.append((section_name, _display_name(section_name)))

    if plot_paths:
        sections.append(
            _render_section("Plots", render_plot_grid(plot_paths), open_by_default=True)
        )
        toc.append(("plots", "Plots"))

    return _assemble_page(
        module_name=module_name,
        run_id=run_id,
        banner_html="",
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
    renderers_with_plots = {
        "diagnostics": render_diagnostics_dashboard,
        "duplicates": render_duplicates_dashboard,
        "outlier detection": render_outlier_detection_dashboard,
        "imputation": render_imputation_dashboard,
    }
    renderers_without_plots = {
        "validation": render_validation_dashboard,
        "final audit": render_final_audit_dashboard,
        "normalization": render_normalization_dashboard,
        "outlier handling": render_outlier_handling_dashboard,
        "auto heal": render_auto_heal_dashboard,
        "data dictionary": render_data_dictionary_dashboard,
        "cockpit dashboard": render_cockpit_dashboard,
        "pipeline dashboard": render_pipeline_dashboard,
    }
    if normalized in renderers_with_plots:
        return renderers_with_plots[normalized](report_tables, run_id, plot_paths)
    if normalized in renderers_without_plots:
        return renderers_without_plots[normalized](report_tables, run_id)
    return _render_generic_dashboard(report_tables, module_name, run_id, plot_paths)


# Backward-compatible alias for older report/export call sites.
generate_html_report = generate_dashboard_html
