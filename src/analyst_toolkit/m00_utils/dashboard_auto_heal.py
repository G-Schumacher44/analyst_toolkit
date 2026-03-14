"""Auto-heal dashboard renderer."""

from __future__ import annotations

import html
from typing import Any

import pandas as pd

from analyst_toolkit.m00_utils.dashboard_core import _assemble_page
from analyst_toolkit.m00_utils.dashboard_shared import (
    _metric_value,
    _normalize_reference_text,
    _render_reference_value,
    _render_section,
    _status_tone_class,
)
from analyst_toolkit.m00_utils.dashboard_tables import (
    _render_auto_heal_summary_table,
    _render_df,
)


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


def render_auto_heal_dashboard(report: dict[str, Any], run_id: str) -> str:
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
        _render_section("Step Outcomes", _render_auto_heal_step_cards(steps), open_by_default=True),
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
