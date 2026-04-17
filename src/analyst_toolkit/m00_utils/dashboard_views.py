"""Pipeline and cockpit dashboard renderers."""

from __future__ import annotations

import html
import posixpath
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from analyst_toolkit.m00_utils.dashboard_core import _assemble_page
from analyst_toolkit.m00_utils.dashboard_shared import (
    _embed_reference_src,
    _metric_value,
    _module_badge,
    _render_reference_value,
    _slugify,
    _status_chip,
    _status_tone_class,
    _tab_status_label,
)
from analyst_toolkit.m00_utils.dashboard_tables import (
    _render_auto_heal_summary_table,
    _render_df,
)

_TEMPLATES_GROUP_TITLE = "templates and contracts"
_COCKPIT_REPORT_DIR = "exports/reports/cockpit"


def _preferred_local_reference(
    destination_delivery: Any,
    destination_key: str,
) -> str:
    if not isinstance(destination_delivery, dict):
        return ""
    destination = destination_delivery.get(destination_key, {})
    if not isinstance(destination, dict):
        return ""
    local = destination.get("local", {})
    if not isinstance(local, dict):
        return ""
    return str(local.get("url", "") or local.get("path", "")).strip()


def _preferred_dashboard_reference(payload: dict[str, Any]) -> str:
    local_ref = _preferred_local_reference(payload.get("destination_delivery", {}), "html_report")
    if local_ref:
        return local_ref
    return str(payload.get("dashboard_url") or payload.get("dashboard_path") or "").strip()


def _preferred_export_reference(payload: dict[str, Any]) -> str:
    local_ref = _preferred_local_reference(payload.get("destination_delivery", {}), "data_export")
    if local_ref:
        return local_ref
    return str(payload.get("export_url") or payload.get("artifact_url") or "").strip()


def _render_resource_inline_item(item: dict[str, Any], *, show_detail: bool = True) -> str:
    detail_html = (
        f"<p class='subtle'>{html.escape(str(item.get('Detail', '')))}</p>" if show_detail else ""
    )
    return (
        "<div class='resource-inline-item'>"
        f"<p class='resource-meta'>{html.escape(str(item.get('Kind', 'resource')).replace('_', ' ').title())}</p>"
        f"<h4>{html.escape(str(item.get('Title', 'Untitled')))}</h4>"
        f"{detail_html}"
        "<p class='subtle'><strong>Open With</strong></p>"
        f"{_render_reference_value(item.get('Reference', ''), empty_label='No reference recorded.')}"
        "</div>"
    )


def _render_cockpit_artifact_reference(value: Any, *, empty_label: str) -> str:
    text = str(value or "").strip()
    if not text:
        return f"<p class='empty'>{html.escape(empty_label)}</p>"
    normalized = text
    parsed = urlparse(normalized)
    if parsed.scheme in {"http", "https"}:
        rendered = html.escape(normalized)
        return (
            "<p class='subtle'><a href='"
            f"{rendered}' target='_blank' rel='noopener noreferrer'>{rendered}</a></p>"
        )
    exports_index = normalized.rfind("exports/")
    if not parsed.scheme and exports_index >= 0:
        normalized = "/" + normalized[exports_index:]

    rendered = html.escape(normalized)
    if normalized.startswith("/exports/"):
        relative = posixpath.relpath(normalized.lstrip("/"), _COCKPIT_REPORT_DIR)
        href = html.escape(relative)
    else:
        return f"<p class='subtle'><code>{rendered}</code></p>"
    return (
        "<p class='subtle'><a href='"
        f"{href}' target='_blank' rel='noopener noreferrer'>{rendered}</a></p>"
    )


def _render_cockpit_overview(
    overview: dict[str, Any],
    operator_brief: dict[str, Any],
    best_surfaces: dict[str, Any],
    blockers: list[dict[str, Any]],
    recent_run_gaps: list[Any],
) -> str:
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
            f"{_render_cockpit_artifact_reference((payload or {}).get('reference', ''), empty_label='No artifact recorded.')}"
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
    return (
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


def _render_cockpit_recent_runs(recent_runs: list[dict[str, Any]]) -> str:
    recent_run_cards: list[str] = []
    for run in recent_runs:
        dashboard_ref = run.get("pipeline_dashboard") or run.get("best_dashboard")
        export_ref = run.get("best_export")
        health_note = html.escape(str(run.get("health_status", "unknown")).upper())
        if bool(run.get("health_advisory", False)):
            certification_status = html.escape(
                str(run.get("certification_status", "unknown")).upper()
            )
            health_note = f"ADVISORY · {health_note} · FINAL_AUDIT {certification_status}"
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
            f"<p class='subtle'>{health_note}</p></div>"
            "<div class='module-mini-card'><h3>Warnings</h3>"
            f"{_metric_value(run.get('warning_count', 0))}"
            f"<p class='subtle'>Modules observed: {html.escape(str(run.get('module_count', 0)))}</p></div>"
            "</div>"
            "<p class='subtle'><strong>Session:</strong> "
            f"{html.escape(str(run.get('session_id') or 'Unavailable'))}</p>"
            "<p class='subtle'><strong>Best Dashboard</strong></p>"
            f"{_render_cockpit_artifact_reference(dashboard_ref, empty_label='No dashboard recorded.')}"
            "<p class='subtle'><strong>Best Export</strong></p>"
            f"{_render_cockpit_artifact_reference(export_ref, empty_label='No export recorded.')}"
            "</div>"
        )
    return (
        "<div class='hub-stack'>" + "".join(recent_run_cards) + "</div>"
        if recent_run_cards
        else "<p class='empty'>No recent runs were discovered in local history.</p>"
    )


def _render_cockpit_resources(
    resources: list[dict[str, Any]], resource_groups: list[dict[str, Any]]
) -> str:
    grouped_resources: list[str] = []
    template_items = [item for item in resources if str(item.get("Kind", "")).lower() == "template"]
    reference_items = [
        item for item in resources if str(item.get("Kind", "")).lower() != "template"
    ]
    for group in resource_groups:
        group_items = group.get("items", [])
        if str(group.get("title", "")).lower() == _TEMPLATES_GROUP_TITLE:
            group_items = template_items
        items_html = []
        for item in group_items:
            items_html.append(_render_resource_inline_item(item, show_detail=True))
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
        _render_resource_inline_item(item, show_detail=False) for item in reference_items
    )
    return (
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


def _render_cockpit_launchpad(
    launchpad: list[dict[str, Any]], launch_sequences: list[dict[str, Any]]
) -> str:
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
    return (
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


def _render_cockpit_artifacts(
    artifacts: list[dict[str, Any]], artifact_server: dict[str, Any]
) -> str:
    artifact_df = pd.DataFrame(artifacts)
    if not artifact_df.empty:
        # Table cells need file-relative links so the standalone cockpit HTML works from disk.
        # The server info cards below are not artifact file targets, so normal reference rendering is fine.
        for column, empty_label in (
            ("Dashboard", "No dashboard recorded."),
            ("Export", "No export recorded."),
            ("Artifact Path", "No artifact path recorded."),
        ):
            if column in artifact_df.columns:
                artifact_df[column] = artifact_df[column].map(
                    lambda value, label=empty_label: _render_cockpit_artifact_reference(
                        value, empty_label=label
                    )
                )
    server_running = bool(artifact_server.get("running"))
    server_base_url = str(artifact_server.get("base_url", ""))
    server_root = str(artifact_server.get("root", ""))
    return (
        "<div class='readme-grid'>"
        "<div class='hub-grid'>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Artifact Server</p>"
        "<h3>Status</h3>"
        f"{_metric_value('Running' if server_running else 'Not Running')}"
        "<p class='subtle'>The local artifact server turns export paths into browsable localhost links.</p>"
        "</div>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Local URL Base</p>"
        "<h3>Base URL</h3>"
        f"{_render_reference_value(server_base_url, empty_label='No local base URL is active.')}"
        "</div>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Served Root</p>"
        "<h3>Artifact Root</h3>"
        f"{_render_reference_value(server_root, empty_label='No artifact root recorded.')}"
        "</div>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Recent Surfaces</p>"
        "<h3>Indexed Items</h3>"
        f"{_metric_value(len(artifacts))}"
        "<p class='subtle'>Recent dashboards and exports discovered from local run history.</p>"
        "</div>"
        "</div>"
        "<div class='readme-section'>"
        "<h3>Artifact Index</h3>"
        "<p class='subtle'>Use this page as the cockpit linkage shelf for recent dashboards, exports, and operator-facing review surfaces.</p>"
        f"{_render_df(artifact_df, full_preview=True, wide_layout=True, allow_html_cols={'Dashboard', 'Export', 'Artifact Path'})}"
        "</div>"
        "</div>"
    )


def _render_cockpit_dictionary(data_dictionary: dict[str, Any]) -> str:
    latest_run_id = str(data_dictionary.get("latest_run_id", ""))
    latest_dashboard = data_dictionary.get("latest_dashboard", "")
    latest_export = data_dictionary.get("latest_export", "")
    preview = data_dictionary.get("cockpit_preview", {})
    overview = preview.get("overview", {}) if isinstance(preview, dict) else {}
    expected_schema_preview = (
        pd.DataFrame(preview.get("expected_schema_preview", []))
        if isinstance(preview, dict)
        else pd.DataFrame()
    )
    readiness_preview = (
        pd.DataFrame(preview.get("readiness_preview", []))
        if isinstance(preview, dict)
        else pd.DataFrame()
    )
    return (
        "<div class='readme-grid'>"
        "<div class='dictionary-top-grid'>"
        "<div class='hub-card dictionary-primary-card'>"
        "<p class='hub-kicker'>Latest Surface</p>"
        "<h3>Recent Dictionary Artifact</h3>"
        f"{_status_chip(str(data_dictionary.get('status', 'not_implemented')))}"
        f"<p class='subtle'>{html.escape(str(data_dictionary.get('direction', '')))}</p>"
        "<div class='terminal-grid'>"
        "<div class='terminal-slot'>"
        "<h4>Latest Run</h4>"
        f"<p class='subtle'>{html.escape(latest_run_id or 'Not recorded')}</p>"
        "</div>"
        "<div class='terminal-slot'>"
        "<h4>Dashboard</h4>"
        f"{_render_cockpit_artifact_reference(latest_dashboard, empty_label='No dictionary dashboard recorded yet.')}"
        "</div>"
        "<div class='terminal-slot'>"
        "<h4>Workbook</h4>"
        f"{_render_cockpit_artifact_reference(latest_export, empty_label='No dictionary workbook recorded yet.')}"
        "</div>"
        "<div class='terminal-slot'>"
        "<h4>Template</h4>"
        f"{_render_reference_value(data_dictionary.get('template_path', ''), empty_label='No template recorded.')}"
        "</div>"
        "<div class='terminal-slot'>"
        "<h4>Implementation Plan</h4>"
        f"{_render_reference_value(data_dictionary.get('implementation_plan', ''), empty_label='No plan recorded.')}"
        "</div>"
        "</div>"
        "</div>"
        "</div>"
        "<div class='dictionary-metric-grid'>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Profile</p>"
        "<h3>Observed Rows</h3>"
        f"{_metric_value(overview.get('rows', 0))}"
        "<p class='subtle'>Rows profiled in the latest dictionary run.</p>"
        "</div>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Profile</p>"
        "<h3>Observed Columns</h3>"
        f"{_metric_value(overview.get('columns', 0))}"
        "<p class='subtle'>Columns present in the current dataset snapshot.</p>"
        "</div>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Contract</p>"
        "<h3>Expected Columns</h3>"
        f"{_metric_value(overview.get('expected_columns', 0))}"
        "<p class='subtle'>Columns represented in the current inferred or baseline contract.</p>"
        "</div>"
        "<div class='hub-card'>"
        "<p class='hub-kicker'>Review</p>"
        "<h3>Open Questions</h3>"
        f"{_metric_value(overview.get('metadata_gaps', 0))}"
        "<p class='subtle'>Readiness items that still need machine confirmation or human input.</p>"
        "</div>"
        "</div>"
        "<div class='readme-section'>"
        "<h3>Expected Schema Preview</h3>"
        "<p class='subtle'>This is the compact cockpit preview of the latest dictionary contract. Open the standalone dictionary dashboard for the full-width review surface.</p>"
        f"{_render_df(expected_schema_preview, full_preview=True, wide_layout=True)}"
        "</div>"
        "<div class='readme-section'>"
        "<h3>Top Readiness Items</h3>"
        "<p class='subtle'>These are the first prelaunch items still blocking a fully trustworthy contract review.</p>"
        f"{_render_df(readiness_preview, full_preview=True, wide_layout=True)}"
        "</div>"
        "</div>"
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
    destination_delivery = effective_payload.get("destination_delivery", {})
    dashboard_url = effective_payload.get("dashboard_url")
    dashboard_path = effective_payload.get("dashboard_path")
    dashboard_ref = _preferred_dashboard_reference(effective_payload)
    local_dashboard_ref = _preferred_local_reference(destination_delivery, "html_report")
    embed_src = local_dashboard_ref or _embed_reference_src(dashboard_path, dashboard_url)
    export_ref = _preferred_export_reference(effective_payload)
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


def render_pipeline_dashboard(report: dict[str, Any], run_id: str) -> str:
    final_status = str(report.get("final_status", "unknown"))
    session_id = str(report.get("session_id", ""))
    health_score = report.get("health_score", "N/A")
    health_status = str(report.get("health_status", "unknown")).upper()
    health_advisory = bool(report.get("health_advisory", False))
    health_message = str(report.get("health_message", "")).strip()
    certification_status = str(report.get("certification_status", "not_run")).upper()
    ready_modules = int(report.get("ready_modules", 0))
    warned_modules = int(report.get("warned_modules", 0))
    failed_modules = int(report.get("failed_modules", 0))
    not_run_modules = int(report.get("not_run_modules", 0))
    module_order = report.get("module_order", [])
    modules = report.get("modules", {})
    final_payload = {
        "dashboard_url": report.get("final_dashboard_url", ""),
        "dashboard_path": report.get("final_dashboard_path", ""),
        "export_url": report.get("final_export_url", ""),
        "destination_delivery": report.get("final_destination_delivery", {}),
    }
    final_dashboard = _preferred_dashboard_reference(final_payload)
    final_export = _preferred_export_reference(final_payload)
    module_ledger_rows = []
    for name in module_order:
        status = (modules.get(name) or {}).get("status", "unknown")
        status_label = _tab_status_label(status)
        module_ledger_rows.append(
            {"Module": name, "Status": status_label, "Badge": _module_badge(status_label)}
        )
    module_ledger_df = pd.DataFrame(module_ledger_rows)

    banner_class = "ok" if failed_modules == 0 and not health_advisory else "warn"
    banner = (
        f"<div class='banner {banner_class}'>"
        "<div class='banner-item'><strong>Stage:</strong> Pipeline Review Shell</div>"
        f"<div class='banner-item'><strong>Final Status:</strong> {html.escape(final_status.upper())}</div>"
        f"<div class='banner-item'><strong>Health:</strong> {html.escape(str(health_score))} ({html.escape(health_status)})</div>"
        f"<div class='banner-item'><strong>Health Mode:</strong> {'ADVISORY' if health_advisory else 'STANDARD'}</div>"
        f"<div class='banner-item'><strong>Session:</strong> {html.escape(session_id or 'Unavailable')}</div>"
        f"<div class='banner-item'><strong>Modules:</strong> {len(module_order)}</div>"
        "</div>"
    )
    advisory_card = ""
    if health_advisory:
        advisory_card = (
            "<div class='card'>"
            "<h3>Health Score Is Advisory</h3>"
            f"<p class='subtle'>{html.escape(health_message or 'Final audit failed certification for this run.')}</p>"
            f"<p class='subtle'><strong>Certification Status:</strong> {html.escape(certification_status)}</p>"
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
        f"{advisory_card}"
        "<div class='card'><h3>Final References</h3>"
        f"{_render_terminal_references(final_dashboard=final_dashboard, final_export=final_export, final_status=final_status, failed_modules=failed_modules, modules=modules, module_order=module_order)}"
        "</div>"
        f"<div class='card'><h3>Module Status Ledger</h3>{_render_df(module_ledger_df, full_preview=True, allow_html_cols={'Badge'})}</div>"
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
    return _assemble_page(
        module_name="Pipeline Dashboard",
        run_id=run_id,
        banner_html=banner,
        toc_items=[],
        sections=sections,
    )


def render_cockpit_dashboard(report: dict[str, Any], run_id: str) -> str:
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
    artifacts = report.get("artifacts", [])
    artifact_server = report.get("artifact_server", {})
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

    overview_section = _render_cockpit_overview(
        overview,
        operator_brief,
        best_surfaces,
        blockers,
        recent_run_gaps,
    )
    recent_runs_section = _render_cockpit_recent_runs(recent_runs)
    resources_panel = _render_cockpit_resources(resources, resource_groups)
    artifacts_panel = _render_cockpit_artifacts(artifacts, artifact_server)
    launchpad_panel = _render_cockpit_launchpad(launchpad, launch_sequences)
    dictionary_tab = _render_cockpit_dictionary(data_dictionary)

    tab_buttons = [
        "<button class='tab-button active' type='button' data-tab-target='cockpit-overview' onclick='window.atkDashboard.openTab(this)'>Overview</button>",
        "<button class='tab-button' type='button' data-tab-target='cockpit-runs' onclick='window.atkDashboard.openTab(this)'>Recent Runs</button>",
        "<button class='tab-button' type='button' data-tab-target='cockpit-resources' onclick='window.atkDashboard.openTab(this)'>Resources</button>",
        "<button class='tab-button' type='button' data-tab-target='cockpit-artifacts' onclick='window.atkDashboard.openTab(this)'>Artifacts</button>",
        "<button class='tab-button' type='button' data-tab-target='cockpit-launchpad' onclick='window.atkDashboard.openTab(this)'>Launchpad</button>",
        "<button class='tab-button' type='button' data-tab-target='cockpit-dictionary' onclick='window.atkDashboard.openTab(this)'>Data Dictionary</button>",
    ]
    tab_panels = [
        f"<div class='tab-panel active' id='cockpit-overview'>{overview_section}</div>",
        f"<div class='tab-panel' id='cockpit-runs'>{recent_runs_section}</div>",
        f"<div class='tab-panel' id='cockpit-resources'>{resources_panel}</div>",
        f"<div class='tab-panel' id='cockpit-artifacts'>{artifacts_panel}</div>",
        f"<div class='tab-panel' id='cockpit-launchpad'>{launchpad_panel}</div>",
        f"<div class='tab-panel' id='cockpit-dictionary'>{dictionary_tab}</div>",
    ]
    sections = [
        "<div class='tab-shell' data-tab-shell='cockpit'>"
        f"<div class='tab-nav'>{''.join(tab_buttons)}</div>"
        f"{''.join(tab_panels)}"
        "</div>"
    ]

    return _assemble_page(
        module_name="Cockpit Dashboard",
        run_id=run_id,
        banner_html=banner,
        toc_items=[],
        sections=sections,
    )
