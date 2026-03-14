"""Module-specific dashboard renderers outside the public facade."""

from __future__ import annotations

import html
from typing import Any

import pandas as pd

from analyst_toolkit.m00_utils.dashboard_core import _assemble_page
from analyst_toolkit.m00_utils.dashboard_plots import render_plot_grid
from analyst_toolkit.m00_utils.dashboard_shared import (
    _display_name,
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
        return (_render_df(key_counts, full_preview=True), _render_df(subset_only, max_rows=20))

    return (
        "<p class='empty'>Subset criteria were not specified, so there is no separate duplicate-key view.</p>",
        _render_df(clusters_df, max_rows=20),
    )


def render_duplicates_dashboard(
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
            _render_section("Plots", render_plot_grid(plot_paths), open_by_default=True)
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


def render_normalization_dashboard(report: dict[str, Any], run_id: str) -> str:
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


def render_outlier_detection_dashboard(
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
            _render_section("Plots", render_plot_grid(plot_paths), open_by_default=True)
        )
        toc.append(("Plots", "Plots"))

    return _assemble_page(
        module_name="Outlier Detection",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
    )


def render_outlier_handling_dashboard(report: dict[str, Any], run_id: str) -> str:
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
    toc = [("Handling Overview", "Handling Overview"), ("Handling Ledger", "Handling Ledger")]

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


def render_imputation_dashboard(
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
            _render_section("Plots", render_plot_grid(plot_paths), open_by_default=True)
        )
        toc.append(("Plots", "Plots"))

    return _assemble_page(
        module_name="Imputation",
        run_id=run_id,
        banner_html=banner,
        toc_items=toc,
        sections=sections,
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
