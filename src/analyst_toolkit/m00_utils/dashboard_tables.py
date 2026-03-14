"""Shared dataframe/table rendering helpers for dashboard HTML."""

from __future__ import annotations

import html
from typing import Any

import pandas as pd

_MAX_PREVIEW_ROWS = 50


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
    df: pd.DataFrame,
    *,
    max_rows: int = _MAX_PREVIEW_ROWS,
    full_preview: bool = False,
    allow_html_cols: set[str] | None = None,
) -> str:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return "<p class='empty'>No data available.</p>"

    total_rows = len(df)
    source = df if full_preview else df.head(max_rows)
    preview = _normalize_df_for_display(source)
    if isinstance(preview.columns, pd.MultiIndex):
        preview.columns = [
            "__".join(str(part) for part in column if str(part)).strip("_")
            for column in preview.columns
        ]
    safe_html_cols = allow_html_cols or set()
    for column in preview.columns:
        if str(column) in safe_html_cols:
            continue
        preview[column] = preview[column].map(
            lambda value: html.escape(value) if isinstance(value, str) else value
        )
    table_html = preview.to_html(index=False, escape=False, border=0)
    wrapped_table = f"<div class='table-wrap'>{table_html}</div>"
    if full_preview or total_rows <= max_rows:
        return wrapped_table
    return f"{wrapped_table}<p class='subtle'>Showing {len(preview):,} of {total_rows:,} rows.</p>"


def _render_auto_heal_summary_table(summary: Any) -> str:
    if not isinstance(summary, dict) or not summary:
        return "<p class='empty'>No step summary available.</p>"
    rows = [
        {"Field": str(key).replace("_", " ").replace("-", " ").title(), "Value": str(value)}
        for key, value in summary.items()
    ]
    return _render_df(pd.DataFrame(rows), full_preview=True)
