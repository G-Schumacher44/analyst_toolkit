"""
🖼️ Module: rendering_utils.py

Lightweight rendering helpers for displaying content in Jupyter notebook environments.

Includes:
- HTML table rendering with row limits and optional full preview
- Markdown table summaries with titles
- Bullet list display for warnings or messages

Used across the Analyst Toolkit for consistent inline diagnostics, QA summaries, and output visualization.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _display_markdown(markdown_text: str) -> None:
    """Render Markdown in notebooks when available, otherwise fall back to stdout."""
    try:
        from IPython.display import Markdown, display
    except ImportError:
        print(markdown_text)
        return

    display(Markdown(markdown_text))


def to_html_table(df, max_rows=25, full_preview=False, escape: bool = True):
    """
    Render a pandas DataFrame as an HTML table for inline display.

    Args:
        df (pd.DataFrame): The DataFrame to render.
        max_rows (int): Number of rows to display (if full_preview is False).
        full_preview (bool): If True, display all rows regardless of max_rows.
        escape (bool): If True, HTML-escape cell contents to prevent injection.

    Returns:
        str: An HTML-formatted string.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return "<p><em>No data available.</em></p>"

    display_df = df if full_preview else df.head(max_rows)
    return display_df.to_html(classes="table table-striped", escape=escape, index=False)


def display_markdown_summary(title: str, df: pd.DataFrame, max_rows: int = 10):
    """
    Display a pandas DataFrame as a Markdown table with a title, limited to a maximum number of rows.

    Args:
        title (str): Heading text displayed above the table.
        df (pd.DataFrame): DataFrame to render.
        max_rows (int): Maximum number of rows to show.
    """
    trimmed_df = df.head(max_rows)
    markdown = f"### {title}\n\n" + trimmed_df.to_markdown(index=False)
    _display_markdown(markdown)


def display_warnings(warning_list: list[Any], title: str = "⚠️ Warnings"):
    """
    Display a list of warning messages as a Markdown bullet list with a section title.

    Args:
        warning_list (list): List of warning message strings.
        title (str): Title displayed above the warnings.
    """
    if warning_list:
        markdown = f"### {title}\n\n"
        markdown += "\n".join(f"- {w}" for w in warning_list)
        _display_markdown(markdown)
