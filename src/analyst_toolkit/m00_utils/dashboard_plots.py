"""Shared plot embedding helpers for standalone dashboards."""

from __future__ import annotations

import base64
import html
import logging
import mimetypes
from pathlib import Path
from typing import Any

from analyst_toolkit.m00_utils.dashboard_shared import _display_name

_SIZE_WARNING_THRESHOLD_MB = 25


def _flatten_plot_paths(plot_paths: dict[str, Any] | None) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    if not plot_paths:
        return items

    for name, value in plot_paths.items():
        if isinstance(value, list):
            for index, item in enumerate(value, start=1):
                if item:
                    label = f"{name} {index}" if len(value) > 1 else name
                    items.append((label, str(item)))
        elif value:
            items.append((name, str(value)))
    return items


def render_plot_grid(plot_paths: dict[str, Any] | None) -> str:
    cards = []
    total_bytes = 0
    for name, path_str in _flatten_plot_paths(plot_paths):
        path = Path(path_str)
        if not path.exists():
            logging.debug("Plot file not found, skipping %s (%s)", name, path)
            continue
        file_bytes = path.read_bytes()
        total_bytes += len(file_bytes)
        encoded = base64.b64encode(file_bytes).decode("utf-8")
        mime_type = mimetypes.guess_type(path.name)[0] or "image/png"
        if not mime_type.startswith("image/"):
            mime_type = "image/png"
        image_src = f"data:{mime_type};base64,{encoded}"
        escaped_title = html.escape(_display_name(name))
        escaped_name = html.escape(name)
        cards.append(
            "<div class='card plot-card'>"
            f"<h3>{escaped_title}</h3>"
            f"<button class='plot-trigger' type='button' data-plot-title='{escaped_title}' data-plot-src='{image_src}'>"
            f"<img src='{image_src}' alt='{escaped_name}'>"
            "</button>"
            "<p class='plot-caption'>Click to expand</p>"
            "</div>"
        )
    if not cards:
        return "<p class='empty'>No plots were generated for this run.</p>"
    if total_bytes > _SIZE_WARNING_THRESHOLD_MB * 1024 * 1024:
        logging.warning(
            "Embedded plot data exceeds %s MB. Consider reducing plot count or resolution.",
            _SIZE_WARNING_THRESHOLD_MB,
        )
    return (
        "<p class='plot-intro'>The standalone export keeps the visuals in the same file so the report travels without sidecar assets.</p>"
        "<div class='plot-grid'>" + "".join(cards) + "</div>"
    )
