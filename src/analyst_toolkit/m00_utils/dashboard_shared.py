"""Shared helper functions for standalone dashboard HTML rendering."""

from __future__ import annotations

import html
from typing import Any

STATUS_PASS_SET = frozenset(
    {
        "pass",
        "passed",
        "ok",
        "available",
        "ready",
        "ready for final audit",
        "healthy",
        "certified",
        "proceed",
    }
)
STATUS_FAIL_SET = frozenset({"fail", "failed", "error", "blocked", "rejected", "repair"})
STATUS_WARN_SET = frozenset({"warn", "warning", "missing", "disabled", "not_run"})


def _slugify(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")


def _display_name(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").title()


def _metric_value(value: Any) -> str:
    rendered = html.escape(str(value))
    compact_class = " compact" if len(str(value)) > 18 else ""
    return f"<p class='metric-stat{compact_class}'>{rendered}</p>"


def _status_tone_class(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in STATUS_PASS_SET:
        return "pass"
    if normalized in STATUS_FAIL_SET:
        return "fail"
    if normalized:
        return "warn"
    return ""


def _normalize_reference_text(value: Any) -> str:
    """Normalize report references into human-friendly URL/path text.

    Falsy values become an empty string. Absolute http/https URLs are preserved.
    Strings containing ``/exports/`` are trimmed to start at ``/exports/...``.
    Strings beginning with ``exports/`` are normalized to ``/exports/...``.
    """

    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    if "/exports/" in text:
        return "/" + text[text.index("exports/") :]
    if text.startswith("exports/"):
        return "/" + text
    return text


def _render_section(title: str, body: str, *, open_by_default: bool = False) -> str:
    open_attr = " open" if open_by_default else ""
    return (
        f"<details class='section'{open_attr} id='{_slugify(title)}'>"
        f"<summary>{html.escape(title)}</summary>"
        f"{body}</details>"
    )


def _render_reference_value(value: Any, *, empty_label: str) -> str:
    if not value:
        return f"<p class='empty'>{html.escape(empty_label)}</p>"

    normalized = _normalize_reference_text(value)
    rendered = html.escape(normalized)
    if normalized.startswith(("http://", "https://", "/exports/")):
        return (
            "<p class='subtle'><a href='"
            f"{rendered}' target='_blank' rel='noopener noreferrer'>{rendered}</a></p>"
        )
    return f"<p class='subtle'><code>{rendered}</code></p>"


def _embed_reference_src(path_value: Any, url_value: Any) -> str:
    normalized_url = _normalize_reference_text(url_value)
    normalized_path = _normalize_reference_text(path_value)
    if normalized_url.startswith(("http://", "https://")):
        return normalized_url
    if normalized_path.startswith(("/exports/", "http://", "https://")):
        return normalized_path
    return ""


def _module_badge(status: str) -> str:
    normalized = str(status or "unknown").lower()
    if normalized in STATUS_PASS_SET:
        return f"<span class='badge-ok'>{html.escape(normalized.upper())}</span>"
    if normalized in STATUS_WARN_SET:
        return f"<span class='badge-warn'>{html.escape(normalized.upper())}</span>"
    return f"<span class='pill warn'>{html.escape(normalized.upper())}</span>"


def _tab_status_label(status: str) -> str:
    normalized = str(status or "not_run").upper()
    if normalized == "UNKNOWN":
        return "NOT_RUN"
    return normalized


def _status_chip(status: str) -> str:
    normalized = _tab_status_label(status).lower()
    chip_class = (
        "ok"
        if normalized in STATUS_PASS_SET
        else "fail"
        if normalized in STATUS_FAIL_SET
        else "warn"
    )
    return f"<span class='status-chip {chip_class}'>{html.escape(normalized.upper())}</span>"
