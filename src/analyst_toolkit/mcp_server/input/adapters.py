"""Source adapters for MCP input ingestion."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from analyst_toolkit.mcp_server.input.models import InputSourceType
from analyst_toolkit.mcp_server.input.storage import validate_server_visible_path


def detect_source_type(reference: str) -> InputSourceType:
    parsed = urlparse(reference)
    if parsed.scheme == "gs":
        return "gcs"
    if parsed.scheme in {"gdrive", "drive"}:
        return "gdrive"
    if parsed.scheme:
        raise ValueError(
            f"Unsupported input scheme '{parsed.scheme}'. Use a server-visible path, gs:// URI, or upload."
        )
    return "server_path"


def resolve_source_reference(
    reference: str, source_type: InputSourceType | None = None
) -> tuple[InputSourceType, str, str]:
    detected_type = detect_source_type(reference)
    if source_type is not None and source_type != detected_type:
        raise ValueError(
            f"Explicit source_type '{source_type}' does not match reference '{reference}'."
        )
    resolved_type = source_type or detected_type
    if resolved_type == "gcs":
        filename = reference.rstrip("/").split("/")[-1] or reference
        return resolved_type, reference, filename
    if resolved_type == "gdrive":
        raise NotImplementedError(
            "Google Drive inputs are not implemented yet. Upload the file, use a server-visible path, or use gs://."
        )
    resolved_path = validate_server_visible_path(reference)
    return resolved_type, str(resolved_path), resolved_path.name
