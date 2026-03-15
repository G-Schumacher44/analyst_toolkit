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
    return "server_path"


def resolve_source_reference(
    reference: str, source_type: InputSourceType | None = None
) -> tuple[InputSourceType, str, str]:
    resolved_type = source_type or detect_source_type(reference)
    if resolved_type == "gcs":
        return resolved_type, reference, Path(reference).name or reference
    if resolved_type == "gdrive":
        raise NotImplementedError(
            "Google Drive inputs are not implemented yet. Upload the file, use a server-visible path, or use gs://."
        )
    resolved_path = validate_server_visible_path(reference)
    return resolved_type, str(resolved_path), resolved_path.name
