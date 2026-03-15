"""Canonical models for MCP input ingestion and resolution."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

InputSourceType = Literal["upload", "server_path", "gcs", "gdrive"]


@dataclass(frozen=True)
class InputDescriptor:
    input_id: str
    source_type: InputSourceType
    original_reference: str
    resolved_reference: str
    display_name: str
    media_type: str
    file_size_bytes: int | None = None
    sha256: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
