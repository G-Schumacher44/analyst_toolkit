"""Canonical models for MCP input ingestion and resolution."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Literal, Mapping

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
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def same_canonical_input(self, other: "InputDescriptor") -> bool:
        return (
            self.input_id == other.input_id
            and self.source_type == other.source_type
            and self.original_reference == other.original_reference
            and self.resolved_reference == other.resolved_reference
            and self.display_name == other.display_name
            and self.media_type == other.media_type
            and self.file_size_bytes == other.file_size_bytes
            and self.sha256 == other.sha256
            and dict(self.metadata) == dict(other.metadata)
        )

    def with_runtime_binding(
        self,
        *,
        session_id: str | None = None,
        run_id: str | None = None,
    ) -> "InputDescriptor":
        return replace(
            self,
            session_id=session_id if session_id is not None else self.session_id,
            run_id=run_id if run_id is not None else self.run_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_id": self.input_id,
            "source_type": self.source_type,
            "original_reference": self.original_reference,
            "resolved_reference": self.resolved_reference,
            "display_name": self.display_name,
            "media_type": self.media_type,
            "file_size_bytes": self.file_size_bytes,
            "sha256": self.sha256,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "metadata": dict(self.metadata),
        }
