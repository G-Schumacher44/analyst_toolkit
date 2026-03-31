"""Input ingest subsystem for MCP data sources."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

    from analyst_toolkit.mcp_server.input.models import InputDescriptor, InputSourceType

__all__ = [
    "get_input_descriptor",
    "ingest_uploaded_bytes",
    "load_dataframe",
    "register_input_source",
]


def get_input_descriptor(input_id: str) -> "InputDescriptor | None":
    from analyst_toolkit.mcp_server.input.ingest import get_input_descriptor as _impl

    return _impl(input_id)


def ingest_uploaded_bytes(
    *,
    filename: str,
    payload: bytes,
    media_type: str | None,
    session_id: str | None = None,
    run_id: str | None = None,
    load_into_session: bool = True,
    idempotency_key: str | None = None,
) -> tuple["InputDescriptor", "pd.DataFrame | None", str | None]:
    from analyst_toolkit.mcp_server.input.ingest import ingest_uploaded_bytes as _impl

    return _impl(
        filename=filename,
        payload=payload,
        media_type=media_type,
        session_id=session_id,
        run_id=run_id,
        load_into_session=load_into_session,
        idempotency_key=idempotency_key,
    )


def load_dataframe(
    *,
    path: str | None = None,
    session_id: str | None = None,
    input_id: str | None = None,
) -> "pd.DataFrame":
    from analyst_toolkit.mcp_server.input.ingest import load_dataframe as _impl

    return _impl(path=path, session_id=session_id, input_id=input_id)


def register_input_source(
    *,
    reference: str,
    source_type: "InputSourceType | None" = None,
    session_id: str | None = None,
    run_id: str | None = None,
    load_into_session: bool = True,
    idempotency_key: str | None = None,
) -> tuple["InputDescriptor", "pd.DataFrame | None", str | None]:
    from analyst_toolkit.mcp_server.input.ingest import register_input_source as _impl

    return _impl(
        reference=reference,
        source_type=source_type,
        session_id=session_id,
        run_id=run_id,
        load_into_session=load_into_session,
        idempotency_key=idempotency_key,
    )
