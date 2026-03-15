"""Input ingest orchestration for the MCP server."""

from __future__ import annotations

import mimetypes
import uuid
from dataclasses import replace
from pathlib import Path

import pandas as pd

from analyst_toolkit.mcp_server.input.adapters import resolve_source_reference
from analyst_toolkit.mcp_server.input.loaders import load_dataframe_from_descriptor
from analyst_toolkit.mcp_server.input.models import InputDescriptor, InputSourceType
from analyst_toolkit.mcp_server.input.registry import (
    bind_session_input,
    get_descriptor,
    get_session_input_id,
    save_descriptor,
)
from analyst_toolkit.mcp_server.input.storage import stage_uploaded_file
from analyst_toolkit.mcp_server.state import StateStore


def _new_input_id() -> str:
    return f"input_{uuid.uuid4().hex[:12]}"


def get_input_descriptor(input_id: str) -> InputDescriptor | None:
    return get_descriptor(input_id)


def ingest_uploaded_bytes(
    *,
    filename: str,
    payload: bytes,
    media_type: str | None,
    session_id: str | None = None,
    run_id: str | None = None,
    load_into_session: bool = True,
) -> tuple[InputDescriptor, pd.DataFrame | None, str | None]:
    staged_path, digest, size = stage_uploaded_file(filename=filename, payload=payload)
    descriptor = InputDescriptor(
        input_id=_new_input_id(),
        source_type="upload",
        original_reference=filename,
        resolved_reference=str(staged_path),
        display_name=Path(filename).name,
        media_type=media_type or mimetypes.guess_type(filename)[0] or "application/octet-stream",
        file_size_bytes=size,
        sha256=digest,
        session_id=session_id,
        run_id=run_id,
    )
    df: pd.DataFrame | None = None
    effective_session_id = session_id
    if load_into_session:
        df = load_dataframe_from_descriptor(descriptor)
        effective_session_id = StateStore.save(df, session_id=session_id, run_id=run_id)
        descriptor = replace(descriptor, session_id=effective_session_id)
    descriptor = save_descriptor(descriptor)
    if load_into_session and effective_session_id is not None:
        bind_session_input(effective_session_id, descriptor.input_id)
    return descriptor, df, effective_session_id


def register_input_source(
    *,
    reference: str,
    source_type: InputSourceType | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    load_into_session: bool = True,
) -> tuple[InputDescriptor, pd.DataFrame | None, str | None]:
    resolved_type, resolved_reference, display_name = resolve_source_reference(
        reference, source_type
    )
    descriptor = InputDescriptor(
        input_id=_new_input_id(),
        source_type=resolved_type,
        original_reference=reference,
        resolved_reference=resolved_reference,
        display_name=display_name,
        media_type=mimetypes.guess_type(display_name)[0] or "application/octet-stream",
        session_id=session_id,
        run_id=run_id,
    )
    df: pd.DataFrame | None = None
    effective_session_id = session_id
    if load_into_session:
        df = load_dataframe_from_descriptor(descriptor)
        effective_session_id = StateStore.save(df, session_id=session_id, run_id=run_id)
        descriptor = replace(descriptor, session_id=effective_session_id)
    descriptor = save_descriptor(descriptor)
    if load_into_session and effective_session_id is not None:
        bind_session_input(effective_session_id, descriptor.input_id)
    return descriptor, df, effective_session_id


def load_dataframe(
    *,
    path: str | None = None,
    session_id: str | None = None,
    input_id: str | None = None,
) -> pd.DataFrame:
    if session_id:
        df = StateStore.get(session_id)
        if df is not None:
            return df
        bound_input_id = get_session_input_id(session_id)
        if bound_input_id:
            descriptor = get_descriptor(bound_input_id)
            if descriptor is not None:
                return load_dataframe_from_descriptor(descriptor)
        if not path and not input_id:
            raise ValueError(f"Session {session_id} not found and no input reference provided.")

    if input_id:
        descriptor = get_descriptor(input_id)
        if descriptor is None:
            raise FileNotFoundError(f"Input ID not found: '{input_id}'")
        return load_dataframe_from_descriptor(descriptor)

    if not path:
        raise ValueError("One of 'path', 'session_id', or 'input_id' must be provided.")

    descriptor, df, _effective_session = register_input_source(
        reference=path,
        load_into_session=False,
    )
    return load_dataframe_from_descriptor(descriptor)
