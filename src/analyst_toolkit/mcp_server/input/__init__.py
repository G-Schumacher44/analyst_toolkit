"""Input ingest subsystem for MCP data sources."""

from __future__ import annotations

from typing import Any

__all__ = [
    "get_input_descriptor",
    "ingest_uploaded_bytes",
    "load_dataframe",
    "register_input_source",
]


def get_input_descriptor(*args: Any, **kwargs: Any):
    from analyst_toolkit.mcp_server.input.ingest import get_input_descriptor as _impl

    return _impl(*args, **kwargs)


def ingest_uploaded_bytes(*args: Any, **kwargs: Any):
    from analyst_toolkit.mcp_server.input.ingest import ingest_uploaded_bytes as _impl

    return _impl(*args, **kwargs)


def load_dataframe(*args: Any, **kwargs: Any):
    from analyst_toolkit.mcp_server.input.ingest import load_dataframe as _impl

    return _impl(*args, **kwargs)


def register_input_source(*args: Any, **kwargs: Any):
    from analyst_toolkit.mcp_server.input.ingest import register_input_source as _impl

    return _impl(*args, **kwargs)
