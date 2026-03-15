"""Input ingest subsystem for MCP data sources."""

from analyst_toolkit.mcp_server.input.ingest import (
    get_input_descriptor,
    ingest_uploaded_bytes,
    load_dataframe,
    register_input_source,
)

__all__ = [
    "get_input_descriptor",
    "ingest_uploaded_bytes",
    "load_dataframe",
    "register_input_source",
]
