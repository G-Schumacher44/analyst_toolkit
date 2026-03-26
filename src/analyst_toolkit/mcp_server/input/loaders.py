"""DataFrame loaders for canonical input descriptors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from analyst_toolkit.mcp_server.input.errors import InputNotSupportedError
from analyst_toolkit.mcp_server.input.limits import (
    enforce_dataframe_limits,
    enforce_input_bytes_limit,
)
from analyst_toolkit.mcp_server.input.models import InputDescriptor
from analyst_toolkit.mcp_server.io_storage import load_from_gcs


def load_dataframe_from_descriptor(descriptor: InputDescriptor) -> pd.DataFrame:
    if descriptor.source_type == "gcs":
        return load_from_gcs(descriptor.resolved_reference)
    if descriptor.source_type == "gdrive":
        raise InputNotSupportedError(
            "Google Drive inputs are not implemented yet. Upload the file, use a server-visible path, or use gs://."
        )
    if descriptor.source_type not in {"upload", "server_path"}:
        raise InputNotSupportedError(
            f"Unsupported source type: {descriptor.source_type}. Supported source types are upload, server_path, and gcs."
        )

    path = Path(descriptor.resolved_reference).resolve(strict=False)
    enforce_input_bytes_limit(path.stat().st_size, reference=str(path))
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
        enforce_dataframe_limits(df, reference=str(path))
        return df
    if path.suffix == ".csv":
        df = pd.read_csv(path, low_memory=False)
        enforce_dataframe_limits(df, reference=str(path))
        return df
    raise InputNotSupportedError(
        f"Unsupported file format: {path.suffix or '<none>'}. Supported formats are .csv and .parquet."
    )
