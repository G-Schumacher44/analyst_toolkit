"""DataFrame loaders for canonical input descriptors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from analyst_toolkit.mcp_server.input.models import InputDescriptor
from analyst_toolkit.mcp_server.io_storage import load_from_gcs


class InputNotSupportedError(Exception):
    """Raised when a descriptor references an unsupported input source or format."""

    code = "INPUT_NOT_SUPPORTED"


def load_dataframe_from_descriptor(descriptor: InputDescriptor) -> pd.DataFrame:
    if descriptor.source_type == "gcs":
        return load_from_gcs(descriptor.resolved_reference)
    if descriptor.source_type == "gdrive":
        raise InputNotSupportedError(
            "Google Drive inputs are not implemented yet. Upload the file, use a server-visible path, or use gs://."
        )

    path = Path(descriptor.resolved_reference)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".csv":
        return pd.read_csv(path, low_memory=False)
    raise InputNotSupportedError(
        f"Unsupported file format: {path.suffix or '<none>'}. Supported formats are .csv and .parquet."
    )
