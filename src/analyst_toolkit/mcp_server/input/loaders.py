"""DataFrame loaders for canonical input descriptors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from analyst_toolkit.mcp_server.input.errors import InputNotSupportedError
from analyst_toolkit.mcp_server.input.limits import (
    enforce_dataframe_limits,
    enforce_input_bytes_limit,
    enforce_tabular_limits,
    materialize_chunked_frames,
)
from analyst_toolkit.mcp_server.input.models import InputDescriptor
from analyst_toolkit.mcp_server.io_storage import load_from_gcs


def _safe_descriptor_reference(descriptor: InputDescriptor, path: Path) -> str:
    return descriptor.display_name or descriptor.original_reference or path.name


def _read_csv_with_limits(path: Path, *, reference: str) -> pd.DataFrame:
    return materialize_chunked_frames(
        pd.read_csv(path, low_memory=False, chunksize=50_000),
        reference=reference,
    )


def _read_parquet_with_limits(path: Path, *, reference: str) -> pd.DataFrame:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        df = pd.read_parquet(path)
        enforce_dataframe_limits(df, reference=reference)
        return df

    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    estimated_bytes = 0
    for idx in range(metadata.num_row_groups):
        estimated_bytes += metadata.row_group(idx).total_byte_size
    enforce_tabular_limits(
        row_count=metadata.num_rows,
        memory_usage_bytes=estimated_bytes,
        reference=reference,
    )
    df = pd.read_parquet(path)
    enforce_dataframe_limits(df, reference=reference)
    return df


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
    safe_reference = _safe_descriptor_reference(descriptor, path)
    enforce_input_bytes_limit(path.stat().st_size, reference=safe_reference)
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return _read_parquet_with_limits(path, reference=safe_reference)
    if suffix == ".csv":
        return _read_csv_with_limits(path, reference=safe_reference)
    raise InputNotSupportedError(
        f"Unsupported file format: {suffix or '<none>'}. Supported formats are .csv and .parquet."
    )
