"""
io.py — Data loading for the analyst_toolkit MCP server.

Dispatches on path prefix:
  gs://...          → GCS pull (parquet or CSV)
  *.parquet         → pd.read_parquet()
  *.csv / default   → pd.read_csv()

GCS auth: GOOGLE_APPLICATION_CREDENTIALS env var (mounted in Docker).
Manifest: if _MANIFEST.json exists at the partition path, reads file list
from it rather than globbing all parquet files (faster for large partitions).
"""

import json
import logging
import os
import tempfile
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def load_input(path: str) -> pd.DataFrame:
    """
    Dispatch on path type and return a DataFrame.

    Args:
        path: Local file path or gs:// URI.

    Returns:
        pd.DataFrame loaded from the specified source.
    """
    if path.startswith("gs://"):
        return load_from_gcs(path)
    elif path.endswith(".parquet"):
        logger.info(f"Loading parquet: {path}")
        return pd.read_parquet(path)
    else:
        logger.info(f"Loading CSV: {path}")
        return pd.read_csv(path, low_memory=False)


def load_from_gcs(gcs_path: str) -> pd.DataFrame:
    """
    Pull data from a GCS path into a DataFrame.

    If a _MANIFEST.json exists at the path, reads the file list from it
    and fetches only those files. Otherwise globs *.parquet files.
    Falls back to CSV if no parquet files found.

    Args:
        gcs_path: GCS URI, e.g. gs://bucket/path/to/partition/

    Returns:
        pd.DataFrame concatenated from all found files.
    """
    try:
        from google.cloud import storage
    except ImportError:
        raise ImportError(
            "google-cloud-storage is required for GCS access. "
            "Install it with: pip install google-cloud-storage"
        )

    # Parse bucket and prefix from gs:// URI
    stripped = gcs_path.removeprefix("gs://")
    bucket_name, _, prefix = stripped.partition("/")
    prefix = prefix.rstrip("/")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Check for _MANIFEST.json
    manifest_blob = bucket.blob(f"{prefix}/_MANIFEST.json")
    file_names: list[str] = []

    if manifest_blob.exists():
        logger.info(f"Found _MANIFEST.json at {gcs_path}")
        manifest_data = json.loads(manifest_blob.download_as_text())
        file_names = manifest_data.get("files", [])
        blobs = [bucket.blob(f"{prefix}/{f}") for f in file_names]
    else:
        logger.info(f"No manifest found, globbing {gcs_path}")
        blobs = list(client.list_blobs(bucket_name, prefix=f"{prefix}/"))
        blobs = [b for b in blobs if b.name.endswith(".parquet") or b.name.endswith(".csv")]

    if not blobs:
        raise FileNotFoundError(f"No data files found at GCS path: {gcs_path}")

    # Download to a temp dir and read
    frames = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for blob in blobs:
            local_name = Path(blob.name).name
            local_path = Path(tmpdir) / local_name
            blob.download_to_filename(str(local_path))
            logger.info(f"Downloaded {blob.name} → {local_path}")
            if local_name.endswith(".parquet"):
                frames.append(pd.read_parquet(local_path))
            else:
                frames.append(pd.read_csv(local_path, low_memory=False))

    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    logger.info(f"Loaded {df.shape[0]} rows × {df.shape[1]} cols from {gcs_path}")
    return df
