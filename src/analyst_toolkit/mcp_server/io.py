"""
io.py — Data loading and artifact upload for the analyst_toolkit MCP server.

Dispatches on path prefix:
  gs://...          → GCS pull (parquet or CSV)
  *.parquet         → pd.read_parquet()
  *.csv / default   → pd.read_csv()

GCS auth: GOOGLE_APPLICATION_CREDENTIALS env var (mounted in Docker).
Manifest: if _MANIFEST.json exists at the partition path, reads file list
from it rather than globbing all parquet files (faster for large partitions).

Report upload:
  ANALYST_REPORT_BUCKET  — GCS bucket, e.g. gs://fridai-reports (no trailing slash)
  ANALYST_REPORT_PREFIX  — optional blob prefix, default "analyst_toolkit/reports"
  If ANALYST_REPORT_BUCKET is unset, upload_report() is a no-op returning "".
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from analyst_toolkit.mcp_server.state import StateStore

logger = logging.getLogger(__name__)


def default_run_id() -> str:
    """Return a UTC timestamp-based run ID, e.g. '20260222_153045'."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def load_input(path: Optional[str] = None, session_id: Optional[str] = None) -> pd.DataFrame:
    """
    Dispatch on path type or session_id and return a DataFrame.

    Args:
        path: Local file path or gs:// URI.
        session_id: In-memory session identifier.

    Returns:
        pd.DataFrame loaded from the specified source.
    """
    if session_id:
        df = StateStore.get(session_id)
        if df is not None:
            logger.info(f"Loaded from session: {session_id}")
            return df
        elif not path:
            raise ValueError(f"Session {session_id} not found and no path provided.")

    if not path:
        raise ValueError("Either 'path' (gcs_path) or 'session_id' must be provided.")

    if path.startswith("gs://"):
        return load_from_gcs(path)
    elif path.endswith(".parquet"):
        logger.info(f"Loading parquet: {path}")
        return pd.read_parquet(path)
    else:
        logger.info(f"Loading CSV: {path}")
        return pd.read_csv(path, low_memory=False)


def save_to_session(df: pd.DataFrame, session_id: Optional[str] = None) -> str:
    """
    Save a DataFrame to the in-memory state store.

    Args:
        df: The DataFrame to save.
        session_id: Optional session ID to use/overwrite.

    Returns:
        The session_id.
    """
    return StateStore.save(df, session_id)


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

    # Direct file path — skip manifest/glob logic
    if prefix.endswith(".parquet") or prefix.endswith(".csv"):
        logger.info(f"Direct file path detected: {gcs_path}")
        blobs = [bucket.blob(prefix)]
    else:
        # Check for _MANIFEST.json
        manifest_blob = bucket.blob(f"{prefix}/_MANIFEST.json")
        has_manifest = False
        try:
            has_manifest = manifest_blob.exists()
        except Exception:
            pass

        if has_manifest:
            logger.info(f"Found _MANIFEST.json at {gcs_path}")
            manifest_data = json.loads(manifest_blob.download_as_text())
            raw_files = manifest_data.get("files", [])
            # files entries may be bare strings or dicts with a "path" key
            file_names: list[str] = [f["path"] if isinstance(f, dict) else f for f in raw_files]
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
            sanitized_name = blob.name.replace("/", "_")
            local_path = Path(tmpdir) / sanitized_name
            blob.download_to_filename(str(local_path))
            logger.info(f"Downloaded {blob.name} → {local_path}")
            if sanitized_name.endswith(".parquet"):
                frames.append(pd.read_parquet(local_path))
            else:
                frames.append(pd.read_csv(local_path, low_memory=False))

    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    logger.info(f"Loaded {df.shape[0]} rows × {df.shape[1]} cols from {gcs_path}")
    return df


def should_export_html(config: dict) -> bool:
    """Return True if HTML export should run.

    Defaults to True when ANALYST_REPORT_BUCKET is set (container/production).
    Callers can explicitly override with export_html: true/false in config.
    """
    if "export_html" in config:
        return bool(config["export_html"])
    return bool(os.environ.get("ANALYST_REPORT_BUCKET", "").strip())


_CONTENT_TYPES = {
    ".html": "text/html",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csv": "text/csv",
    ".joblib": "application/octet-stream",
    ".json": "application/json",
}


def upload_artifact(local_path: str, run_id: str, module: str) -> str:
    """
    Upload any local artifact to GCS and return its public HTTPS URL.

    Reads ANALYST_REPORT_BUCKET and ANALYST_REPORT_PREFIX from env.
    If ANALYST_REPORT_BUCKET is not set, returns "" (no-op for local dev).

    Blob path: {prefix}/{run_id}/{module}/{filename}
    Public URL: https://storage.googleapis.com/{bucket}/{blob}

    Supports HTML, XLSX, CSV, joblib, and JSON — content type is inferred
    from the file extension.

    Args:
        local_path: Absolute or relative path to the file on disk.
        run_id:     Run identifier (used as a path component in GCS).
        module:     Module name (e.g. "outliers", "imputation").

    Returns:
        Public GCS HTTPS URL string, or "" if upload is disabled or file missing.
    """
    bucket_uri = os.environ.get("ANALYST_REPORT_BUCKET", "").strip().rstrip("/")
    if not bucket_uri:
        return ""

    p = Path(local_path)
    if not p.exists():
        logger.warning(f"Artifact not found, skipping upload: {local_path}")
        return ""

    try:
        from google.cloud import storage
    except ImportError:
        logger.warning("google-cloud-storage not installed; skipping artifact upload.")
        return ""

    prefix = os.environ.get("ANALYST_REPORT_PREFIX", "analyst_toolkit/reports").strip().strip("/")
    content_type = _CONTENT_TYPES.get(p.suffix.lower(), "application/octet-stream")

    bucket_name = bucket_uri.removeprefix("gs://")
    blob_path = f"{prefix}/{run_id}/{module}/{p.name}"

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(str(p), content_type=content_type)
        url = f"https://storage.googleapis.com/{bucket_name}/{blob_path}"
        logger.info(f"Uploaded {p.suffix} artifact → {url}")
        return url
    except Exception as exc:
        logger.warning(f"GCS upload failed for {local_path}: {exc}")
        return ""


def upload_report(local_path: str, run_id: str, module: str) -> str:
    """Alias for upload_artifact — retained for backwards compatibility."""
    return upload_artifact(local_path, run_id, module)


def append_to_run_history(run_id: str, entry: dict):
    """
    Append a tool result entry to the run's history ledger.
    Saves to exports/reports/history/{run_id}_history.json
    """
    history_dir = Path("exports/reports/history")
    history_dir.mkdir(parents=True, exist_ok=True)

    history_file = history_dir / f"{run_id}_history.json"

    history = []
    if history_file.exists():
        try:
            with open(history_file, "r") as f:
                history = json.load(f)
        except Exception:
            history = []

    entry["timestamp"] = datetime.now(timezone.utc).isoformat()
    history.append(entry)

    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)

    # Also upload to GCS if possible
    upload_artifact(str(history_file), run_id, "history")


def get_run_history(run_id: str) -> list:
    """Retrieve the history ledger for a run."""
    history_file = Path("exports/reports/history") / f"{run_id}_history.json"
    if history_file.exists():
        with open(history_file, "r") as f:
            return json.load(f)
    return []
