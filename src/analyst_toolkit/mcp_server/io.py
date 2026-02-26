"""
io.py — Data loading and artifact upload for the analyst_toolkit MCP server.
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pandas as pd
import yaml

from analyst_toolkit.mcp_server.state import StateStore

logger = logging.getLogger(__name__)


def coerce_config(config: Optional[dict], module: str) -> dict:
    """
    Ensure the config passed to a tool is a properly structured dict.

    Handles three agent failure modes:
    1. Agent passes a YAML string instead of a parsed dict — auto-parses it.
    2. Agent passes the full inferred config with the module key containing a YAML
       string — auto-parses: {"normalization": "<yaml>"} → {"normalization": {...}}
    3. Agent double-wraps the module key — auto-unwraps one level:
       {"normalization": {"normalization": {...}}} → {"normalization": {...}}

    Logs a warning whenever a correction is made so it's visible in server logs.
    """
    if config is None:
        return {}

    # If the entire config is a YAML string, parse it first
    if isinstance(config, str):
        logger.warning(
            f"[{module}] config was a raw YAML string — auto-parsing. "
            "Pass a parsed dict to avoid this."
        )
        try:
            config = yaml.safe_load(config)
        except yaml.YAMLError as e:
            logger.error(f"[{module}] Failed to parse YAML string config: {e}")
            return {}

    if not isinstance(config, dict):
        return {}

    # If the module key's value is a YAML string, parse it
    if module in config and isinstance(config[module], str):
        logger.warning(
            f"[{module}] config['{module}'] was a YAML string — auto-parsing. "
            "Pass a parsed dict to avoid this."
        )
        try:
            config = {module: yaml.safe_load(config[module])}
        except yaml.YAMLError as e:
            logger.error(f"[{module}] Failed to parse YAML string in config: {e}")
            return {}

    # If double-wrapped ({"normalization": {"normalization": {...}}}), unwrap one level
    if module in config and isinstance(config[module], dict) and module in config[module]:
        logger.warning(
            f"[{module}] config was double-wrapped — auto-unwrapping. "
            "Pass a single-level dict to avoid this."
        )
        config = config[module]

    return config


def default_run_id() -> str:
    """Return a UTC timestamp-based run ID."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def load_input(path: Optional[str] = None, session_id: Optional[str] = None) -> pd.DataFrame:
    """Load data from GCS, local file, or in-memory session."""
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
        return pd.read_parquet(path)
    else:
        return pd.read_csv(path, low_memory=False)


def save_to_session(
    df: pd.DataFrame, session_id: Optional[str] = None, run_id: Optional[str] = None
) -> str:
    """Save to in-memory store."""
    return StateStore.save(df, session_id, run_id=run_id)


def get_session_run_id(session_id: str) -> Optional[str]:
    return StateStore.get_run_id(session_id)


def get_session_start(session_id: str) -> Optional[str]:
    return StateStore.get_session_start(session_id)


def get_session_metadata(session_id: str) -> Optional[dict]:
    """Retrieve metadata for a session."""
    return StateStore.get_metadata(session_id)


def _resolve_path_root(run_id: str, session_id: Optional[str] = None) -> str:
    """
    Resolve storage root using session + run identity.

    Session-aware layout:
      <session_timestamp>/<session_id>/<run_id>

    Non-session layout:
      <current_timestamp>/<run_id>
    """
    if session_id:
        session_ts = get_session_start(session_id) or datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )
        return f"{session_ts}/{session_id}/{run_id}"

    current_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{current_ts}/{run_id}"


def generate_default_export_path(
    run_id: str, module: str, extension: str = "csv", session_id: Optional[str] = None
) -> str:
    """Generate default path: prefix/path_root/module_output.csv"""
    bucket_uri = os.environ.get("ANALYST_REPORT_BUCKET", "").strip().rstrip("/")
    prefix = os.environ.get("ANALYST_REPORT_PREFIX", "analyst_toolkit/reports").strip().strip("/")

    path_root = _resolve_path_root(run_id, session_id)

    if bucket_uri:
        return f"{bucket_uri}/{prefix}/{path_root}/{module}_output.{extension}"

    base_dir = Path("exports/data") / path_root
    base_dir.mkdir(parents=True, exist_ok=True)
    return str((base_dir / f"{module}_output.{extension}").absolute())


def load_from_gcs(gcs_path: str) -> pd.DataFrame:
    stripped = gcs_path.removeprefix("gs://")
    bucket_name, _, prefix = stripped.partition("/")
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Direct file path — download and read without listing
    if prefix.endswith(".parquet") or prefix.endswith(".csv"):
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / Path(prefix).name
            bucket.blob(prefix).download_to_filename(str(local_path))
            if local_path.suffix == ".parquet":
                return pd.read_parquet(local_path)
            else:
                return pd.read_csv(local_path, low_memory=False)

    # Directory path — list and concat all matching files
    blobs = list(client.list_blobs(bucket_name, prefix=f"{prefix.rstrip('/')}/"))
    blobs = [b for b in blobs if b.name.endswith(".parquet") or b.name.endswith(".csv")]

    if not blobs:
        raise FileNotFoundError(f"No .parquet or .csv files found at gs://{bucket_name}/{prefix}")

    frames = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for blob in blobs:
            local_path = Path(tmpdir) / blob.name.replace("/", "_")
            blob.download_to_filename(str(local_path))
            if local_path.suffix == ".parquet":
                frames.append(pd.read_parquet(local_path))
            else:
                frames.append(pd.read_csv(local_path, low_memory=False))
    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]


def should_export_html(config: dict) -> bool:
    if "export_html" in config:
        return bool(config["export_html"])
    return bool(os.environ.get("ANALYST_REPORT_BUCKET", "").strip())


def save_output(df: pd.DataFrame, path: str) -> str:
    if path.startswith("gs://"):
        # Write to a local temp file first, then upload via google-cloud-storage.
        # This avoids filesystem-layer overwrite flows that may require delete perms.
        suffix = ".parquet" if path.endswith(".parquet") else ".csv"
        content_type = "application/octet-stream" if suffix == ".parquet" else "text/csv"

        tmp_path = ""
        try:
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp_path = tmp.name
            if suffix == ".parquet":
                df.to_parquet(tmp_path, index=False)
            else:
                df.to_csv(tmp_path, index=False)

            try:
                from google.cloud import storage

                stripped = path.removeprefix("gs://")
                bucket_name, _, blob_path = stripped.partition("/")
                if not bucket_name or not blob_path:
                    raise ValueError(f"Invalid GCS path: {path}")

                client = storage.Client()
                bucket = client.bucket(bucket_name)
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(tmp_path, content_type=content_type)
            except ImportError:
                # Backward-compatible fallback for environments using gcsfs-style paths.
                if suffix == ".parquet":
                    df.to_parquet(path, index=False)
                else:
                    df.to_csv(path, index=False)
        finally:
            if tmp_path:
                try:
                    Path(tmp_path).unlink(missing_ok=True)
                except Exception:
                    pass
        return path
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if path.endswith(".parquet"):
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)
    return str(p.absolute())


_CONTENT_TYPES = {
    ".html": "text/html",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csv": "text/csv",
    ".json": "application/json",
    ".png": "image/png",
}


def upload_artifact(
    local_path: str,
    run_id: str,
    module: str,
    config: Optional[dict] = None,
    session_id: Optional[str] = None,
) -> str:
    """Uploads artifact to: prefix/path_root/module/filename"""
    config = config or {}
    bucket_uri = config.get("output_bucket") or os.environ.get(
        "ANALYST_REPORT_BUCKET", ""
    ).strip().rstrip("/")
    if not bucket_uri:
        return ""

    p = Path(local_path)
    if not p.exists():
        return ""

    try:
        from google.cloud import storage
    except ImportError:
        return ""

    prefix = config.get("output_prefix") or os.environ.get(
        "ANALYST_REPORT_PREFIX", "analyst_toolkit/reports"
    ).strip().strip("/")
    content_type = _CONTENT_TYPES.get(p.suffix.lower(), "application/octet-stream")

    path_root = _resolve_path_root(run_id, session_id)
    bucket_name = bucket_uri.removeprefix("gs://")
    blob_path = f"{prefix}/{path_root}/{module}/{p.name}"

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    def _upload(path: str) -> str:
        blob = bucket.blob(path)
        blob.upload_from_filename(str(p), content_type=content_type)
        return f"https://storage.googleapis.com/{bucket_name}/{path}"

    try:
        return _upload(blob_path)
    except Exception as first_exc:
        # Fallback for idempotent reruns where same-key overwrite/delete permissions vary.
        alt_name = f"{p.stem}_{uuid4().hex[:8]}{p.suffix}"
        alt_path = f"{prefix}/{path_root}/{module}/{alt_name}"
        try:
            return _upload(alt_path)
        except Exception:
            logger.warning(
                "Artifact upload failed for primary and fallback paths: %s ; %s",
                first_exc,
                alt_path,
            )
            return ""


def check_upload(url: str, label: str, warnings: list) -> str:
    """Append a warning if the upload failed (url is empty). Returns url unchanged."""
    if not url:
        warnings.append(f"Upload failed or file not found: {label}")
    return url


def append_to_run_history(run_id: str, entry: dict, session_id: Optional[str] = None):
    """Save history to: exports/reports/history/path_root/run_history.json"""
    path_root = _resolve_path_root(run_id, session_id)
    history_dir = Path("exports/reports/history") / path_root
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

    upload_artifact(str(history_file), run_id, "history", session_id=session_id)


def get_run_history(run_id: str, session_id: Optional[str] = None) -> list:
    history_root = Path("exports/reports/history")
    if not history_root.exists():
        return []

    if session_id:
        path_root = _resolve_path_root(run_id, session_id)
        history_file = history_root / path_root / f"{run_id}_history.json"
        if history_file.exists():
            with open(history_file, "r") as f:
                return json.load(f)
        return []

    candidates = sorted(
        history_root.glob(f"**/{run_id}_history.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if candidates:
        with open(candidates[0], "r") as f:
            return json.load(f)
    return []
