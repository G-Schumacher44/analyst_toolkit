"""Storage and transfer helpers for MCP IO."""

import logging
import os
import tempfile
from pathlib import Path
from typing import Callable
from uuid import uuid4

import pandas as pd

_CONTENT_TYPES = {
    ".html": "text/html",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csv": "text/csv",
    ".json": "application/json",
    ".png": "image/png",
}


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


def _collect_export_html_flags(
    config: object,
    *,
    path: tuple[object, ...] = (),
    runtime_flags: list[bool] | None = None,
    module_flags: list[bool] | None = None,
) -> tuple[list[bool], list[bool]]:
    sanctioned_module_suffixes = {
        ("settings", "export_html"),
        ("profile", "settings", "export_html"),
        ("export", "export_html"),
        ("settings", "export", "export_html"),
    }

    def _is_sanctioned_module_path(candidate: tuple[object, ...]) -> bool:
        if candidate == ("export_html",):
            return True
        return any(candidate[-len(suffix) :] == suffix for suffix in sanctioned_module_suffixes)

    if runtime_flags is None:
        runtime_flags = []
    if module_flags is None:
        module_flags = []
    if isinstance(config, dict):
        for key, value in config.items():
            next_path = path + (key,)
            if key == "export_html":
                if not isinstance(value, bool):
                    continue
                if next_path == ("runtime", "artifacts", "export_html"):
                    runtime_flags.append(value)
                elif _is_sanctioned_module_path(next_path):
                    module_flags.append(value)
                continue
            _collect_export_html_flags(
                value,
                path=next_path,
                runtime_flags=runtime_flags,
                module_flags=module_flags,
            )
    elif isinstance(config, list):
        for idx, value in enumerate(config):
            _collect_export_html_flags(
                value,
                path=path + (idx,),
                runtime_flags=runtime_flags,
                module_flags=module_flags,
            )
    return runtime_flags, module_flags


def should_export_html(config: dict) -> bool:
    runtime_flags, module_flags = _collect_export_html_flags(config)
    if runtime_flags:
        return runtime_flags[-1]
    if module_flags:
        return len(set(module_flags)) == 1 and module_flags[0]
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
                try:
                    blob.upload_from_filename(tmp_path, content_type=content_type)
                except Exception as first_exc:
                    alt_blob_path = _versioned_blob_path(blob_path)
                    alt_blob = bucket.blob(alt_blob_path)
                    try:
                        alt_blob.upload_from_filename(tmp_path, content_type=content_type)
                    except Exception:
                        raise first_exc
                    return f"gs://{bucket_name}/{alt_blob_path}"
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
    if not p.exists():
        raise FileNotFoundError(f"Local export write failed: '{p}'")
    return str(p.absolute())


def upload_artifact(
    *,
    local_path: str,
    run_id: str,
    module: str,
    config: dict,
    session_id: str | None,
    resolve_path_root: Callable[[str, str | None], str],
    logger: logging.Logger,
) -> str:
    """Uploads artifact to: prefix/path_root/module/filename."""
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

    path_root = resolve_path_root(run_id, session_id)
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


def _versioned_blob_path(blob_path: str) -> str:
    p = Path(blob_path)
    return str(p.with_name(f"{p.stem}_{uuid4().hex[:8]}{p.suffix}"))
