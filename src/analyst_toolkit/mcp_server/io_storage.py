"""Storage and transfer helpers for MCP IO."""

import logging
import os
import tempfile
from pathlib import Path
from typing import Callable
from uuid import uuid4

import pandas as pd

from analyst_toolkit.mcp_server.input.errors import InputNotFoundError
from analyst_toolkit.mcp_server.input.limits import (
    enforce_dataframe_limits,
    enforce_gcs_prefix_object_limit,
    enforce_input_bytes_limit,
    materialize_chunked_frames,
)

logger = logging.getLogger(__name__)

_CONTENT_TYPES = {
    ".html": "text/html",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csv": "text/csv",
    ".json": "application/json",
    ".png": "image/png",
}


def _gcs_url(bucket_name: str, blob_path: str) -> str:
    return f"https://storage.googleapis.com/{bucket_name}/{blob_path}"


def _gcs_uri(bucket_name: str, blob_path: str) -> str:
    return f"gs://{bucket_name}/{blob_path}"


def _blob_exists(bucket: object, blob_path: str) -> bool:
    get_blob = getattr(bucket, "get_blob", None)
    if callable(get_blob):
        try:
            return get_blob(blob_path) is not None
        except Exception:
            return False

    blob_factory = getattr(bucket, "blob", None)
    if callable(blob_factory):
        try:
            blob = blob_factory(blob_path)
        except Exception:
            return False
        exists = getattr(blob, "exists", None)
        if callable(exists):
            try:
                return bool(exists())
            except Exception:
                return False
    return False


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
    from analyst_toolkit.mcp_server.input.limits import enforce_tabular_limits

    enforce_tabular_limits(
        row_count=metadata.num_rows,
        memory_usage_bytes=estimated_bytes,
        reference=reference,
    )
    df = pd.read_parquet(path)
    enforce_dataframe_limits(df, reference=reference)
    return df


def load_from_gcs(gcs_path: str) -> pd.DataFrame:
    stripped = gcs_path.removeprefix("gs://")
    bucket_name, _, prefix = stripped.partition("/")
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Direct file path — download and read without listing
    if prefix.endswith(".parquet") or prefix.endswith(".csv"):
        blob = bucket.get_blob(prefix)
        if blob is None:
            raise InputNotFoundError(f"No file found at gs://{bucket_name}/{prefix}")
        enforce_input_bytes_limit(blob.size, reference=gcs_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / Path(prefix).name
            blob.download_to_filename(str(local_path))
            if local_path.suffix == ".parquet":
                df = _read_parquet_with_limits(local_path, reference=gcs_path)
            else:
                df = _read_csv_with_limits(local_path, reference=gcs_path)
            return df

    # Directory path — list and concat all matching files
    blobs = []
    total_bytes = 0
    for blob in client.list_blobs(bucket_name, prefix=f"{prefix.rstrip('/')}/"):
        if not blob.name.endswith(".parquet") and not blob.name.endswith(".csv"):
            continue
        blobs.append(blob)
        total_bytes += int(getattr(blob, "size", 0) or 0)
        enforce_gcs_prefix_object_limit(object_count=len(blobs), reference=gcs_path)
        enforce_input_bytes_limit(total_bytes, reference=gcs_path)

    if not blobs:
        raise FileNotFoundError(f"No .parquet or .csv files found at gs://{bucket_name}/{prefix}")

    frames = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for blob in blobs:
            local_path = Path(tmpdir) / blob.name.replace("/", "_")
            blob.download_to_filename(str(local_path))
            if local_path.suffix == ".parquet":
                frames.append(_read_parquet_with_limits(local_path, reference=gcs_path))
            else:
                frames.append(_read_csv_with_limits(local_path, reference=gcs_path))
    result = materialize_chunked_frames(frames, reference=gcs_path, copy=False)
    enforce_dataframe_limits(result, reference=gcs_path)
    return result


def _collect_export_html_flags(
    config: object,
    *,
    path: tuple[object, ...] = (),
    runtime_flags: list[bool] | None = None,
    module_flags: list[bool] | None = None,
    malformed_flags: list[bool] | None = None,
) -> tuple[list[bool], list[bool], list[bool]]:
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
    if malformed_flags is None:
        malformed_flags = []
    if isinstance(config, dict):
        for key, value in config.items():
            next_path = path + (key,)
            if key == "export_html":
                if not isinstance(value, bool):
                    if next_path == (
                        "runtime",
                        "artifacts",
                        "export_html",
                    ) or _is_sanctioned_module_path(next_path):
                        malformed_flags.append(True)
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
                malformed_flags=malformed_flags,
            )
    elif isinstance(config, list):
        for idx, value in enumerate(config):
            _collect_export_html_flags(
                value,
                path=path + (idx,),
                runtime_flags=runtime_flags,
                module_flags=module_flags,
                malformed_flags=malformed_flags,
            )
    return runtime_flags, module_flags, malformed_flags


def should_export_html(config: dict) -> bool:
    runtime_flags, module_flags, malformed_flags = _collect_export_html_flags(config)
    if malformed_flags:
        logger.warning(
            "Malformed export_html configuration detected on sanctioned paths; "
            "disabling HTML export."
        )
        return False
    if runtime_flags:
        if len(runtime_flags) > 1:
            logger.warning(
                "Multiple runtime.artifacts.export_html values detected: %s; using last value.",
                runtime_flags,
            )
        return runtime_flags[-1]
    if module_flags:
        if len(set(module_flags)) != 1:
            logger.warning(
                "Conflicting module export_html flags detected: %s; disabling HTML export.",
                module_flags,
            )
            return False
        return module_flags[0]
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
                    if _blob_exists(bucket, blob_path):
                        return _gcs_uri(bucket_name, blob_path)
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
        return _gcs_url(bucket_name, path)

    try:
        return _upload(blob_path)
    except Exception as first_exc:
        if _blob_exists(bucket, blob_path):
            return _gcs_url(bucket_name, blob_path)
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
