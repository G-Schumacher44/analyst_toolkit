"""Runtime-aware artifact destination routing helpers."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any, Callable

from analyst_toolkit.mcp_server.io_storage import upload_artifact as _upload_artifact


def _bucket_uri_from_config(config: dict[str, Any]) -> str:
    return (config.get("output_bucket") or os.environ.get("ANALYST_REPORT_BUCKET", "")).strip()


def _local_relative_path(local_path: str) -> Path:
    path = Path(local_path)
    if not path.is_absolute():
        return path

    cwd = Path.cwd()
    try:
        return path.relative_to(cwd)
    except ValueError:
        if "exports" in path.parts:
            exports_index = path.parts.index("exports")
            return Path(*path.parts[exports_index:])
    return Path(path.name)


def _copy_to_local_root(local_path: str, root: str) -> str:
    source = Path(local_path)
    relative = _local_relative_path(local_path)
    destination = Path(root) / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return str(destination)


def deliver_artifact(
    local_path: str,
    *,
    run_id: str,
    module: str,
    config: dict[str, Any],
    session_id: str | None,
    resolve_path_root: Callable[[str, str | None], str],
    logger: logging.Logger,
) -> dict[str, Any]:
    """
    Route a locally generated artifact to configured destinations.

    The artifact is always generated locally first. This helper can then:
    - mirror it to an alternate local root
    - upload it to GCS when configured
    - emit an explicit unsupported warning for Drive requests
    """

    result: dict[str, Any] = {
        "reference": "",
        "local_path": "",
        "url": "",
        "warnings": [],
        "destinations": {
            "local": {"status": "missing", "path": ""},
            "gcs": {"status": "disabled", "url": ""},
            "drive": {"status": "disabled", "folder_id": ""},
        },
    }

    if not local_path:
        return result

    source = Path(local_path)
    if not source.exists():
        result["warnings"].append(f"Artifact not found for routing: {local_path}")
        return result

    effective_local_path = str(source)
    result["destinations"]["local"] = {"status": "available", "path": effective_local_path}

    local_root = str(config.get("local_output_root") or "").strip()
    if local_root:
        effective_local_path = _copy_to_local_root(str(source), local_root)
        result["destinations"]["local"] = {"status": "available", "path": effective_local_path}

    drive_folder_id = str(config.get("drive_folder_id") or "").strip()
    if drive_folder_id:
        result["destinations"]["drive"] = {
            "status": "unsupported",
            "folder_id": drive_folder_id,
        }
        result["warnings"].append(
            "Google Drive destination requested but Drive uploads are not implemented yet."
        )

    upload_artifacts = config.get("upload_artifacts")
    bucket_uri = _bucket_uri_from_config(config)
    if upload_artifacts is False:
        result["destinations"]["gcs"] = {"status": "disabled", "url": ""}
    elif bucket_uri:
        url = _upload_artifact(
            local_path=effective_local_path,
            run_id=run_id,
            module=module,
            config=config,
            session_id=session_id,
            resolve_path_root=resolve_path_root,
            logger=logger,
        )
        if url:
            result["url"] = url
            result["destinations"]["gcs"] = {"status": "available", "url": url}
        else:
            result["destinations"]["gcs"] = {"status": "missing", "url": ""}
            result["warnings"].append(f"Upload failed or file not found: {effective_local_path}")

    result["local_path"] = effective_local_path
    result["reference"] = result["url"] or effective_local_path
    return result
