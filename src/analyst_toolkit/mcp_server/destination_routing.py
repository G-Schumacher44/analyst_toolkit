"""Runtime-aware artifact destination routing helpers."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from analyst_toolkit.mcp_server.io_storage import upload_artifact as _upload_artifact
from analyst_toolkit.mcp_server.local_artifact_server import build_local_artifact_url


def _bucket_uri_from_config(config: dict[str, Any]) -> str:
    return (config.get("output_bucket") or os.environ.get("ANALYST_REPORT_BUCKET", "")).strip()


def _is_remote_reference(reference: str) -> bool:
    return urlparse(reference).scheme in {"gs", "http", "https"}


def split_artifact_reference(reference: str) -> tuple[str, str]:
    """Split an artifact reference into local-path and URL channels without probing the filesystem."""
    if not reference:
        return "", ""
    if _is_remote_reference(reference):
        return "", reference
    return reference, ""


def compact_destination_metadata(destinations: dict[str, Any]) -> dict[str, Any]:
    """Trim destination metadata down to user-facing essentials."""
    compact: dict[str, Any] = {}
    for name, payload in destinations.items():
        if not isinstance(payload, dict):
            continue
        status = payload.get("status", "")
        if status in {"", "disabled"}:
            continue
        item = {"status": status}
        for key in ("path", "url", "folder_id"):
            value = payload.get(key)
            if value:
                item[key] = value
        compact[name] = item
    return compact


def _local_relative_path(local_path: str) -> Path:
    path = Path(local_path)
    if any(part == ".." for part in path.parts):
        raise ValueError("Local artifact path must not contain parent-directory traversal.")
    resolved = path.resolve(strict=False)
    if not path.is_absolute():
        return path

    cwd = Path.cwd()
    try:
        return resolved.relative_to(cwd.resolve(strict=False))
    except ValueError:
        if "exports" in resolved.parts:
            exports_index = resolved.parts.index("exports")
            # Return path *after* "exports" to avoid doubled prefix when the
            # local_output_root already resolves to an exports directory.
            after_exports = resolved.parts[exports_index + 1 :]
            if after_exports:
                return Path(*after_exports)
    return Path(resolved.name)


def _resolve_local_output_root(root: str) -> Path:
    raw_root = Path(root).expanduser()
    if raw_root.is_absolute():
        raise ValueError("local_output_root must be relative to the configured local output base.")
    if any(part == ".." for part in raw_root.parts):
        raise ValueError("local_output_root must not contain parent-directory traversal.")

    base_root = Path(os.environ.get("ANALYST_MCP_LOCAL_OUTPUT_BASE", ".")).expanduser()
    resolved_base = base_root.resolve(strict=False)
    resolved_target = (resolved_base / raw_root).resolve(strict=False)
    try:
        resolved_target.relative_to(resolved_base)
    except ValueError as exc:
        raise ValueError("local_output_root escapes the configured local output base.") from exc
    return resolved_target


def _copy_to_local_root(local_path: str, root: str) -> str:
    source = Path(local_path)
    relative = _local_relative_path(local_path)
    resolved_root = _resolve_local_output_root(root)
    destination = (resolved_root / relative).resolve(strict=False)
    try:
        destination.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(
            "Resolved local artifact destination escapes the configured root."
        ) from exc
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
        "local_url": "",
        "remote_url": "",
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
        try:
            effective_local_path = _copy_to_local_root(str(source), local_root)
            result["destinations"]["local"] = {"status": "available", "path": effective_local_path}
        except (ValueError, OSError) as exc:
            logger.warning(
                "Local artifact routing rejected for module=%s run_id=%s target=%s: %s",
                module,
                run_id,
                local_root,
                exc,
            )
            result["destinations"]["local"] = {"status": "rejected", "path": ""}
            result["warnings"].append(str(exc))

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
    result["local_url"] = build_local_artifact_url(effective_local_path)
    result["remote_url"] = result["url"]
    result["url"] = result["remote_url"] or result["local_url"]
    result["reference"] = result["url"] or effective_local_path
    if result["local_url"]:
        result["destinations"]["local"]["url"] = result["local_url"]
    return result
