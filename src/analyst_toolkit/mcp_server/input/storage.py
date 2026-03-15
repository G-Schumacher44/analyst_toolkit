"""Storage helpers for staged ingest inputs."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

from analyst_toolkit.mcp_server.input.errors import (
    InputPathDeniedError,
    InputPathNotFoundError,
    InputPayloadTooLargeError,
)

_MAX_UPLOAD_BYTES = int(os.environ.get("ANALYST_MCP_MAX_UPLOAD_BYTES", 50 * 1024 * 1024))


def _safe_name(name: str) -> str:
    basename = Path(name).name
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", basename.strip())
    return cleaned or "input.bin"


def input_root() -> Path:
    configured = os.environ.get("ANALYST_MCP_INPUT_ROOT", "").strip()
    root = Path(configured).expanduser() if configured else (Path.cwd() / "exports" / "inputs")
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def allowed_server_input_roots() -> list[Path]:
    """Return the explicit allowlist of server-visible local input roots."""
    configured = os.environ.get("ANALYST_MCP_ALLOWED_INPUT_ROOTS", "").strip()
    if configured:
        roots = [Path(item).expanduser().resolve() for item in configured.split(os.pathsep) if item]
    else:
        roots = [input_root()]
    unique: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if root not in seen:
            seen.add(root)
            unique.append(root)
    return unique


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def stage_uploaded_file(*, filename: str, payload: bytes) -> tuple[Path, str, int]:
    if len(payload) > _MAX_UPLOAD_BYTES:
        raise InputPayloadTooLargeError(
            f"Upload exceeds maximum allowed size ({_MAX_UPLOAD_BYTES} bytes)."
        )
    digest = sha256_hex(payload)
    safe_name = _safe_name(filename)
    staged_dir = input_root() / digest[:2] / digest[2:4]
    staged_dir.mkdir(parents=True, exist_ok=True)
    staged_path = staged_dir / f"{digest}_{safe_name}"
    if not staged_path.exists():
        with tempfile.NamedTemporaryFile(dir=staged_dir, delete=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            tmp_path = Path(handle.name)
        os.replace(tmp_path, staged_path)
    return staged_path.resolve(), digest, len(payload)


def validate_server_visible_path(path_text: str) -> Path:
    if path_text.startswith("~"):
        raise InputPathDeniedError(
            "Local path is not visible to the MCP runtime. Upload the file, mount the directory, or use gs://."
        )
    roots = allowed_server_input_roots()
    candidate = Path(path_text).resolve(strict=False)
    if not candidate.exists():
        raise InputPathNotFoundError("Input path not found.")
    for root in roots:
        try:
            candidate.relative_to(root)
            return candidate
        except ValueError:
            continue
    raise InputPathDeniedError(
        "Local path is not visible to the MCP runtime. "
        "Upload the file, mount the directory, or use gs://."
    )
