"""Storage helpers for staged ingest inputs."""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path


def _safe_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    return cleaned or "input.bin"


def input_root() -> Path:
    configured = os.environ.get("ANALYST_MCP_INPUT_ROOT", "").strip()
    root = Path(configured).expanduser() if configured else (Path.cwd() / "exports" / "inputs")
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def allowed_server_input_roots() -> list[Path]:
    configured = os.environ.get("ANALYST_MCP_ALLOWED_INPUT_ROOTS", "").strip()
    if configured:
        roots = [Path(item).expanduser().resolve() for item in configured.split(os.pathsep) if item]
    else:
        roots = [
            (Path.cwd() / "data").resolve(),
            (Path.cwd() / "exports").resolve(),
            input_root(),
        ]
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
    digest = sha256_hex(payload)
    safe_name = _safe_name(filename)
    staged_dir = input_root() / digest[:2] / digest[2:4]
    staged_dir.mkdir(parents=True, exist_ok=True)
    staged_path = staged_dir / f"{digest}_{safe_name}"
    if not staged_path.exists():
        staged_path.write_bytes(payload)
    return staged_path.resolve(), digest, len(payload)


def validate_server_visible_path(path_text: str) -> Path:
    candidate = Path(path_text).expanduser().resolve(strict=False)
    if not candidate.exists():
        raise FileNotFoundError(f"Input path not found: '{path_text}'")
    for root in allowed_server_input_roots():
        try:
            candidate.relative_to(root)
            return candidate
        except ValueError:
            continue
    allowed = ", ".join(str(root) for root in allowed_server_input_roots())
    raise PermissionError(
        "Local path is not visible to the MCP runtime. "
        f"Allowed roots: {allowed}. Upload the file, mount the directory, or use gs://."
    )
