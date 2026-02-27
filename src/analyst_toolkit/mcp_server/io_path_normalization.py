"""Input path normalization helpers for MCP IO."""

import re
from pathlib import Path

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$")


def normalize_input_path(path: str) -> tuple[str, str]:
    stripped = path.strip()
    if stripped.startswith("gs://"):
        return stripped, ""

    if looks_like_bucket_path(stripped) and not Path(stripped).exists():
        return f"gs://{stripped}", f"Auto-normalized bucket-like input path to gs://{stripped}"
    return stripped, ""


def looks_like_bucket_path(path: str) -> bool:
    if not path or "://" in path:
        return False
    if path.startswith(("/", ".", "~")):
        return False
    if "\\" in path:
        return False
    parts = path.split("/", 1)
    if len(parts) != 2:
        return False
    bucket = parts[0].strip()
    prefix = parts[1].strip()
    if not bucket or not prefix:
        return False
    if "-" not in bucket and "." not in bucket:
        return False
    return bool(_BUCKET_RE.match(bucket))
