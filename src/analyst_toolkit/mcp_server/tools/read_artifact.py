"""MCP tool: read_artifact — read container-local artifact content through MCP."""

import base64
import logging
import mimetypes
import os
from pathlib import Path

from analyst_toolkit.mcp_server.registry import register_tool
from analyst_toolkit.mcp_server.response_utils import new_trace_id

logger = logging.getLogger("analyst_toolkit.mcp_server.read_artifact")


def _is_stdio_mode() -> bool:
    return os.environ.get("ANALYST_MCP_STDIO", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


_MAX_ARTIFACT_BYTES = int(os.environ.get("ANALYST_MCP_MAX_ARTIFACT_READ_BYTES", 10 * 1024 * 1024))

_ARTIFACT_ROOT = Path(os.environ.get("ANALYST_MCP_ARTIFACT_SERVER_ROOT", "exports")).resolve(
    strict=False
)

_TEXT_EXTENSIONS = {
    ".html",
    ".htm",
    ".csv",
    ".tsv",
    ".json",
    ".yaml",
    ".yml",
    ".txt",
    ".md",
    ".xml",
    ".log",
}


def _is_text_artifact(path: Path) -> bool:
    return path.suffix.lower() in _TEXT_EXTENSIONS


def _validate_artifact_path(artifact_path: str) -> tuple[Path | None, str | None]:
    """Validate and resolve an artifact path, returning (resolved_path, error_message)."""
    if not artifact_path or not artifact_path.strip():
        return None, "artifact_path is empty."

    raw = Path(artifact_path)
    if any(part == ".." for part in raw.parts):
        return None, "artifact_path must not contain parent-directory traversal."

    # Support both relative paths (exports/reports/...) and absolute container paths
    if raw.is_absolute():
        resolved = raw.resolve(strict=False)
    else:
        resolved = (Path.cwd() / raw).resolve(strict=False)

    # In stdio mode the client is local, so CWD is a reasonable root.
    # In HTTP mode restrict to the artifact root only — CWD could expose
    # source code, configs, or secrets to remote callers.
    allowed_roots: list[Path] = [_ARTIFACT_ROOT]
    if _is_stdio_mode():
        allowed_roots.append(Path.cwd().resolve(strict=False))

    within_root = False
    for allowed_root in allowed_roots:
        try:
            resolved.relative_to(allowed_root)
            within_root = True
            break
        except ValueError:
            continue

    if not within_root:
        return None, "artifact_path is outside the allowed artifact root."

    if not resolved.exists():
        return None, f"Artifact not found: {artifact_path}"

    if not resolved.is_file():
        return None, f"artifact_path is not a file: {artifact_path}"

    return resolved, None


async def _toolkit_read_artifact(
    artifact_path: str,
    encoding: str = "auto",
) -> dict:
    """Read a container-local artifact and return its content through MCP.

    This bridges the container isolation gap: when the artifact server at
    127.0.0.1:8765 is not reachable from the client, agents can use this
    tool to retrieve artifact content directly through the MCP protocol.

    Text artifacts (HTML, CSV, JSON, etc.) are returned as plain text.
    Binary artifacts are returned as base64-encoded strings.
    """
    trace_id = new_trace_id()

    resolved, error = _validate_artifact_path(artifact_path)
    if error:
        return {
            "status": "error",
            "module": "read_artifact",
            "code": "ARTIFACT_PATH_DENIED",
            "message": error,
            "trace_id": trace_id,
        }

    assert resolved is not None  # validated above

    file_size = resolved.stat().st_size
    if file_size > _MAX_ARTIFACT_BYTES:
        return {
            "status": "error",
            "module": "read_artifact",
            "code": "ARTIFACT_TOO_LARGE",
            "message": (
                f"Artifact is {file_size} bytes, exceeding the "
                f"{_MAX_ARTIFACT_BYTES} byte limit. Set "
                "ANALYST_MCP_MAX_ARTIFACT_READ_BYTES to increase."
            ),
            "trace_id": trace_id,
        }

    is_text = _is_text_artifact(resolved)
    if encoding == "base64":
        is_text = False
    elif encoding == "text":
        is_text = True

    media_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"

    try:
        if is_text:
            content = resolved.read_text(encoding="utf-8")
            return {
                "status": "pass",
                "module": "read_artifact",
                "artifact_path": str(resolved),
                "filename": resolved.name,
                "media_type": media_type,
                "encoding": "text",
                "size_bytes": file_size,
                "artifact_content": content,
                "trace_id": trace_id,
            }
        else:
            raw = resolved.read_bytes()
            content_b64 = base64.b64encode(raw).decode("ascii")
            return {
                "status": "pass",
                "module": "read_artifact",
                "artifact_path": str(resolved),
                "filename": resolved.name,
                "media_type": media_type,
                "encoding": "base64",
                "size_bytes": file_size,
                "content_base64": content_b64,
                "trace_id": trace_id,
            }
    except Exception:
        logger.exception("read_artifact failed for %s (trace_id=%s)", artifact_path, trace_id)
        return {
            "status": "error",
            "module": "read_artifact",
            "code": "ARTIFACT_READ_FAILED",
            "message": "Failed to read artifact content.",
            "trace_id": trace_id,
        }


register_tool(
    name="read_artifact",
    fn=_toolkit_read_artifact,
    description=(
        "Read a container-local artifact file and return its content through MCP. "
        "Use this when localhost artifact URLs are not reachable from the client. "
        "Text files (HTML, CSV, JSON) are returned as plain text; binary files "
        "as base64. Pass the artifact_path returned by module tools."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "artifact_path": {
                "type": "string",
                "description": (
                    "Path to the artifact file. Accepts the artifact_path or "
                    "dashboard_path value returned by module tools."
                ),
            },
            "encoding": {
                "type": "string",
                "enum": ["auto", "text", "base64"],
                "description": (
                    "Output encoding. 'auto' detects from extension, "
                    "'text' forces UTF-8 text, 'base64' forces base64. Default: auto."
                ),
                "default": "auto",
            },
        },
        "required": ["artifact_path"],
    },
)
