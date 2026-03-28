"""Opt-in local web server for exported artifact browsing."""

from __future__ import annotations

import errno
import ipaddress
import json
import logging
import os
import threading
import time
import urllib.request
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger("analyst_toolkit.mcp_server.local_artifact_server")

_SERVER_GUARD = threading.Lock()
_SERVER: "_ArtifactHTTPServer | None" = None
_SERVER_THREAD: threading.Thread | None = None
_SERVER_STARTING = False
_SERVER_BASE_URL = ""
_SERVER_ROOT = Path("exports").resolve(strict=False)
_WORKSPACE_ROOT = Path.cwd().resolve(strict=False)


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed >= 0 else default


def artifact_server_enabled() -> bool:
    return _env_bool("ANALYST_MCP_ENABLE_ARTIFACT_SERVER", _env_bool("ANALYST_MCP_STDIO", False))


def _artifact_server_host() -> str:
    host = os.environ.get("ANALYST_MCP_ARTIFACT_SERVER_HOST", "127.0.0.1").strip() or "127.0.0.1"
    allow_bind_all = _env_bool("ANALYST_MCP_ALLOW_BIND_ALL", False)
    try:
        is_loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        is_loopback = host == "localhost"
    if not is_loopback:
        if not allow_bind_all:
            logger.warning(
                "Rejected non-loopback artifact server host %r; "
                "set ANALYST_MCP_ALLOW_BIND_ALL=1 to enable remote binding. Falling back to 127.0.0.1.",
                host,
            )
            return "127.0.0.1"
        logger.warning("Artifact server binding to non-loopback host %r", host)
    return host


def _artifact_server_port() -> int:
    port = _env_int("ANALYST_MCP_ARTIFACT_SERVER_PORT", 8765)
    if port == 0:
        raise ValueError("ANALYST_MCP_ARTIFACT_SERVER_PORT must be a positive, non-zero port.")
    return port


def _artifact_server_root() -> Path:
    """Resolve the artifact root with basic trust-boundary checks.

    The default is the repository-local ``exports`` directory. Custom roots
    outside the working tree are allowed but logged, and a small denylist of
    known sensitive roots is rejected.
    """
    raw_root = Path(os.environ.get("ANALYST_MCP_ARTIFACT_SERVER_ROOT", "exports")).expanduser()
    candidate = raw_root.resolve(strict=False)
    sensitive_roots = (
        Path("/etc"),
        Path("/root"),
        Path("/var/run"),
        Path.home() / ".ssh",
    )
    for sensitive_root in sensitive_roots:
        sensitive_root = sensitive_root.resolve(strict=False)
        if (
            candidate == sensitive_root
            or sensitive_root in candidate.parents
            or candidate in sensitive_root.parents
        ):
            raise ValueError(
                f"ANALYST_MCP_ARTIFACT_SERVER_ROOT points at a protected path: {candidate}"
            )
    try:
        candidate.relative_to(_WORKSPACE_ROOT)
    except ValueError:
        logger.warning(
            "Artifact server root %s is outside the working directory %s",
            candidate,
            _WORKSPACE_ROOT,
        )
    return candidate


class _ArtifactRequestHandler(SimpleHTTPRequestHandler):
    server_version = "AnalystToolkitArtifactServer/1.0"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        logger.info("artifact_http %s", format % args)

    def _serve_health(self) -> None:
        payload = {
            "status": "ok",
            "base_url": getattr(self.server, "base_url", ""),
            "root": str(getattr(self.server, "artifact_root", "")),
        }
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed_path = urlparse(self.path).path
        if parsed_path == "/__health":
            self._serve_health()
            return
        if parsed_path in {"", "/"}:
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", "/exports/")
            self.end_headers()
            return
        super().do_GET()

    def translate_path(self, path: str) -> str:
        parsed_path = urlparse(path).path
        artifact_root = getattr(self.server, "artifact_root", None)
        if artifact_root is None:
            logger.error("artifact_root not set on artifact server instance")
            return str(Path("__missing__").resolve(strict=False))
        if parsed_path in {"/exports", "/exports/"}:
            relative = PurePosixPath(".")
        elif parsed_path.startswith("/exports/"):
            relative = PurePosixPath(parsed_path.removeprefix("/exports/"))
        else:
            return str((artifact_root / "__missing__").resolve(strict=False))
        if ".." in relative.parts:
            return str((artifact_root / "__missing__").resolve(strict=False))
        candidate = (artifact_root / Path(relative)).resolve(strict=False)
        try:
            candidate.relative_to(artifact_root)
        except ValueError:
            return str((artifact_root / "__missing__").resolve(strict=False))
        return str(candidate)


class _ArtifactHTTPServer(ThreadingHTTPServer):
    """Typed HTTP server carrying artifact-serving metadata."""

    artifact_root: Path
    base_url: str


def _probe_server(base_url: str, timeout_sec: float = 0.25) -> bool:
    try:
        with urllib.request.urlopen(f"{base_url}/__health", timeout=timeout_sec) as response:
            return response.status == 200
    except Exception:
        return False


def _read_server_health(base_url: str, timeout_sec: float = 0.25) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(f"{base_url}/__health", timeout=timeout_sec) as response:
            if response.status != 200:
                return None
            payload = json.loads(response.read().decode("utf-8"))
            return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def get_local_artifact_server_status() -> dict[str, Any]:
    with _SERVER_GUARD:
        running = bool(
            _SERVER and _SERVER_THREAD and _SERVER_THREAD.is_alive() and _SERVER_BASE_URL
        )
        if running and not _probe_server(_SERVER_BASE_URL):
            running = False
        return {
            "enabled": artifact_server_enabled(),
            "running": running,
            "base_url": _SERVER_BASE_URL if running else "",
            "root": str(_SERVER_ROOT),
        }


def ensure_local_artifact_server() -> dict[str, Any]:
    if not artifact_server_enabled():
        return {
            "status": "disabled",
            "enabled": False,
            "running": False,
            "base_url": "",
            "root": str(_artifact_server_root()),
            "message": "Local artifact server is disabled. Set ANALYST_MCP_ENABLE_ARTIFACT_SERVER=1.",
        }

    with _SERVER_GUARD:
        global _SERVER, _SERVER_THREAD, _SERVER_STARTING, _SERVER_BASE_URL, _SERVER_ROOT
        if (
            _SERVER
            and _SERVER_THREAD
            and _SERVER_THREAD.is_alive()
            and _probe_server(_SERVER_BASE_URL)
        ):
            return {
                "status": "pass",
                "enabled": True,
                "running": True,
                "already_running": True,
                "base_url": _SERVER_BASE_URL,
                "root": str(_SERVER_ROOT),
            }
        if _SERVER_STARTING and _SERVER_BASE_URL:
            base_url = _SERVER_BASE_URL
            root = _SERVER_ROOT
            created = False
            server = None
            thread = None
        else:
            root = _artifact_server_root()
            root.mkdir(parents=True, exist_ok=True)
            host = _artifact_server_host()
            requested_port = _artifact_server_port()
            advertised_host = "127.0.0.1" if host == "0.0.0.0" else host
            base_url = f"http://{advertised_host}:{requested_port}"
            try:
                server = _ArtifactHTTPServer((host, requested_port), _ArtifactRequestHandler)
            except OSError as exc:
                if exc.errno == errno.EADDRINUSE:
                    health = _read_server_health(base_url)
                    if health is not None:
                        return {
                            "status": "pass",
                            "enabled": True,
                            "running": True,
                            "already_running": True,
                            "base_url": str(health.get("base_url") or base_url),
                            "root": str(health.get("root") or root),
                        }
                raise
            server.artifact_root = root
            # Advertise 127.0.0.1 in URLs even when bound to 0.0.0.0 —
            # 0.0.0.0 is not a valid browsable address.
            server.base_url = f"http://{advertised_host}:{server.server_address[1]}"
            thread = threading.Thread(
                target=server.serve_forever,
                name="analyst-toolkit-artifact-server",
                daemon=True,
            )
            _SERVER_STARTING = True
            thread.start()
            _SERVER = server
            _SERVER_THREAD = thread
            _SERVER_BASE_URL = server.base_url
            _SERVER_ROOT = root
            base_url = _SERVER_BASE_URL
            created = True

    for _ in range(20):
        if _probe_server(base_url):
            with _SERVER_GUARD:
                if _SERVER_BASE_URL == base_url:
                    _SERVER_STARTING = False
            return {
                "status": "pass",
                "enabled": True,
                "running": True,
                "already_running": not created,
                "base_url": base_url,
                "root": str(root),
            }
        if not created:
            with _SERVER_GUARD:
                if not _SERVER_STARTING:
                    break
        time.sleep(0.05)
    if created and server is not None and thread is not None:
        try:
            server.shutdown()
            server.server_close()
        except Exception:
            logger.exception("Failed to clean up local artifact server after startup timeout")
        try:
            thread.join(timeout=5)
            if thread.is_alive():
                logger.warning("Local artifact server thread did not exit after startup timeout")
        except Exception:
            logger.exception("Failed to join local artifact server thread after startup timeout")
        with _SERVER_GUARD:
            if _SERVER is server:
                _SERVER = None
                _SERVER_THREAD = None
                _SERVER_STARTING = False
                _SERVER_BASE_URL = ""
                _SERVER_ROOT = Path("exports").resolve(strict=False)
    return {
        "status": "error",
        "error_code": "ARTIFACT_SERVER_STARTUP_TIMEOUT",
        "enabled": True,
        "running": False,
        "already_running": not created,
        "base_url": "",
        "root": str(root),
        "message": "Artifact server failed to start within timeout.",
    }


def build_local_artifact_url(local_path: str) -> str:
    status = get_local_artifact_server_status()
    if not status.get("running") or not local_path:
        return ""
    artifact_root = Path(str(status.get("root", ""))).resolve(strict=False)
    candidate = Path(local_path).expanduser().resolve(strict=False)
    try:
        relative = candidate.relative_to(artifact_root)
    except ValueError:
        return ""
    return f"{status['base_url']}/exports/{relative.as_posix()}"


def _reset_local_artifact_server_for_tests() -> None:
    with _SERVER_GUARD:
        global _SERVER, _SERVER_THREAD, _SERVER_STARTING, _SERVER_BASE_URL, _SERVER_ROOT
        if _SERVER is not None:
            try:
                _SERVER.shutdown()
                _SERVER.server_close()
            except Exception:
                logger.exception("Failed to stop local artifact server during test reset")
        if _SERVER_THREAD is not None:
            try:
                _SERVER_THREAD.join(timeout=5)
                if _SERVER_THREAD.is_alive():
                    logger.warning("Local artifact server thread did not exit during test reset")
            except Exception:
                logger.exception("Failed to join local artifact server thread during test reset")
        _SERVER = None
        _SERVER_THREAD = None
        _SERVER_STARTING = False
        _SERVER_BASE_URL = ""
        _SERVER_ROOT = Path("exports").resolve(strict=False)
