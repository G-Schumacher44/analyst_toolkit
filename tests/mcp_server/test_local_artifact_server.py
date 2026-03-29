import errno
from pathlib import Path

import analyst_toolkit.mcp_server.local_artifact_server as artifact_server_module


def test_ensure_local_artifact_server_detects_existing_server_on_same_port(
    monkeypatch, tmp_path, reset_artifact_server
):
    root = tmp_path / "exports"
    root.mkdir(parents=True)
    port = 8765
    base_url = f"http://127.0.0.1:{port}"

    monkeypatch.setenv("ANALYST_MCP_ENABLE_ARTIFACT_SERVER", "true")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_HOST", "127.0.0.1")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_PORT", str(port))
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_ROOT", str(root))
    monkeypatch.setattr(
        artifact_server_module,
        "_ArtifactHTTPServer",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError(errno.EADDRINUSE, "in use")),
    )
    monkeypatch.setattr(
        artifact_server_module,
        "_read_server_health",
        lambda *_args, **_kwargs: {"base_url": base_url, "root": str(root)},
    )

    result = artifact_server_module.ensure_local_artifact_server()

    assert result["status"] == "pass"
    assert result["running"] is True
    assert result["already_running"] is True
    assert result["base_url"] == base_url
    assert result["root"] == str(root)


def test_build_local_artifact_url_serves_paths_relative_to_exports_root(
    monkeypatch, tmp_path, reset_artifact_server
):
    root = tmp_path / "exports"
    local_artifact = root / "reports" / "pipeline" / "run1_dashboard.html"
    local_artifact.parent.mkdir(parents=True)
    local_artifact.write_text("<html>ok</html>", encoding="utf-8")

    monkeypatch.setenv("ANALYST_MCP_ENABLE_ARTIFACT_SERVER", "true")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_HOST", "127.0.0.1")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_PORT", "8765")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_ROOT", str(root))

    monkeypatch.setattr(
        artifact_server_module,
        "get_local_artifact_server_status",
        lambda: {
            "running": True,
            "base_url": "http://127.0.0.1:8765",
            "root": str(root.resolve(strict=False)),
        },
    )

    url = artifact_server_module.build_local_artifact_url(str(local_artifact))

    assert url == "http://127.0.0.1:8765/reports/pipeline/run1_dashboard.html"
    assert "/exports/" not in url


def test_build_local_artifact_url_preserves_exports_prefix_outside_reports(
    monkeypatch, tmp_path, reset_artifact_server
):
    root = tmp_path / "exports"
    local_artifact = root / "data" / "run1.csv"
    local_artifact.parent.mkdir(parents=True)
    local_artifact.write_text("id,value\n1,a\n", encoding="utf-8")

    monkeypatch.setattr(
        artifact_server_module,
        "get_local_artifact_server_status",
        lambda: {
            "running": True,
            "base_url": "http://127.0.0.1:8765",
            "root": str(root.resolve(strict=False)),
        },
    )

    url = artifact_server_module.build_local_artifact_url(str(local_artifact))

    assert url == "http://127.0.0.1:8765/exports/data/run1.csv"


def test_translate_path_accepts_reports_prefix(tmp_path):
    root = tmp_path / "exports"
    handler = artifact_server_module._ArtifactRequestHandler.__new__(
        artifact_server_module._ArtifactRequestHandler
    )
    handler.server = type("Server", (), {"artifact_root": root})()

    translated = Path(handler.translate_path("/reports/pipeline/run1_dashboard.html"))

    assert translated == (root / "reports" / "pipeline" / "run1_dashboard.html")


def test_ensure_local_artifact_server_returns_conflict_without_health_match(
    monkeypatch, tmp_path, reset_artifact_server
):
    root = tmp_path / "exports"
    root.mkdir(parents=True)

    monkeypatch.setenv("ANALYST_MCP_ENABLE_ARTIFACT_SERVER", "true")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_HOST", "127.0.0.1")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_PORT", "8765")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_ROOT", str(root))
    monkeypatch.setattr(
        artifact_server_module,
        "_ArtifactHTTPServer",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError(errno.EADDRINUSE, "in use")),
    )
    monkeypatch.setattr(
        artifact_server_module,
        "_read_server_health",
        lambda *_args, **_kwargs: None,
    )

    result = artifact_server_module.ensure_local_artifact_server()

    assert result["status"] == "error"
    assert result["error_code"] == "ARTIFACT_SERVER_BIND_CONFLICT"
    assert result["already_running"] is False
    assert result["base_url"] == ""
    assert "no compatible health response" in result["message"]


def test_ensure_local_artifact_server_returns_conflict_for_incompatible_health(
    monkeypatch, tmp_path, reset_artifact_server
):
    root = tmp_path / "exports"
    root.mkdir(parents=True)

    monkeypatch.setenv("ANALYST_MCP_ENABLE_ARTIFACT_SERVER", "true")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_HOST", "127.0.0.1")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_PORT", "8765")
    monkeypatch.setenv("ANALYST_MCP_ARTIFACT_SERVER_ROOT", str(root))
    monkeypatch.setattr(
        artifact_server_module,
        "_ArtifactHTTPServer",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError(errno.EADDRINUSE, "in use")),
    )
    monkeypatch.setattr(
        artifact_server_module,
        "_read_server_health",
        lambda *_args, **_kwargs: {
            "base_url": "http://127.0.0.1:8765",
            "root": str((tmp_path / "other_exports").resolve(strict=False)),
        },
    )

    result = artifact_server_module.ensure_local_artifact_server()

    assert result["status"] == "error"
    assert result["error_code"] == "ARTIFACT_SERVER_BIND_CONFLICT"
    assert result["already_running"] is False
    assert result["base_url"] == ""
    assert "incompatible server" in result["message"]
