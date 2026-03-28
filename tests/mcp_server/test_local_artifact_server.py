import errno

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
