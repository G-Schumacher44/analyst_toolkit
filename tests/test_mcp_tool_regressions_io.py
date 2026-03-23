import base64

import pandas as pd
import pytest

import analyst_toolkit.mcp_server.tools.read_artifact as read_artifact_tool
import analyst_toolkit.mcp_server.tools.session as session_tool
import analyst_toolkit.mcp_server.tools.upload_input as upload_input_tool
from analyst_toolkit.mcp_server.state import StateStore

# ── manage_session tool tests ──


@pytest.mark.asyncio
async def test_manage_session_list():
    StateStore.clear()
    df = pd.DataFrame({"a": [1, 2]})
    sid1 = StateStore.save(df, run_id="run_1")
    sid2 = StateStore.save(df, run_id="run_2")

    result = await session_tool._toolkit_manage_session(action="list")
    assert result["status"] == "pass"
    assert result["session_count"] == 2
    session_ids = {s["session_id"] for s in result["sessions"]}
    assert sid1 in session_ids
    assert sid2 in session_ids
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_inspect():
    StateStore.clear()
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = StateStore.save(df, run_id="inspect_run")
    StateStore.save_config(sid, "validation", "validation:\n  run: true\n")

    result = await session_tool._toolkit_manage_session(action="inspect", session_id=sid)
    assert result["status"] == "pass"
    assert result["session"]["session_id"] == sid
    assert result["session"]["run_id"] == "inspect_run"
    assert result["session"]["row_count"] == 3
    assert "validation" in result["session"]["stored_configs"]
    assert "next_actions" in result
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_inspect_missing():
    StateStore.clear()
    result = await session_tool._toolkit_manage_session(
        action="inspect", session_id="sess_nonexistent"
    )
    assert result["status"] == "error"
    assert result["error_code"] == "SESSION_NOT_FOUND"
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_fork():
    StateStore.clear()
    df = pd.DataFrame({"col": [10, 20, 30]})
    sid = StateStore.save(df, run_id="original_run")
    StateStore.save_config(sid, "diagnostics", "diagnostics:\n  run: true\n")

    result = await session_tool._toolkit_manage_session(
        action="fork", session_id=sid, run_id="forked_run"
    )
    assert result["status"] == "pass"
    assert result["source_session_id"] == sid
    new_sid = result["new_session_id"]
    assert new_sid != sid
    assert result["run_id"] == "forked_run"
    assert result["configs_copied"] is True

    forked_df = StateStore.get(new_sid)
    assert forked_df is not None
    assert len(forked_df) == 3
    assert StateStore.get_run_id(new_sid) == "forked_run"
    assert StateStore.get_config(new_sid, "diagnostics") is not None

    assert StateStore.get_run_id(sid) == "original_run"
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_fork_without_configs():
    StateStore.clear()
    df = pd.DataFrame({"v": [1]})
    sid = StateStore.save(df, run_id="r1")
    StateStore.save_config(sid, "validation", "yaml")

    result = await session_tool._toolkit_manage_session(
        action="fork", session_id=sid, run_id="r2", copy_configs=False
    )
    assert result["status"] == "pass"
    new_sid = result["new_session_id"]
    assert StateStore.get_configs(new_sid) == {}
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_fork_generates_run_id():
    StateStore.clear()
    df = pd.DataFrame({"v": [1]})
    sid = StateStore.save(df)

    result1 = await session_tool._toolkit_manage_session(action="fork", session_id=sid)
    result2 = await session_tool._toolkit_manage_session(action="fork", session_id=sid)
    assert result1["status"] == "pass"
    assert result2["status"] == "pass"
    assert result1["run_id"]
    assert result2["run_id"]
    assert result1["run_id"] != result2["run_id"]
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_fork_missing_source():
    StateStore.clear()
    result = await session_tool._toolkit_manage_session(
        action="fork", session_id="sess_gone", run_id="new"
    )
    assert result["status"] == "error"
    assert result["error_code"] == "SESSION_NOT_FOUND"


@pytest.mark.asyncio
async def test_manage_session_rebind():
    StateStore.clear()
    df = pd.DataFrame({"v": [1]})
    sid = StateStore.save(df, run_id="old_run")

    result = await session_tool._toolkit_manage_session(
        action="rebind", session_id=sid, run_id="new_run"
    )
    assert result["status"] == "pass"
    assert result["previous_run_id"] == "old_run"
    assert result["new_run_id"] == "new_run"
    assert StateStore.get_run_id(sid) == "new_run"
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_rebind_missing_run_id():
    StateStore.clear()
    df = pd.DataFrame({"v": [1]})
    sid = StateStore.save(df, run_id="r1")

    result = await session_tool._toolkit_manage_session(action="rebind", session_id=sid)
    assert result["status"] == "error"
    assert result["error_code"] == "MISSING_RUN_ID"
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_unknown_action():
    result = await session_tool._toolkit_manage_session(action="delete")
    assert result["status"] == "error"
    assert result["error_code"] == "UNKNOWN_ACTION"


# ── upload_input tests ──


@pytest.mark.asyncio
async def test_upload_input_accepts_base64_csv(monkeypatch, tmp_path):
    monkeypatch.setenv("ANALYST_MCP_INPUT_ROOT", str(tmp_path / "inputs"))
    StateStore.clear()

    csv_content = b"species,bill_length_mm\nAdelie,39.1\nGentoo,46.5\n"
    encoded = base64.b64encode(csv_content).decode("ascii")

    result = await upload_input_tool._toolkit_upload_input(
        filename="penguins.csv",
        content_base64=encoded,
        load_into_session=True,
    )
    assert result["status"] == "pass"
    assert result["module"] == "upload_input"
    assert result["input"]["source_type"] == "upload"
    assert result["session_id"].startswith("sess_")
    assert result["summary"]["row_count"] == 2
    assert result["summary"]["column_count"] == 2
    StateStore.clear()


@pytest.mark.asyncio
async def test_upload_input_rejects_empty_base64():
    result = await upload_input_tool._toolkit_upload_input(
        filename="data.csv",
        content_base64="",
    )
    assert result["status"] == "error"
    assert result["code"] == "INPUT_EMPTY_UPLOAD"


@pytest.mark.asyncio
async def test_upload_input_rejects_invalid_base64():
    result = await upload_input_tool._toolkit_upload_input(
        filename="data.csv",
        content_base64="not!!!valid!!!base64",
    )
    assert result["status"] == "error"
    assert result["code"] == "INPUT_INVALID_BASE64"


# ── read_artifact tests ──


@pytest.mark.asyncio
async def test_read_artifact_returns_text_html(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "exports" / "reports" / "diagnostics"
    artifact_dir.mkdir(parents=True)
    artifact = artifact_dir / "run1_diagnostics_report.html"
    artifact.write_text("<html><body>Dashboard</body></html>", encoding="utf-8")
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")

    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(artifact),
    )
    assert result["status"] == "pass"
    assert result["encoding"] == "text"
    assert "<html>" in result["artifact_content"]
    assert result["filename"] == "run1_diagnostics_report.html"
    assert result["media_type"] == "text/html"


@pytest.mark.asyncio
async def test_read_artifact_returns_base64_for_binary(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "exports" / "plots"
    artifact_dir.mkdir(parents=True)
    artifact = artifact_dir / "chart.png"
    raw_bytes = b"\x89PNG\r\n\x1a\nfake_png_data"
    artifact.write_bytes(raw_bytes)
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")

    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(artifact),
    )
    assert result["status"] == "pass"
    assert result["encoding"] == "base64"
    decoded = base64.b64decode(result["content_base64"])
    assert decoded == raw_bytes


@pytest.mark.asyncio
async def test_read_artifact_rejects_traversal():
    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path="../../../etc/passwd",
    )
    assert result["status"] == "error"
    assert result["code"] == "ARTIFACT_PATH_DENIED"
    assert "traversal" in result["message"]


@pytest.mark.asyncio
async def test_read_artifact_rejects_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")
    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(tmp_path / "exports" / "reports" / "nonexistent.html"),
    )
    assert result["status"] == "error"
    assert result["code"] == "ARTIFACT_PATH_DENIED"
    assert "not found" in result["message"].lower()


@pytest.mark.asyncio
async def test_read_artifact_http_mode_rejects_cwd_path(tmp_path, monkeypatch):
    """In HTTP mode (non-stdio), only _ARTIFACT_ROOT is allowed, not CWD."""
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")
    monkeypatch.delenv("ANALYST_MCP_STDIO", raising=False)

    secret = tmp_path / "src" / "secret.py"
    secret.parent.mkdir(parents=True)
    secret.write_text("SECRET_KEY = 'oops'")

    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(secret),
    )
    assert result["status"] == "error"
    assert result["code"] == "ARTIFACT_PATH_DENIED"


@pytest.mark.asyncio
async def test_read_artifact_stdio_mode_allows_cwd_path(tmp_path, monkeypatch):
    """In stdio mode, CWD is an allowed root because the client is local."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")
    monkeypatch.setenv("ANALYST_MCP_STDIO", "true")

    report = tmp_path / "my_report.html"
    report.write_text("<html>local</html>")

    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(report),
    )
    assert result["status"] == "pass"
    assert result["encoding"] == "text"
