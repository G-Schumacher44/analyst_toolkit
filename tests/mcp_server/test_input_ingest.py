from pathlib import Path

import pandas as pd

from analyst_toolkit.mcp_server.input import registry as input_registry
from analyst_toolkit.mcp_server.state import StateStore
from analyst_toolkit.mcp_server.tools import diagnostics as diagnostics_tool


def _write_sample_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "species": ["Adelie", "Gentoo"],
            "bill_length_mm": [39.1, 46.5],
        }
    ).to_csv(path, index=False)


def test_inputs_upload_creates_session_and_descriptor(client, monkeypatch, tmp_path):
    monkeypatch.setenv("ANALYST_MCP_INPUT_ROOT", str(tmp_path / "inputs"))
    monkeypatch.setenv("ANALYST_MCP_ALLOWED_INPUT_ROOTS", str(tmp_path))
    StateStore.clear()
    input_registry.clear()

    response = client.post(
        "/inputs/upload",
        files={"file": ("dirty_penguins.csv", b"species,bill_length_mm\nAdelie,39.1\n", "text/csv")},
        data={"load_into_session": "true"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "pass"
    assert payload["session_id"].startswith("sess_")
    assert payload["input"]["source_type"] == "upload"
    assert payload["summary"]["row_count"] == 1
    assert payload["summary"]["column_count"] == 2


def test_inputs_register_server_path_loads_into_session(client, monkeypatch, tmp_path):
    monkeypatch.setenv("ANALYST_MCP_INPUT_ROOT", str(tmp_path / "inputs"))
    monkeypatch.setenv("ANALYST_MCP_ALLOWED_INPUT_ROOTS", str(tmp_path))
    StateStore.clear()
    input_registry.clear()

    source = tmp_path / "dirty_penguins.csv"
    _write_sample_csv(source)

    response = client.post(
        "/inputs/register",
        json={"uri": str(source), "load_into_session": True},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "pass"
    assert payload["input"]["source_type"] == "server_path"
    assert payload["session_id"].startswith("sess_")
    assert payload["summary"]["row_count"] == 2


def test_inputs_register_rejects_path_outside_allowed_roots(client, monkeypatch, tmp_path):
    monkeypatch.setenv("ANALYST_MCP_INPUT_ROOT", str(tmp_path / "inputs"))
    monkeypatch.setenv("ANALYST_MCP_ALLOWED_INPUT_ROOTS", str(tmp_path / "allowed"))
    StateStore.clear()
    input_registry.clear()

    source = tmp_path / "dirty_penguins.csv"
    _write_sample_csv(source)

    response = client.post(
        "/inputs/register",
        json={"uri": str(source), "load_into_session": True},
    )
    assert response.status_code == 400
    assert "not visible to the MCP runtime" in response.json()["detail"]["error"]


def test_register_input_tool_and_diagnostics_input_id_flow(client, monkeypatch, mocker, tmp_path):
    monkeypatch.setenv("ANALYST_MCP_INPUT_ROOT", str(tmp_path / "inputs"))
    monkeypatch.setenv("ANALYST_MCP_ALLOWED_INPUT_ROOTS", str(tmp_path))
    StateStore.clear()
    input_registry.clear()
    mocker.patch.object(diagnostics_tool, "run_diag_pipeline", return_value=None)
    mocker.patch.object(diagnostics_tool, "save_output", return_value="gs://dummy/diagnostics.csv")
    mocker.patch.object(diagnostics_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(diagnostics_tool, "should_export_html", return_value=False)

    source = tmp_path / "dirty_penguins.csv"
    _write_sample_csv(source)

    register_payload = {
        "jsonrpc": "2.0",
        "id": 901,
        "method": "tools/call",
        "params": {
            "name": "register_input",
            "arguments": {"uri": str(source), "load_into_session": True},
        },
    }
    register_response = client.post("/rpc", json=register_payload)
    assert register_response.status_code == 200
    register_result = register_response.json()["result"]
    assert register_result["status"] == "pass"
    input_id = register_result["input"]["input_id"]

    diagnostics_payload = {
        "jsonrpc": "2.0",
        "id": 902,
        "method": "tools/call",
        "params": {
            "name": "diagnostics",
            "arguments": {
                "input_id": input_id,
                "export_path": str(tmp_path / "diagnostics.csv"),
            },
        },
    }
    diagnostics_response = client.post("/rpc", json=diagnostics_payload)
    assert diagnostics_response.status_code == 200
    diagnostics_result = diagnostics_response.json()["result"]
    assert diagnostics_result["status"] in {"pass", "warn"}
    assert diagnostics_result["module"] == "diagnostics"
    assert diagnostics_result["summary"]["row_count"] == 2
