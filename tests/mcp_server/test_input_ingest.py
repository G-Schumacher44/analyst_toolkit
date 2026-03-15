from pathlib import Path

import pandas as pd
import pytest

from analyst_toolkit.mcp_server.input import registry as input_registry
from analyst_toolkit.mcp_server.state import StateStore
from analyst_toolkit.mcp_server.tools import diagnostics as diagnostics_tool


@pytest.fixture
def clean_input_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ANALYST_MCP_INPUT_ROOT", str(tmp_path / "inputs"))
    monkeypatch.setenv("ANALYST_MCP_ALLOWED_INPUT_ROOTS", str(tmp_path))
    StateStore.clear()
    input_registry.clear()
    return tmp_path


def _write_sample_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "species": ["Adelie", "Gentoo"],
            "bill_length_mm": [39.1, 46.5],
        }
    ).to_csv(path, index=False)


def test_inputs_upload_creates_session_and_descriptor(client, clean_input_env):
    response = client.post(
        "/inputs/upload",
        files={
            "file": ("dirty_penguins.csv", b"species,bill_length_mm\nAdelie,39.1\n", "text/csv")
        },
        data={"load_into_session": "true"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "pass"
    assert payload["session_id"].startswith("sess_")
    assert payload["input"]["source_type"] == "upload"
    assert payload["summary"]["row_count"] == 1
    assert payload["summary"]["column_count"] == 2


def test_inputs_upload_rejects_payload_over_limit(client, monkeypatch, clean_input_env):
    monkeypatch.setattr(
        "analyst_toolkit.mcp_server.input.storage._MAX_UPLOAD_BYTES",
        8,
    )

    response = client.post(
        "/inputs/upload",
        files={"file": ("dirty_penguins.csv", b"species,bill_length_mm\n", "text/csv")},
        data={"load_into_session": "false"},
    )

    assert response.status_code == 413
    detail = response.json()["detail"]
    assert detail["code"] == "INPUT_PAYLOAD_TOO_LARGE"
    assert isinstance(detail["trace_id"], str)


def test_inputs_upload_rejects_empty_payload(client, clean_input_env):
    response = client.post(
        "/inputs/upload",
        files={"file": ("empty.csv", b"", "text/csv")},
        data={"load_into_session": "false"},
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "INPUT_EMPTY_UPLOAD"
    assert isinstance(detail["trace_id"], str)


def test_inputs_upload_reuses_input_id_for_same_payload(client, clean_input_env):
    response_one = client.post(
        "/inputs/upload",
        files={
            "file": ("dirty_penguins.csv", b"species,bill_length_mm\nAdelie,39.1\n", "text/csv")
        },
        data={"load_into_session": "false"},
    )
    response_two = client.post(
        "/inputs/upload",
        files={
            "file": ("dirty_penguins.csv", b"species,bill_length_mm\nAdelie,39.1\n", "text/csv")
        },
        data={"load_into_session": "false"},
    )

    assert response_one.status_code == 200
    assert response_two.status_code == 200
    assert response_one.json()["status"] == "pass"
    assert response_two.json()["status"] == "pass"
    assert response_one.json()["input"]["input_id"] == response_two.json()["input"]["input_id"]


def test_inputs_register_server_path_loads_into_session(client, clean_input_env):
    tmp_path = clean_input_env

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


def test_inputs_register_gcs_source_without_session_load(client, clean_input_env):
    response = client.post(
        "/inputs/register",
        json={"uri": "gs://bucket/dirty_penguins.csv", "load_into_session": False},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "pass"
    assert payload["input"]["source_type"] == "gcs"
    assert payload["input"]["resolved_reference"] == "gs://bucket/dirty_penguins.csv"
    assert payload["session_id"] == ""
    assert payload["summary"] == {}


def test_inputs_register_reuses_input_id_with_stable_idempotency_key(client, clean_input_env):
    tmp_path = clean_input_env

    source = tmp_path / "dirty_penguins.csv"
    _write_sample_csv(source)

    response_one = client.post(
        "/inputs/register",
        json={
            "uri": str(source),
            "load_into_session": False,
            "idempotency_key": "stable-register-key",
        },
    )
    response_two = client.post(
        "/inputs/register",
        json={
            "uri": str(source),
            "load_into_session": False,
            "idempotency_key": "stable-register-key",
        },
    )

    assert response_one.status_code == 200
    assert response_two.status_code == 200
    assert response_one.json()["status"] == "pass"
    assert response_two.json()["status"] == "pass"
    assert response_one.json()["input"]["input_id"] == response_two.json()["input"]["input_id"]


def test_inputs_register_uses_distinct_input_ids_for_distinct_idempotency_keys(
    client, clean_input_env
):
    tmp_path = clean_input_env

    source = tmp_path / "dirty_penguins.csv"
    _write_sample_csv(source)

    response_one = client.post(
        "/inputs/register",
        json={
            "uri": str(source),
            "load_into_session": False,
            "idempotency_key": "stable-register-key-a",
        },
    )
    response_two = client.post(
        "/inputs/register",
        json={
            "uri": str(source),
            "load_into_session": False,
            "idempotency_key": "stable-register-key-b",
        },
    )

    assert response_one.status_code == 200
    assert response_two.status_code == 200
    assert response_one.json()["status"] == "pass"
    assert response_two.json()["status"] == "pass"
    assert response_one.json()["input"]["input_id"] != response_two.json()["input"]["input_id"]


def test_inputs_register_rejects_path_outside_allowed_roots(client, monkeypatch, clean_input_env):
    tmp_path = clean_input_env
    monkeypatch.setenv("ANALYST_MCP_ALLOWED_INPUT_ROOTS", str(tmp_path / "allowed"))

    source = tmp_path / "dirty_penguins.csv"
    _write_sample_csv(source)

    response = client.post(
        "/inputs/register",
        json={"uri": str(source), "load_into_session": True},
    )
    assert response.status_code == 400
    assert "not visible to the MCP runtime" in response.json()["detail"]["error"]


def test_inputs_register_rejects_unsupported_local_format(client, clean_input_env):
    tmp_path = clean_input_env
    source = tmp_path / "dirty_penguins.json"
    source.write_text('{"species":"Adelie"}', encoding="utf-8")

    response = client.post(
        "/inputs/register",
        json={"uri": str(source), "load_into_session": True},
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "INPUT_NOT_SUPPORTED"
    assert "Unsupported file format" in detail["error"]
    assert isinstance(detail["trace_id"], str)


def test_inputs_register_rejects_gdrive_source(client, clean_input_env):
    response = client.post(
        "/inputs/register",
        json={"uri": "gdrive://folder/dirty_penguins.csv", "load_into_session": False},
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "INPUT_NOT_SUPPORTED"
    assert "Google Drive inputs are not implemented yet" in detail["error"]
    assert isinstance(detail["trace_id"], str)


def test_inputs_register_returns_conflict_for_descriptor_reuse_mismatch(client, clean_input_env):
    tmp_path = clean_input_env

    source_one = tmp_path / "dirty_penguins.csv"
    source_two = tmp_path / "clean_penguins.csv"
    _write_sample_csv(source_one)
    _write_sample_csv(source_two)

    first = client.post(
        "/inputs/register",
        json={
            "uri": str(source_one),
            "load_into_session": False,
            "idempotency_key": "shared-key",
        },
    )
    second = client.post(
        "/inputs/register",
        json={
            "uri": str(source_two),
            "load_into_session": False,
            "idempotency_key": "shared-key",
        },
    )

    assert first.status_code == 200
    assert second.status_code == 409
    detail = second.json()["detail"]
    assert detail["code"] == "INPUT_CONFLICT"
    assert isinstance(detail["trace_id"], str)


def test_get_input_descriptor_tool_returns_not_found_for_unknown_input_id(client, clean_input_env):
    payload = {
        "jsonrpc": "2.0",
        "id": 903,
        "method": "tools/call",
        "params": {
            "name": "get_input_descriptor",
            "arguments": {"input_id": "input_deadbeefcafe"},
        },
    }

    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "error"
    assert result["module"] == "get_input_descriptor"
    assert result["code"] == "INPUT_NOT_FOUND"
    assert isinstance(result["trace_id"], str)


def test_register_input_tool_and_diagnostics_input_id_flow(client, mocker, clean_input_env):
    tmp_path = clean_input_env
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
