import pandas as pd
import pytest
from fastapi.testclient import TestClient

import analyst_toolkit.mcp_server.local_artifact_server as artifact_server_module
import analyst_toolkit.mcp_server.tools.cockpit as cockpit_module
import analyst_toolkit.mcp_server.tools.data_dictionary as data_dictionary_tool
from analyst_toolkit.mcp_server.server import TOOL_REGISTRY, app


@pytest.fixture
def reset_artifact_server():
    artifact_server_module._reset_local_artifact_server_for_tests()
    yield
    artifact_server_module._reset_local_artifact_server_for_tests()


def test_rpc_initialize(client):
    """Verify the MCP 'initialize' method via JSON-RPC."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["protocolVersion"] == "2024-05-01"
    assert result["serverInfo"]["name"] == "analyst-toolkit"
    assert "resources" in result["capabilities"]


def test_rpc_tools_list(client):
    """Verify the MCP 'tools/list' method returns registered tool schemas."""
    payload = {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert "tools" in result
    tool_names = [t["name"] for t in result["tools"]]
    assert "diagnostics" in tool_names
    assert "outliers" in tool_names
    assert "get_agent_playbook" in tool_names
    assert "get_user_quickstart" in tool_names
    assert "get_capability_catalog" in tool_names
    assert "get_cockpit_dashboard" in tool_names
    assert "get_pipeline_dashboard" in tool_names
    assert "ensure_artifact_server" in tool_names
    assert "data_dictionary" in tool_names
    assert "register_input" in tool_names
    assert "get_input_descriptor" in tool_names
    assert "preflight_config" in tool_names
    assert "get_job_status" in tool_names
    assert "list_jobs" in tool_names
    assert "get_cockpit_help" not in tool_names
    assert "get_agent_instructions" not in tool_names


def test_rpc_capability_catalog_tool(client):
    """Verify capability catalog exposes editable knobs including fuzzy matching."""
    payload = {
        "jsonrpc": "2.0",
        "id": 24,
        "method": "tools/call",
        "params": {"name": "get_capability_catalog", "arguments": {}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["summary"]["editable_configs"] is True
    assert result["summary"]["auto_heal_template_path"] == "config/auto_heal_request_template.yaml"
    assert (
        result["summary"]["data_dictionary_template_path"]
        == "config/data_dictionary_request_template.yaml"
    )
    assert any(
        p.endswith("fuzzy_matching.settings.<column>.score_cutoff")
        for item in result["highlight_examples"]
        for p in item["paths"]
    )
    assert any(
        item["tool"] == "auto_heal"
        and item["template_path"] == "config/auto_heal_request_template.yaml"
        for item in result["workflow_templates"]
    )
    assert any(
        item["tool"] == "data_dictionary"
        and item["template_path"] == "config/data_dictionary_request_template.yaml"
        for item in result["workflow_templates"]
    )
    modules = {item["tool"]: item for item in result["modules"]}
    final_audit_knobs = {k["path"]: k["default"] for k in modules["final_audit"]["key_knobs"]}
    assert "summary.run" not in final_audit_knobs
    assert final_audit_knobs["final_edits.run"] is True

    global_controls = result["global_controls"]
    plotting_control = next(c for c in global_controls if "Plotting toggles" in c["description"])
    assert plotting_control["path"] == "module-specific"
    assert "outlier_detection.plotting.run" in plotting_control["example_paths"]


def test_rpc_capability_catalog_filters_and_compact(client):
    payload = {
        "jsonrpc": "2.0",
        "id": 32,
        "method": "tools/call",
        "params": {
            "name": "get_capability_catalog",
            "arguments": {
                "module": "normalization",
                "search": "fuzzy",
                "path_prefix": "rules.fuzzy_matching",
                "compact": True,
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert "global_controls" not in result
    assert "highlight_examples" not in result
    assert result["summary"]["filters_applied"]["module"] == "normalization"
    assert result["summary"]["filters_applied"]["compact"] is True
    assert len(result["modules"]) == 1
    assert result["modules"][0]["tool"] == "normalization"
    assert all(
        knob["path"].startswith("rules.fuzzy_matching")
        for knob in result["modules"][0]["key_knobs"]
    )


def test_rpc_user_quickstart_tool(client, monkeypatch):
    """Verify user quickstart tool returns human-readable guide text."""
    monkeypatch.setenv("ANALYST_MCP_ENABLE_TRUSTED_HISTORY_TOOL", "false")
    monkeypatch.setenv("ANALYST_MCP_STDIO", "false")
    payload = {
        "jsonrpc": "2.0",
        "id": 25,
        "method": "tools/call",
        "params": {"name": "get_user_quickstart", "arguments": {}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["content"]["format"] == "markdown"
    assert result["content"]["title"] == "Analyst Toolkit Quickstart"
    assert "fuzzy matching" in result["content"]["markdown"].lower()
    assert "plotting" in result["content"]["markdown"].lower()
    assert "auto_heal" in result["content"]["markdown"]
    assert "cockpit dashboard" in result["content"]["markdown"].lower()
    assert "ensure_artifact_server" in result["content"]["markdown"]
    assert "machine_guide" in result
    assert result["machine_guide"]["ordered_steps"][0]["tool"] == "diagnostics"
    assert result["machine_guide"]["ordered_steps"][1]["tool"] == "infer_configs"
    assert result["machine_guide"]["ordered_steps"][1]["required_inputs"] == ["gcs_path|session_id"]
    assert len(result["quick_actions"]) >= 2
    assert not any(a["tool"] == "get_cockpit_dashboard" for a in result["quick_actions"])
    assert any(a["tool"] == "diagnostics" for a in result["quick_actions"])
    assert any(a["tool"] == "infer_configs" for a in result["quick_actions"])
    assert any(a["tool"] == "auto_heal" for a in result["quick_actions"])
    infer_quick = next(a for a in result["quick_actions"] if a["tool"] == "infer_configs")
    assert infer_quick["arguments_schema_hint"]["required"] == ["gcs_path|session_id"]
    assert isinstance(result.get("trace_id"), str)
    assert result["trace_id"]


def test_rpc_agent_playbook_infer_configs_inputs_allow_path_or_session(client, monkeypatch):
    monkeypatch.setenv("ANALYST_MCP_ENABLE_TRUSTED_HISTORY_TOOL", "false")
    monkeypatch.setenv("ANALYST_MCP_STDIO", "false")
    payload = {
        "jsonrpc": "2.0",
        "id": 31,
        "method": "tools/call",
        "params": {"name": "get_agent_playbook", "arguments": {}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["ordered_steps"][0]["tool"] == "ensure_artifact_server"
    assert result["ordered_steps"][1]["tool"] == "diagnostics"
    infer_step = next(
        step for step in result["ordered_steps"] if step.get("tool") == "infer_configs"
    )
    assert infer_step["required_inputs"] == ["gcs_path|session_id"]
    auto_heal_step = next(
        step for step in result["ordered_steps"] if step.get("tool") == "auto_heal"
    )
    assert auto_heal_step["required_inputs"] == [
        "gcs_path|session_id|runtime.run.input_path",
        "run_id|runtime.run.run_id",
    ]


def test_rpc_ensure_artifact_server_tool(client, monkeypatch, mocker, reset_artifact_server):
    monkeypatch.setenv("ANALYST_MCP_ENABLE_ARTIFACT_SERVER_TOOL", "true")
    mock_ensure = mocker.patch.object(
        cockpit_module,
        "ensure_local_artifact_server",
        return_value={
            "status": "pass",
            "enabled": True,
            "running": True,
            "already_running": False,
            "base_url": "http://127.0.0.1:8765",
            "root": "exports",
        },
    )
    payload = {
        "jsonrpc": "2.0",
        "id": 41,
        "method": "tools/call",
        "params": {"name": "ensure_artifact_server", "arguments": {}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["summary"]["running"] is True
    assert result["summary"]["already_running"] is False
    assert result["base_url"] == "http://127.0.0.1:8765"
    mock_ensure.assert_called_once()


def test_rpc_ensure_artifact_server_tool_disabled(client, monkeypatch, reset_artifact_server):
    monkeypatch.setenv("ANALYST_MCP_ENABLE_ARTIFACT_SERVER_TOOL", "false")
    payload = {
        "jsonrpc": "2.0",
        "id": 42,
        "method": "tools/call",
        "params": {"name": "ensure_artifact_server", "arguments": {}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "error"
    assert result["code"] == "ARTIFACT_SERVER_CONTROL_DISABLED"


def test_rpc_get_config_schema_supports_final_audit(client):
    """Verify get_config_schema returns final_audit schema."""
    payload = {
        "jsonrpc": "2.0",
        "id": 29,
        "method": "tools/call",
        "params": {"name": "get_config_schema", "arguments": {"module_name": "final_audit"}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["module"] == "final_audit"
    props = result["schema"]["properties"]
    assert "certification" in props
    cert_props = props["certification"]["$ref"]
    assert cert_props


def test_rpc_get_config_schema_outliers_matches_runtime_contract_paths(client):
    """Verify outliers schema exposes canonical outlier_detection.detection_specs path."""
    payload = {
        "jsonrpc": "2.0",
        "id": 30,
        "method": "tools/call",
        "params": {"name": "get_config_schema", "arguments": {"module_name": "outliers"}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["module"] == "outliers"
    props = result["schema"]["properties"]
    assert "outlier_detection" in props
    outlier_ref = props["outlier_detection"]["$ref"]
    assert outlier_ref.endswith("OutlierDetectionConfig")
    defs = result["schema"]["$defs"]
    detection_props = defs["OutlierDetectionConfig"]["properties"]
    assert "detection_specs" in detection_props


def test_rpc_tool_not_found(client):
    """Verify proper error handling for a missing tool."""
    payload = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {"name": "non_existent_tool", "arguments": {}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    error = response.json()["error"]
    assert error["code"] == -32601
    assert "Tool not found" in error["message"]


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_builds_tabbed_artifact(mocker):
    history = [
        {
            "module": "diagnostics",
            "status": "pass",
            "session_id": "sess_pipeline",
            "summary": {"rows": 100},
            "dashboard_url": "https://example.com/diag.html",
            "export_url": "gs://bucket/diag.csv",
        },
        {
            "module": "validation",
            "status": "warn",
            "session_id": "sess_pipeline",
            "summary": {"passed": False, "failed_rules": 2},
            "dashboard_path": "exports/reports/validation/run_val.html",
            "warnings": ["rule mismatch"],
        },
        {
            "module": "final_audit",
            "status": "fail",
            "session_id": "sess_pipeline",
            "summary": {"passed": False},
            "dashboard_path": "exports/reports/final_audit/run_final.html",
            "export_url": "gs://bucket/final.csv",
        },
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    export_html = mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/run-pipeline-001_pipeline_dashboard.html",
    )
    append_history = mocker.patch.object(cockpit_module, "append_to_run_history")
    deliver = mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "https://example.com/pipeline.html",
            "local_path": "/tmp/run-pipeline-001_pipeline_dashboard.html",
            "url": "https://example.com/pipeline.html",
            "warnings": [],
            "destinations": {
                "gcs": {"status": "available", "url": "https://example.com/pipeline.html"}
            },
        },
    )

    result = await cockpit_module._toolkit_get_pipeline_dashboard(run_id="run-pipeline-001")

    assert result["status"] == "pass"
    assert result["module"] == "pipeline_dashboard"
    assert result["session_id"] == ""
    assert result["dashboard_label"] == "Pipeline dashboard"
    assert result["artifact_url"] == "https://example.com/pipeline.html"
    assert result["summary"]["failed_modules"] == 1
    assert result["summary"]["warned_modules"] == 1
    assert result["summary"]["ready_modules"] == 1
    assert result["summary"]["not_run_modules"] == 6
    export_html.assert_called_once_with(
        mocker.ANY,
        "exports/reports/pipeline/run-pipeline-001_pipeline_dashboard.html",
        "Pipeline Dashboard",
        "run-pipeline-001",
    )
    deliver.assert_called_once_with(
        "/tmp/run-pipeline-001_pipeline_dashboard.html",
        run_id="run-pipeline-001",
        module="pipeline_dashboard",
        config={},
        session_id=None,
    )
    append_history.assert_called_once_with(
        "run-pipeline-001",
        mocker.ANY,
        session_id=None,
    )


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_sanitizes_run_id_for_artifact_path(mocker):
    mocker.patch.object(cockpit_module, "get_run_history", return_value=[])
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    export_html = mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/pipeline_dashboard.html",
    )
    mocker.patch.object(cockpit_module, "append_to_run_history")
    mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "",
            "local_path": "/tmp/pipeline_dashboard.html",
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    await cockpit_module._toolkit_get_pipeline_dashboard(run_id="../unsafe run")

    export_html.assert_called_once_with(
        mocker.ANY,
        "exports/reports/pipeline/unsafe_run_pipeline_dashboard.html",
        "Pipeline Dashboard",
        "unsafe_run",
    )


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_uses_session_specific_artifact_path(mocker):
    mocker.patch.object(cockpit_module, "get_run_history", return_value=[])
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    export_html = mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/pipeline_dashboard.html",
    )
    mocker.patch.object(cockpit_module, "append_to_run_history")
    mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "",
            "local_path": "/tmp/pipeline_dashboard.html",
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    await cockpit_module._toolkit_get_pipeline_dashboard(
        run_id="run-pipeline-001",
        session_id="session-42",
    )

    export_html.assert_called_once_with(
        mocker.ANY,
        "exports/reports/pipeline/run-pipeline-001_session-42_pipeline_dashboard.html",
        "Pipeline Dashboard",
        "run-pipeline-001",
    )


@pytest.mark.asyncio
async def test_toolkit_get_cockpit_dashboard_builds_operator_hub(mocker):
    mocker.patch.object(cockpit_module, "TRUSTED_HISTORY_ENABLED", True)
    mocker.patch.object(
        cockpit_module,
        "_build_cockpit_dashboard_report",
        return_value={
            "overview": {
                "recent_run_count": 2,
                "warning_runs": 1,
                "failed_runs": 1,
                "pipeline_dashboards_available": 1,
                "auto_heal_dashboards_available": 1,
            },
            "recent_runs": [],
            "resources": [],
            "launchpad": [],
        },
    )
    export_html = mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/cockpit_dashboard.html",
    )
    deliver = mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "https://example.com/cockpit.html",
            "local_path": "/tmp/cockpit_dashboard.html",
            "url": "https://example.com/cockpit.html",
            "warnings": [],
            "destinations": {
                "gcs": {"status": "available", "url": "https://example.com/cockpit.html"}
            },
        },
    )

    result = await cockpit_module._toolkit_get_cockpit_dashboard(limit=5)

    assert result["status"] == "pass"
    assert result["module"] == "cockpit_dashboard"
    assert result["dashboard_label"] == "Cockpit dashboard"
    assert result["artifact_url"] == "https://example.com/cockpit.html"
    assert result["summary"]["recent_run_count"] == 2
    export_html.assert_called_once_with(
        mocker.ANY,
        "exports/reports/cockpit/cockpit_dashboard_limit_5.html",
        "Cockpit Dashboard",
        "cockpit_dashboard_limit_5",
    )
    deliver.assert_called_once_with(
        "/tmp/cockpit_dashboard.html",
        run_id="cockpit_dashboard_limit_5",
        module="cockpit_dashboard",
        config={"upload_artifacts": False},
        session_id=None,
    )


@pytest.mark.asyncio
async def test_toolkit_get_cockpit_dashboard_denies_when_untrusted(mocker):
    mocker.patch.object(cockpit_module, "TRUSTED_HISTORY_ENABLED", False)
    export_html = mocker.patch.object(cockpit_module, "export_html_report")
    deliver = mocker.patch.object(cockpit_module, "deliver_artifact")

    result = await cockpit_module._toolkit_get_cockpit_dashboard(limit=5)

    assert result["status"] == "error"
    assert result["code"] == "COCKPIT_HISTORY_DISABLED"
    export_html.assert_not_called()
    deliver.assert_not_called()


def test_rpc_data_dictionary_tool(client, mocker, tmp_path):
    """Verify tools/call data_dictionary returns artifact-first prelaunch output seeded by inference."""
    dataframe = pd.DataFrame(
        {
            "customer_id": [1, 2],
            "status": ["new", "done"],
            "amount": [10.5, 12.0],
        }
    )
    load_input = mocker.patch.object(data_dictionary_tool, "load_input", return_value=dataframe)
    save_to_session = mocker.patch.object(
        data_dictionary_tool, "save_to_session", return_value="sess_dictionary"
    )
    append_to_run_history = mocker.patch.object(
        data_dictionary_tool, "append_to_run_history", return_value=None
    )
    export_dataframes = mocker.patch.object(
        data_dictionary_tool, "export_dataframes", return_value=None
    )
    export_html_report = mocker.patch.object(
        data_dictionary_tool,
        "export_html_report",
        return_value=str(tmp_path / "dictionary.html"),
    )
    deliver_artifact = mocker.patch.object(
        data_dictionary_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": local_path,
            "local_path": local_path,
            "url": "" if local_path.endswith(".xlsx") else "https://example.com/dictionary.html",
            "warnings": [],
            "destinations": {},
        },
    )
    infer_configs = mocker.patch.object(
        data_dictionary_tool,
        "_toolkit_infer_configs",
        mocker.AsyncMock(
            return_value={
                "status": "pass",
                "configs": {
                    "validation": (
                        "validation:\n"
                        "  schema_validation:\n"
                        "    rules:\n"
                        "      expected_columns: [customer_id, status, amount]\n"
                        "      categorical_values:\n"
                        "        status: [new, done]\n"
                    ),
                    "normalization": (
                        "normalization:\n  rules:\n    coerce_dtypes:\n      amount: float64\n"
                    ),
                },
                "warnings": [],
            }
        ),
    )

    payload = {
        "jsonrpc": "2.0",
        "id": 41,
        "method": "tools/call",
        "params": {
            "name": "data_dictionary",
            "arguments": {
                "gcs_path": "gs://bucket/data.csv",
                "run_id": "dictionary_prelaunch_001",
                "prelaunch_report": True,
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] in {"pass", "warn"}
    assert result["module"] == "data_dictionary"
    assert result["template_path"] == "config/data_dictionary_request_template.yaml"
    assert result["summary"]["prelaunch_report"] is True
    assert result["summary"]["inference_status"] == "pass"
    assert result["dashboard_label"] == "Data dictionary dashboard"
    assert result["artifact_url"] == "https://example.com/dictionary.html"
    assert result["xlsx_path"].endswith("dictionary_prelaunch_001_data_dictionary_report.xlsx")
    assert result["cockpit_preview"]["overview"]["rows"] == 2
    assert result["cockpit_preview"]["overview"]["expected_columns"] == 3
    assert result["cockpit_preview"]["expected_schema_preview"][0]["Column"] == "customer_id"
    assert result["next_actions"][0]["tool"] == "get_cockpit_dashboard"
    load_input.assert_called_once_with("gs://bucket/data.csv", session_id=None, input_id=None)
    save_to_session.assert_called_once_with(dataframe, run_id="dictionary_prelaunch_001")
    infer_configs.assert_awaited_once_with(
        gcs_path="gs://bucket/data.csv",
        session_id="sess_dictionary",
        runtime=None,
        run_id="dictionary_prelaunch_001",
    )
    export_dataframes.assert_called_once()
    export_html_report.assert_called_once_with(
        mocker.ANY,
        "exports/reports/data_dictionary/dictionary_prelaunch_001_data_dictionary_report.html",
        "Data Dictionary",
        "dictionary_prelaunch_001",
    )
    assert deliver_artifact.call_count == 2
    append_to_run_history.assert_called_once_with(
        "dictionary_prelaunch_001", mocker.ANY, session_id="sess_dictionary"
    )


@pytest.mark.asyncio
async def test_rpc_tool_invocation_structure(mocker):
    """
    Verify that tools/call correctly dispatches to the registered function.
    Mocks the actual diagnostics tool to avoid data loading/GCS overhead.
    """
    mock_result = {"status": "pass", "module": "diagnostics", "summary": {"test": True}}

    mocker.patch.dict(
        TOOL_REGISTRY["diagnostics"], {"fn": mocker.AsyncMock(return_value=mock_result)}
    )

    client = TestClient(app)
    payload = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "diagnostics",
            "arguments": {"gcs_path": "gs://fake/data.parquet"},
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == mock_result["status"]
    assert result["module"] == mock_result["module"]
    assert result["summary"] == mock_result["summary"]
    assert isinstance(result.get("trace_id"), str)
