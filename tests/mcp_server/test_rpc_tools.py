import pandas as pd
import pytest
from fastapi.testclient import TestClient

import analyst_toolkit.mcp_server.local_artifact_server as artifact_server_module
import analyst_toolkit.mcp_server.tools.cockpit as cockpit_module
import analyst_toolkit.mcp_server.tools.cockpit_history as cockpit_history_module
import analyst_toolkit.mcp_server.tools.data_dictionary as data_dictionary_tool
from analyst_toolkit.mcp_server.input.models import INPUT_ID_PATTERN
from analyst_toolkit.mcp_server.server import TOOL_REGISTRY, app
from analyst_toolkit.mcp_server.tools.cockpit_templates import (
    build_cockpit_resource_groups,
    build_cockpit_resources,
)


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


def test_rpc_tools_list_exposes_data_dictionary_input_id_schema(client):
    """Verify data_dictionary advertises the shared input_id and anyOf input contract."""
    payload = {"jsonrpc": "2.0", "id": 33, "method": "tools/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    tools = response.json()["result"]["tools"]
    data_dictionary = next(tool for tool in tools if tool["name"] == "data_dictionary")
    input_schema = data_dictionary["inputSchema"]

    assert input_schema["properties"]["input_id"]["pattern"] == INPUT_ID_PATTERN
    assert input_schema["properties"]["input_id"]["description"].endswith(
        "If provided, gcs_path and session_id are ignored."
    )
    assert {"required": ["input_id"]} in input_schema["anyOf"]
    assert {"required": ["gcs_path"]} in input_schema["anyOf"]
    assert {"required": ["session_id"]} in input_schema["anyOf"]


def test_rpc_tools_list_standardizes_input_id_pattern_across_tool_schemas(client):
    """Verify all remaining bespoke tool schemas reuse the shared input_id contract."""
    payload = {"jsonrpc": "2.0", "id": 34, "method": "tools/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    tools = {tool["name"]: tool for tool in response.json()["result"]["tools"]}

    for tool_name in ("infer_configs", "auto_heal", "get_input_descriptor"):
        assert tool_name in tools, f"Expected tool '{tool_name}' not found in tools/list response"
        input_id_schema = tools[tool_name]["inputSchema"]["properties"]["input_id"]
        assert input_id_schema["pattern"] == INPUT_ID_PATTERN
        assert "Canonical server-managed input reference" in input_id_schema["description"]


def test_build_cockpit_resource_groups_uses_reference_lookup_not_position() -> None:
    resources = build_cockpit_resources()
    reordered = [resources[5], resources[2], resources[0], resources[4], resources[1], resources[3]]

    groups = build_cockpit_resource_groups(reordered)

    assert [item["Reference"] for item in groups[0]["items"]] == [
        "analyst://docs/quickstart",
        "analyst://docs/agent-playbook",
        "analyst://templates/config/runtime_overlay_template.yaml",
    ]
    assert [item["Reference"] for item in groups[1]["items"]] == [
        "analyst://templates/config/runtime_overlay_template.yaml",
        "analyst://templates/config/auto_heal_request_template.yaml",
        "analyst://templates/config/data_dictionary_request_template.yaml",
    ]
    assert [item["Reference"] for item in groups[2]["items"]] == ["analyst://catalog/capabilities"]


def test_build_cockpit_resource_groups_raises_clear_error_for_missing_reference() -> None:
    resources = [
        resource
        for resource in build_cockpit_resources()
        if resource["Reference"] != "analyst://docs/agent-playbook"
    ]

    with pytest.raises(
        ValueError,
        match=(
            "build_cockpit_resource_groups missing resource reference: "
            "analyst://docs/agent-playbook"
        ),
    ):
        build_cockpit_resource_groups(resources)


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
    assert result["format"] == "markdown"
    assert result["title"] == "Analyst Toolkit Quickstart"
    assert "fuzzy matching" in result["markdown"].lower()
    assert "plotting" in result["markdown"].lower()
    assert "auto_heal" in result["markdown"]
    assert "cockpit dashboard" in result["markdown"].lower()
    assert "ensure_artifact_server" in result["markdown"]
    assert "register_input" in result["markdown"]
    assert "/inputs/upload" in result["markdown"]
    assert "machine_guide" in result
    assert result["machine_guide"]["ordered_steps"][0]["tool"] == "register_input"
    assert result["machine_guide"]["ordered_steps"][1]["tool"] == "diagnostics"
    assert result["machine_guide"]["ordered_steps"][1]["required_inputs"] == [
        "input_id|gcs_path|session_id|runtime.run.input_path",
        "run_id|runtime.run.run_id",
    ]
    assert result["machine_guide"]["ordered_steps"][2]["tool"] == "infer_configs"
    assert result["machine_guide"]["ordered_steps"][2]["required_inputs"] == [
        "input_id|gcs_path|session_id"
    ]
    assert len(result["quick_actions"]) >= 2
    assert not any(a["tool"] == "get_cockpit_dashboard" for a in result["quick_actions"])
    assert any(a["tool"] == "register_input" for a in result["quick_actions"])
    assert any(a["tool"] == "diagnostics" for a in result["quick_actions"])
    assert any(a["tool"] == "infer_configs" for a in result["quick_actions"])
    assert any(a["tool"] == "auto_heal" for a in result["quick_actions"])
    infer_quick = next(a for a in result["quick_actions"] if a["tool"] == "infer_configs")
    assert infer_quick["arguments_schema_hint"]["required"] == ["input_id|gcs_path|session_id"]
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
    assert result["ordered_steps"][1]["tool"] == "register_input"
    assert result["ordered_steps"][2]["tool"] == "diagnostics"
    infer_step = next(
        step for step in result["ordered_steps"] if step.get("tool") == "infer_configs"
    )
    assert infer_step["required_inputs"] == ["input_id|gcs_path|session_id"]
    auto_heal_step = next(
        step for step in result["ordered_steps"] if step.get("tool") == "auto_heal"
    )
    assert auto_heal_step["required_inputs"] == [
        "input_id|gcs_path|session_id|runtime.run.input_path",
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
    assert result["summary"]["not_run_modules"] == 5
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
async def test_toolkit_get_pipeline_dashboard_hides_internal_outlier_handling_stage(mocker):
    history = [
        {
            "module": "outlier_handling",
            "status": "pass",
            "session_id": "sess_pipeline",
            "summary": {"handled_rows": 5},
        }
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    captured_report: dict[str, object] = {}

    def fake_export_html(report, artifact_path, title, safe_run_id):
        captured_report["report"] = report
        return "/tmp/internal-filtered_pipeline_dashboard.html"

    mocker.patch.object(cockpit_module, "export_html_report", side_effect=fake_export_html)
    mocker.patch.object(cockpit_module, "append_to_run_history")
    mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "",
            "local_path": "/tmp/internal-filtered_pipeline_dashboard.html",
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await cockpit_module._toolkit_get_pipeline_dashboard(run_id="run-pipeline-002")

    assert result["status"] == "pass"
    report = captured_report["report"]
    assert isinstance(report, dict)
    assert "Outlier Handling" not in report["module_order"]
    assert "Outlier Handling" not in report["modules"]


def test_build_data_health_report_marks_failed_final_audit_as_advisory():
    health = cockpit_module.build_data_health_report(
        run_id="run-health-001",
        session_id="sess-health-001",
        history=[
            {
                "module": "diagnostics",
                "status": "pass",
                "summary": {"null_rate": 0.0, "row_count": 100},
            },
            {
                "module": "validation",
                "status": "pass",
                "summary": {"passed": True, "row_count": 100},
            },
            {
                "module": "final_audit",
                "status": "fail",
                "summary": {"passed": False, "row_count": 100},
            },
        ],
        history_meta={"parse_errors": [], "skipped_records": 0},
    )

    assert health["status"] == "warn"
    assert health["health_score"] == 100.0
    assert health["health_advisory"] is True
    assert health["certification_status"] == "fail"
    assert health["certification_passed"] is False
    assert "Advisory Data Health Score" in health["message"]
    assert any("final_audit reported certification failures" in msg for msg in health["warnings"])


def test_build_data_health_report_tolerates_malformed_final_audit_summary():
    health = cockpit_module.build_data_health_report(
        run_id="run-health-002",
        session_id="sess-health-002",
        history=[
            {
                "module": "final_audit",
                "status": "fail",
                "summary": ["unexpected", "shape"],
            }
        ],
        history_meta={"parse_errors": [], "skipped_records": 0},
    )

    assert health["status"] == "warn"
    assert health["health_advisory"] is True
    assert health["certification_status"] == "fail"
    assert health["certification_passed"] is None


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_surfaces_advisory_health_when_final_audit_failed(
    mocker,
):
    history = [
        {
            "module": "final_audit",
            "status": "fail",
            "summary": {"passed": False},
            "dashboard_path": "exports/reports/final_audit/run_final.html",
        }
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/run-health-001_pipeline_dashboard.html",
    )
    append_history = mocker.patch.object(cockpit_module, "append_to_run_history")
    mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "https://example.com/pipeline.html",
            "local_path": "/tmp/run-health-001_pipeline_dashboard.html",
            "url": "https://example.com/pipeline.html",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await cockpit_module._toolkit_get_pipeline_dashboard(run_id="run-health-001")

    assert result["summary"]["health_advisory"] is True
    assert result["summary"]["certification_status"] == "fail"
    assert any("Health score is advisory only" in warning for warning in result["warnings"])
    append_history.assert_called_once()


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
async def test_toolkit_get_pipeline_dashboard_does_not_append_duplicate_history_on_retry(mocker):
    history = [
        {
            "module": "pipeline_dashboard",
            "status": "pass",
            "session_id": "",
            "artifact_path": "exports/reports/pipeline/run-pipeline-001_pipeline_dashboard.html",
            "artifact_url": "https://example.com/pipeline.html",
            "summary": {"health_score": 95},
        }
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/run-pipeline-001_pipeline_dashboard.html",
    )
    append_history = mocker.patch.object(cockpit_module, "append_to_run_history")
    mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "https://example.com/pipeline.html",
            "local_path": "/tmp/run-pipeline-001_pipeline_dashboard.html",
            "url": "https://example.com/pipeline.html",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await cockpit_module._toolkit_get_pipeline_dashboard(run_id="run-pipeline-001")

    assert result["status"] == "pass"
    append_history.assert_not_called()


@pytest.mark.asyncio
async def test_toolkit_get_cockpit_dashboard_builds_operator_hub(mocker):
    mocker.patch.object(cockpit_module, "_trusted_history_enabled", return_value=True)
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
    mocker.patch.object(cockpit_module, "_trusted_history_enabled", return_value=False)
    export_html = mocker.patch.object(cockpit_module, "export_html_report")
    deliver = mocker.patch.object(cockpit_module, "deliver_artifact")

    result = await cockpit_module._toolkit_get_cockpit_dashboard(limit=5)

    assert result["status"] == "error"
    assert result["code"] == "COCKPIT_HISTORY_DISABLED"
    assert isinstance(result["trace_id"], str)
    assert result["trace_id"]
    export_html.assert_not_called()
    deliver.assert_not_called()


def test_build_recent_run_cards_discovers_local_dashboards(mocker, tmp_path, monkeypatch):
    history_file = tmp_path / "exports" / "reports" / "history" / "run_local_history.json"
    history_file.parent.mkdir(parents=True, exist_ok=True)
    history_file.write_text("[]", encoding="utf-8")
    run_id = "run_local"
    auto_heal_path = (
        tmp_path / "exports" / "reports" / "auto_heal" / f"{run_id}_auto_heal_report.html"
    )
    final_audit_path = (
        tmp_path / "exports" / "reports" / "final_audit" / f"{run_id}_final_audit_report.html"
    )
    pipeline_path = (
        tmp_path / "exports" / "reports" / "pipeline" / f"{run_id}_pipeline_dashboard.html"
    )
    for artifact in (auto_heal_path, final_audit_path, pipeline_path):
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text("<html></html>", encoding="utf-8")

    mocker.patch.object(
        cockpit_history_module,
        "_iter_recent_history_files",
        return_value=[history_file],
    )
    mocker.patch.object(
        cockpit_history_module,
        "_read_history_entries",
        return_value=[
            {
                "run_id": run_id,
                "session_id": "",
                "module": "diagnostics",
                "status": "pass",
                "timestamp": "2026-03-22T12:00:00Z",
                "warnings": [],
            }
        ],
    )
    mocker.patch.object(
        cockpit_history_module,
        "build_data_health_report",
        return_value={"health_score": 94.0, "health_status": "green"},
    )
    mocker.patch.object(cockpit_history_module, "_WORKSPACE_ROOT", tmp_path)
    monkeypatch.chdir(tmp_path)

    cards = cockpit_module._build_recent_run_cards(limit=5)

    assert len(cards) == 1
    card = cards[0]
    assert card["pipeline_dashboard"].endswith(f"{run_id}_pipeline_dashboard.html")
    assert card["auto_heal_dashboard"].endswith(f"{run_id}_auto_heal_report.html")
    assert card["final_audit_dashboard"].endswith(f"{run_id}_final_audit_report.html")
    assert card["best_dashboard"] == card["final_audit_dashboard"]


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


def test_rpc_data_dictionary_tool_passes_explicit_input_id(client, mocker, tmp_path):
    dataframe = pd.DataFrame(
        {
            "customer_id": [1, 2],
            "status": ["new", "done"],
            "amount": [10.5, 12.0],
        }
    )
    load_input = mocker.patch.object(data_dictionary_tool, "load_input", return_value=dataframe)
    mocker.patch.object(data_dictionary_tool, "save_to_session", return_value="sess_dictionary")
    mocker.patch.object(data_dictionary_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(data_dictionary_tool, "export_dataframes", return_value=None)
    mocker.patch.object(
        data_dictionary_tool,
        "export_html_report",
        return_value=str(tmp_path / "dictionary.html"),
    )
    mocker.patch.object(
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
    mocker.patch.object(
        data_dictionary_tool,
        "_toolkit_infer_configs",
        mocker.AsyncMock(return_value={"status": "pass", "configs": {}, "warnings": []}),
    )

    payload = {
        "jsonrpc": "2.0",
        "id": 42,
        "method": "tools/call",
        "params": {
            "name": "data_dictionary",
            "arguments": {
                "gcs_path": "gs://bucket/data.csv",
                "input_id": "input_deadbeefcafebabe",
                "run_id": "dictionary_prelaunch_002",
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] in {"pass", "warn"}
    load_input.assert_called_once_with(
        "gs://bucket/data.csv",
        session_id=None,
        input_id="input_deadbeefcafebabe",
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
