import pytest
from fastapi.testclient import TestClient

from analyst_toolkit.mcp_server.server import TOOL_REGISTRY, app


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
    assert any(
        p.endswith("fuzzy_matching.settings.<column>.score_cutoff")
        for item in result["highlight_examples"]
        for p in item["paths"]
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


def test_rpc_user_quickstart_tool(client):
    """Verify user quickstart tool returns human-readable guide text."""
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
    assert "machine_guide" in result
    assert result["machine_guide"]["ordered_steps"][1]["tool"] == "infer_configs"
    assert result["machine_guide"]["ordered_steps"][1]["required_inputs"] == ["gcs_path|session_id"]
    assert len(result["quick_actions"]) >= 2
    assert any(a["tool"] == "diagnostics" for a in result["quick_actions"])
    assert any(a["tool"] == "infer_configs" for a in result["quick_actions"])
    infer_quick = next(a for a in result["quick_actions"] if a["tool"] == "infer_configs")
    assert infer_quick["arguments_schema_hint"]["required"] == ["gcs_path|session_id"]
    assert isinstance(result.get("trace_id"), str)
    assert result["trace_id"]


def test_rpc_agent_playbook_infer_configs_inputs_allow_path_or_session(client):
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
    infer_step = next(
        step for step in result["ordered_steps"] if step.get("tool") == "infer_configs"
    )
    assert infer_step["required_inputs"] == ["gcs_path|session_id"]


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
