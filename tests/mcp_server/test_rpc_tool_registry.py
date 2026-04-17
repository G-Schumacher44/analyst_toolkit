import pytest
from fastapi.testclient import TestClient

from analyst_toolkit.mcp_server.input.models import INPUT_ID_PATTERN
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
    """Verify data_dictionary advertises the shared input_id schema."""
    payload = {"jsonrpc": "2.0", "id": 33, "method": "tools/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    tools = response.json()["result"]["tools"]
    data_dictionary = next(tool for tool in tools if tool["name"] == "data_dictionary")
    input_schema = data_dictionary["inputSchema"]

    assert input_schema["properties"]["input_id"]["pattern"] == INPUT_ID_PATTERN
    assert "mutually exclusive" in input_schema["properties"]["input_id"]["description"].lower()


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
