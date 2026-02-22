"""
test_mcp_server.py — Smoke tests for the analyst_toolkit MCP server.

Uses FastAPI's TestClient to verify the JSON-RPC 2.0 dispatcher
without requiring a live network port.
"""

import pytest
from fastapi.testclient import TestClient

from analyst_toolkit.mcp_server.server import TOOL_REGISTRY, app

client = TestClient(app)


def test_health_check():
    """Verify the /health endpoint returns the registered tools."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "toolkit_diagnostics" in data["tools"]
    assert "toolkit_outliers" in data["tools"]


def test_rpc_initialize():
    """Verify the MCP 'initialize' method via JSON-RPC."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["protocolVersion"] == "2024-05-01"
    assert result["serverInfo"]["name"] == "analyst-toolkit"


def test_rpc_tools_list():
    """Verify the MCP 'tools/list' method returns registered tool schemas."""
    payload = {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert "tools" in result
    tool_names = [t["name"] for t in result["tools"]]
    assert "toolkit_diagnostics" in tool_names
    assert "toolkit_outliers" in tool_names


def test_rpc_tool_not_found():
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
    # Mock the tool function
    mock_result = {"status": "pass", "module": "diagnostics", "summary": {"test": True}}

    # We need to mock the function in the registry since server.py already imported it
    mocker.patch.dict(
        TOOL_REGISTRY["toolkit_diagnostics"], {"fn": mocker.AsyncMock(return_value=mock_result)}
    )

    payload = {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "toolkit_diagnostics",
            "arguments": {"gcs_path": "gs://fake/data.parquet"},
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    assert response.json()["result"] == mock_result
