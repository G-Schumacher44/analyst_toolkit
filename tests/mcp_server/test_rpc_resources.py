import asyncio
import time

import pytest

import analyst_toolkit.mcp_server.server as server_module
import analyst_toolkit.mcp_server.tools.cockpit as cockpit_module


def test_rpc_resources_list(client):
    """Verify template resources are discoverable via MCP resources/list."""
    payload = {"jsonrpc": "2.0", "id": 20, "method": "resources/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert "resources" in result
    uris = [r["uri"] for r in result["resources"]]
    assert any(uri.startswith("analyst://templates/golden/") for uri in uris)
    assert any(uri.startswith("analyst://templates/config/") for uri in uris)


def test_rpc_resource_templates_list(client):
    """Verify MCP resources/templates/list is empty by default to avoid client duplication."""
    payload = {"jsonrpc": "2.0", "id": 23, "method": "resources/templates/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert "resourceTemplates" in result
    assert result["resourceTemplates"] == []


def test_rpc_resource_templates_list_when_enabled(client, monkeypatch):
    """Verify MCP resources/templates/list returns URI templates when explicitly enabled."""
    monkeypatch.setattr(server_module, "ADVERTISE_RESOURCE_TEMPLATES", True)
    payload = {"jsonrpc": "2.0", "id": 35, "method": "resources/templates/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    template_uris = [t["uriTemplate"] for t in result["resourceTemplates"]]
    assert "analyst://templates/config/{name}_template.yaml" in template_uris
    assert "analyst://templates/golden/{name}.yaml" in template_uris


def test_rpc_resources_read(client):
    """Verify MCP resources/read returns YAML for a known template URI."""
    payload = {
        "jsonrpc": "2.0",
        "id": 21,
        "method": "resources/read",
        "params": {"uri": "analyst://templates/golden/fraud_detection.yaml"},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    contents = response.json()["result"]["contents"]
    assert len(contents) == 1
    assert contents[0]["uri"] == "analyst://templates/golden/fraud_detection.yaml"
    assert "fraud" in contents[0]["text"].lower()


def test_rpc_resources_read_not_found(client):
    """Verify resources/read returns invalid params for unknown resource URI."""
    payload = {
        "jsonrpc": "2.0",
        "id": 22,
        "method": "resources/read",
        "params": {"uri": "analyst://templates/golden/does_not_exist.yaml"},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    error = response.json()["error"]
    assert error["code"] == -32602
    assert "Resource not found" in error["message"]
    assert error["data"]["error"]["code"] == "resource_not_found"
    assert error["data"]["error"]["category"] == "io"
    assert isinstance(error["data"]["error"]["trace_id"], str)


def test_rpc_resources_list_timeout(client, mocker):
    """Verify resources/list surfaces timeout as a non-hanging RPC error."""
    mocker.patch.object(
        server_module,
        "_resource_models_with_timeout",
        mocker.AsyncMock(side_effect=asyncio.TimeoutError),
    )
    payload = {"jsonrpc": "2.0", "id": 26, "method": "resources/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    error = response.json()["error"]
    assert error["code"] == -32000
    assert "timed out" in error["message"].lower()
    assert error["data"]["error"]["code"] == "resources_list_timeout"
    assert error["data"]["error"]["retryable"] is True
    assert isinstance(error["data"]["error"]["trace_id"], str)


def test_rpc_resources_read_timeout(client, mocker):
    """Verify resources/read surfaces timeout as a non-hanging RPC error."""
    mocker.patch.object(
        server_module,
        "_read_template_with_timeout",
        mocker.AsyncMock(side_effect=asyncio.TimeoutError),
    )
    payload = {
        "jsonrpc": "2.0",
        "id": 27,
        "method": "resources/read",
        "params": {"uri": "analyst://templates/config/outlier_config_template.yaml"},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    error = response.json()["error"]
    assert error["code"] == -32000
    assert "timed out" in error["message"].lower()
    assert error["data"]["error"]["code"] == "resource_read_timeout"
    assert error["data"]["error"]["retryable"] is True
    assert isinstance(error["data"]["error"]["trace_id"], str)


@pytest.mark.asyncio
async def test_toolkit_get_capability_catalog_timeout(mocker):
    """Verify capability catalog fails fast on template read timeout."""
    mocker.patch.object(cockpit_module, "TEMPLATE_IO_TIMEOUT_SEC", 0.01)
    mocker.patch.object(
        cockpit_module,
        "_build_capability_catalog",
        side_effect=lambda: time.sleep(0.05),
    )
    result = await cockpit_module._toolkit_get_capability_catalog()
    assert result["status"] == "error"
    assert "timed out" in result["error"].lower()


@pytest.mark.asyncio
async def test_toolkit_get_golden_templates_timeout(mocker):
    """Verify golden template loading fails fast on timeout."""
    mocker.patch.object(cockpit_module, "TEMPLATE_IO_TIMEOUT_SEC", 0.01)
    mocker.patch.object(
        cockpit_module,
        "get_golden_configs",
        side_effect=lambda: time.sleep(0.05),
    )
    result = await cockpit_module._toolkit_get_golden_templates()
    assert result["status"] == "error"
    assert "timed out" in result["error"].lower()
