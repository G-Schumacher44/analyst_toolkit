"""
test_mcp_server.py — Smoke tests for the analyst_toolkit MCP server.

Uses FastAPI's TestClient to verify the JSON-RPC 2.0 dispatcher
without requiring a live network port.
"""

import asyncio
import time

import pytest
from fastapi.testclient import TestClient

import analyst_toolkit.mcp_server.server as server_module
import analyst_toolkit.mcp_server.tools.cockpit as cockpit_module
from analyst_toolkit.mcp_server.server import TOOL_REGISTRY, app

client = TestClient(app)


def test_health_check():
    """Verify the /health endpoint returns the registered tools."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "diagnostics" in data["tools"]
    assert "outliers" in data["tools"]


def test_rpc_initialize():
    """Verify the MCP 'initialize' method via JSON-RPC."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["protocolVersion"] == "2024-05-01"
    assert result["serverInfo"]["name"] == "analyst-toolkit"
    assert "resources" in result["capabilities"]


def test_rpc_tools_list():
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


def test_rpc_capability_catalog_tool():
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


def test_rpc_capability_catalog_filters_and_compact():
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


def test_rpc_user_quickstart_tool():
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


def test_rpc_agent_playbook_infer_configs_inputs_allow_path_or_session():
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


def test_rpc_get_run_history_supports_summary_modes(mocker):
    history = [
        {
            "module": "diagnostics",
            "status": "pass",
            "summary": {"passed": True, "row_count": 5},
            "timestamp": "2026-02-25T00:00:00Z",
        },
        {
            "module": "validation",
            "status": "fail",
            "summary": {"passed": False, "violations_found": ["schema_conformity"]},
            "timestamp": "2026-02-25T00:01:00Z",
        },
        {
            "module": "imputation",
            "status": "warn",
            "summary": {"nulls_filled": 4},
            "timestamp": "2026-02-25T00:02:00Z",
        },
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)

    payload = {
        "jsonrpc": "2.0",
        "id": 33,
        "method": "tools/call",
        "params": {
            "name": "get_run_history",
            "arguments": {
                "run_id": "run_b3",
                "failures_only": True,
                "latest_errors": True,
                "latest_status_by_module": True,
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["filters"]["failures_only"] is True
    assert result["history_count"] == 1
    assert result["ledger"][0]["module"] == "validation"
    assert len(result["latest_errors"]) == 1
    assert result["latest_errors"][0]["module"] == "validation"
    assert "validation" in result["latest_status_by_module"]
    assert result["latest_status_by_module"]["validation"]["status"] == "fail"


def test_rpc_get_config_schema_supports_final_audit():
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


def test_rpc_get_config_schema_outliers_matches_runtime_contract_paths():
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


def test_rpc_preflight_config_normalizes_validation_shape():
    payload = {
        "jsonrpc": "2.0",
        "id": 36,
        "method": "tools/call",
        "params": {
            "name": "preflight_config",
            "arguments": {
                "module_name": "validation",
                "config": {
                    "rules": {
                        "schema_validation": {
                            "rules": {
                                "expected_columns": ["tag_id", "species"],
                            }
                        },
                        "expected_types": {"tag_id": "str"},
                    }
                },
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["module"] == "validation"
    assert result["summary"]["effective_rules_path"] == "validation.schema_validation.rules.*"
    assert isinstance(result["warnings"], list)
    assert result["warnings"]
    assert "schema_validation" in result["warnings"][0]
    assert "schema_validation" in result["effective_config"]
    assert "expected_types" in result["effective_config"]["schema_validation"]["rules"]


def test_rpc_preflight_config_normalizes_outliers_shorthand():
    payload = {
        "jsonrpc": "2.0",
        "id": 37,
        "method": "tools/call",
        "params": {
            "name": "preflight_config",
            "arguments": {
                "module_name": "outliers",
                "config": {
                    "method": "iqr",
                    "iqr_multiplier": 1.1,
                    "columns": ["transaction_amount", "frequency_24h"],
                },
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["module"] == "outliers"
    assert (
        result["summary"]["effective_rules_path"] == "outlier_detection.detection_specs.<column>.*"
    )
    specs = result["effective_config"]["detection_specs"]
    assert specs["transaction_amount"]["method"] == "iqr"
    assert specs["frequency_24h"]["method"] == "iqr"


def test_rpc_tools_call_returns_structured_error_envelope_for_tool_failure():
    """
    Verify tool runtime failures are normalized to structured status=error payloads.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 28,
        "method": "tools/call",
        "params": {"name": "diagnostics", "arguments": {}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "error"
    assert result["module"] == "diagnostics"
    assert isinstance(result.get("trace_id"), str)
    assert result["error"]["category"] == "internal"
    assert result["error"]["code"] == "tool_execution_failed"
    assert result["error"]["retryable"] is False
    assert result["error"]["trace_id"] == result["trace_id"]


def test_rpc_resources_list():
    """Verify template resources are discoverable via MCP resources/list."""
    payload = {"jsonrpc": "2.0", "id": 20, "method": "resources/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert "resources" in result
    uris = [r["uri"] for r in result["resources"]]
    assert any(uri.startswith("analyst://templates/golden/") for uri in uris)
    assert any(uri.startswith("analyst://templates/config/") for uri in uris)


def test_rpc_resource_templates_list():
    """Verify MCP resources/templates/list is empty by default to avoid client duplication."""
    payload = {"jsonrpc": "2.0", "id": 23, "method": "resources/templates/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert "resourceTemplates" in result
    assert result["resourceTemplates"] == []


def test_rpc_resource_templates_list_when_enabled(monkeypatch):
    """Verify MCP resources/templates/list returns URI templates when explicitly enabled."""
    monkeypatch.setattr(server_module, "ADVERTISE_RESOURCE_TEMPLATES", True)
    payload = {"jsonrpc": "2.0", "id": 35, "method": "resources/templates/list", "params": {}}
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    template_uris = [t["uriTemplate"] for t in result["resourceTemplates"]]
    assert "analyst://templates/config/{name}_template.yaml" in template_uris
    assert "analyst://templates/golden/{name}.yaml" in template_uris


def test_rpc_resources_read():
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


def test_rpc_resources_read_not_found():
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


def test_rpc_resources_list_timeout(mocker):
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


def test_rpc_resources_read_timeout(mocker):
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
        TOOL_REGISTRY["diagnostics"], {"fn": mocker.AsyncMock(return_value=mock_result)}
    )

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
