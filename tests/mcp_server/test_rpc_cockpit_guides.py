import pytest

import analyst_toolkit.mcp_server.tools.cockpit as cockpit_module
from analyst_toolkit.mcp_server.tools.cockpit_templates import (
    build_cockpit_resource_groups,
    build_cockpit_resources,
)


def test_build_cockpit_resource_groups_uses_reference_lookup_not_position() -> None:
    resources = build_cockpit_resources()
    resources_by_reference = {resource["Reference"]: resource for resource in resources}
    reordered = [
        resources_by_reference["analyst://templates/config/data_dictionary_request_template.yaml"],
        resources_by_reference["analyst://catalog/capabilities"],
        resources_by_reference["analyst://docs/quickstart"],
        resources_by_reference["analyst://templates/config/auto_heal_request_template.yaml"],
        resources_by_reference["analyst://docs/agent-playbook"],
        resources_by_reference["analyst://templates/config/runtime_overlay_template.yaml"],
    ]

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
    assert all(str(item["template_path"]).startswith("config/") for item in result["modules"])
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
