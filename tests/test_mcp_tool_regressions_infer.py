import sys
import types
from pathlib import Path

import pandas as pd
import pytest

import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_tool
import analyst_toolkit.mcp_server.tools.infer_configs as infer_configs_tool
from analyst_toolkit.mcp_server.state import StateStore


@pytest.mark.asyncio
async def test_infer_configs_gcs_path_uses_local_snapshot(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    captured: dict[str, str] = {}

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)
    mocker.patch.object(infer_configs_tool, "save_to_session", return_value="sess_gcs")

    def fake_infer_configs(**kwargs):
        captured["input_path"] = kwargs["input_path"]
        assert Path(kwargs["input_path"]).exists()
        return {"validation": "validation:\n  schema_validation:\n    run: true\n"}

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        gcs_path="gs://my-bucket/path/ingest_dt=2020-08-08"
    )

    assert result["status"] == "pass"
    assert captured["input_path"].endswith(".csv")
    assert not captured["input_path"].startswith("gs://")


@pytest.mark.asyncio
async def test_infer_configs_replaces_transient_final_audit_path_with_stable_input(
    monkeypatch, mocker, tmp_path
):
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    source_path = tmp_path / "source.csv"
    df.to_csv(source_path, index=False)

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "final_audit": (
                "final_audit:\n"
                "  raw_data_path: /tmp/transient-source.csv\n"
                "  certification:\n"
                "    schema_validation:\n"
                "      rules:\n"
                "        expected_columns: [id, value]\n"
            )
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        gcs_path=str(source_path),
    )

    assert result["status"] == "pass"
    assert "/tmp/transient-source.csv" not in result["configs"]["final_audit"]
    assert str(source_path) in result["configs"]["final_audit"]


@pytest.mark.asyncio
async def test_infer_configs_strips_temp_paths_from_generated_yaml(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        temp_input_path = kwargs["input_path"]
        return {
            "validation": (
                "validation:\n"
                f"  input_path: {temp_input_path}\n"
                "  schema_validation:\n"
                "    run: true\n"
            ),
            "final_audit": (
                "final_audit:\n"
                f"  raw_data_path: {temp_input_path}\n"
                f"  input_path: {temp_input_path}\n"
                f"  input_df_path: {temp_input_path}\n"
                "  certification:\n"
                "    schema_validation:\n"
                "      rules:\n"
                "        expected_columns: [id, value]\n"
            ),
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_strip_paths",
        modules=["validation", "final_audit"],
    )

    assert result["status"] == "pass"
    final_yaml = result["configs"]["final_audit"]
    assert "/tmp/" not in final_yaml
    assert "input_df_path" not in final_yaml
    assert "raw_data_path" not in final_yaml
    validation_yaml = result["configs"]["validation"]
    assert "/tmp/" not in validation_yaml
    assert "input_path" not in validation_yaml


@pytest.mark.asyncio
async def test_infer_configs_strips_non_text_categorical_rules(monkeypatch, mocker):
    df = pd.DataFrame({"value": [1.0, 2.0], "captured_at": ["2024-01-01", "2024-01-02"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "validation": (
                "validation:\n"
                "  schema_validation:\n"
                "    rules:\n"
                "      expected_types:\n"
                "        value: float64\n"
                "        captured_at: datetime64[ns]\n"
                "      categorical_values:\n"
                "        value: ['1.0', '2.0']\n"
                "        captured_at: ['2024-01-01', '2024-01-02']\n"
                "        status: ['OK', 'WARN']\n"
            ),
            "final_audit": (
                "final_audit:\n"
                "  certification:\n"
                "    schema_validation:\n"
                "      rules:\n"
                "        expected_types:\n"
                "          value: float64\n"
                "          captured_at: datetime64[ns]\n"
                "        categorical_values:\n"
                "          value: ['1.0', '2.0']\n"
                "          captured_at: ['2024-01-01', '2024-01-02']\n"
                "          status: ['OK', 'WARN']\n"
            ),
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_strip_non_text_categories",
        modules=["validation", "final_audit"],
    )

    assert result["status"] == "pass"
    validation_yaml = result["configs"]["validation"]
    _, _, validation_categories = validation_yaml.partition("categorical_values:")
    assert "value:" not in validation_categories
    assert "captured_at:" not in validation_categories
    assert "status:" in validation_categories

    final_yaml = result["configs"]["final_audit"]
    _, _, final_categories = final_yaml.partition("categorical_values:")
    assert "value:" not in final_categories
    assert "captured_at:" not in final_categories
    assert "status:" in final_categories


@pytest.mark.asyncio
async def test_infer_configs_ignores_internal_handling_output(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "value": [1.0, 2.0]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "outliers": "outlier_detection:\n  detection_specs:\n    value:\n      method: iqr\n",
            "handling": "outlier_handling:\n  handling_specs:\n    value:\n      strategy: median\n",
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_outlier_contract",
        modules=["outliers"],
    )

    assert result["status"] == "pass"
    assert result["covered_modules"] == ["outliers"]
    assert "outlier_detection" in result["configs"]["outliers"]
    assert "outlier_handling" not in result["configs"]["outliers"]


@pytest.mark.asyncio
async def test_infer_configs_surfaces_covered_and_unsupported_modules(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "validation": "validation:\n  schema_validation:\n    run: true\n",
            "normalization": "normalization:\n  rules: {}\n",
            "outliers": "outlier_detection:\n  run: true\n",
            "imputation": "imputation:\n  rules: {}\n",
            "diagnostics": "diagnostics:\n  run: true\n",
            "duplicates": "duplicates:\n  subset: []\n",
            "certification": "validation:\n  schema_validation:\n    run: true\n",
            "handling": "outlier_detection:\n  run: true\n",
            "final_audit": "final_audit:\n  certification_rules: {}\n",
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(session_id="sess_full")

    assert result["status"] == "pass"
    assert "covered_modules" in result
    assert "unsupported_modules" in result
    assert set(result["covered_modules"]) == {
        "certification",
        "diagnostics",
        "duplicates",
        "final_audit",
        "imputation",
        "normalization",
        "outliers",
        "validation",
    }
    assert result["unsupported_modules"] == []


@pytest.mark.asyncio
async def test_infer_configs_reports_unsupported_when_partial(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "validation": "validation:\n  schema_validation:\n    run: true\n",
            "outliers": "outlier_detection:\n  run: true\n",
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_partial",
        modules=["validation", "outliers", "normalization"],
    )

    assert result["status"] == "pass"
    assert result["covered_modules"] == ["outliers", "validation"]
    assert result["unsupported_modules"] == ["normalization"]
    assert any("not generated for" in w for w in result["warnings"])
    assert any("partial MCP workflow coverage" in w for w in result["warnings"])


@pytest.mark.asyncio
async def test_infer_configs_aliased_requests_resolve_before_unsupported_check(monkeypatch, mocker):
    """Requesting 'handling' (alias for outliers) should not appear unsupported."""
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "outliers": "outlier_detection:\n  run: true\n",
            "certification": "validation:\n  schema_validation:\n    run: true\n",
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_alias_req",
        modules=["handling", "certification"],
    )

    assert result["status"] == "pass"
    assert "certification" in result["configs"]
    assert "outliers" in result["configs"]
    assert result["unsupported_modules"] == []


@pytest.mark.asyncio
async def test_infer_configs_routes_certification_next_action_through_final_audit(
    monkeypatch, mocker
):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "certification": (
                "validation:\n"
                "  schema_validation:\n"
                "    rules:\n"
                "      expected_columns: [id, name]\n"
            )
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_cert_next_action",
        modules=["certification"],
    )

    assert result["status"] == "pass"
    tools = [action["tool"] for action in result["next_actions"]]
    assert "certification" not in tools
    assert "final_audit" in tools


@pytest.mark.asyncio
async def test_infer_configs_returns_structured_error_on_load_failure(monkeypatch, mocker):
    """infer_configs should return a structured error, not raise, when load_input fails."""
    mocker.patch.object(
        infer_configs_tool, "load_input", side_effect=ValueError("Session not found")
    )

    result = await infer_configs_tool._toolkit_infer_configs(
        input_id="input_nonexistent",
        run_id="infer_load_fail",
    )

    assert result["status"] == "error"
    assert result["error_code"] == "INPUT_LOAD_FAILED"
    assert "Session not found" in result["error"]


@pytest.mark.asyncio
async def test_infer_configs_passes_run_id_to_save_to_session(monkeypatch, mocker):
    """When infer_configs creates a new session it should associate the run_id."""
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    captured_save_args: list[dict] = []

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    original_save = infer_configs_tool.save_to_session

    def tracking_save(df, session_id=None, run_id=None):
        captured_save_args.append({"session_id": session_id, "run_id": run_id})
        return original_save(df, session_id=session_id, run_id=run_id)

    mocker.patch.object(infer_configs_tool, "save_to_session", side_effect=tracking_save)

    def fake_infer_configs(**kwargs):
        return {"validation": "validation:\n  schema_validation:\n    run: true\n"}

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        input_id="input_test_run_id",
        run_id="my_explicit_run",
    )

    assert result["status"] == "pass"
    assert len(captured_save_args) >= 1
    assert captured_save_args[0]["run_id"] == "my_explicit_run"


@pytest.mark.asyncio
async def test_auto_heal_returns_error_when_infer_configs_load_fails(monkeypatch, mocker):
    """auto_heal should propagate infer_configs load errors, not crash with -32603."""

    async def failing_infer_configs(**kwargs):
        return {
            "status": "error",
            "module": "infer_configs",
            "error": "Failed to load input: InputNotFoundError: not found",
            "error_code": "INPUT_LOAD_FAILED",
            "config_yaml": "",
        }

    monkeypatch.setattr(auto_heal_tool, "_toolkit_infer_configs", failing_infer_configs)
    monkeypatch.setattr(auto_heal_tool, "append_to_run_history", lambda *a, **kw: None)
    monkeypatch.setattr(auto_heal_tool, "get_session_metadata", lambda sid: {"row_count": 0})

    result = await auto_heal_tool._toolkit_auto_heal(
        input_id="input_nonexistent",
        run_id="auto_heal_load_fail",
    )

    assert result["status"] == "error"
    assert result["error_code"] == "INPUT_LOAD_FAILED"


@pytest.mark.asyncio
async def test_auto_heal_accepts_input_id_without_internal_error(monkeypatch):
    infer_calls: list[dict] = []

    async def fake_infer_configs(**kwargs):
        infer_calls.append(kwargs)
        return {
            "status": "pass",
            "module": "infer_configs",
            "run_id": "auto_heal_input_id_ok",
            "session_id": "sess_autoheal_input",
            "configs": {},
            "warnings": [],
            "next_actions": [],
        }

    monkeypatch.setattr(auto_heal_tool, "_toolkit_infer_configs", fake_infer_configs)
    monkeypatch.setattr(auto_heal_tool, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_tool, "get_session_metadata", lambda session_id: {"row_count": 2})

    result = await auto_heal_tool._toolkit_auto_heal(
        input_id="input_test_autoheal",
        run_id="auto_heal_input_id_ok",
    )

    assert infer_calls[0]["input_id"] == "input_test_autoheal"
    assert result["status"] == "warn"
    assert result["session_id"] == "sess_autoheal_input"
    assert "Internal server error" not in result.get("message", "")


@pytest.mark.asyncio
async def test_infer_configs_persists_configs_to_session(monkeypatch, mocker):
    """infer_configs should store inferred configs in StateStore for downstream tools."""
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "final_audit": "final_audit:\n  certification:\n    schema_validation:\n      rules:\n        expected_columns: [id, value]\n",
            "certification": "validation:\n  schema_validation:\n    rules:\n      expected_columns: [id, value]\n",
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_persist_test",
        run_id="persist_run",
    )

    assert result["status"] == "pass"

    stored_fa = StateStore.get_config("sess_persist_test", "final_audit")
    stored_cert = StateStore.get_config("sess_persist_test", "certification")
    assert stored_fa is not None
    assert "expected_columns" in stored_fa
    assert stored_cert is not None
    StateStore.clear()


@pytest.mark.asyncio
async def test_infer_configs_accepts_input_id_with_session_id(monkeypatch, mocker):
    """infer_configs should accept input_id + session_id together."""
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {"normalization": "normalization:\n  rules: {}\n"}

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        input_id="input_test_combo",
        session_id="sess_combo_test",
        run_id="combo_run",
        modules=["normalization"],
    )

    assert result["status"] == "pass"
    assert result["session_id"] == "sess_combo_test"
    StateStore.clear()


@pytest.mark.asyncio
async def test_infer_configs_repopulates_stale_session_resolved_from_input_id(monkeypatch, mocker):
    """infer_configs should recreate a cleared descriptor-linked session before saving configs."""
    from analyst_toolkit.mcp_server.input.models import InputDescriptor

    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)
    descriptor = InputDescriptor(
        input_id="input_stale_session",
        source_type="server_path",
        original_reference="/tmp/test.csv",
        resolved_reference="/tmp/test.csv",
        display_name="test.csv",
        media_type="text/csv",
        session_id="sess_stale_input",
        run_id="stale_session_repopulate",
    )
    mocker.patch.object(infer_configs_tool, "get_input_descriptor", return_value=descriptor)

    def fake_infer_configs(**kwargs):
        return {"validation": "validation:\n  schema_validation:\n    run: true\n"}

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        input_id="input_stale_session",
        run_id="stale_session_repopulate",
        modules=["validation"],
    )

    assert result["status"] == "pass"
    assert result["session_id"] == "sess_stale_input"
    assert StateStore.get("sess_stale_input") is not None
    assert StateStore.get_config("sess_stale_input", "validation") is not None
    StateStore.clear()


@pytest.mark.asyncio
async def test_infer_configs_resolves_session_from_input_id(monkeypatch, mocker):
    """infer_configs should resolve session_id from input_id descriptor."""
    from analyst_toolkit.mcp_server.input.models import InputDescriptor

    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()
    session_id = StateStore.save(df, run_id="infer_resolve_run")

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)
    descriptor = InputDescriptor(
        input_id="input_resolve_test",
        source_type="server_path",
        original_reference="/tmp/test.csv",
        resolved_reference="/tmp/test.csv",
        display_name="test.csv",
        media_type="text/csv",
        session_id=session_id,
        run_id="infer_resolve_run",
    )
    mocker.patch.object(infer_configs_tool, "get_input_descriptor", return_value=descriptor)

    def fake_infer_configs(**kwargs):
        return {"normalization": "normalization:\n  rules: {}\n"}

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        input_id="input_resolve_test",
        run_id="infer_resolve_run",
        modules=["normalization"],
    )

    assert result["status"] == "pass"
    assert result["session_id"] == session_id
    stored = StateStore.get_config(session_id, "normalization")
    assert stored is not None
    StateStore.clear()
