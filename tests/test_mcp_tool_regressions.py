import sys
import types

import pandas as pd
import pytest

import analyst_toolkit.mcp_server.tools.diagnostics as diagnostics_tool
import analyst_toolkit.mcp_server.tools.duplicates as duplicates_tool
import analyst_toolkit.mcp_server.tools.final_audit as final_audit_tool
import analyst_toolkit.mcp_server.tools.infer_configs as infer_configs_tool
import analyst_toolkit.mcp_server.tools.normalization as normalization_tool
import analyst_toolkit.mcp_server.tools.validation as validation_tool


@pytest.mark.asyncio
async def test_toolkit_validation_applies_shorthand_rules(mocker):
    df = pd.DataFrame({"value": [1, 2]})

    mocker.patch.object(validation_tool, "load_input", return_value=df)
    mocker.patch.object(validation_tool, "save_to_session", return_value="sess_test")
    mocker.patch.object(validation_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(validation_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(validation_tool, "run_validation_pipeline", return_value=df)
    mocker.patch.object(validation_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(validation_tool, "should_export_html", return_value=False)

    result = await validation_tool._toolkit_validation(
        session_id="sess_test",
        run_id="validation_regression",
        config={"rules": {"expected_columns": ["value", "missing"]}},
    )

    assert result["passed"] is False
    assert result["summary"]["checks_run"] == 4
    assert "schema_conformity" in result["violations_found"]
    assert "effective_config" in result
    assert "schema_validation" in result["effective_config"]
    assert "next_actions" in result
    assert any(a["tool"] == "infer_configs" for a in result["next_actions"])


@pytest.mark.asyncio
async def test_toolkit_final_audit_applies_shorthand_rules(mocker):
    df = pd.DataFrame({"value": [1]})

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_test")
    mocker.patch.object(final_audit_tool, "get_session_metadata", return_value={"row_count": 1})
    mocker.patch.object(final_audit_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(
        final_audit_tool, "upload_artifact", return_value="https://example.com/artifact"
    )
    mocker.patch.object(final_audit_tool, "append_to_run_history", return_value=None)

    result = await final_audit_tool._toolkit_final_audit(
        session_id="sess_test",
        run_id="final_audit_regression",
        config={"rules": {"expected_columns": ["missing_column"]}},
    )

    assert result["status"] == "fail"
    assert result["passed"] is False
    assert "schema_conformity" in result["violations_found"]
    assert result["summary"]["checks_run"] == 4
    assert "effective_config" in result
    assert "certification" in result["effective_config"]
    assert "next_actions" in result
    assert any(a["tool"] == "get_run_history" for a in result["next_actions"])
    assert any(a["tool"] == "get_data_health_report" for a in result["next_actions"])


@pytest.mark.asyncio
async def test_toolkit_duplicates_accepts_drop_alias(mocker):
    df = pd.DataFrame({"id": [1, 1, 2], "v": [10, 10, 20]})
    captured = {}

    def fake_run_duplicates_pipeline(config, df, notebook, run_id):
        captured["mode"] = config["duplicates"]["mode"]
        subset = config["duplicates"].get("subset_columns")
        return df.drop_duplicates(subset=subset, keep="first")

    mocker.patch.object(duplicates_tool, "load_input", return_value=df)
    mocker.patch.object(
        duplicates_tool, "run_duplicates_pipeline", side_effect=fake_run_duplicates_pipeline
    )
    mocker.patch.object(duplicates_tool, "save_to_session", return_value="sess_test")
    mocker.patch.object(duplicates_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(duplicates_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(duplicates_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(duplicates_tool, "should_export_html", return_value=False)

    result = await duplicates_tool._toolkit_duplicates(
        session_id="sess_test",
        run_id="duplicates_alias_regression",
        config={"mode": "drop", "subset_columns": ["id"]},
    )

    assert captured["mode"] == "remove"
    assert result["mode"] == "remove"
    assert result["duplicate_count"] == 1


@pytest.mark.asyncio
async def test_toolkit_diagnostics_includes_next_actions(mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(diagnostics_tool, "load_input", return_value=df)
    mocker.patch.object(diagnostics_tool, "save_to_session", return_value="sess_diag")
    mocker.patch.object(diagnostics_tool, "run_diag_pipeline", return_value=None)
    mocker.patch.object(diagnostics_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(diagnostics_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(diagnostics_tool, "should_export_html", return_value=False)

    result = await diagnostics_tool._toolkit_diagnostics(
        session_id="sess_diag",
        run_id="diag_next_actions",
        config={},
    )

    assert result["status"] == "pass"
    assert "next_actions" in result
    tools = [a["tool"] for a in result["next_actions"]]
    assert "infer_configs" in tools
    assert "auto_heal" in tools


@pytest.mark.asyncio
async def test_toolkit_infer_configs_includes_apply_next_actions(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "normalization": "normalization:\\n  rules: {}\\n",
            "validation": "validation:\\n  schema_validation:\\n    run: true\\n",
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")

    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_infer",
        modules=["normalization", "validation"],
    )

    assert result["status"] == "pass"
    assert "next_actions" in result
    tools = [a["tool"] for a in result["next_actions"]]
    assert "normalization" in tools
    assert "validation" in tools
    assert "get_capability_catalog" in tools


@pytest.mark.asyncio
async def test_infer_configs_yaml_roundtrip_into_tools(monkeypatch, mocker):
    """
    Contract test: infer_configs YAML output can be passed directly into module tools.
    """
    df = pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "normalization": ("normalization:\n  rules:\n    standardize_text_columns: [name]\n"),
            "validation": ("validation:\n  rules:\n    expected_columns: [id, name]\n"),
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    inferred = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_roundtrip",
        modules=["normalization", "validation"],
    )
    assert inferred["status"] == "pass"
    assert "normalization" in inferred["configs"]
    assert "validation" in inferred["configs"]

    mocker.patch.object(normalization_tool, "load_input", return_value=df)
    mocker.patch.object(normalization_tool, "apply_normalization", return_value=(df, None, {}))
    mocker.patch.object(normalization_tool, "run_normalization_pipeline", return_value=df)
    mocker.patch.object(normalization_tool, "save_to_session", return_value="sess_roundtrip")
    mocker.patch.object(normalization_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(normalization_tool, "save_output", return_value="gs://dummy/norm.csv")
    mocker.patch.object(normalization_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(normalization_tool, "should_export_html", return_value=False)

    norm_result = await normalization_tool._toolkit_normalization(
        session_id="sess_roundtrip",
        run_id="run_roundtrip",
        config={"normalization": inferred["configs"]["normalization"]},
    )
    assert norm_result["status"] == "pass"

    mocker.patch.object(validation_tool, "load_input", return_value=df)
    mocker.patch.object(validation_tool, "save_to_session", return_value="sess_roundtrip")
    mocker.patch.object(validation_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(validation_tool, "save_output", return_value="gs://dummy/val.csv")
    mocker.patch.object(validation_tool, "run_validation_pipeline", return_value=df)
    mocker.patch.object(validation_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(validation_tool, "should_export_html", return_value=False)
    mocker.patch.object(
        validation_tool,
        "run_validation_suite",
        return_value={"schema_conformity": {"passed": True}},
    )

    val_result = await validation_tool._toolkit_validation(
        session_id="sess_roundtrip",
        run_id="run_roundtrip",
        config={"validation": inferred["configs"]["validation"]},
    )
    assert val_result["status"] == "pass"
