import sys
import types

import pandas as pd
import pytest

import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_tool
import analyst_toolkit.mcp_server.tools.diagnostics as diagnostics_tool
import analyst_toolkit.mcp_server.tools.drift as drift_tool
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
    assert "violations_detail" in result
    assert "schema_conformity" in result["violations_detail"]
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
        final_audit_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path,
            "url": "https://example.com/artifact",
            "warnings": [],
            "destinations": {},
        },
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
    assert "violations_detail" in result
    assert "schema_conformity" in result["violations_detail"]
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
async def test_toolkit_drift_detection_reports_artifact_contract(mocker):
    base_df = pd.DataFrame({"value": [1.0, 2.0]})
    target_df = pd.DataFrame({"value": [1.1, 2.2]})
    captured = {"calls": 0}

    def fake_load_input(path=None, session_id=None):
        captured["calls"] += 1
        return base_df if captured["calls"] == 1 else target_df

    mocker.patch.object(drift_tool, "load_input", side_effect=fake_load_input)
    mocker.patch.object(drift_tool, "save_output", return_value="gs://dummy/drift.csv")
    mocker.patch.object(drift_tool, "append_to_run_history", return_value=None)

    result = await drift_tool._toolkit_drift_detection(
        base_path="gs://bucket/base.csv",
        target_path="gs://bucket/target.csv",
        run_id="drift_contract",
    )

    assert result["module"] == "drift_detection"
    assert "artifact_matrix" in result
    assert result["artifact_matrix"]["data_export"]["status"] == "available"
    assert result["export_url"] == "gs://dummy/drift.csv"
    assert result["destination_delivery"]["data_export"] == {}


@pytest.mark.asyncio
async def test_toolkit_diagnostics_accepts_runtime_overrides(mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    captured = {}

    def fake_load_input(path=None, session_id=None, input_id=None):
        captured["input_path"] = path
        captured["input_session_id"] = session_id
        captured["input_id"] = input_id
        return df

    mocker.patch.object(diagnostics_tool, "load_input", side_effect=fake_load_input)
    mocker.patch.object(diagnostics_tool, "save_to_session", return_value="sess_diag")
    mocker.patch.object(diagnostics_tool, "run_diag_pipeline", return_value=None)
    mocker.patch.object(diagnostics_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(diagnostics_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(
        diagnostics_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path,
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await diagnostics_tool._toolkit_diagnostics(
        runtime={
            "run": {"run_id": "diag_runtime", "input_path": "gs://bucket/runtime.csv"},
            "artifacts": {"export_html": True},
            "destinations": {
                "gcs": {
                    "enabled": True,
                    "bucket_uri": "gs://artifact-bucket",
                    "prefix": "runtime/reports",
                }
            },
        },
        config={},
    )

    assert result["run_id"] == "diag_runtime"
    assert result["runtime_applied"] is True
    assert captured["input_path"] == "gs://bucket/runtime.csv"
    assert captured["input_id"] is None
    assert result["artifact_path"].endswith("diag_runtime_diagnostics_report.html")


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
async def test_toolkit_infer_configs_routes_certification_to_final_audit(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "certification": "validation:\n  schema_validation:\n    run: true\n",
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")

    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_cert_only",
        modules=["certification"],
    )

    assert result["status"] == "pass"
    actions = result["next_actions"]
    assert all(action["tool"] != "certification" for action in actions)
    assert any(action["tool"] == "final_audit" for action in actions)


@pytest.mark.asyncio
async def test_toolkit_infer_configs_ignores_unsupported_external_modules_kw(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(*, root, input_path, sample_rows=None):
        assert root == "."
        assert input_path
        assert sample_rows == 5
        return {"normalization": "normalization:\\n  rules: {}\\n"}

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")

    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_compat",
        modules=["normalization"],
        sample_rows=5,
    )

    assert result["status"] == "pass"
    assert "normalization" in result["configs"]
    assert any(
        "does not support the following arguments" in warning for warning in result["warnings"]
    )


@pytest.mark.asyncio
async def test_toolkit_infer_configs_reads_generated_directory_results(
    monkeypatch, mocker, tmp_path
):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    monkeypatch.chdir(tmp_path)
    generated_dir = tmp_path / "config" / "generated"
    generated_dir.mkdir(parents=True)
    (generated_dir / "normalization_config.yaml").write_text(
        "normalization:\n  rules: {}\n",
        encoding="utf-8",
    )
    (generated_dir / "validation_config.yaml").write_text(
        "validation:\n  schema_validation:\n    run: true\n",
        encoding="utf-8",
    )

    def fake_infer_configs(**kwargs):
        return str(generated_dir)

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")

    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_generated",
        modules=["normalization", "validation"],
    )

    assert result["status"] == "pass"
    assert result["config_dir"] == str(generated_dir)
    assert "normalization" in result["configs"]
    assert "validation" in result["configs"]
    assert "normalization:" in result["configs"]["normalization"]
    assert "validation:" in result["configs"]["validation"]


@pytest.mark.asyncio
async def test_toolkit_infer_configs_maps_generated_yaml_by_root_key(monkeypatch, mocker, tmp_path):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    monkeypatch.chdir(tmp_path)
    generated_dir = tmp_path / "config" / "generated_by_content"
    generated_dir.mkdir(parents=True)
    (generated_dir / "penguins_profile.yaml").write_text(
        "normalization:\n  rules: {}\n",
        encoding="utf-8",
    )

    def fake_infer_configs(**kwargs):
        return str(generated_dir)

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")

    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_generated_by_content",
        modules=["normalization"],
    )

    assert result["status"] == "pass"
    assert result["config_dir"] == str(generated_dir)
    assert "normalization" in result["configs"]
    assert any(
        "session was recreated before saving inferred configs" in w for w in result["warnings"]
    )


@pytest.mark.asyncio
async def test_toolkit_infer_configs_maps_autofill_files_with_metadata_prefix(
    monkeypatch, mocker, tmp_path
):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    monkeypatch.chdir(tmp_path)
    generated_dir = tmp_path / "config" / "generated_autofill"
    generated_dir.mkdir(parents=True)
    (generated_dir / "outlier_config_autofill.yaml").write_text(
        "notebook: true\nrun_id: ''\nlogging: auto\noutlier_detection:\n  run: true\n",
        encoding="utf-8",
    )
    (generated_dir / "validation_config_autofill.yaml").write_text(
        "notebook: true\nrun_id: ''\nlogging: auto\nvalidation:\n  schema_validation:\n    run: true\n",
        encoding="utf-8",
    )

    def fake_infer_configs(**kwargs):
        return str(generated_dir)

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")

    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_generated_autofill",
        modules=["outliers", "validation"],
    )

    assert result["status"] == "pass"
    assert result["config_dir"] == str(generated_dir)
    assert "outliers" in result["configs"]
    assert "validation" in result["configs"]
    assert any(
        "session was recreated before saving inferred configs" in w for w in result["warnings"]
    )


@pytest.mark.asyncio
async def test_toolkit_infer_configs_rejects_untrusted_generated_directory(
    monkeypatch, mocker, tmp_path
):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    monkeypatch.chdir(tmp_path)
    generated_dir = tmp_path / "outside_generated"
    generated_dir.mkdir()
    (generated_dir / "validation_config.yaml").write_text(
        "validation:\n  schema_validation:\n    run: true\n",
        encoding="utf-8",
    )

    def fake_infer_configs(**kwargs):
        return str(generated_dir)

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")

    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_untrusted_generated",
        modules=["validation"],
    )

    assert result["status"] == "pass"
    assert result["configs"] == {}
    assert result["config_dir"] == ""
    assert any(
        "Rejected untrusted generated config directory" in warning for warning in result["warnings"]
    )


@pytest.mark.asyncio
async def test_toolkit_infer_configs_normalizes_dict_module_aliases(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        return {
            "certification": "validation:\n  schema_validation:\n    run: true\n",
            "outlier": "outlier_detection:\n  run: true\n",
            "dups": "duplicates:\n  subset: []\n",
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")

    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        session_id="sess_alias_dict",
        modules=["certification", "outliers", "duplicates"],
    )

    assert result["status"] == "pass"
    assert "certification" in result["configs"]
    assert "outliers" in result["configs"]
    assert "outlier_detection" in result["configs"]["outliers"]
    assert "outlier_handling" not in result["configs"]["outliers"]
    assert "duplicates" in result["configs"]
    assert result["unsupported_modules"] == []


@pytest.mark.asyncio
async def test_infer_configs_default_request_uses_public_module_surface(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    captured = {}

    mocker.patch.object(infer_configs_tool, "load_input", return_value=df)

    def fake_infer_configs(**kwargs):
        captured["modules"] = kwargs["modules"]
        return {
            "validation": "validation:\n  schema_validation:\n    run: true\n",
            "certification": "validation:\n  schema_validation:\n    run: true\n",
            "outliers": "outlier_detection:\n  run: true\n",
            "diagnostics": "diagnostics:\n  run: true\n",
            "normalization": "normalization:\n  rules: {}\n",
            "duplicates": "duplicates:\n  subset_columns: []\n",
            "imputation": "imputation:\n  rules: {}\n",
            "final_audit": "final_audit:\n  certification:\n    rules: {}\n",
        }

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(session_id="sess_public_surface")

    assert result["status"] == "pass"
    assert set(captured["modules"]) == {
        "certification",
        "diagnostics",
        "duplicates",
        "final_audit",
        "imputation",
        "normalization",
        "outliers",
        "validation",
    }
    assert "handling" not in captured["modules"]
    assert set(result["covered_modules"]) == set(captured["modules"])


@pytest.mark.asyncio
async def test_toolkit_infer_configs_accepts_runtime_overrides(monkeypatch, mocker):
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    captured = {}

    def fake_load_input(path=None, session_id=None, input_id=None):
        captured["input_path"] = path
        captured["input_session_id"] = session_id
        captured["input_id"] = input_id
        return df

    mocker.patch.object(infer_configs_tool, "load_input", side_effect=fake_load_input)
    mocker.patch.object(infer_configs_tool, "save_to_session", return_value="sess_runtime")

    def fake_infer_configs(**kwargs):
        return {"normalization": "normalization:\\n  rules: {}\\n"}

    infer_mod = types.ModuleType("analyst_toolkit_deploy.infer_configs")
    setattr(infer_mod, "infer_configs", fake_infer_configs)
    pkg_mod = types.ModuleType("analyst_toolkit_deploy")
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy", pkg_mod)
    monkeypatch.setitem(sys.modules, "analyst_toolkit_deploy.infer_configs", infer_mod)

    result = await infer_configs_tool._toolkit_infer_configs(
        runtime={"run": {"run_id": "infer_runtime", "input_path": "gs://bucket/runtime.csv"}},
        modules=["normalization"],
    )

    assert result["runtime_applied"] is True
    assert result["run_id"] == "infer_runtime"
    assert captured["input_path"] == "gs://bucket/runtime.csv"
    assert captured["input_id"] is None


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
    run_id = inferred["run_id"]

    mocker.patch.object(normalization_tool, "load_input", return_value=df)
    mocker.patch.object(normalization_tool, "run_normalization_pipeline", return_value=df)
    mocker.patch.object(normalization_tool, "save_to_session", return_value="sess_roundtrip")
    mocker.patch.object(normalization_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(normalization_tool, "save_output", return_value="gs://dummy/norm.csv")
    mocker.patch.object(normalization_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(normalization_tool, "should_export_html", return_value=False)

    norm_result = await normalization_tool._toolkit_normalization(
        session_id="sess_roundtrip",
        run_id=run_id,
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
        run_id=run_id,
        config={"validation": inferred["configs"]["validation"]},
    )
    assert val_result["status"] == "pass"


@pytest.mark.asyncio
async def test_toolkit_auto_heal_async_mode_returns_job_id_and_queues(mocker):
    auto_heal_tool.JobStore.clear()

    captured = {}

    def fake_create_task(coro):
        captured["coro"] = coro
        return object()

    mocker.patch.object(auto_heal_tool.asyncio, "create_task", side_effect=fake_create_task)

    result = await auto_heal_tool._toolkit_auto_heal(
        gcs_path="gs://bucket/path.csv",
        run_id="auto_heal_async_test",
        async_mode=True,
    )

    assert result["status"] == "accepted"
    assert result["module"] == "auto_heal"
    assert isinstance(result["job_id"], str)

    job = auto_heal_tool.JobStore.get(result["job_id"])
    assert job is not None
    assert job["state"] == "queued"

    coro = captured.get("coro")
    if coro is not None:
        coro.close()


@pytest.mark.asyncio
async def test_toolkit_auto_heal_accepts_runtime_overrides(monkeypatch):
    infer_calls: list[dict] = []
    norm_calls: list[dict] = []
    imp_calls: list[dict] = []

    async def fake_infer_configs(**kwargs):
        infer_calls.append(kwargs)
        return {
            "status": "pass",
            "module": "infer_configs",
            "session_id": "sess_inferred",
            "configs": {
                "normalization": "normalization:\n  rules: {}\n",
                "imputation": "imputation:\n  rules:\n    strategies:\n      value: mean\n",
            },
        }

    async def fake_norm(**kwargs):
        norm_calls.append(kwargs)
        return {
            "status": "pass",
            "module": "normalization",
            "session_id": "sess_norm",
            "summary": {"changes_made": 1},
            "artifact_path": "exports/reports/normalization/auto_runtime_normalization_report.html",
            "artifact_url": "",
            "export_url": "gs://bucket/norm.csv",
            "plot_urls": {},
        }

    async def fake_imp(**kwargs):
        imp_calls.append(kwargs)
        return {
            "status": "pass",
            "module": "imputation",
            "session_id": "sess_imp",
            "summary": {"nulls_filled": 1},
            "artifact_path": "exports/reports/imputation/auto_runtime_imputation_report.html",
            "artifact_url": "",
            "export_url": "gs://bucket/imp.csv",
            "plot_urls": {},
        }

    monkeypatch.setattr(auto_heal_tool, "_toolkit_infer_configs", fake_infer_configs)
    monkeypatch.setattr(auto_heal_tool, "_toolkit_normalization", fake_norm)
    monkeypatch.setattr(auto_heal_tool, "_toolkit_imputation", fake_imp)
    monkeypatch.setattr(auto_heal_tool, "export_html_report", lambda *args, **kwargs: "auto.html")
    monkeypatch.setattr(
        auto_heal_tool,
        "deliver_artifact",
        lambda local_path, *args, **kwargs: {
            "reference": "https://example.com/auto",
            "local_path": local_path,
            "url": "https://example.com/auto",
            "warnings": [],
            "destinations": {"gcs": {"status": "available", "url": "https://example.com/auto"}},
        },
    )
    monkeypatch.setattr(auto_heal_tool, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_tool, "get_session_metadata", lambda sid: {"row_count": 3})

    result = await auto_heal_tool._toolkit_auto_heal(
        runtime={
            "run": {"run_id": "auto_runtime", "input_path": "gs://bucket/runtime.csv"},
            "artifacts": {"export_html": False},
        }
    )

    assert result["runtime_applied"] is True
    assert result["run_id"] == "auto_runtime"
    assert infer_calls[0]["gcs_path"] == "gs://bucket/runtime.csv"
    assert infer_calls[0]["run_id"] == "auto_runtime"
    assert result["dashboard_label"] == ""
    assert result["artifact_url"] == ""
    assert result["artifact_matrix"]["html_report"]["status"] == "disabled"
    assert norm_calls[0]["runtime"]["artifacts"]["export_html"] is False
    assert imp_calls[0]["runtime"]["artifacts"]["export_html"] is False


@pytest.mark.asyncio
async def test_auto_heal_worker_marks_failed_when_result_status_is_error(mocker):
    auto_heal_tool.JobStore.clear()
    job_id = auto_heal_tool.JobStore.create(module="auto_heal", run_id="run_auto")
    mocker.patch.object(
        auto_heal_tool,
        "_run_auto_heal_pipeline",
        return_value={"status": "error", "module": "auto_heal"},
    )

    await auto_heal_tool._auto_heal_worker(
        job_id=job_id,
        gcs_path=None,
        session_id="sess_test",
        runtime=None,
        run_id="run_auto",
        input_id=None,
    )
    job = auto_heal_tool.JobStore.get(job_id)

    assert job is not None
    assert job["state"] == "failed"
    assert job["error"]["terminal_result_status"] == "error"
