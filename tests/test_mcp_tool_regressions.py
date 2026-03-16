import sys
import types
from pathlib import Path

import pandas as pd
import pytest

import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_tool
import analyst_toolkit.mcp_server.tools.diagnostics as diagnostics_tool
import analyst_toolkit.mcp_server.tools.drift as drift_tool
import analyst_toolkit.mcp_server.tools.duplicates as duplicates_tool
import analyst_toolkit.mcp_server.tools.final_audit as final_audit_tool
import analyst_toolkit.mcp_server.tools.imputation as imputation_tool
import analyst_toolkit.mcp_server.tools.infer_configs as infer_configs_tool
import analyst_toolkit.mcp_server.tools.normalization as normalization_tool
import analyst_toolkit.mcp_server.tools.outliers as outliers_tool
import analyst_toolkit.mcp_server.tools.validation as validation_tool
from analyst_toolkit.mcp_server.state import StateStore


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
    assert result["warnings"] == []


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
    assert result["warnings"] == []


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
    # certification is its own supported module, not aliased
    assert "certification" in result["configs"]
    # outlier → outliers alias still works
    assert "outliers" in result["configs"]
    # dups → duplicates alias still works
    assert "duplicates" in result["configs"]
    assert result["unsupported_modules"] == []


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

    mocker.patch.object(normalization_tool, "load_input", return_value=df)
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

    # Close captured coroutine to avoid un-awaited coroutine warnings in tests.
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


@pytest.mark.asyncio
async def test_tool_run_id_mismatch_is_coerced_to_session_run_by_default(monkeypatch, mocker):
    import analyst_toolkit.mcp_server.io as io_module

    StateStore.clear()
    df = pd.DataFrame({"id": [1], "name": ["a"]})
    session_id = StateStore.save(df, run_id="run_bound")

    mocker.patch.object(diagnostics_tool, "load_input", return_value=df)
    mocker.patch.object(diagnostics_tool, "save_to_session", return_value=session_id)
    mocker.patch.object(diagnostics_tool, "run_diag_pipeline", return_value=None)
    mocker.patch.object(diagnostics_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(diagnostics_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(diagnostics_tool, "should_export_html", return_value=False)
    monkeypatch.setattr(io_module, "RUN_ID_OVERRIDE_ALLOWED", False)

    result = await diagnostics_tool._toolkit_diagnostics(
        session_id=session_id,
        run_id="run_conflict",
        config={},
    )

    assert result["run_id"] == "run_bound"
    assert result["lifecycle"]["coerced"] is True
    assert any("Coerced to session run_id" in warning for warning in result["warnings"])
    StateStore.clear()


@pytest.mark.asyncio
async def test_toolkit_imputation_handles_duplicate_column_names_in_summary(mocker):
    df = pd.DataFrame([[1.0, None], [2.0, 3.0]], columns=["metric", "metric"])
    df_imputed = pd.DataFrame([[1.0, 0.0], [2.0, 3.0]], columns=["metric", "metric"])

    mocker.patch.object(imputation_tool, "load_input", return_value=df)
    mocker.patch.object(imputation_tool, "run_imputation_pipeline", return_value=df_imputed)
    mocker.patch.object(imputation_tool, "save_to_session", return_value="sess_imp")
    mocker.patch.object(imputation_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(imputation_tool, "save_output", return_value="gs://dummy/imp.csv")
    mocker.patch.object(imputation_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(imputation_tool, "should_export_html", return_value=False)

    result = await imputation_tool._toolkit_imputation(
        session_id="sess_imp",
        run_id="imputation_duplicate_columns",
        config={"imputation": {"rules": {"strategies": {"metric": "median"}}}},
    )

    assert result["status"] in {"pass", "warn"}
    assert result["nulls_filled"] == 1
    assert result["columns_imputed"] == ["metric"]


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
    # handling resolves to outliers, certification is its own module — neither unsupported
    assert result["unsupported_modules"] == []


@pytest.mark.asyncio
async def test_final_audit_fails_closed_when_cert_rules_empty(mocker, monkeypatch):
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
            "url": "https://example.com/a",
            "warnings": [],
            "destinations": {},
        },
    )
    mocker.patch.object(final_audit_tool, "append_to_run_history", return_value=None)
    monkeypatch.setattr(final_audit_tool, "ALLOW_EMPTY_CERT_RULES", False)

    result = await final_audit_tool._toolkit_final_audit(
        session_id="sess_test",
        run_id="final_audit_empty_rules",
        config={},
    )

    assert result["status"] == "fail"
    assert result["passed"] is False
    assert "rule_contract_missing" in result["violations_found"]


@pytest.mark.asyncio
async def test_normalization_reports_artifact_contract(mocker):
    df = pd.DataFrame({"name": ["Alice"]})

    mocker.patch.object(normalization_tool, "load_input", return_value=df)
    mocker.patch.object(
        normalization_tool,
        "run_normalization_pipeline",
        return_value=(
            df,
            {
                "changelog": {"values_mapped": pd.DataFrame([{"Mappings Applied": 2}])},
                "changes_made": 2,
            },
        ),
    )
    mocker.patch.object(normalization_tool, "save_to_session", return_value="sess_norm")
    mocker.patch.object(normalization_tool, "get_session_metadata", return_value={"row_count": 1})
    mocker.patch.object(normalization_tool, "save_output", return_value="gs://dummy/norm.csv")
    mocker.patch.object(normalization_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(normalization_tool, "should_export_html", return_value=True)
    mocker.patch.object(
        normalization_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path,
            "url": "",
            "warnings": [f"Upload failed or file not found: {local_path}"],
            "destinations": {},
        },
    )

    result = await normalization_tool._toolkit_normalization(
        session_id="sess_norm",
        run_id="norm_artifact_contract",
        config={},
    )

    # Artifact delivery warnings are informational and do not escalate status
    assert result["status"] == "pass"
    assert "artifact_matrix" in result
    assert "html_report" in result["artifact_matrix"]
    # HTML/XLSX are expected but not required — missing ones do not force warn
    assert "html_report" not in result["missing_required_artifacts"]
    assert "xlsx_report" in result["artifact_matrix"]
    assert result["artifact_matrix"]["xlsx_report"]["status"] == "missing"
    # Delivery warnings still appear in the response for client visibility
    assert any("Upload failed" in w for w in result["warnings"])
    assert (
        result["dashboard_path"]
        == "exports/reports/normalization/norm_artifact_contract_normalization_report.html"
    )
    assert result["dashboard_url"] == ""
    assert result["dashboard_label"] == "Normalization dashboard"


@pytest.mark.asyncio
async def test_normalization_disabled_html_when_no_changes(mocker):
    """When normalization makes 0 changes, HTML/XLSX should be disabled, not missing."""
    df = pd.DataFrame({"name": ["Alice"]})

    mocker.patch.object(normalization_tool, "load_input", return_value=df)
    mocker.patch.object(
        normalization_tool,
        "run_normalization_pipeline",
        return_value=(df, {"changelog": {}, "changes_made": 0}),
    )
    mocker.patch.object(normalization_tool, "save_to_session", return_value="sess_norm")
    mocker.patch.object(normalization_tool, "get_session_metadata", return_value={"row_count": 1})
    mocker.patch.object(normalization_tool, "save_output", return_value="gs://bucket/norm.csv")
    mocker.patch.object(normalization_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(normalization_tool, "should_export_html", return_value=True)
    mocker.patch.object(
        normalization_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path,
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await normalization_tool._toolkit_normalization(
        session_id="sess_norm",
        run_id="norm_no_changes",
        config={},
    )

    assert result["status"] == "pass"
    assert result["missing_required_artifacts"] == []
    # With 0 changes, reports should be disabled, not missing
    assert result["artifact_matrix"]["html_report"]["expected"] is False
    assert result["artifact_matrix"]["html_report"]["status"] == "disabled"
    assert result["artifact_matrix"]["xlsx_report"]["status"] == "disabled"


@pytest.mark.asyncio
async def test_imputation_disabled_html_when_no_nulls_filled(mocker):
    """When imputation fills 0 nulls, HTML/XLSX should be disabled, not missing."""
    df = pd.DataFrame({"value": [1.0, 2.0]})

    mocker.patch.object(imputation_tool, "load_input", return_value=df)
    mocker.patch.object(
        imputation_tool,
        "run_imputation_pipeline",
        return_value=df,
    )
    mocker.patch.object(imputation_tool, "save_to_session", return_value="sess_imp")
    mocker.patch.object(imputation_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(imputation_tool, "save_output", return_value="gs://bucket/imp.csv")
    mocker.patch.object(imputation_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(imputation_tool, "should_export_html", return_value=True)
    mocker.patch.object(
        imputation_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path,
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await imputation_tool._toolkit_imputation(
        session_id="sess_imp",
        run_id="imp_no_nulls",
        config={},
    )

    assert result["status"] == "pass"
    assert result["missing_required_artifacts"] == []
    # With 0 nulls filled, reports should be disabled, not missing
    assert result["artifact_matrix"]["html_report"]["expected"] is False
    assert result["artifact_matrix"]["html_report"]["status"] == "disabled"
    assert result["artifact_matrix"]["xlsx_report"]["status"] == "disabled"


@pytest.mark.asyncio
async def test_outliers_disabled_html_when_no_outliers(mocker):
    """When outlier detection finds 0 outliers, HTML/XLSX should be disabled, not missing."""
    df = pd.DataFrame({"value": [1.0, 2.0, 3.0]})

    mocker.patch.object(outliers_tool, "load_input", return_value=df)
    mocker.patch.object(
        outliers_tool,
        "run_outlier_detection_pipeline",
        return_value=(df, {"outlier_log": pd.DataFrame()}),
    )
    mocker.patch.object(outliers_tool, "save_to_session", return_value="sess_out")
    mocker.patch.object(outliers_tool, "get_session_metadata", return_value={"row_count": 3})
    mocker.patch.object(outliers_tool, "save_output", return_value="gs://bucket/out.csv")
    mocker.patch.object(outliers_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(outliers_tool, "should_export_html", return_value=True)
    mocker.patch.object(
        outliers_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path,
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await outliers_tool._toolkit_outliers(
        session_id="sess_out",
        run_id="out_no_outliers",
        config={},
    )

    assert result["status"] == "pass"
    assert result["missing_required_artifacts"] == []
    # With 0 outliers, reports should be disabled, not missing
    assert result["artifact_matrix"]["html_report"]["expected"] is False
    assert result["artifact_matrix"]["html_report"]["status"] == "disabled"
    assert result["artifact_matrix"]["xlsx_report"]["status"] == "disabled"


@pytest.mark.asyncio
async def test_toolkit_validation_runtime_can_disable_html_artifacts(mocker):
    df = pd.DataFrame({"value": [1, 2]})

    mocker.patch.object(validation_tool, "load_input", return_value=df)
    mocker.patch.object(validation_tool, "save_to_session", return_value="sess_val")
    mocker.patch.object(validation_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(validation_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(validation_tool, "run_validation_pipeline", return_value=df)
    mocker.patch.object(validation_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(
        validation_tool,
        "run_validation_suite",
        return_value={"schema_conformity": {"passed": True, "details": {}}},
    )

    result = await validation_tool._toolkit_validation(
        session_id="sess_val",
        config={},
        runtime={"artifacts": {"export_html": False}},
    )

    assert result["runtime_applied"] is True
    assert result["artifact_path"] == ""
    assert result["artifact_matrix"]["html_report"]["status"] == "disabled"


@pytest.mark.asyncio
async def test_toolkit_duplicates_runtime_can_override_input_and_html(mocker):
    df = pd.DataFrame({"id": [1, 1], "value": [10, 10]})
    captured = {}

    def fake_load_input(path=None, session_id=None, input_id=None):
        captured["input_path"] = path
        captured["input_id"] = input_id
        return df

    mocker.patch.object(duplicates_tool, "load_input", side_effect=fake_load_input)
    mocker.patch.object(
        duplicates_tool,
        "run_duplicates_pipeline",
        return_value=df.assign(is_duplicate=[False, True]),
    )
    mocker.patch.object(duplicates_tool, "save_to_session", return_value="sess_dup")
    mocker.patch.object(duplicates_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(duplicates_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(duplicates_tool, "append_to_run_history", return_value=None)

    result = await duplicates_tool._toolkit_duplicates(
        runtime={
            "run": {"run_id": "dup_runtime", "input_path": "gs://bucket/dup.csv"},
            "artifacts": {"export_html": False},
        },
        config={},
    )

    assert result["runtime_applied"] is True
    assert result["run_id"] == "dup_runtime"
    assert captured["input_path"] == "gs://bucket/dup.csv"
    assert captured["input_id"] is None
    assert result["artifact_matrix"]["html_report"]["status"] == "disabled"


@pytest.mark.asyncio
async def test_toolkit_final_audit_runtime_can_override_input_path(mocker, monkeypatch):
    df = pd.DataFrame({"value": [1]})
    captured = {}

    def fake_load_input(path=None, session_id=None, input_id=None):
        captured["input_path"] = path
        captured["input_id"] = input_id
        return df

    mocker.patch.object(final_audit_tool, "load_input", side_effect=fake_load_input)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_final")
    mocker.patch.object(final_audit_tool, "get_session_metadata", return_value={"row_count": 1})
    mocker.patch.object(final_audit_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(
        final_audit_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path,
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )
    mocker.patch.object(final_audit_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(
        final_audit_tool,
        "run_validation_suite",
        return_value={"schema_conformity": {"passed": True, "details": {}}},
    )
    monkeypatch.setattr(final_audit_tool, "ALLOW_EMPTY_CERT_RULES", True)

    result = await final_audit_tool._toolkit_final_audit(
        runtime={"run": {"run_id": "final_runtime", "input_path": "gs://bucket/final.csv"}},
        config={},
    )

    assert result["runtime_applied"] is True
    assert result["run_id"] == "final_runtime"
    assert captured["input_path"] == "gs://bucket/final.csv"
    assert captured["input_id"] is None
    assert "final_runtime" in result["artifact_path"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "tool_module",
        "tool_name",
        "run_return_name",
        "run_return_value",
        "run_id",
        "expected_path",
        "expected_label",
    ),
    [
        (
            diagnostics_tool,
            "_toolkit_diagnostics",
            "run_diag_pipeline",
            None,
            "diag_artifact_contract",
            "exports/reports/diagnostics/diag_artifact_contract_diagnostics_report.html",
            "Diagnostics dashboard",
        ),
        (
            duplicates_tool,
            "_toolkit_duplicates",
            "run_duplicates_pipeline",
            pd.DataFrame({"id": [1], "is_duplicate": [False]}),
            "dup_artifact_contract",
            "exports/reports/duplicates/dup_artifact_contract_duplicates_report.html",
            "Duplicates dashboard",
        ),
        (
            outliers_tool,
            "_toolkit_outliers",
            "run_outlier_detection_pipeline",
            (
                pd.DataFrame({"value": [1]}),
                {"outlier_log": pd.DataFrame({"column": ["value"], "score": [3.5]})},
            ),
            "outlier_artifact_contract",
            "exports/reports/outliers/detection/outlier_artifact_contract_outlier_report.html",
            "Outlier detection dashboard",
        ),
        (
            imputation_tool,
            "_toolkit_imputation",
            "run_imputation_pipeline",
            pd.DataFrame({"value": [1.0, 0.0]}),
            "imputation_artifact_contract",
            "exports/reports/imputation/imputation_artifact_contract_imputation_report.html",
            "Imputation dashboard",
        ),
        (
            final_audit_tool,
            "_toolkit_final_audit",
            "run_final_audit_pipeline",
            pd.DataFrame({"value": [1]}),
            "final_audit_artifact_contract",
            "exports/reports/final_audit/final_audit_artifact_contract_final_audit_report.html",
            "Final audit dashboard",
        ),
    ],
)
async def test_other_modules_report_dashboard_artifact_contract(
    mocker,
    monkeypatch,
    tool_module,
    tool_name,
    run_return_name,
    run_return_value,
    run_id,
    expected_path,
    expected_label,
):
    df = pd.DataFrame({"value": [1, None]})
    mocker.patch.object(tool_module, "load_input", return_value=df)
    mocker.patch.object(tool_module, "save_to_session", return_value="sess_dash")
    mocker.patch.object(tool_module, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(tool_module, "append_to_run_history", return_value=None)
    if hasattr(tool_module, "deliver_artifact"):
        mocker.patch.object(
            tool_module,
            "deliver_artifact",
            side_effect=lambda local_path, *args, **kwargs: {
                "reference": "",
                "local_path": local_path,
                "url": "",
                "warnings": [],
                "destinations": {},
            },
        )
    elif hasattr(tool_module, "upload_artifact"):
        mocker.patch.object(tool_module, "upload_artifact", return_value="")
    mocker.patch.object(tool_module, run_return_name, return_value=run_return_value)

    if hasattr(tool_module, "get_session_metadata"):
        mocker.patch.object(tool_module, "get_session_metadata", return_value={"row_count": 1})
    if hasattr(tool_module, "should_export_html"):
        mocker.patch.object(tool_module, "should_export_html", return_value=True)
    if tool_module is final_audit_tool:
        monkeypatch.setattr(tool_module, "ALLOW_EMPTY_CERT_RULES", True)

    result = await getattr(tool_module, tool_name)(
        session_id="sess_dash",
        run_id=run_id,
        config={},
    )

    assert result["dashboard_path"] == expected_path
    assert result["dashboard_url"] == ""
    assert result["dashboard_label"] == expected_label


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
    # The save_to_session call for the new session should include run_id
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

    # Configs should now be persisted in StateStore
    stored_fa = StateStore.get_config("sess_persist_test", "final_audit")
    stored_cert = StateStore.get_config("sess_persist_test", "certification")
    assert stored_fa is not None
    assert "expected_columns" in stored_fa
    assert stored_cert is not None
    StateStore.clear()


@pytest.mark.asyncio
async def test_final_audit_auto_discovers_inferred_cert_config(mocker, monkeypatch):
    """final_audit should use inferred certification config from session when none provided."""
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()

    # Pre-populate session with inferred certification config
    session_id = StateStore.save(df, run_id="fa_inferred_run")
    StateStore.save_config(
        session_id,
        "final_audit",
        "final_audit:\n  certification:\n    schema_validation:\n      rules:\n        expected_columns:\n          - id\n          - value\n",
    )

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value=session_id)
    mocker.patch.object(final_audit_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(final_audit_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(
        final_audit_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path,
            "url": "https://example.com/a",
            "warnings": [],
            "destinations": {},
        },
    )
    mocker.patch.object(final_audit_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(
        final_audit_tool,
        "run_validation_suite",
        return_value={"schema_conformity": {"passed": True, "details": {}}},
    )
    monkeypatch.setattr(final_audit_tool, "ALLOW_EMPTY_CERT_RULES", False)

    result = await final_audit_tool._toolkit_final_audit(
        session_id=session_id,
        run_id="fa_inferred_run",
        config={},  # No explicit config — should auto-discover from session
    )

    # With inferred rules discovered, should NOT fail with rule_contract_missing
    assert "rule_contract_missing" not in result.get("violations_found", [])
    # The effective config should contain the inferred certification rules
    cert_cfg = result["effective_config"].get("certification", {})
    schema_cfg = cert_cfg.get("schema_validation", {})
    rules = schema_cfg.get("rules", {})
    assert rules.get("expected_columns") == ["id", "value"]
    StateStore.clear()


@pytest.mark.asyncio
async def test_final_audit_explicit_config_overrides_inferred(mocker, monkeypatch):
    """Explicit config should take precedence over inferred session config."""
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()

    session_id = StateStore.save(df, run_id="fa_override_run")
    # Store inferred config with expected_columns: [x, y]
    StateStore.save_config(
        session_id,
        "final_audit",
        "final_audit:\n  certification:\n    schema_validation:\n      rules:\n        expected_columns: [x, y]\n",
    )

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value=session_id)
    mocker.patch.object(final_audit_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(final_audit_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(
        final_audit_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path,
            "url": "https://example.com/a",
            "warnings": [],
            "destinations": {},
        },
    )
    mocker.patch.object(final_audit_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(
        final_audit_tool,
        "run_validation_suite",
        return_value={"schema_conformity": {"passed": True, "details": {}}},
    )
    monkeypatch.setattr(final_audit_tool, "ALLOW_EMPTY_CERT_RULES", False)

    # Explicit config with expected_columns: [id, value]
    result = await final_audit_tool._toolkit_final_audit(
        session_id=session_id,
        run_id="fa_override_run",
        config={
            "certification": {
                "schema_validation": {
                    "rules": {"expected_columns": ["id", "value"]},
                }
            }
        },
    )

    # Explicit config should win over inferred
    cert_cfg = result["effective_config"].get("certification", {})
    schema_cfg = cert_cfg.get("schema_validation", {})
    rules = schema_cfg.get("rules", {})
    assert rules.get("expected_columns") == ["id", "value"]
    StateStore.clear()
