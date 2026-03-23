import base64
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_tool
import analyst_toolkit.mcp_server.tools.diagnostics as diagnostics_tool
import analyst_toolkit.mcp_server.tools.duplicates as duplicates_tool
import analyst_toolkit.mcp_server.tools.final_audit as final_audit_tool
import analyst_toolkit.mcp_server.tools.imputation as imputation_tool
import analyst_toolkit.mcp_server.tools.infer_configs as infer_configs_tool
import analyst_toolkit.mcp_server.tools.normalization as normalization_tool
import analyst_toolkit.mcp_server.tools.outliers as outliers_tool
import analyst_toolkit.mcp_server.tools.read_artifact as read_artifact_tool
import analyst_toolkit.mcp_server.tools.session as session_tool
import analyst_toolkit.mcp_server.tools.upload_input as upload_input_tool
import analyst_toolkit.mcp_server.tools.validation as validation_tool
from analyst_toolkit.mcp_server.state import StateStore


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
    # handling resolves to outliers, certification is its own module — neither unsupported
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
    assert any("Run infer_configs first" in warning for warning in result["warnings"])


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
    assert any("Run infer_configs first" in warning for warning in result["warnings"])


@pytest.mark.asyncio
async def test_imputation_runtime_can_disable_plot_artifacts(mocker):
    df = pd.DataFrame({"value": [1.0, None]})

    mocker.patch.object(imputation_tool, "load_input", return_value=df)
    mocker.patch.object(imputation_tool, "run_imputation_pipeline", return_value=df.fillna(0.0))
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
        run_id="imp_runtime_no_plots",
        config={},
        runtime={"artifacts": {"export_html": True, "plotting": False}},
    )

    assert result["runtime_applied"] is True
    assert result["artifact_matrix"]["plots"]["status"] == "disabled"
    assert result["artifact_matrix"]["html_report"]["expected"] is True


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
    assert any("Run infer_configs first" in warning for warning in result["warnings"])


@pytest.mark.asyncio
async def test_outliers_runtime_requests_are_explained_when_no_findings(mocker):
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
        run_id="out_runtime_requests",
        config={},
        runtime={"artifacts": {"export_html": True, "plotting": True}},
    )

    assert result["runtime_applied"] is True
    assert result["artifact_matrix"]["html_report"]["status"] == "disabled"
    assert result["artifact_matrix"]["plots"]["status"] == "disabled"
    assert any("runtime.artifacts.export_html=true" in warning for warning in result["warnings"])
    assert any("runtime.artifacts.plotting=true" in warning for warning in result["warnings"])


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
async def test_toolkit_validation_warns_without_inferred_or_explicit_config(mocker):
    df = pd.DataFrame({"value": [1, 2]})

    mocker.patch.object(validation_tool, "load_input", return_value=df)
    mocker.patch.object(validation_tool, "save_to_session", return_value="sess_val")
    mocker.patch.object(validation_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(validation_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(validation_tool, "run_validation_pipeline", return_value=df)
    mocker.patch.object(validation_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(validation_tool, "should_export_html", return_value=False)
    mocker.patch.object(
        validation_tool,
        "run_validation_suite",
        return_value={"schema_conformity": {"passed": True, "details": {}}},
    )

    result = await validation_tool._toolkit_validation(
        session_id="sess_val",
        run_id="val_missing_config",
        config={},
    )

    assert any("Run infer_configs first" in warning for warning in result["warnings"])


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
async def test_normalization_runtime_export_request_warns_when_no_changes(mocker):
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
        run_id="norm_runtime_request",
        config={},
        runtime={"artifacts": {"export_html": True}},
    )

    assert result["runtime_applied"] is True
    assert result["artifact_matrix"]["html_report"]["status"] == "disabled"
    assert any("runtime.artifacts.export_html=true" in warning for warning in result["warnings"])


@pytest.mark.asyncio
async def test_toolkit_duplicates_warns_without_inferred_or_explicit_config(mocker):
    df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})

    mocker.patch.object(duplicates_tool, "load_input", return_value=df)
    mocker.patch.object(duplicates_tool, "run_duplicates_pipeline", return_value=df)
    mocker.patch.object(duplicates_tool, "save_to_session", return_value="sess_dup")
    mocker.patch.object(duplicates_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(duplicates_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(duplicates_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(duplicates_tool, "should_export_html", return_value=False)

    result = await duplicates_tool._toolkit_duplicates(
        session_id="sess_dup",
        run_id="dup_missing_config",
        config={},
    )

    assert any("Run infer_configs first" in warning for warning in result["warnings"])


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
async def test_final_audit_disables_xlsx_expectation_when_no_xlsx_artifact(mocker, monkeypatch):
    df = pd.DataFrame({"value": [1]})

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_final")
    mocker.patch.object(final_audit_tool, "get_session_metadata", return_value={"row_count": 1})
    mocker.patch.object(final_audit_tool, "save_output", return_value="gs://dummy/out.csv")
    mocker.patch.object(
        final_audit_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": "",
            "local_path": local_path if local_path.endswith(".html") else "",
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
        session_id="sess_final",
        run_id="final_no_xlsx",
        config={},
    )

    assert result["artifact_matrix"]["xlsx_report"]["expected"] is False
    assert result["artifact_matrix"]["xlsx_report"]["status"] == "disabled"
    assert not any("Artifact not found for routing" in warning for warning in result["warnings"])


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
async def test_register_input_tool_falls_back_to_allowlisted_error_code(monkeypatch):
    from analyst_toolkit.mcp_server.input.errors import InputError

    class FutureInputError(InputError):
        code = "INPUT_FUTURE_MODE"

    async def run():
        return await input_ingest_tool._toolkit_register_input(uri="gs://bucket/data.csv")

    def raise_future_error(*args, **kwargs):
        raise FutureInputError("Unexpected future input error")

    import analyst_toolkit.mcp_server.tools.input_ingest as input_ingest_tool

    monkeypatch.setattr(input_ingest_tool, "register_input_source", raise_future_error)

    result = await run()

    assert result["status"] == "error"
    assert result["code"] == "INPUT_ERROR"
    assert "trace_id" in result
    assert isinstance(result["trace_id"], str)
    assert result["trace_id"]


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


@pytest.mark.asyncio
async def test_final_audit_accepts_certification_rules_shorthand(mocker, monkeypatch):
    """certification.rules shorthand should be lifted into schema_validation.rules."""
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_cert_rules")
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

    # Pass rules inside certification (common agent shorthand) rather than
    # the canonical certification.schema_validation.rules path.
    result = await final_audit_tool._toolkit_final_audit(
        session_id="sess_cert_rules",
        run_id="cert_rules_shorthand",
        config={
            "final_audit": {
                "certification": {
                    "run": True,
                    "fail_on_error": True,
                    "rules": {
                        "expected_columns": ["id", "value"],
                        "expected_types": {"id": "int64", "value": "str"},
                    },
                }
            }
        },
    )

    cert_cfg = result["effective_config"].get("certification", {})
    schema_cfg = cert_cfg.get("schema_validation", {})
    rules = schema_cfg.get("rules", {})
    assert rules.get("expected_columns") == ["id", "value"]
    assert rules.get("expected_types") == {"id": "int64", "value": "str"}
    # Should not have rule_contract_missing violation
    assert "rule_contract_missing" not in result.get("violations_found", [])
    StateStore.clear()


@pytest.mark.asyncio
async def test_final_audit_strips_stale_inferred_paths(mocker, monkeypatch):
    """Inferred config with stale raw_data_path/input_path should not crash final_audit."""
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()

    session_id = StateStore.save(df, run_id="fa_stale_path")
    # Simulate infer_configs output that embeds a temp file path (now deleted)
    StateStore.save_config(
        session_id,
        "final_audit",
        (
            "final_audit:\n"
            "  raw_data_path: /tmp/does_not_exist.csv\n"
            "  input_path: /tmp/also_gone.csv\n"
            "  input_df_path: exports/joblib/{run_id}/gone.joblib\n"
            "  certification:\n"
            "    schema_validation:\n"
            "      rules:\n"
            "        expected_columns:\n"
            "          - id\n"
            "          - value\n"
        ),
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

    # Should not crash with FileNotFoundError on the stale temp path
    result = await final_audit_tool._toolkit_final_audit(
        session_id=session_id,
        run_id="fa_stale_path",
        config={},
    )

    assert result["status"] != "error"
    # Stale paths should be stripped; effective config should not contain them
    assert "raw_data_path" not in result["effective_config"]
    assert "input_path" not in result["effective_config"]
    # Certification rules should still be discovered
    assert "rule_contract_missing" not in result.get("violations_found", [])
    StateStore.clear()


@pytest.mark.asyncio
async def test_final_audit_resolves_session_from_input_id(mocker, monkeypatch):
    """final_audit should discover inferred configs via input_id's session_id."""
    from analyst_toolkit.mcp_server.input.models import InputDescriptor

    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()

    # Pre-populate session with inferred certification config
    session_id = StateStore.save(df, run_id="fa_input_id_run")
    StateStore.save_config(
        session_id,
        "final_audit",
        (
            "final_audit:\n"
            "  certification:\n"
            "    schema_validation:\n"
            "      rules:\n"
            "        expected_columns:\n"
            "          - id\n"
            "          - value\n"
        ),
    )

    # Mock input descriptor that points back to the session
    descriptor = InputDescriptor(
        input_id="input_test_resolve",
        source_type="server_path",
        original_reference="/tmp/test.csv",
        resolved_reference="/tmp/test.csv",
        display_name="test.csv",
        media_type="text/csv",
        session_id=session_id,
        run_id="fa_input_id_run",
    )
    mocker.patch.object(final_audit_tool, "get_input_descriptor", return_value=descriptor)
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

    # Call with input_id only — no session_id provided
    result = await final_audit_tool._toolkit_final_audit(
        input_id="input_test_resolve",
        run_id="fa_input_id_run",
        config={},
    )

    # Should discover cert rules from the input_id's linked session
    assert "rule_contract_missing" not in result.get("violations_found", [])
    cert_cfg = result["effective_config"].get("certification", {})
    schema_cfg = cert_cfg.get("schema_validation", {})
    rules = schema_cfg.get("rules", {})
    assert rules.get("expected_columns") == ["id", "value"]
    StateStore.clear()


@pytest.mark.asyncio
async def test_final_audit_strips_stale_paths_from_provided_config(mocker, monkeypatch):
    """When the agent passes raw inferred YAML as explicit config, stale /tmp paths
    should still be stripped so final_audit doesn't crash with FileNotFoundError."""
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    StateStore.clear()

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_provided")
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
    monkeypatch.setattr(final_audit_tool, "ALLOW_EMPTY_CERT_RULES", True)

    # Agent passes inferred YAML verbatim as explicit config with stale temp paths
    result = await final_audit_tool._toolkit_final_audit(
        session_id="sess_provided",
        run_id="fa_provided_stale",
        config={
            "raw_data_path": "/tmp/tmpABCDEF.csv",
            "input_path": "/tmp/tmpGHIJKL.csv",
            "input_df_path": "/tmp/tmpMNOPQR.csv",
            "certification": {
                "schema_validation": {
                    "rules": {"expected_columns": ["id", "value"]},
                },
            },
        },
    )

    # Should not crash; stale paths stripped before pipeline runs
    assert result["status"] != "error"
    assert result["effective_config"].get("raw_data_path") is None or not result[
        "effective_config"
    ].get("raw_data_path", "").startswith("/tmp/")
    StateStore.clear()


@pytest.mark.asyncio
async def test_final_audit_creates_output_directories(mocker, monkeypatch, tmp_path):
    """final_audit should auto-create directories referenced in settings.paths."""
    df = pd.DataFrame({"id": [1], "value": ["a"]})
    StateStore.clear()

    # Track the module_cfg that run_final_audit_pipeline receives
    captured_cfg = {}

    def fake_pipeline(config, df, run_id, notebook):
        captured_cfg.update(config)
        return df

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", side_effect=fake_pipeline)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_dirs")
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
    mocker.patch.object(
        final_audit_tool,
        "run_validation_suite",
        return_value={"schema_conformity": {"passed": True, "details": {}}},
    )
    monkeypatch.setattr(final_audit_tool, "ALLOW_EMPTY_CERT_RULES", True)

    result = await final_audit_tool._toolkit_final_audit(
        session_id="sess_dirs",
        run_id="fa_dirs_test",
        config={},
    )

    assert result["status"] != "error"
    # The default paths should have their parent directories created
    paths = captured_cfg.get("final_audit", {}).get("settings", {}).get("paths", {})
    for path_template in paths.values():
        resolved = path_template.replace("{run_id}", "fa_dirs_test")
        parent = Path(resolved).parent
        assert parent.exists(), f"Expected directory {parent} to be auto-created"
    StateStore.clear()


@pytest.mark.asyncio
async def test_final_audit_rejects_path_traversal(mocker, monkeypatch, tmp_path):
    """Paths that traverse outside the project root must not create directories."""
    df = pd.DataFrame({"id": [1], "value": ["a"]})
    StateStore.clear()

    traversal_target = tmp_path / "escaped"
    captured_cfg = {}

    def fake_pipeline(config, df, run_id, notebook):
        captured_cfg.update(config)
        return df

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", side_effect=fake_pipeline)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_trav")
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
    mocker.patch.object(
        final_audit_tool,
        "run_validation_suite",
        return_value={"schema_conformity": {"passed": True, "details": {}}},
    )
    monkeypatch.setattr(final_audit_tool, "ALLOW_EMPTY_CERT_RULES", True)

    # Inject a traversal path into the config
    result = await final_audit_tool._toolkit_final_audit(
        session_id="sess_trav",
        run_id="fa_traversal",
        config={
            "settings": {
                "paths": {
                    "checkpoint_csv": f"../../../{traversal_target}/evil.csv",
                },
            },
        },
    )

    assert result["status"] != "error"
    # The traversal target must NOT have been created
    assert not traversal_target.exists(), (
        f"Path traversal created directory outside project root: {traversal_target}"
    )
    # The traversal path must be stripped from the config passed to the pipeline
    pipeline_paths = captured_cfg.get("final_audit", {}).get("settings", {}).get("paths", {})
    assert "checkpoint_csv" not in pipeline_paths, (
        "Traversal path was not removed from pipeline config"
    )
    StateStore.clear()


# ── manage_session tool tests ──


@pytest.mark.asyncio
async def test_manage_session_list():
    StateStore.clear()
    df = pd.DataFrame({"a": [1, 2]})
    sid1 = StateStore.save(df, run_id="run_1")
    sid2 = StateStore.save(df, run_id="run_2")

    result = await session_tool._toolkit_manage_session(action="list")
    assert result["status"] == "pass"
    assert result["session_count"] == 2
    session_ids = {s["session_id"] for s in result["sessions"]}
    assert sid1 in session_ids
    assert sid2 in session_ids
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_inspect():
    StateStore.clear()
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = StateStore.save(df, run_id="inspect_run")
    StateStore.save_config(sid, "validation", "validation:\n  run: true\n")

    result = await session_tool._toolkit_manage_session(action="inspect", session_id=sid)
    assert result["status"] == "pass"
    assert result["session"]["session_id"] == sid
    assert result["session"]["run_id"] == "inspect_run"
    assert result["session"]["row_count"] == 3
    assert "validation" in result["session"]["stored_configs"]
    assert "next_actions" in result
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_inspect_missing():
    StateStore.clear()
    result = await session_tool._toolkit_manage_session(
        action="inspect", session_id="sess_nonexistent"
    )
    assert result["status"] == "error"
    assert result["error_code"] == "SESSION_NOT_FOUND"
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_fork():
    StateStore.clear()
    df = pd.DataFrame({"col": [10, 20, 30]})
    sid = StateStore.save(df, run_id="original_run")
    StateStore.save_config(sid, "diagnostics", "diagnostics:\n  run: true\n")

    result = await session_tool._toolkit_manage_session(
        action="fork", session_id=sid, run_id="forked_run"
    )
    assert result["status"] == "pass"
    assert result["source_session_id"] == sid
    new_sid = result["new_session_id"]
    assert new_sid != sid
    assert result["run_id"] == "forked_run"
    assert result["configs_copied"] is True

    # Verify the forked session has the data and configs
    forked_df = StateStore.get(new_sid)
    assert forked_df is not None
    assert len(forked_df) == 3
    assert StateStore.get_run_id(new_sid) == "forked_run"
    assert StateStore.get_config(new_sid, "diagnostics") is not None

    # Verify source session is unchanged
    assert StateStore.get_run_id(sid) == "original_run"
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_fork_without_configs():
    StateStore.clear()
    df = pd.DataFrame({"v": [1]})
    sid = StateStore.save(df, run_id="r1")
    StateStore.save_config(sid, "validation", "yaml")

    result = await session_tool._toolkit_manage_session(
        action="fork", session_id=sid, run_id="r2", copy_configs=False
    )
    assert result["status"] == "pass"
    new_sid = result["new_session_id"]
    assert StateStore.get_configs(new_sid) == {}
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_fork_generates_run_id():
    StateStore.clear()
    df = pd.DataFrame({"v": [1]})
    sid = StateStore.save(df)

    result1 = await session_tool._toolkit_manage_session(action="fork", session_id=sid)
    result2 = await session_tool._toolkit_manage_session(action="fork", session_id=sid)
    assert result1["status"] == "pass"
    assert result2["status"] == "pass"
    assert result1["run_id"]  # auto-generated, non-empty
    assert result2["run_id"]
    assert result1["run_id"] != result2["run_id"]  # unique even within same second
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_fork_missing_source():
    StateStore.clear()
    result = await session_tool._toolkit_manage_session(
        action="fork", session_id="sess_gone", run_id="new"
    )
    assert result["status"] == "error"
    assert result["error_code"] == "SESSION_NOT_FOUND"


@pytest.mark.asyncio
async def test_manage_session_rebind():
    StateStore.clear()
    df = pd.DataFrame({"v": [1]})
    sid = StateStore.save(df, run_id="old_run")

    result = await session_tool._toolkit_manage_session(
        action="rebind", session_id=sid, run_id="new_run"
    )
    assert result["status"] == "pass"
    assert result["previous_run_id"] == "old_run"
    assert result["new_run_id"] == "new_run"
    assert StateStore.get_run_id(sid) == "new_run"
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_rebind_missing_run_id():
    StateStore.clear()
    df = pd.DataFrame({"v": [1]})
    sid = StateStore.save(df, run_id="r1")

    result = await session_tool._toolkit_manage_session(action="rebind", session_id=sid)
    assert result["status"] == "error"
    assert result["error_code"] == "MISSING_RUN_ID"
    StateStore.clear()


@pytest.mark.asyncio
async def test_manage_session_unknown_action():
    result = await session_tool._toolkit_manage_session(action="delete")
    assert result["status"] == "error"
    assert result["error_code"] == "UNKNOWN_ACTION"


# ── upload_input tests ──


@pytest.mark.asyncio
async def test_upload_input_accepts_base64_csv(monkeypatch, tmp_path):
    monkeypatch.setenv("ANALYST_MCP_INPUT_ROOT", str(tmp_path / "inputs"))
    StateStore.clear()

    csv_content = b"species,bill_length_mm\nAdelie,39.1\nGentoo,46.5\n"
    encoded = base64.b64encode(csv_content).decode("ascii")

    result = await upload_input_tool._toolkit_upload_input(
        filename="penguins.csv",
        content_base64=encoded,
        load_into_session=True,
    )
    assert result["status"] == "pass"
    assert result["module"] == "upload_input"
    assert result["input"]["source_type"] == "upload"
    assert result["session_id"].startswith("sess_")
    assert result["summary"]["row_count"] == 2
    assert result["summary"]["column_count"] == 2
    StateStore.clear()


@pytest.mark.asyncio
async def test_upload_input_rejects_empty_base64():
    result = await upload_input_tool._toolkit_upload_input(
        filename="data.csv",
        content_base64="",
    )
    assert result["status"] == "error"
    assert result["code"] == "INPUT_EMPTY_UPLOAD"


@pytest.mark.asyncio
async def test_upload_input_rejects_invalid_base64():
    result = await upload_input_tool._toolkit_upload_input(
        filename="data.csv",
        content_base64="not!!!valid!!!base64",
    )
    assert result["status"] == "error"
    assert result["code"] == "INPUT_INVALID_BASE64"


# ── read_artifact tests ──


@pytest.mark.asyncio
async def test_read_artifact_returns_text_html(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "exports" / "reports" / "diagnostics"
    artifact_dir.mkdir(parents=True)
    artifact = artifact_dir / "run1_diagnostics_report.html"
    artifact.write_text("<html><body>Dashboard</body></html>", encoding="utf-8")
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")

    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(artifact),
    )
    assert result["status"] == "pass"
    assert result["encoding"] == "text"
    assert "<html>" in result["artifact_content"]
    assert result["filename"] == "run1_diagnostics_report.html"
    assert result["media_type"] == "text/html"


@pytest.mark.asyncio
async def test_read_artifact_returns_base64_for_binary(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "exports" / "plots"
    artifact_dir.mkdir(parents=True)
    artifact = artifact_dir / "chart.png"
    raw_bytes = b"\x89PNG\r\n\x1a\nfake_png_data"
    artifact.write_bytes(raw_bytes)
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")

    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(artifact),
    )
    assert result["status"] == "pass"
    assert result["encoding"] == "base64"
    decoded = base64.b64decode(result["content_base64"])
    assert decoded == raw_bytes


@pytest.mark.asyncio
async def test_read_artifact_rejects_traversal():
    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path="../../../etc/passwd",
    )
    assert result["status"] == "error"
    assert result["code"] == "ARTIFACT_PATH_DENIED"
    assert "traversal" in result["message"]


@pytest.mark.asyncio
async def test_read_artifact_rejects_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")
    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(tmp_path / "exports" / "reports" / "nonexistent.html"),
    )
    assert result["status"] == "error"
    assert result["code"] == "ARTIFACT_PATH_DENIED"
    assert "not found" in result["message"].lower()


@pytest.mark.asyncio
async def test_read_artifact_http_mode_rejects_cwd_path(tmp_path, monkeypatch):
    """In HTTP mode (non-stdio), only _ARTIFACT_ROOT is allowed — not CWD."""
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")
    monkeypatch.delenv("ANALYST_MCP_STDIO", raising=False)

    # Create a file under CWD but outside artifact root
    secret = tmp_path / "src" / "secret.py"
    secret.parent.mkdir(parents=True)
    secret.write_text("SECRET_KEY = 'oops'")

    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(secret),
    )
    assert result["status"] == "error"
    assert result["code"] == "ARTIFACT_PATH_DENIED"


@pytest.mark.asyncio
async def test_read_artifact_stdio_mode_allows_cwd_path(tmp_path, monkeypatch):
    """In stdio mode, CWD is an allowed root (client is local)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(read_artifact_tool, "_ARTIFACT_ROOT", tmp_path / "exports")
    monkeypatch.setenv("ANALYST_MCP_STDIO", "true")

    report = tmp_path / "my_report.html"
    report.write_text("<html>local</html>")

    result = await read_artifact_tool._toolkit_read_artifact(
        artifact_path=str(report),
    )
    assert result["status"] == "pass"
    assert result["encoding"] == "text"
