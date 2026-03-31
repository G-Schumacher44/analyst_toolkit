from pathlib import Path

import pandas as pd
import pytest

import analyst_toolkit.mcp_server.tools.diagnostics as diagnostics_tool
import analyst_toolkit.mcp_server.tools.duplicates as duplicates_tool
import analyst_toolkit.mcp_server.tools.final_audit as final_audit_tool
import analyst_toolkit.mcp_server.tools.imputation as imputation_tool
import analyst_toolkit.mcp_server.tools.normalization as normalization_tool
import analyst_toolkit.mcp_server.tools.outliers as outliers_tool
import analyst_toolkit.mcp_server.tools.validation as validation_tool
from analyst_toolkit.mcp_server.config_normalizers import _is_non_text_expected_type
from analyst_toolkit.mcp_server.state import StateStore


def test_is_non_text_expected_type_avoids_false_positive_substrings():
    assert _is_non_text_expected_type("Int64") is True
    assert _is_non_text_expected_type("datetime64[ns]") is True
    assert _is_non_text_expected_type("coordinator") is False
    assert _is_non_text_expected_type("interval") is False


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
async def test_validation_aligns_inferred_rules_to_transformed_session_state(mocker):
    df = pd.DataFrame(
        {
            "species": ["adelie", "gentoo"],
            "body_mass_g": [3000.0, 4200.0],
            "body_mass_g_iqr_outlier": [False, False],
        }
    )

    mocker.patch.object(validation_tool, "load_input", return_value=df)
    mocker.patch.object(validation_tool, "save_to_session", return_value="sess_validation")
    mocker.patch.object(validation_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(validation_tool, "save_output", return_value="gs://dummy/validation.csv")
    mocker.patch.object(validation_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(validation_tool, "should_export_html", return_value=False)
    mocker.patch.object(validation_tool, "run_validation_pipeline", return_value=None)

    result = await validation_tool._toolkit_validation(
        session_id="sess_validation",
        run_id="validation_runtime_alignment",
        config={
            "validation": {
                "schema_validation": {
                    "rules": {
                        "expected_columns": ["species", "body_mass_g"],
                        "expected_types": {
                            "species": "object",
                            "body_mass_g": "float64",
                        },
                        "categorical_values": {
                            "species": ["Adelie", "Gentoo"],
                            "body_mass_g": ["3000.0", "4200.0"],
                        },
                    }
                }
            }
        },
    )

    assert result["status"] == "pass"
    assert result["passed"] is True
    assert result["violations_found"] == []
    effective_rules = result["effective_config"]["schema_validation"]["rules"]
    assert "body_mass_g" not in effective_rules["categorical_values"]
    assert effective_rules["categorical_values"]["species"] == ["adelie", "gentoo"]
    assert "body_mass_g_iqr_outlier" in effective_rules["expected_columns"]


@pytest.mark.asyncio
async def test_final_audit_aligns_inferred_rules_to_transformed_session_state(mocker):
    df = pd.DataFrame(
        {
            "species": ["adelie", "gentoo"],
            "body_mass_g": [3000.0, 4200.0],
            "body_mass_g_iqr_outlier": [False, False],
        }
    )

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_final")
    mocker.patch.object(final_audit_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(final_audit_tool, "save_output", return_value="gs://dummy/final.csv")
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

    result = await final_audit_tool._toolkit_final_audit(
        session_id="sess_final",
        run_id="final_audit_runtime_alignment",
        config={
            "final_audit": {
                "certification": {
                    "schema_validation": {
                        "rules": {
                            "expected_columns": ["species", "body_mass_g"],
                            "expected_types": {
                                "species": "object",
                                "body_mass_g": "float64",
                            },
                            "categorical_values": {
                                "species": ["Adelie", "Gentoo"],
                                "body_mass_g": ["3000.0", "4200.0"],
                            },
                        }
                    }
                }
            }
        },
    )

    assert result["status"] == "pass"
    assert result["passed"] is True
    assert result["violations_found"] == []
    effective_rules = result["effective_config"]["certification"]["schema_validation"]["rules"]
    assert "body_mass_g" not in effective_rules["categorical_values"]
    assert effective_rules["categorical_values"]["species"] == ["adelie", "gentoo"]
    assert "body_mass_g_iqr_outlier" in effective_rules["expected_columns"]


@pytest.mark.asyncio
async def test_validation_aligns_object_date_rules_to_transformed_datetime_state(mocker):
    df = pd.DataFrame(
        {
            "capture_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "species": ["adelie", "gentoo"],
        }
    )

    mocker.patch.object(validation_tool, "load_input", return_value=df)
    mocker.patch.object(validation_tool, "save_to_session", return_value="sess_validation_date")
    mocker.patch.object(validation_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(validation_tool, "save_output", return_value="gs://dummy/validation.csv")
    mocker.patch.object(validation_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(validation_tool, "should_export_html", return_value=False)
    mocker.patch.object(validation_tool, "run_validation_pipeline", return_value=None)

    result = await validation_tool._toolkit_validation(
        session_id="sess_validation_date",
        run_id="validation_runtime_date_alignment",
        config={
            "validation": {
                "schema_validation": {
                    "rules": {
                        "expected_columns": ["capture_date", "species"],
                        "expected_types": {
                            "capture_date": "object",
                            "species": "object",
                        },
                        "categorical_values": {
                            "capture_date": ["2024-01-01", "2024-01-02"],
                            "species": ["Adelie", "Gentoo"],
                        },
                    }
                }
            }
        },
    )

    assert result["status"] == "pass"
    assert result["passed"] is True
    effective_rules = result["effective_config"]["schema_validation"]["rules"]
    assert effective_rules["expected_types"]["capture_date"] == "datetime64[ns]"
    assert "capture_date" not in effective_rules["categorical_values"]
    assert effective_rules["categorical_values"]["species"] == ["adelie", "gentoo"]


@pytest.mark.asyncio
async def test_final_audit_aligns_object_date_rules_to_transformed_datetime_state(mocker):
    df = pd.DataFrame(
        {
            "capture_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "species": ["adelie", "gentoo"],
        }
    )

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_final_date")
    mocker.patch.object(final_audit_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(final_audit_tool, "save_output", return_value="gs://dummy/final.csv")
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

    result = await final_audit_tool._toolkit_final_audit(
        session_id="sess_final_date",
        run_id="final_audit_runtime_date_alignment",
        config={
            "final_audit": {
                "certification": {
                    "schema_validation": {
                        "rules": {
                            "expected_columns": ["capture_date", "species"],
                            "expected_types": {
                                "capture_date": "object",
                                "species": "object",
                            },
                            "categorical_values": {
                                "capture_date": ["2024-01-01", "2024-01-02"],
                                "species": ["Adelie", "Gentoo"],
                            },
                        }
                    }
                }
            }
        },
    )

    assert result["status"] == "pass"
    assert result["passed"] is True
    effective_rules = result["effective_config"]["certification"]["schema_validation"]["rules"]
    assert effective_rules["expected_types"]["capture_date"] == "datetime64[ns]"
    assert "capture_date" not in effective_rules["categorical_values"]
    assert effective_rules["categorical_values"]["species"] == ["adelie", "gentoo"]


@pytest.mark.asyncio
async def test_validation_aligns_datetime_expectation_to_transformed_object_state(mocker):
    df = pd.DataFrame(
        {
            "capture_date": ["2024-01-01", "2024-01-02"],
            "species": ["adelie", "gentoo"],
        }
    )

    mocker.patch.object(validation_tool, "load_input", return_value=df)
    mocker.patch.object(
        validation_tool, "save_to_session", return_value="sess_validation_date_text"
    )
    mocker.patch.object(validation_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(validation_tool, "save_output", return_value="gs://dummy/validation.csv")
    mocker.patch.object(validation_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(validation_tool, "should_export_html", return_value=False)
    mocker.patch.object(validation_tool, "run_validation_pipeline", return_value=None)

    result = await validation_tool._toolkit_validation(
        session_id="sess_validation_date_text",
        run_id="validation_runtime_date_text_alignment",
        config={
            "validation": {
                "schema_validation": {
                    "rules": {
                        "expected_columns": ["capture_date", "species"],
                        "expected_types": {
                            "capture_date": "datetime64[ns]",
                            "species": "object",
                        },
                        "categorical_values": {
                            "species": ["Adelie", "Gentoo"],
                        },
                    }
                }
            }
        },
    )

    assert result["status"] == "pass"
    assert result["passed"] is True
    effective_rules = result["effective_config"]["schema_validation"]["rules"]
    assert effective_rules["expected_types"]["capture_date"] == "object"
    assert effective_rules["categorical_values"]["species"] == ["adelie", "gentoo"]


@pytest.mark.asyncio
async def test_final_audit_aligns_datetime_expectation_to_transformed_object_state(mocker):
    df = pd.DataFrame(
        {
            "capture_date": ["2024-01-01", "2024-01-02"],
            "species": ["adelie", "gentoo"],
        }
    )

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(final_audit_tool, "run_final_audit_pipeline", return_value=df)
    mocker.patch.object(final_audit_tool, "save_to_session", return_value="sess_final_date_text")
    mocker.patch.object(final_audit_tool, "get_session_metadata", return_value={"row_count": 2})
    mocker.patch.object(final_audit_tool, "save_output", return_value="gs://dummy/final.csv")
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

    result = await final_audit_tool._toolkit_final_audit(
        session_id="sess_final_date_text",
        run_id="final_audit_runtime_date_text_alignment",
        config={
            "final_audit": {
                "certification": {
                    "schema_validation": {
                        "rules": {
                            "expected_columns": ["capture_date", "species"],
                            "expected_types": {
                                "capture_date": "datetime64[ns]",
                                "species": "object",
                            },
                            "categorical_values": {
                                "species": ["Adelie", "Gentoo"],
                            },
                        }
                    }
                }
            }
        },
    )

    assert result["status"] == "pass"
    assert result["passed"] is True
    effective_rules = result["effective_config"]["certification"]["schema_validation"]["rules"]
    assert effective_rules["expected_types"]["capture_date"] == "object"
    assert effective_rules["categorical_values"]["species"] == ["adelie", "gentoo"]


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

    assert result["status"] == "warn"
    assert result["passed"] is False
    assert result["summary"]["passed"] is False
    assert any("Run infer_configs first" in warning for warning in result["warnings"])
    assert result["next_actions"][0]["tool"] == "infer_configs"
    assert all(action["tool"] != "final_audit" for action in result["next_actions"])


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
async def test_final_audit_uses_configured_artifact_paths_and_strips_nested_transient_paths(
    mocker, monkeypatch, tmp_path
):
    df = pd.DataFrame({"value": [1]})
    run_id = "final_audit_custom_paths"
    session_id = "sess_final_custom"
    monkeypatch.chdir(tmp_path)

    html_rel = f"exports/reports/custom/{run_id}_custom.html"
    xlsx_rel = f"exports/reports/custom/{run_id}_custom.xlsx"
    delivered_paths: list[str] = []
    captured_config: dict = {}

    def fake_run_final_audit_pipeline(*, config, df, run_id, notebook):
        captured_config.update(config)
        html_path = tmp_path / html_rel
        xlsx_path = tmp_path / xlsx_rel
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text("<html>final</html>", encoding="utf-8")
        xlsx_path.write_text("xlsx", encoding="utf-8")
        return df

    def fake_deliver_artifact(local_path, *args, **kwargs):
        delivered_paths.append(local_path)
        return {
            "reference": local_path,
            "local_path": local_path,
            "url": "",
            "warnings": [],
            "destinations": {},
        }

    mocker.patch.object(final_audit_tool, "load_input", return_value=df)
    mocker.patch.object(
        final_audit_tool, "run_final_audit_pipeline", side_effect=fake_run_final_audit_pipeline
    )
    mocker.patch.object(final_audit_tool, "save_to_session", return_value=session_id)
    mocker.patch.object(final_audit_tool, "get_session_metadata", return_value={"row_count": 1})
    mocker.patch.object(final_audit_tool, "save_output", return_value="gs://dummy/final.csv")
    mocker.patch.object(final_audit_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(final_audit_tool, "deliver_artifact", side_effect=fake_deliver_artifact)
    mocker.patch.object(
        final_audit_tool,
        "run_validation_suite",
        return_value={"schema_conformity": {"passed": True, "details": {}}},
    )

    result = await final_audit_tool._toolkit_final_audit(
        session_id=session_id,
        run_id=run_id,
        config={
            "final_audit": {
                "raw_data_path": "/tmp/stale.csv",
                "certification": {"schema_validation": {"rules": {"expected_columns": ["value"]}}},
                "settings": {
                    "paths": {
                        "report_html": html_rel,
                        "report_excel": xlsx_rel,
                    }
                },
            }
        },
    )

    assert result["status"] in {"pass", "warn"}
    assert html_rel in delivered_paths
    assert xlsx_rel in delivered_paths
    assert captured_config["final_audit"]["raw_data_path"] != "/tmp/stale.csv"


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
    assert result["run_id"] == "fa_input_id_run"
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
