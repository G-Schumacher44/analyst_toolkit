"""
test_golden_template_execution.py — Smoke tests for golden template execution.
"""

import pandas as pd
import pytest

import analyst_toolkit.mcp_server.tools.duplicates as duplicates_tool
import analyst_toolkit.mcp_server.tools.imputation as imputation_tool
import analyst_toolkit.mcp_server.tools.normalization as normalization_tool
import analyst_toolkit.mcp_server.tools.outliers as outliers_tool
import analyst_toolkit.mcp_server.tools.validation as validation_tool
from analyst_toolkit.mcp_server.templates import get_golden_configs


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transaction_amount": [10.0, 25000.0, 42.5],
            "frequency_24h": [1.0, 65.0, 3.0],
            "device_id": ["d1", "d2", "d3"],
            "user_email": ["a@example.com", "b@example.com", "c@example.com"],
            "billing_zip": ["00001", "00002", "00003"],
            "ssn_hash": ["x1", None, "x3"],
            "consent_flag": ["Y", "N", "PENDING"],
            "user_id": [1, 2, 3],
            "first_name": [" Alice ", "Bob", "Cara"],
            "last_name": ["Smith", " Jones", "White "],
            "city": [" New York ", "Seattle", "Austin"],
            "created_at": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "is_active": [True, False, True],
        }
    )


def _patch_common_module(mocker, module, df: pd.DataFrame, session_id: str):
    mocker.patch.object(module, "load_input", return_value=df.copy())
    mocker.patch.object(module, "save_to_session", return_value=session_id)
    mocker.patch.object(module, "get_session_metadata", return_value={"row_count": len(df)})
    mocker.patch.object(module, "save_output", return_value="gs://dummy/output.csv")
    mocker.patch.object(module, "append_to_run_history", return_value=None)
    mocker.patch.object(module, "should_export_html", return_value=False)


@pytest.mark.asyncio
async def test_golden_fraud_template_executes_across_modules(mocker):
    templates = get_golden_configs()
    fraud = templates["fraud_detection"]
    df = _sample_df()
    run_id = "golden_fraud_smoke"
    session_id = "sess_golden_fraud"

    # Outliers (verify shorthand converts to canonical detection_specs)
    _patch_common_module(mocker, outliers_tool, df, session_id)
    captured = {}

    def fake_outlier_pipeline(config, df, notebook, run_id):
        captured["cfg"] = config
        return df.copy(), {"outlier_log": pd.DataFrame(columns=["column"])}

    mocker.patch.object(
        outliers_tool, "run_outlier_detection_pipeline", side_effect=fake_outlier_pipeline
    )
    out_res = await outliers_tool._toolkit_outliers(
        session_id=session_id,
        run_id=run_id,
        config=fraud["outliers"],
    )
    assert out_res["status"] in {"pass", "warn"}
    specs = captured["cfg"]["outlier_detection"]["detection_specs"]
    assert specs["transaction_amount"]["method"] == "iqr"
    assert specs["frequency_24h"]["method"] == "iqr"

    # Duplicates
    _patch_common_module(mocker, duplicates_tool, df, session_id)
    mocker.patch.object(
        duplicates_tool,
        "run_duplicates_pipeline",
        return_value=df.assign(is_duplicate=False),
    )
    dup_res = await duplicates_tool._toolkit_duplicates(
        session_id=session_id,
        run_id=run_id,
        config=fraud["duplicates"],
    )
    assert dup_res["status"] in {"pass", "warn"}

    # Validation
    _patch_common_module(mocker, validation_tool, df, session_id)
    mocker.patch.object(validation_tool, "run_validation_pipeline", return_value=df)
    val_res = await validation_tool._toolkit_validation(
        session_id=session_id,
        run_id=run_id,
        config=fraud["validation"],
    )
    assert val_res["status"] in {"pass", "warn", "fail"}


@pytest.mark.asyncio
async def test_golden_quick_migration_template_executes(mocker):
    templates = get_golden_configs()
    quick = templates["quick_migration"]
    df = _sample_df()
    run_id = "golden_quick_migration_smoke"
    session_id = "sess_golden_quick"

    _patch_common_module(mocker, normalization_tool, df, session_id)
    mocker.patch.object(normalization_tool, "apply_normalization", return_value=(df, None, {}))
    mocker.patch.object(normalization_tool, "run_normalization_pipeline", return_value=df.copy())
    norm_res = await normalization_tool._toolkit_normalization(
        session_id=session_id,
        run_id=run_id,
        config=quick["normalization"],
    )
    assert norm_res["status"] in {"pass", "warn"}

    _patch_common_module(mocker, imputation_tool, df, session_id)
    mocker.patch.object(imputation_tool, "run_imputation_pipeline", return_value=df.fillna(""))
    imp_res = await imputation_tool._toolkit_imputation(
        session_id=session_id,
        run_id=run_id,
        config=quick["imputation"],
    )
    assert imp_res["status"] in {"pass", "warn"}


@pytest.mark.asyncio
async def test_golden_compliance_template_executes_validation(mocker):
    templates = get_golden_configs()
    compliance = templates["compliance_audit"]
    df = _sample_df()
    run_id = "golden_compliance_smoke"
    session_id = "sess_golden_compliance"

    _patch_common_module(mocker, validation_tool, df, session_id)
    mocker.patch.object(validation_tool, "run_validation_pipeline", return_value=df)
    res = await validation_tool._toolkit_validation(
        session_id=session_id,
        run_id=run_id,
        config=compliance["validation"],
    )
    assert res["status"] in {"pass", "warn", "fail"}
