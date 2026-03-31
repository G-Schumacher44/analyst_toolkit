import importlib
import inspect
import sys

import pandas as pd


def test_normalization_changes_made_rename():
    """apply_normalization changelog counts renamed columns correctly."""
    from analyst_toolkit.m03_normalization.normalize_data import apply_normalization

    df = pd.DataFrame({"old_name": [1, 2], "b": [3, 4]})
    config = {"rules": {"rename_columns": {"old_name": "new_name"}}}
    _, df_norm, changelog = apply_normalization(df, config)

    assert "new_name" in df_norm.columns
    assert "renamed_columns" in changelog
    assert len(changelog["renamed_columns"]) == 1


def test_normalization_changes_made_text_standardize():
    """apply_normalization changelog counts standardized text columns correctly."""
    from analyst_toolkit.m03_normalization.normalize_data import apply_normalization

    df = pd.DataFrame({"name": ["  Alice  ", "BOB"]})
    config = {"rules": {"standardize_text_columns": ["name"]}}
    _, df_norm, changelog = apply_normalization(df, config)

    assert df_norm["name"].tolist() == ["alice", "bob"]
    assert "strings_cleaned" in changelog
    assert len(changelog["strings_cleaned"]) == 1


def test_normalization_no_rules_returns_unchanged():
    """Empty rules -> changelog is empty, df unchanged."""
    from analyst_toolkit.m03_normalization.normalize_data import apply_normalization

    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    _, df_norm, changelog = apply_normalization(df, {"rules": {}})

    pd.testing.assert_frame_equal(df, df_norm)
    assert changelog == {}


def test_normalization_value_mapping_does_not_mutate_config():
    from analyst_toolkit.m03_normalization.normalize_data import apply_normalization

    df = pd.DataFrame({"status": ["ok", None]})
    config = {"rules": {"value_mappings": {"status": {"null": "UNKNOWN", "ok": "OK"}}}}

    apply_normalization(df, config)

    assert config["rules"]["value_mappings"]["status"] == {"null": "UNKNOWN", "ok": "OK"}


def test_imputation_empty_strategies_returns_unchanged():
    """Empty strategy map should be treated as no-op, not an error."""
    from analyst_toolkit.m07_imputation.run_imputation_pipeline import run_imputation_pipeline

    df = pd.DataFrame({"a": [1, None], "b": ["x", "y"]})
    cfg = {"imputation": {"rules": {"strategies": {}}, "settings": {"plotting": {"run": False}}}}

    out = run_imputation_pipeline(config=cfg, df=df, notebook=False, run_id="run_imp_empty")
    pd.testing.assert_frame_equal(out, df)


def test_imputation_mode_all_nan_column_is_noop():
    from analyst_toolkit.m07_imputation.impute_data import apply_imputation

    df = pd.DataFrame({"score": [None, None, None]})

    out, changelog = apply_imputation(df, {"rules": {"strategies": {"score": "mode"}}})

    pd.testing.assert_frame_equal(out, df)
    assert changelog.empty


def test_validation_suite_passes_with_correct_schema():
    """run_validation_suite returns passed=True when schema matches."""
    from analyst_toolkit.m02_validation.validate_data import run_validation_suite

    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    config = {
        "schema_validation": {
            "rules": {
                "expected_columns": ["a", "b"],
                "expected_types": {},
                "categorical_values": {},
                "numeric_ranges": {},
            }
        }
    }
    results = run_validation_suite(df, config)
    assert results["schema_conformity"]["passed"] is True


def test_validation_suite_fails_missing_columns():
    """run_validation_suite detects missing columns and marks schema_conformity failed."""
    from analyst_toolkit.m02_validation.validate_data import run_validation_suite

    df = pd.DataFrame({"a": [1, 2]})
    config = {
        "schema_validation": {
            "rules": {
                "expected_columns": ["a", "b"],
                "expected_types": {},
                "categorical_values": {},
                "numeric_ranges": {},
            }
        }
    }
    results = run_validation_suite(df, config)
    assert results["schema_conformity"]["passed"] is False
    assert "b" in results["schema_conformity"]["details"]["missing_columns"]


def test_validation_suite_fails_dtype_mismatch():
    """run_validation_suite detects dtype mismatches."""
    from analyst_toolkit.m02_validation.validate_data import run_validation_suite

    df = pd.DataFrame({"score": ["high", "low"]})
    config = {
        "schema_validation": {
            "rules": {
                "expected_columns": [],
                "expected_types": {"score": "int64"},
                "categorical_values": {},
                "numeric_ranges": {},
            }
        }
    }
    results = run_validation_suite(df, config)
    assert results["dtype_enforcement"]["passed"] is False
    assert "score" in results["dtype_enforcement"]["details"]


def test_validation_suite_fails_categorical_violation():
    """run_validation_suite detects values outside allowed set."""
    from analyst_toolkit.m02_validation.validate_data import run_validation_suite

    df = pd.DataFrame({"color": ["red", "blue", "purple"]})
    config = {
        "schema_validation": {
            "rules": {
                "expected_columns": [],
                "expected_types": {},
                "categorical_values": {"color": ["red", "blue"]},
                "numeric_ranges": {},
            }
        }
    }
    results = run_validation_suite(df, config)
    assert results["categorical_values"]["passed"] is False


def test_run_data_profile_uses_none_default_config():
    from analyst_toolkit.m01_diagnostics.data_diag import run_data_profile

    default = inspect.signature(run_data_profile).parameters["config"].default

    assert default is None


def test_run_diag_pipeline_does_not_import_plotting_stack_when_disabled(monkeypatch):
    module_name = "analyst_toolkit.m01_diagnostics.run_diag_pipeline"
    plotting_modules = (
        "analyst_toolkit.m08_visuals.distributions",
        "analyst_toolkit.m08_visuals.summary_plots",
    )
    for name in plotting_modules:
        sys.modules.pop(name, None)
    sys.modules.pop(module_name, None)

    diag_module = importlib.import_module(module_name)
    diag_module = importlib.reload(diag_module)

    monkeypatch.setattr(
        diag_module,
        "run_data_profile",
        lambda df, config: {
            "for_display": {"shape": pd.DataFrame([{"Rows": len(df), "Columns": len(df.columns)}])},
            "for_export": {"shape": pd.DataFrame([{"Rows": len(df), "Columns": len(df.columns)}])},
        },
    )
    monkeypatch.setattr(diag_module, "export_dataframes", lambda *args, **kwargs: None)
    monkeypatch.setattr(diag_module, "export_html_report", lambda *args, **kwargs: None)

    df = pd.DataFrame({"value": [1, 2, 3]})
    diag_module.run_diag_pipeline(
        config={
            "diagnostics": {
                "profile": {"run": True, "settings": {"export": False}},
                "plotting": {"run": False},
                "logging": "off",
            }
        },
        df=df,
        notebook=False,
        run_id="diag_no_plots",
    )

    for name in plotting_modules:
        assert name not in sys.modules


def test_apply_final_edits_handles_dtype_coercion_failures():
    from analyst_toolkit.m10_final_audit.final_audit_producer import _apply_final_edits

    df = pd.DataFrame({"score": ["bad", "2"], "flag": ["1", "0"]})

    out, changelog = _apply_final_edits(
        df,
        {"coerce_dtypes": {"score": "float64", "flag": "int64"}},
    )

    assert out["flag"].dtype == "int64"
    assert out["score"].tolist() == ["bad", "2"]
    assert set(changelog["Action"]) == {"coerce_dtypes", "coerce_dtypes_failed"}


def test_apply_final_edits_logs_dtype_coercion_failures(caplog):
    from analyst_toolkit.m10_final_audit.final_audit_producer import _apply_final_edits

    df = pd.DataFrame({"score": ["bad"]})

    with caplog.at_level("WARNING"):
        _apply_final_edits(df, {"coerce_dtypes": {"score": "float64"}})

    assert "Final audit dtype coercion failed" in caplog.text
