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


def test_imputation_empty_strategies_returns_unchanged():
    """Empty strategy map should be treated as no-op, not an error."""
    from analyst_toolkit.m07_imputation.run_imputation_pipeline import run_imputation_pipeline

    df = pd.DataFrame({"a": [1, None], "b": ["x", "y"]})
    cfg = {"imputation": {"rules": {"strategies": {}}, "settings": {"plotting": {"run": False}}}}

    out = run_imputation_pipeline(config=cfg, df=df, notebook=False, run_id="run_imp_empty")
    pd.testing.assert_frame_equal(out, df)


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
