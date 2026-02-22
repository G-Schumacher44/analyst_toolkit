"""
test_validation.py — Core logic tests for M02 Validation.
"""

import pandas as pd
import pytest
from analyst_toolkit.m02_validation.validate_data import run_validation_suite

def test_validation_suite_checks():
    """Verify that the validation suite correctly identifies schema and range issues."""
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "age": [25, 30, 150],  # 150 is out of range
        "gender": ["M", "F", "X"] # X might be an invalid category
    })
    
    # run_validation_suite expects a dict where config['schema_validation']['rules'] exists
    config = {
        "schema_validation": {
            "rules": {
                "expected_columns": ["id", "age", "gender"],
                "numeric_ranges": {
                    "age": {"min": 0, "max": 120}
                },
                "categorical_values": {
                    "gender": ["M", "F"]
                }
            }
        }
    }
    
    results = run_validation_suite(df, config=config)
    
    # Check that individual rules were evaluated
    assert "schema_conformity" in results
    assert "numeric_ranges" in results
    assert "categorical_values" in results
    
    # Verify range check failure for 'age'
    assert results["numeric_ranges"]["passed"] is False
    assert "age" in results["numeric_ranges"]["details"]
    
    # Verify categorical check failure for 'gender'
    assert results["categorical_values"]["passed"] is False
    assert "gender" in results["categorical_values"]["details"]
