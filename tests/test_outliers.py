"""
test_outliers.py — Core logic tests for M05 Outlier Detection.
"""

import pandas as pd
import pytest
from analyst_toolkit.m05_detect_outliers.detect_outliers import detect_outliers

def test_iqr_outlier_detection():
    """Verify that IQR detection correctly identifies points outside bounds."""
    df = pd.DataFrame({"val": [1, 2, 3, 4, 5, 6, 7, 8, 9, 20]}) # 20 is outlier
    config = {
        "detection_specs": {
            "val": {"method": "iqr", "iqr_multiplier": 1.5}
        }
    }
    results = detect_outliers(df, config)
    outlier_log = results.get("outlier_log", pd.DataFrame())
    
    assert not outlier_log.empty
    assert outlier_log.iloc[0]["column"] == "val"
    assert outlier_log.iloc[0]["outlier_count"] == 1

def test_zscore_outlier_detection():
    """Verify that Z-score detection identifies points beyond a threshold."""
    # Need enough data points so the outlier doesn't push the mean/std too far
    df = pd.DataFrame({"val": [10] * 20 + [100]})
    config = {
        "detection_specs": {
            "val": {"method": "zscore", "zscore_threshold": 3.0}
        }
    }
    results = detect_outliers(df, config)
    outlier_log = results.get("outlier_log", pd.DataFrame())
    
    # 100 in [10]*20 + [100] should definitely be > 3 sigma
    assert not outlier_log.empty
    assert outlier_log.iloc[0]["outlier_count"] == 1

def test_empty_dataframe_handling():
    """Ensure detection logic doesn't crash on empty input."""
    df = pd.DataFrame({"val": []}, dtype=float)
    config = {"detection_specs": {"val": {"method": "iqr"}}}
    results = detect_outliers(df, config)
    
    assert results["outlier_log"].empty
    assert results["outlier_flags"].empty
