"""
🔬 Module: detect_outliers.py

Core producer for the M05 Outlier Detection module.

This module analyzes numeric columns to flag outliers based on configurable detection
rules using IQR or Z-score methods. It generates a boolean mask of detected outliers,
a summary log of outlier boundaries and counts, and a DataFrame of affected rows.

Used by the M05 pipeline orchestrator for QA and risk flagging steps.
"""


import pandas as pd
import numpy as np
import logging

def _detect_by_iqr(series, config):
    Q1, Q3 = series.quantile(0.25), series.quantile(0.75)
    IQR = Q3 - Q1
    multiplier = config.get("iqr_multiplier", 1.5)
    lower_bound, upper_bound = Q1 - (multiplier * IQR), Q3 + (multiplier * IQR)
    return (series < lower_bound) | (series > upper_bound), lower_bound, upper_bound

def _detect_by_zscore(series, config):
    mean, std = series.mean(), series.std()
    threshold = config.get("zscore_threshold", 3.0)
    lower_bound, upper_bound = mean - (threshold * std), mean + (threshold * std)
    return ((series - mean) / std).abs() > threshold, lower_bound, upper_bound

def detect_outliers(df: pd.DataFrame, config: dict) -> dict:
    """Detects outliers and returns a comprehensive results dictionary."""
    detection_specs = config.get("detection_specs", {})
    exclude_columns = config.get("exclude_columns", [])
    if exclude_columns is None: exclude_columns = []
    
    outlier_log_entries = []
    outlier_flags = pd.DataFrame(index=df.index)
    numeric_cols = df.select_dtypes(include=['number']).columns.drop(exclude_columns, errors='ignore')
    
    for col in numeric_cols:
        col_spec = detection_specs.get(col, detection_specs.get("__default__", {}))
        method = col_spec.get("method")
        if not method: continue
            
        series = df[col].dropna()
        if series.empty: continue
            
        if method == 'iqr':
            is_outlier, lower, upper = _detect_by_iqr(series, col_spec)
        elif method == 'zscore':
            is_outlier, lower, upper = _detect_by_zscore(series, col_spec)
        else: continue
            
        if is_outlier.any():
            outlier_indices = series[is_outlier].index
            outlier_values = series[outlier_indices].tolist()
            outlier_log_entries.append({
                'column': col, 'method': method, 'outlier_count': int(is_outlier.sum()),
                'lower_bound': lower, 'upper_bound': upper,
                'outlier_examples': str(outlier_values[:5])
            })
            outlier_flags[f"{col}_{method}_outlier"] = is_outlier
            
    outlier_log_df = pd.DataFrame(outlier_log_entries)

    if not outlier_flags.empty:
        overall_outlier_mask = outlier_flags.any(axis=1)
        outlier_rows_df = df[overall_outlier_mask].copy()
    else:
        outlier_rows_df = pd.DataFrame(columns=df.columns)
            
    return {
        "outlier_log": outlier_log_df,
        "outlier_flags": outlier_flags,
        "outlier_rows": outlier_rows_df
    }