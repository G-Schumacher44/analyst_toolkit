"""
🧼 Module: outlier_handler.py

Core logic for the M06 Outlier Handling module in the Analyst Toolkit.

This script applies configurable strategies to clean or transform outliers 
previously flagged in detection. Supported strategies include:
- 'clip': Clamp outliers within lower/upper bounds
- 'median' / 'mean': Replace with aggregated values
- 'constant': Replace with user-defined value
- 'drop': (if set globally) Remove all rows with any outlier

Returns the cleaned DataFrame and a summary log of applied transformations.
"""

import pandas as pd
import numpy as np
import logging

def handle_outliers(df: pd.DataFrame, detection_results: dict, handling_specs: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply configured outlier handling strategies to a DataFrame using detection flags.

    Args:
        df (pd.DataFrame): Input DataFrame with outliers previously detected.
        detection_results (dict): Dictionary with 'outlier_flags' and 'outlier_log'.
        handling_specs (dict): Dict mapping column names or defaults to handling strategies.

    Returns:
        tuple:
            - pd.DataFrame: DataFrame with handled outliers
            - pd.DataFrame: Summary log of all transformations applied
    """
    df_handled = df.copy()
    outlier_flags = detection_results.get("outlier_flags")
    outlier_log = detection_results.get("outlier_log")

    if outlier_flags is None or outlier_log is None or outlier_flags.empty:
        logging.warning("Outlier detection results not found or empty. Skipping handling.")
        return df_handled, pd.DataFrame()

    summary_log_rows = []
    
    global_strategy = handling_specs.get('__global__', {}).get('strategy', 'none').lower()
    
    if global_strategy == 'drop':
        rows_before = len(df_handled)
        combined_mask = (outlier_flags.any(axis=1)).fillna(False)
        df_handled = df_handled[~combined_mask]
        rows_removed = rows_before - len(df_handled)
        if rows_removed > 0:
            summary_log_rows.append({
                "strategy": "global_drop", "column": "ALL",
                "outliers_handled": rows_removed, "details": f"Removed {rows_removed} rows with any outlier."
            })
        return df_handled, pd.DataFrame(summary_log_rows)

    default_spec = handling_specs.get("__default__", {})
    
    for col in outlier_log['column'].unique():
        col_spec = handling_specs.get(col, default_spec)
        strategy = col_spec.get("strategy")
        if not strategy or strategy == 'none': continue

        log_entry = outlier_log[outlier_log['column'] == col].iloc[0]
        method = log_entry['method']
        flag_col_name = f"{col}_{method}_outlier"
        
        if flag_col_name not in outlier_flags.columns: continue
            
        outlier_mask = (outlier_flags[flag_col_name] == True)
        outlier_count = int(outlier_mask.sum())
        if outlier_count == 0: continue

        details = ""
        if strategy == 'clip':
            bounds = log_entry
            df_handled.loc[outlier_mask, col] = df_handled.loc[outlier_mask, col].clip(lower=bounds.get('lower_bound'), upper=bounds.get('upper_bound'))
            details = f"Clipped {outlier_count} values to bounds."
        elif strategy in ['median', 'mean']:
            replacement = df_handled[col].agg(strategy)
            df_handled.loc[outlier_mask, col] = replacement
            details = f"Imputed {outlier_count} values with {strategy} ({replacement:.2f})."
        elif strategy == 'constant':
            fill_value = col_spec.get('fill_value')
            if fill_value is None: continue
            df_handled.loc[outlier_mask, col] = fill_value
            details = f"Replaced {outlier_count} values with constant ({fill_value})."
        
        summary_log_rows.append({"strategy": strategy, "column": col, "outliers_handled": outlier_count, "details": details})

    return df_handled, pd.DataFrame(summary_log_rows)