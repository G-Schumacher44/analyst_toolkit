"""
💧 Module: impute_data.py

Core transformation logic for the M07 Imputation module in the Analyst Toolkit.

This script applies column-specific imputation strategies to fill missing values
using mean, median, mode, or constant replacement. It returns the modified DataFrame
and a detailed changelog capturing fill values and number of nulls handled.

Used by the pipeline runner to perform final imputation prior to audit or modeling.
"""

import pandas as pd
import numpy as np
import logging

def apply_imputation(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply column-wise imputation strategies to a DataFrame and return detailed logs.

    Args:
        df (pd.DataFrame): The input DataFrame with missing values.
        config (dict): The configuration for the imputation module,
                       containing the 'rules'.

    Returns:
        tuple:
            - pd.DataFrame: The imputed DataFrame.
            - pd.DataFrame: A changelog detailing strategy, fill value, and nulls filled.
    """
    df_imputed = df.copy()
    rules = config.get("rules", {})
    strategies = rules.get("strategies", {})
    change_log_rows = []

    for column, spec in strategies.items():
        if column in df_imputed.columns and df_imputed[column].isnull().any():
            strategy = spec['strategy'] if isinstance(spec, dict) else spec
            fill_value = None
            original_null_count = int(df_imputed[column].isnull().sum())

            if original_null_count == 0:
                continue

            col_dtype = df_imputed[column].dtype

            if strategy == "mean":
                fill_value = df_imputed[column].mean()
            elif strategy == "median":
                fill_value = df_imputed[column].median()
            elif strategy == "mode":
                fill_value = df_imputed[column].mode()[0]
            elif strategy == "constant":
                fill_value = spec.get('value')

            # Handle datetime fill coercion safely
            if pd.api.types.is_datetime64_any_dtype(col_dtype):
                try:
                    fill_value = pd.to_datetime(fill_value)
                except Exception as e:
                    logging.warning(f"Failed to convert fill value to datetime for column '{column}': {e}")
                    fill_value = None

            if fill_value is not None:
                # Use direct assignment to avoid inplace warnings
                df_imputed[column] = df_imputed[column].fillna(fill_value)
                
                # Log the detailed action with the calculated value
                change_log_rows.append({
                    "Column": column,
                    "Strategy": strategy,
                    "Fill Value": f"{fill_value:.2f}" if isinstance(fill_value, (np.number, int, float)) else fill_value,
                    "Nulls Filled": original_null_count
                })
            else:
                logging.warning(f"Could not determine fill value for column '{column}' with strategy '{strategy}'.")

    return df_imputed, pd.DataFrame(change_log_rows)