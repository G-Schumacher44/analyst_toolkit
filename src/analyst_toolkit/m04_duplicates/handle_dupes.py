"""
♻️ Module: handle_dupes.py

Duplicate detection and handling logic for the Analyst Toolkit.

This module provides configurable logic for identifying and optionally removing
duplicate rows from a pandas DataFrame using parameters specified in a config dictionary.

Modes:
- "flag": Marks and previews duplicate rows without removing them
- "remove": Returns a cleaned DataFrame with duplicates removed

Returns the cleaned DataFrame and optionally produces a summary of duplicates.
"""
import pandas as pd

def handle_duplicates(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Identifies and handles duplicate rows based on configuration rules.

    Args:
        df (pd.DataFrame): Input DataFrame to process.
        config (dict): Dictionary containing 'subset_columns' and 'keep' settings.

    Returns:
        pd.DataFrame: The deduplicated DataFrame.
    """
    subset = config.get("subset_columns", None)
    keep = config.get("keep", "first")

    # Renamed for clarity, this is the sole output
    df_deduplicated = df.drop_duplicates(subset=subset, keep=keep)
    
    return df_deduplicated
