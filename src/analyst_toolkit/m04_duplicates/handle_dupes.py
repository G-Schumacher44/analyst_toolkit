"""
♻️ Module: handle_duplicates.py

This module contains the destructive logic for removing duplicate rows from a
DataFrame. It is designed to be called after the detection step.
"""
import pandas as pd
import logging

def handle_duplicates(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Removes duplicate rows from a DataFrame based on the 'keep' strategy
    defined in the configuration.

    Args:
        df (pd.DataFrame): Input DataFrame to process.
        config (dict): Dictionary containing 'subset_columns' and 'keep' settings.

    Returns:
        pd.DataFrame: A DataFrame with duplicate rows removed.
    """
    subset = config.get("subset_columns")
    keep = config.get("keep", "first")
    logging.info(f"Removing duplicates with subset={subset} and keep='{keep}'")
    return df.drop_duplicates(subset=subset, keep=keep)
