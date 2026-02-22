"""
🔎 Module: detect_dupes.py

This module contains the non-destructive logic for detecting duplicate rows
in a DataFrame. It identifies duplicates based on a subset of columns (or all),
flags them, and returns both a flagged DataFrame and a summary of findings.
"""

import pandas as pd


def detect_duplicates(df: pd.DataFrame, subset: list = None) -> tuple[pd.DataFrame, dict]:
    """
    Detects duplicate rows in a DataFrame and returns a flagged DataFrame
    along with detection results.

    Args:
        df (pd.DataFrame): The input DataFrame to check for duplicates.
        subset (list, optional): A list of column names to consider for
                                 identifying duplicates. If None, all columns
                                 are used. Defaults to None.

    Returns:
        tuple[pd.DataFrame, dict]:
            - A new DataFrame with an 'is_duplicate' boolean column.
            - A dictionary containing:
                - 'duplicate_count': The total number of rows that are part of a duplicate set.
                - 'duplicate_clusters': A DataFrame containing all rows that are duplicates,
                                        grouped by the duplicate values.
    """
    df_flagged = df.copy()
    duplicate_mask = df.duplicated(subset=subset, keep=False)
    df_flagged["is_duplicate"] = duplicate_mask
    duplicate_clusters = df[duplicate_mask].sort_values(
        by=subset if subset else df.columns.tolist()
    )
    detection_results = {
        "duplicate_count": int(duplicate_mask.sum()),
        "duplicate_clusters": duplicate_clusters,
    }
    return df_flagged, detection_results
