"""
📦 Module: load_data.py

Utility functions for loading tabular data into pandas DataFrames.

This module provides reusable loading utilities used throughout the
Analyst Toolkit. All functions are intentionally non-transformative,
serving as clean entry points for pipeline ingestion.

Functions:
- load_csv(path): Loads a CSV file into a pandas DataFrame.
- load_joblib(path): Loads a joblib file.
"""

import joblib
import pandas as pd


def load_csv(path: str) -> pd.DataFrame:
    """
    Loads a CSV file from a given path.

    Args:
        path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Loaded data as a pandas DataFrame.
    """
    return pd.read_csv(path)


def load_joblib(path: str):
    """
    Loads a joblib file from a given path.

    Args:
        path (str): Path to the joblib file.

    Returns:
        Any: The Python object stored in the file.
    """
    return joblib.load(path)
