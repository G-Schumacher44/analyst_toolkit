"""
Module: load_data.py

Utility functions for loading tabular data into pandas DataFrames.

This module provides reusable loading utilities used throughout the
Analyst Toolkit. All functions are intentionally non-transformative,
serving as clean entry points for pipeline ingestion.

Functions:
- load_csv(path): Loads a CSV file into a pandas DataFrame.
- load_joblib(path): Loads a joblib file.
"""

import logging
import os

import joblib
import pandas as pd

logger = logging.getLogger(__name__)

_TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}
_ALLOW_UNSAFE_JOBLIB_ENV = "ANALYST_TOOLKIT_ALLOW_UNSAFE_JOBLIB"


def load_csv(path: str) -> pd.DataFrame:
    """
    Loads a CSV file from a given path.

    Args:
        path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Loaded data as a pandas DataFrame.
    """
    return pd.read_csv(path)


def _unsafe_joblib_loading_enabled() -> bool:
    value = os.environ.get(_ALLOW_UNSAFE_JOBLIB_ENV, "")
    return value.strip().lower() in _TRUTHY_ENV_VALUES


def load_joblib(path: str):
    """
    Loads a joblib file from a given path.

    Args:
        path (str): Path to the joblib file.

    Returns:
        Any: The Python object stored in the file.
    """
    if not _unsafe_joblib_loading_enabled():
        raise ValueError(
            "Refusing to load joblib artifact from "
            f"'{path}'. joblib deserialization is unsafe by default. "
            f"Set {_ALLOW_UNSAFE_JOBLIB_ENV}=1 only for trusted local artifacts."
        )
    logger.warning("Loading trusted joblib artifact from %s", path)
    return joblib.load(path)
