"""
scoring.py — Data Health Scoring Engine for the Analyst Toolkit.
Calculates a 0-100 "Data Credit Score" based on quality metrics.
"""

import numpy as np
import pandas as pd


def calculate_health_score(metrics: dict) -> dict:
    """
    Calculates a weighted health score (0-100).

    Expected metrics:
    - null_rate (float 0-1)
    - validation_pass_rate (float 0-1)
    - outlier_ratio (float 0-1)
    - duplicate_ratio (float 0-1)
    """

    # Weights for the "Data Credit Score"
    weights = {
        "completeness": 0.40,  # Nulls
        "validity": 0.30,  # Validation rules
        "uniqueness": 0.15,  # Duplicates
        "consistency": 0.15,  # Outliers
    }

    # Extract values with defaults
    null_rate = metrics.get("null_rate", 0.0)
    val_pass_rate = metrics.get("validation_pass_rate", 1.0)
    outlier_ratio = metrics.get("outlier_ratio", 0.0)
    dup_ratio = metrics.get("duplicate_ratio", 0.0)

    # Calculate components (higher is better)
    comp_score = (1.0 - null_rate) * 100
    valid_score = val_pass_rate * 100
    unique_score = (1.0 - dup_ratio) * 100
    consist_score = (1.0 - outlier_ratio) * 100

    total_score = (
        (comp_score * weights["completeness"])
        + (valid_score * weights["validity"])
        + (unique_score * weights["uniqueness"])
        + (consist_score * weights["consistency"])
    )

    # Red / Yellow / Green Status
    status = "green"
    if total_score < 70:
        status = "red"
    elif total_score < 90:
        status = "yellow"

    return {
        "overall_score": round(total_score, 1),
        "status": status,
        "breakdown": {
            "completeness": round(comp_score, 1),
            "validity": round(valid_score, 1),
            "uniqueness": round(unique_score, 1),
            "consistency": round(consist_score, 1),
        },
    }
