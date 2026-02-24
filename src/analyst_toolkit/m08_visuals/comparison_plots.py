"""
📈 Module: comparison_plots.py

Visual comparison utilities for the M07 Imputation module.

This module provides functions for plotting side-by-side comparisons of
numeric and categorical data before and after imputation. It includes:

- KDE overlays for numeric distributions
- Bar charts comparing categorical value counts

All plots are saved to disk and intended for use in summary dashboards or
widget-based reviewers.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_imputation_comparison(
    s_before: pd.Series, s_after: pd.Series, save_dir: Path, run_id: str
) -> Path:
    """
    Generates an overlaid KDE plot to compare a Series before and after imputation.

    Args:
        s_before (pd.Series): The data series before imputation.
        s_after (pd.Series): The data series after imputation.
        save_dir (Path): The directory to save the plot.
        run_id (str): The run identifier for filename traceability.

    Returns:
        Path: The relative path to the saved plot.
    """
    col_name = s_before.name
    try:
        plt.style.use("seaborn-v0_8-whitegrid")
        fig, ax = plt.subplots(figsize=(10, 6))

        # Plot both distributions on the same axis for direct comparison
        sns.kdeplot(s_before, ax=ax, label="Before Imputation", fill=True, alpha=0.5)
        sns.kdeplot(s_after, ax=ax, label="After Imputation", fill=True, alpha=0.5)

        ax.set_title(f"Distribution Change After Imputation: {col_name}")
        ax.set_xlabel(str(col_name))
        ax.legend()
        plt.tight_layout()

        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / f"plot_{col_name}_imputation_comp_{run_id}.png"
        plt.savefig(save_path, bbox_inches="tight")
        plt.close(fig)
        logging.info(f"Generated imputation comparison plot for '{col_name}'")
        return save_path
    except Exception as e:
        logging.error(f"Failed to generate imputation plot for '{col_name}': {e}")
        if "fig" in locals():
            plt.close(fig)
        return None


def plot_categorical_imputation_comparison(
    s_before: pd.Series, s_after: pd.Series, save_dir: Path, run_id: str
) -> Path:
    """
    Generates a comparative bar chart to show changes in value counts after imputation.
    """
    col_name = s_before.name
    try:
        # Combine value counts into a single DataFrame for plotting
        df_counts = (
            pd.DataFrame(
                {
                    "Before": s_before.value_counts(dropna=False),
                    "After": s_after.value_counts(dropna=False),
                }
            )
            .fillna(0)
            .astype(int)
        )

        plt.style.use("seaborn-v0_8-whitegrid")
        fig, ax = plt.subplots(figsize=(10, 6))

        df_counts.plot(kind="bar", ax=ax, position=0.5, width=0.4)

        ax.set_title(f"Value Counts Before vs. After Imputation: {col_name}")
        ax.set_ylabel("Count")
        ax.set_xlabel("Category")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / f"plot_{col_name}_imputation_comp_{run_id}.png"
        plt.savefig(save_path, bbox_inches="tight")
        plt.close(fig)
        logging.info(f"Generated categorical imputation comparison plot for '{col_name}'")
        return save_path
    except Exception as e:
        logging.error(f"Failed to generate categorical imputation plot for '{col_name}': {e}")
        if "fig" in locals():
            plt.close(fig)
        return None
