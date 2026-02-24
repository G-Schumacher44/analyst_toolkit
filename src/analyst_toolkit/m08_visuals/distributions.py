"""
📊 Module: distributions.py

Contains visual helper functions for generating distribution plots
of numeric and categorical columns. This module is shared across
EDA and diagnostics phases of the Analyst Toolkit.

Exports:
- `plot_continuous_distribution`: Histogram + KDE overlay
- `plot_categorical_distribution`: Horizontal bar chart of top categories

All plots are saved to disk with run-aware filenames for audit traceability.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def plot_continuous_distribution(series: pd.Series, save_dir: Path, run_id: str) -> Path:
    """Generates and saves a histogram for a continuous numerical series using Matplotlib."""
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(8, 4))

    series.plot(kind="hist", ax=ax, bins=30, alpha=0.7, color="#4c72b0")
    series.plot(kind="kde", ax=ax, secondary_y=True, color="#c44e52")

    ax.set_title(f"Distribution of {series.name}")
    ax.set_xlabel(str(series.name))
    ax.set_ylabel("Frequency")
    plt.tight_layout()

    save_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{run_id}_dist_cont_{str(series.name).replace(' ', '_')}.png"
    save_path = save_dir / filename
    plt.savefig(save_path)
    plt.close(fig)
    logging.info(f"Generated continuous distribution plot for '{series.name}' at {save_path}")
    return save_path


def plot_categorical_distribution(
    series: pd.Series, save_dir: Path, run_id: str, top_n: int = 20
) -> Path:
    """Generates and saves a bar plot for a discrete categorical series using Matplotlib."""
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(10, 5))

    counts = series.value_counts().nlargest(top_n).sort_values()
    counts.plot(kind="barh", ax=ax, color="#6495ED")

    ax.set_title(f"Distribution of Top {len(counts)} Categories in {series.name}")
    ax.set_xlabel("Count")
    plt.tight_layout()

    save_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{run_id}_dist_cat_{str(series.name).replace(' ', '_')}.png"
    save_path = save_dir / filename
    plt.savefig(save_path)
    plt.close(fig)
    logging.info(f"Generated categorical distribution plot for '{series.name}' at {save_path}")
    return save_path
