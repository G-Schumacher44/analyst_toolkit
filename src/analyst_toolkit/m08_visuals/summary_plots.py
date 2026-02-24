"""
📊 Module: summary_plots.py

A centralized collection of summary visualizations for diagnostics and reporting.

This module includes reusable plotting functions to:
- Visualize missingness percentages
- Display correlation heatmaps for numeric features
- Show dtype distributions in a donut chart
- Summarize deduplication impacts with a bar chart

All plots are saved to disk using consistent run-aware naming conventions.
Designed for use in diagnostic modules and notebook inline displays.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_missingness(df: pd.DataFrame, save_dir: Path, run_id: str) -> Path:
    """Generates and saves a bar chart of missing value percentages."""
    missing_counts = df.isnull().sum()
    missing_pct = (missing_counts[missing_counts > 0] / len(df) * 100).sort_values()

    if missing_pct.empty:
        logging.info("No missing values found. Skipping missingness plot.")
        return None

    fig, ax = plt.subplots(figsize=(10, 6))
    missing_pct.plot(kind="barh", ax=ax, color="#6495ED")
    ax.set_title("Percentage of Missing Values by Column")
    ax.set_xlabel("Percent Missing (%)")
    plt.tight_layout()

    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = save_dir / f"{run_id}_missingness_summary.png"
    plt.savefig(save_path)
    plt.close(fig)
    logging.info(f"Generated missingness plot at {save_path}")
    return save_path


def plot_correlation_heatmap(df: pd.DataFrame, save_dir: Path, run_id: str) -> Path:
    """Generates and saves a correlation heatmap for numeric columns."""
    numeric_df = df.select_dtypes(include="number")
    if numeric_df.shape[1] < 2:
        logging.info("Not enough numeric columns for a correlation heatmap. Skipping.")
        return None

    corr = numeric_df.corr()
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", ax=ax)
    ax.set_title("Numeric Feature Correlation Heatmap")
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()

    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = save_dir / f"{run_id}_correlation_heatmap.png"
    plt.savefig(save_path)
    plt.close(fig)
    logging.info(f"Generated correlation heatmap at {save_path}")
    return save_path


def plot_dtype_summary(df: pd.DataFrame, save_dir: Path, run_id: str) -> Path:
    """Generates and saves a donut chart of data type distribution."""
    dtype_counts = df.dtypes.value_counts()

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.pie(
        dtype_counts,
        labels=[str(label) for label in dtype_counts.index],
        autopct="%1.1f%%",
        startangle=90,
        wedgeprops=dict(width=0.4),
        pctdistance=0.85,
    )
    ax.set_title("Data Type Composition")

    # Equal aspect ratio ensures that pie is drawn as a circle.
    ax.axis("equal")
    plt.tight_layout()

    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = save_dir / f"{run_id}_dtype_summary.png"
    plt.savefig(save_path)
    plt.close(fig)
    logging.info(f"Generated dtype summary plot at {save_path}")
    return save_path


def plot_duplication_summary(summary_df: pd.DataFrame, save_dir: Path, run_id: str) -> Path:
    """
    Generates and saves a bar chart summarizing rows removed during deduplication.
    """
    if summary_df is None or summary_df.empty:
        logging.warning("Duplicates summary data is empty. Skipping plot generation.")
        return None

    try:
        metrics = summary_df.set_index("Metric")["Value"]
        plot_data = {
            "Original Rows": metrics.get("Original Row Count", 0),
            "Rows Removed": metrics.get("Rows Removed", 0),
            "Final Rows": metrics.get("Deduplicated Row Count", 0),  # Corrected key
        }

        plt.style.use("seaborn-v0_8-whitegrid")
        fig, ax = plt.subplots(figsize=(8, 5))

        keys = list(plot_data.keys())
        values = list(plot_data.values())
        colors = ["#4c72b0", "#c44e52", "#55a868"]
        bars = ax.bar(keys, values, color=colors)

        for bar in bars:
            yval = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.0,
                yval,
                str(int(yval)),
                va="bottom",
                ha="center",
            )

        ax.set_title("Deduplication Summary: Row Counts")
        ax.set_ylabel("Number of Rows")

        # --- THIS IS THE FIX ---
        # Set the ticks before setting the labels to prevent the warning.
        ax.set_xticks(range(len(keys)))
        ax.set_xticklabels(keys, rotation=0)

        plt.tight_layout()

        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / f"{run_id}_duplication_summary.png"
        plt.savefig(save_path)
        plt.close(fig)
        logging.info(f"Generated duplication summary plot at {save_path}")
        return save_path

    except Exception as e:
        logging.error(f"Failed to generate duplication summary plot: {e}")
        return None
