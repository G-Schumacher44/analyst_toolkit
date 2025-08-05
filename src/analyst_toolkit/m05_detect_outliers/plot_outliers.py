"""
📊 Module: plot_outliers.py

Visual producer for the M05 Outlier Detection module.

This module generates and saves histogram, box, and violin plots for columns
flagged as having outliers. Plot annotations may include boundary lines and 
groupings by a categorical 'hue' column. Results are saved to disk and paths
returned for optional widget-based viewing or HTML inclusion.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from pathlib import Path

def _generate_histograms(df: pd.DataFrame, outlier_log: pd.DataFrame, hue_col: str, save_dir: Path, run_id: str) -> dict:
    """Helper function to generate only histogram plots."""
    plot_paths = {}
    for _, log_row in outlier_log.iterrows():
        col = log_row['column']
        if col not in df.columns or df[col].dropna().empty: continue
        plot_paths.setdefault(col, [])
        try:
            plt.style.use('seaborn-v0_8-whitegrid')
            fig, ax = plt.subplots(figsize=(12, 7))
            title = f"Distribution of {col}{f' by {hue_col}' if hue_col else ''}"
            ax.set_title(title, fontsize=16)

            sns.histplot(data=df, x=col, hue=hue_col, bins=30, stat="count", element="bars", ax=ax)
            ax.set_ylabel("Count")

            lower, upper = log_row.get("lower_bound"), log_row.get("upper_bound")
            if pd.notna(lower): ax.axvline(lower, color="r", linestyle="--", label=f"Bounds")
            if pd.notna(upper): ax.axvline(upper, color="r", linestyle="--")
            
            if pd.notna(lower) or pd.notna(upper) or hue_col:
                if ax.get_legend(): sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))

            plt.tight_layout()
            if hue_col: plt.subplots_adjust(right=0.85)

            plot_path = save_dir / f"plot_{col}{f'_by_{hue_col}' if hue_col else ''}_hist_{run_id}.png"
            plt.savefig(plot_path, bbox_inches='tight')
            plot_paths[col].append(str(plot_path))
        except Exception as e:
            logging.error(f"Failed to generate histogram for column '{col}': {e}")
        finally:
            if 'fig' in locals(): plt.close(fig)
            
    return plot_paths

def _generate_box_violin_plots(df: pd.DataFrame, outlier_log: pd.DataFrame, plot_types: list, hue_col: str, save_dir: Path, run_id: str) -> dict:
    """Helper function to generate only box and violin plots."""
    plot_paths = {}
    for _, log_row in outlier_log.iterrows():
        col = log_row['column']
        if col not in df.columns or df[col].dropna().empty: continue
        plot_paths.setdefault(col, [])
        for plot_kind in plot_types:
            if plot_kind not in ['box', 'violin']: continue
            try:
                plt.style.use('seaborn-v0_8-whitegrid')
                fig, ax = plt.subplots(figsize=(12, 7))
                ax.set_title(f"{plot_kind.title()} of {col}{f' by {hue_col}' if hue_col else ''}", fontsize=16)

                plot_func = sns.boxplot if plot_kind == "box" else sns.violinplot
                
                # --- THIS IS THE FINAL FIX ---
                if hue_col:
                    plot_func(data=df, x=hue_col, y=col, hue=hue_col, ax=ax)
                    if ax.get_legend(): ax.get_legend().remove()
                else:
                    plot_func(data=df, y=col, ax=ax)
                
                lower, upper = log_row.get("lower_bound"), log_row.get("upper_bound")
                if pd.notna(lower): ax.axhline(lower, color="r", linestyle="--", label="Bounds")
                if pd.notna(upper): ax.axhline(upper, color="r", linestyle="--")
                if (pd.notna(lower) or pd.notna(upper)) and not hue_col: ax.legend()
                
                plt.tight_layout()
                plot_path = save_dir / f"plot_{col}{f'_by_{hue_col}' if hue_col else ''}_{plot_kind}_{run_id}.png"
                plt.savefig(plot_path, bbox_inches='tight')
                plot_paths[col].append(str(plot_path))
            except Exception as e:
                logging.error(f"Failed to generate {plot_kind} plot for column '{col}': {e}")
            finally:
                if 'fig' in locals(): plt.close(fig)

    return plot_paths

def generate_outlier_plots(df: pd.DataFrame, outlier_log: pd.DataFrame, plot_config: dict) -> dict:
    """Main orchestrator function that generates all requested outlier plots by calling specialized helpers."""
    plot_save_dir = Path(plot_config.get("plot_save_dir", "exports/plots/outliers/"))
    run_id = plot_config.get("run_id", "default")
    plot_types = plot_config.get("plot_types", ['box', 'hist', 'violin'])
    hue_col = plot_config.get("hue")
    
    plot_save_dir.mkdir(parents=True, exist_ok=True)
    
    if hue_col and hue_col not in df.columns:
        logging.warning(f"Hue column '{hue_col}' not found. Disabling grouping.")
        hue_col = None

    all_plot_paths = {}

    if 'hist' in plot_types:
        hist_paths = _generate_histograms(df, outlier_log, hue_col, plot_save_dir, run_id)
        for col, paths in hist_paths.items():
            all_plot_paths.setdefault(col, []).extend(paths)

    if 'box' in plot_types or 'violin' in plot_types:
        bv_paths = _generate_box_violin_plots(df, outlier_log, plot_types, hue_col, plot_save_dir, run_id)
        for col, paths in bv_paths.items():
            all_plot_paths.setdefault(col, []).extend(paths)
    
    return all_plot_paths