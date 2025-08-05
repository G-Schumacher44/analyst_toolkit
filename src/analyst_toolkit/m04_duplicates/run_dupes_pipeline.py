
"""
🚀 Module: run_dupes_pipeline.py

Runner script for the M04 Duplicates module of the Analyst Toolkit.

This pipeline detects and optionally removes duplicate rows from a DataFrame,
based on configuration settings. It supports both notebook and CLI contexts,
and handles loading, deduplication, reporting, plotting, and checkpointing.

Example:
    >>> from m04_duplicates.run_dupes_pipeline import run_duplicates_pipeline
    >>> from m00_utils.config_loader import load_config
    >>> config = load_config("config/run_duplicates_config.yaml")
    >>> diag_cfg = config.get("diagnostics", {})
    >>> notebook_mode = config.get("notebook", True)
    >>> run_id = config.get("run_id")
    >>> df = run_duplicates_pipeline(config=config, df=df, run_id=run_id, notebook=notebook_mode)
"""


import logging
import pandas as pd
from pathlib import Path
from analyst_toolkit.m00_utils.load_data import load_csv
from analyst_toolkit.m00_utils.export_utils import save_joblib, export_dataframes
from analyst_toolkit.m04_duplicates.handle_dupes import handle_duplicates
from analyst_toolkit.m00_utils.report_generator import generate_duplicates_report
from analyst_toolkit.m04_duplicates.dup_display import display_dupes_summary
from analyst_toolkit.m08_visuals.summary_plots import plot_duplication_summary

def configure_logging(notebook: bool = True, logging_mode: str = "auto"):
    # ... (Standard logging config) ...
    if logging_mode == "off": logging.disable(logging.CRITICAL)
    else:
        level = logging.INFO if logging_mode == "on" or not notebook else logging.WARNING
        logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s", force=True)

def run_duplicates_pipeline(config: dict, df: pd.DataFrame = None, notebook: bool = False, run_id: str = None):
    """Executes the full deduplication pipeline with decoupled reporting."""
    configure_logging(notebook=notebook, logging_mode=config.get("logging", "auto"))
    if not run_id: raise ValueError("A 'run_id' must be provided.")
    
    if df is None:
        if "input_path" not in config: raise KeyError("Missing 'input_path' in duplicates config.")
        df = load_csv(config["input_path"])

    df_original = df.copy()
    
    df_deduplicated = handle_duplicates(df, config)

    subset_cols = config.get("subset_columns") or list(df.columns)
    duplicates_report = generate_duplicates_report(df_original, df_deduplicated, subset_cols)

    settings = config.get("settings", {})
    
    plot_paths = {}
    if settings.get("plotting", {}).get("run", True):
        save_dir = Path(settings.get("plotting", {}).get("save_dir", "exports/plots/duplicates/")) / run_id
        summary_plot_path = plot_duplication_summary(duplicates_report.get("summary"), save_dir, run_id)
        if summary_plot_path:
            plot_paths["Duplication Summary"] = [summary_plot_path]

    if settings.get("show_inline", True) and notebook:
        display_dupes_summary(duplicates_report, subset_cols, plot_paths)
    
    if settings.get("export", False) and duplicates_report:
        export_dataframes(
            data_dict=duplicates_report,
            export_path=settings.get("export_path", f"exports/reports/duplicates/{run_id}_duplicates_report.xlsx"),
            file_format="csv" if settings.get("as_csv", False) else "excel",
            run_id=run_id
        )
    
    checkpoint_cfg = settings.get("checkpoint", {})
    if isinstance(checkpoint_cfg, dict) and checkpoint_cfg.get("run", False):
        checkpoint_path = checkpoint_cfg.get("checkpoint_path", "").format(run_id=run_id)
        if not checkpoint_path: raise ValueError("Checkpoint enabled but 'checkpoint_path' is missing.")
        save_joblib(df_deduplicated, path=checkpoint_path)

    return df_deduplicated