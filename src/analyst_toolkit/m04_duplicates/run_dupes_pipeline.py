
"""
🚀 Module: run_dupes_pipeline.py

Runner script for the M04 Duplicates module of the Analyst Toolkit.

This pipeline detects and handles duplicate rows from a DataFrame,
based on a decoupled detect-then-handle workflow. It supports both notebook
and CLI contexts, and handles loading, deduplication, reporting, and checkpointing.

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
from analyst_toolkit.m00_utils.load_data import load_csv, load_joblib
from analyst_toolkit.m00_utils.export_utils import export_duplicates_report, save_joblib
from .detect_dupes import detect_duplicates
from .handle_dupes import handle_duplicates
from analyst_toolkit.m00_utils.report_generator import generate_duplicates_report
from .dup_display import display_dupes_summary
from analyst_toolkit.m08_visuals.summary_plots import plot_duplication_summary

def configure_logging(notebook: bool = True, logging_mode: str = "auto"):
    # ... (Standard logging config) ...
    if logging_mode == "off": logging.disable(logging.CRITICAL)
    else:
        level = logging.INFO if logging_mode == "on" or not notebook else logging.WARNING
        logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s", force=True)

def run_duplicates_pipeline(config: dict, df: pd.DataFrame = None, notebook: bool = False, run_id: str = None):
    """Executes the full deduplication pipeline with decoupled reporting."""
    # This runner can now accept either the full config or just the 'duplicates' block.
    if "duplicates" in config:
        dupes_cfg = config.get("duplicates")
    else:
        dupes_cfg = config # Assume the duplicates block was passed directly.

    # Logging should be configured from the top-level config if available.
    configure_logging(notebook=notebook, logging_mode=config.get("logging", "auto"))
    if not run_id: raise ValueError("A 'run_id' must be provided.")
    
    if df is None:
        if "input_path" not in dupes_cfg: raise KeyError("Missing 'input_path' in duplicates config.")
        input_file = dupes_cfg["input_path"].format(run_id=run_id)
        if input_file.endswith('.joblib'):
            df = load_joblib(input_file)
        else:
            df = load_csv(input_file)
            
    df_original = df.copy()
    
    # --- Step 1: Always detect duplicates first (non-destructive) ---
    subset_cols = dupes_cfg.get("subset_columns")
    df_flagged, detection_results = detect_duplicates(df_original.copy(), subset_cols)
    
    # --- Step 2: Decide whether to handle (remove) or just return the flagged DF ---
    mode = dupes_cfg.get("mode", "remove")
    logging.info(f"Duplicate processing mode: '{mode}'")
    
    # In 'flag' mode, we simply return the dataframe with the 'is_duplicate' column.
    # No rows are removed.
    if mode == 'flag':
        df_processed = df_flagged
        logging.info(f"Flagged {detection_results['duplicate_count']} rows. No rows were removed.")
    # In 'remove' mode, we perform the destructive action.
    elif mode == 'remove':
        settings = dupes_cfg.get("settings", {})
        checkpoint_cfg = settings.get("checkpoint", {})
        # As an artifact, save the flagged df before removing rows if checkpointing is on.
        if isinstance(checkpoint_cfg, dict) and checkpoint_cfg.get("run", False):
            flagged_path_template = checkpoint_cfg.get("flagged_checkpoint_path", "exports/joblib/{run_id}/{run_id}_m04_df_flagged.joblib")
            if flagged_path_template:
                save_joblib(df_flagged, path=flagged_path_template.format(run_id=run_id))
        df_processed = handle_duplicates(df_original, dupes_cfg)
        logging.info(f"Removed {len(df_original) - len(df_processed)} rows based on 'keep: {dupes_cfg.get('keep', 'first')}' strategy.")
    else:
        raise ValueError(f"Invalid mode specified in configuration: '{mode}'. Must be 'flag' or 'remove'.")

    # --- Step 3: Generate report based on detection results and final state ---
    duplicates_report = generate_duplicates_report(df_original, df_processed, detection_results, mode, df_flagged=df_flagged)
    
    settings = dupes_cfg.get("settings", {})
    
    plot_paths = {}
    if settings.get("plotting", {}).get("run", True):
        save_dir = Path(settings.get("plotting", {}).get("save_dir", "exports/plots/duplicates/")).joinpath(run_id)
        # Ensure summary_df is valid before plotting
        summary_plot_path = plot_duplication_summary(duplicates_report.get("summary"), save_dir, run_id)
        if summary_plot_path:
            plot_paths["Duplication Summary"] = [summary_plot_path]

    if settings.get("show_inline", True) and notebook:
        display_dupes_summary(duplicates_report, subset_cols, plot_paths)
    
    if settings.get("export", False) and duplicates_report:
        export_duplicates_report(
            report=duplicates_report,
            config=settings,
            run_id=run_id
        )
    
    checkpoint_cfg = settings.get("checkpoint", {})
    if isinstance(checkpoint_cfg, dict) and checkpoint_cfg.get("run", False):
        checkpoint_path_template = checkpoint_cfg.get("checkpoint_path", "")
        if not checkpoint_path_template: 
            raise ValueError("Checkpoint enabled but 'checkpoint_path' is missing.")
        final_path = checkpoint_path_template.format(run_id=run_id)
        save_joblib(df_processed, path=final_path)

    return df_processed