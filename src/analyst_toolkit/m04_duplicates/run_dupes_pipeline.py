"""
🚀 Module: run_dupes_pipeline.py

This module serves as the primary runner for the M04 Duplicates pipeline.
It orchestrates the detection and handling of duplicate rows in a DataFrame,
adhering to a decoupled "detect-then-handle" workflow. The pipeline supports
both notebook and CLI execution contexts, managing data loading, deduplication,
reporting, and checkpointing based on the provided configuration.

Example:
    from analyst_toolkit.m04_duplicates import run_duplicates_pipeline
    from analyst_toolkit.m00_utils.config_loader import load_config
    
    config = load_config("config/dups_config_template.yaml")
    # df_processed will be either flagged or deduplicated based on config
    df_processed = run_duplicates_pipeline(config=config, df=my_dataframe)
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
    """
    Executes the full deduplication pipeline with decoupled reporting.

    This function first detects all duplicates based on the configuration, then
    either flags them by adding a boolean column or removes them. It generates
    a comprehensive report of its actions and can save the processed DataFrame
    as a checkpoint.

    Args:
        config (dict): The configuration dictionary, which can be the full
                       toolkit config or just the 'duplicates' block.
        df (pd.DataFrame, optional): The DataFrame to process. If None, it will be loaded based on 'input_path' in the config.
        notebook (bool): If True, renders rich HTML outputs in a notebook environment.
        run_id (str): A unique identifier for the pipeline run, used for versioning outputs.
    """
    # This runner can now accept either the full config or just the 'duplicates' block.
    if "duplicates" in config:
        dupes_cfg = config.get("duplicates")
    else:
        dupes_cfg = config # Assume the duplicates block was passed directly.

    # Configure logging from the module's config block for consistency.
    configure_logging(notebook=notebook, logging_mode=dupes_cfg.get("logging", "auto"))
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
    
    settings = dupes_cfg.get("settings", {})
    
    # In 'flag' mode, the processed DataFrame is the original with the 'is_duplicate' column added.
    # No rows are removed, allowing for non-destructive analysis.
    if mode == 'flag':
        df_processed = df_flagged
        logging.info(f"Flagged {detection_results['duplicate_count']} rows. No rows were removed.")
    # In 'remove' mode, the destructive action is performed.
    elif mode == 'remove':
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