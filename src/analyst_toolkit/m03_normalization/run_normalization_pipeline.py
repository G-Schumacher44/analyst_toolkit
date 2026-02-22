"""
🚀 Module: run_normalization_pipeline.py

Runner script for the M03 Normalization module of the Analyst Toolkit.

This orchestrator applies rule-based cleaning transformations to the dataset.
It loads configuration, applies normalization logic, generates a changelog report,
and optionally exports reports, plots, and checkpointed artifacts.

Supports both pipeline and notebook-based execution contexts.

Example:
    >>> from m03_normalization.run_normalization_pipeline import run_normalization_pipeline
    >>> from m00_utils.config_loader import load_config
    >>> config = load_config("config/run_normalization_config.yaml")
    >>> diag_cfg = config.get("diagnostics", {})
    >>> notebook_mode = config.get("notebook", True)
    >>> run_id = config.get("run_id")
    >>> df = run_normalization_pipeline(config=config, df=df,notebook=notebook_mode, run_id=run_id)
"""

import logging

import pandas as pd

from analyst_toolkit.m00_utils.export_utils import (
    export_dataframes,
    export_html_report,
    save_joblib,
)
from analyst_toolkit.m00_utils.load_data import load_csv
from analyst_toolkit.m00_utils.report_generator import generate_transformation_report

from analyst_toolkit.m03_normalization.normalize_data import apply_normalization


def configure_logging(notebook: bool = True, logging_mode: str = "auto"):
    if logging_mode == "off":
        logging.disable(logging.CRITICAL)
    else:
        level = logging.INFO if logging_mode == "on" or not notebook else logging.WARNING
        logging.basicConfig(
            level=level, format="%(asctime)s - %(levelname)s - %(message)s", force=True
        )


def run_normalization_pipeline(
    config: dict, notebook: bool = False, df: pd.DataFrame = None, run_id: str = None
):
    """Executes the data normalization pipeline with robust configuration handling."""
    if "normalization" in config:
        module_cfg = config.get("normalization", {})
    else:
        module_cfg = config

    if not module_cfg:
        raise ValueError("Configuration for 'normalization' module not found or is empty.")

    configure_logging(notebook=notebook, logging_mode=module_cfg.get("logging", "auto"))
    if not run_id:
        raise ValueError("A 'run_id' must be provided.")

    if df is None:
        input_path = module_cfg.get("input_path")
        if not input_path:
            raise KeyError("Missing 'input_path' in normalization config.")
        df = load_csv(input_path)

    if not module_cfg.get("rules"):
        logging.warning("No normalization rules found. Returning original DataFrame.")
        return df

    # Pass the entire module config to the producer
    df_original, df_normalized, changelog = apply_normalization(df, module_cfg)

    settings = module_cfg.get("settings", {})
    report_tables = generate_transformation_report(
        df_original=df_original,
        df_transformed=df_normalized,
        changelog=changelog,
        module_name="normalization",
        run_id=run_id,
        export_config=settings,
    )

    if settings.get("show_inline", True) and notebook:
        from analyst_toolkit.m03_normalization.display_normalization import display_normalization_summary

        display_normalization_summary(
            changelog, df_original, df_normalized, module_cfg.get("rules", {})
        )

    if settings.get("export", True):
        export_path = settings.get(
            "export_path", f"exports/reports/normalization/normalization_report_{run_id}.xlsx"
        )
        export_dataframes(report_tables, export_path)
        if settings.get("export_html", False):
            html_path = settings.get(
                "export_html_path",
                "exports/reports/normalization/{run_id}_normalization_report.html",
            ).format(run_id=run_id)
            export_html_report(report_tables, html_path, "Normalization", run_id)

    checkpoint_cfg = settings.get("checkpoint", {})
    if isinstance(checkpoint_cfg, dict) and checkpoint_cfg.get("run", False):
        checkpoint_path = checkpoint_cfg.get("checkpoint_path", "").format(run_id=run_id)
        if not checkpoint_path:
            raise ValueError("Checkpoint enabled but 'checkpoint_path' is missing.")
        save_joblib(df_normalized, path=checkpoint_path)

    return df_normalized
