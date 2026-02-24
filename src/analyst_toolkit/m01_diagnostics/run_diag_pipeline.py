"""
🚀 Module: run_diag_pipeline.py

Runner script for the M01 Diagnostics module of the Analyst Toolkit.

Loads configuration, optionally ingests a CSV, generates a structured
data profile, saves summary reports, and produces visualizations.
Handles notebook and pipeline modes with configurable logging, export,
and inline display settings.

This is the primary orchestrator for executing the diagnostics stage.

Example:
    >>> from m01_diagnostics.run_diag_pipeline import run_diag_pipeline
    >>> config = load_config("config/run_diagnostics_config.yaml")
    >>> diag_cfg = config.get("diagnostics", {})
    >>> notebook_mode = config.get("notebook", True)
    >>> run_id = config.get("run_id")
    >>> df = run_diag_pipeline(config=config, df=df, notebook=notebook_mode, run_id=run_id)
"""

import logging
from pathlib import Path

import pandas as pd

from analyst_toolkit.m00_utils.export_utils import (
    export_dataframes,
    export_html_report,
)
from analyst_toolkit.m00_utils.load_data import load_csv
from analyst_toolkit.m01_diagnostics.data_diag import run_data_profile
from analyst_toolkit.m08_visuals.distributions import (
    plot_categorical_distribution,
    plot_continuous_distribution,
)
from analyst_toolkit.m08_visuals.summary_plots import (
    plot_correlation_heatmap,
    plot_dtype_summary,
    plot_missingness,
)


def configure_logging(notebook: bool = True, logging_mode: str = "auto"):
    if logging_mode == "off":
        logging.disable(logging.CRITICAL)
    else:
        level = logging.INFO if logging_mode == "on" or not notebook else logging.WARNING
        logging.basicConfig(
            level=level, format="%(asctime)s - %(levelname)s - %(message)s", force=True
        )


def run_diag_pipeline(
    config: dict, notebook: bool = False, df: pd.DataFrame = None, run_id: str = None
):
    """Executes the diagnostics pipeline with robust configuration handling."""
    if "diagnostics" in config:
        module_cfg = config.get("diagnostics", {})
    else:
        module_cfg = config

    if not module_cfg:
        raise ValueError("Configuration for 'diagnostics' module not found or is empty.")

    configure_logging(notebook=notebook, logging_mode=module_cfg.get("logging", "auto"))
    if not run_id:
        raise ValueError("A 'run_id' must be provided.")

    if df is None:
        input_path = module_cfg.get("input_path")
        if not input_path:
            raise KeyError("Missing 'input_path' and no DataFrame provided.")
        df = load_csv(input_path)

    profile_cfg = module_cfg.get("profile", {})
    if profile_cfg.get("run", False):
        full_profile = run_data_profile(df, config=module_cfg)
        settings = profile_cfg.get("settings", {})

        if settings.get("export", False):
            export_path = settings.get(
                "export_path", f"exports/reports/diagnostics/{run_id}_diagnostics_report.xlsx"
            )
            export_dataframes(
                data_dict=full_profile["for_export"], export_path=export_path, run_id=run_id
            )
            if settings.get("export_html", False):
                html_path = settings.get(
                    "export_html_path",
                    "exports/reports/diagnostics/{run_id}_diagnostics_report.html",
                ).format(run_id=run_id)
                export_html_report(full_profile["for_export"], html_path, "Diagnostics", run_id)

        plot_paths = {}
        plotting_cfg = module_cfg.get("plotting", {})
        if plotting_cfg.get("run", True):
            logging.info("Generating diagnostic plots...")
            save_dir = Path(plotting_cfg.get("save_dir", "exports/plots/diagnostics/")) / run_id

            # Generate high-level summary plots (Fast and high-value)
            plot_paths["Summary Plots"] = [
                p
                for p in [
                    plot_missingness(df, save_dir, run_id),
                    plot_correlation_heatmap(df, save_dir, run_id),
                    plot_dtype_summary(df, save_dir, run_id),
                ]
                if p is not None
            ]

            # --- OPTIMIZATION: Limit distribution plots to prevent timeouts ---
            include_dist = plotting_cfg.get("include_distributions", True)
            max_plots = plotting_cfg.get("max_distribution_plots", 20)

            if include_dist:
                numeric_cols = df.select_dtypes(include="number").columns[:max_plots]
                categorical_cols = df.select_dtypes(include=["object", "category"]).columns[
                    :max_plots
                ]

                if len(df.columns) > max_plots:
                    logging.info(
                        f"Wide dataset detected (> {max_plots} cols). Limiting distribution plots to first {max_plots} columns to prevent timeout."
                    )

                dist_num_paths = [
                    plot_continuous_distribution(df[col], save_dir, run_id) for col in numeric_cols
                ]
                dist_cat_paths = [
                    plot_categorical_distribution(df[col], save_dir, run_id)
                    for col in categorical_cols
                ]

                plot_paths["Numeric Distributions"] = [p for p in dist_num_paths if p]
                plot_paths["Categorical Distributions"] = [p for p in dist_cat_paths if p]
            else:
                logging.info("Individual distribution plots skipped (include_distributions=False).")
            # --- END OF OPTIMIZATION ---

        if settings.get("show_inline", False) and notebook:
            from analyst_toolkit.m01_diagnostics.diag_display import display_profile_summary

            display_profile_summary(
                full_profile["for_display"], plot_paths=plot_paths, settings=settings
            )

    return df
