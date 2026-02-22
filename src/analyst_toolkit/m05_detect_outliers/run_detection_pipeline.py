"""
🚀 Module: run_detection_pipeline.py

Runner script for the M05 Outlier Detection module in the Analyst Toolkit.

This pipeline executes outlier detection using configurable IQR and Z-score methods.
It handles loading input data, applying detection logic, exporting reports and plots,
and saving checkpointed results. Also supports inline visualization in notebook mode.

Example:
    >>> from m05_detect_outliers.run_detection_pipeline import run_outlier_detection_pipeline
    >>> from m00_utils.config_loader import load_config
    >>> config = load_config("config/run_outlier_detection_config.yaml")
    >>> diag_cfg = config.get("diagnostics", {})
    >>> notebook_mode = config.get("notebook", True)
    >>> run_id = config.get("run_id")
    >>> df_flagged, detection_results = run_outlier_detection_pipeline(
    ...     config=config, notebook=notebook_mode, df=df, run_id=run_id
    ... )
"""

import logging

import pandas as pd

from analyst_toolkit.m00_utils.export_utils import (
    export_dataframes,
    export_html_report,
    save_joblib,
)
from analyst_toolkit.m00_utils.load_data import load_csv
from analyst_toolkit.m00_utils.report_generator import generate_outlier_report
from analyst_toolkit.m05_detect_outliers.detect_outliers import detect_outliers

from analyst_toolkit.m05_detect_outliers.plot_outliers import generate_outlier_plots


def configure_logging(notebook: bool = True, logging_mode: str = "auto"):
    if logging_mode == "off":
        logging.disable(logging.CRITICAL)
    else:
        level = logging.INFO if logging_mode == "on" or not notebook else logging.WARNING
        logging.basicConfig(
            level=level, format="%(asctime)s - %(levelname)s - %(message)s", force=True
        )


def run_outlier_detection_pipeline(
    config: dict, df: pd.DataFrame = None, notebook: bool = False, run_id: str = None
):
    """Executes the outlier detection pipeline with robust configuration handling."""
    # --- ROBUST CONFIGURATION HANDLING ---
    if "outlier_detection" in config:
        module_cfg = config.get("outlier_detection", {})
    else:
        module_cfg = config

    configure_logging(notebook=notebook, logging_mode=module_cfg.get("logging", "auto"))
    if not run_id:
        raise ValueError("A 'run_id' must be provided.")

    if df is None:
        input_path = module_cfg.get("input_path")
        if not input_path:
            raise KeyError("Missing 'input_path' and no DataFrame provided.")
        df = load_csv(input_path.format(run_id=run_id))

    detection_results = detect_outliers(df, module_cfg)
    outlier_log_df = detection_results.get("outlier_log")

    plot_save_dir = None
    plotting_cfg = module_cfg.get("plotting", {})
    if plotting_cfg.get("run") and outlier_log_df is not None and not outlier_log_df.empty:
        save_dir_template = plotting_cfg.get("plot_save_dir", "exports/plots/outliers/{run_id}")
        plot_save_dir = save_dir_template.format(run_id=run_id)

        plotting_cfg["run_id"] = run_id
        plotting_cfg["plot_save_dir"] = plot_save_dir
        generate_outlier_plots(df, outlier_log_df, plotting_cfg)

    outlier_report = generate_outlier_report(detection_results)

    df_out = df.copy()
    if module_cfg.get("append_flags", False):
        outlier_flags_df = detection_results.get("outlier_flags")
        if outlier_flags_df is not None:
            df_out = pd.concat([df_out, outlier_flags_df], axis=1)

    export_cfg = module_cfg.get("export", {})
    if export_cfg.get("run") and outlier_report:
        export_path_template = export_cfg.get(
            "export_path", "exports/reports/outliers/detection/{run_id}_outlier_report.xlsx"
        )
        export_dataframes(
            data_dict=outlier_report, export_path=export_path_template.format(run_id=run_id)
        )
        if export_cfg.get("export_html", False):
            html_path_template = export_cfg.get(
                "export_html_path",
                "exports/reports/outliers/detection/{run_id}_outlier_report.html",
            )
            export_html_report(
                outlier_report,
                html_path_template.format(run_id=run_id),
                "Outlier Detection",
                run_id,
            )

    if plotting_cfg.get("show_plots_inline") and notebook:
        from analyst_toolkit.m05_detect_outliers.display_detection import display_detection_summary

        display_detection_summary(detection_results, plot_save_dir=plot_save_dir)

    # This block will now execute correctly.
    checkpoint_cfg = module_cfg.get("checkpoint", {})
    if checkpoint_cfg.get("run"):
        checkpoint_path = checkpoint_cfg.get("checkpoint_path", "").format(run_id=run_id)
        if not checkpoint_path:
            raise ValueError("Checkpoint enabled but 'checkpoint_path' is missing.")
        save_joblib(df_out, path=checkpoint_path)
        logging.info(f"✅ Outlier-flagged DataFrame checkpoint saved to {checkpoint_path}")

    return df_out, detection_results
