"""
🚀 Module: run_handling_pipeline.py

Runner script for the M06 Outlier Handling module in the Analyst Toolkit.

This pipeline applies outlier handling strategies such as clipping, replacement, or constant
substitution to previously flagged data. It loads both the flagged DataFrame and detection
results, performs the cleaning, exports a detailed audit report, and optionally saves
the checkpointed cleaned DataFrame.

Example:
    >>> from m06_outlier_handling.run_handling_pipeline import run_outlier_handling_pipeline
    >>> from m00_utils.config_loader import load_config
    >>> config = load_config("config/run_outlier_handling_config.yaml")
    >>> diag_cfg = config.get("diagnostics", {})
    >>> notebook_mode = config.get("notebook", True)
    >>> run_id = config.get("run_id")
    >>> df_cleaned = run_outlier_handling_pipeline(
    ...     config=config, run_id=run_id, notebook=notebook_mode, df=df
    ... )
"""

import logging

import pandas as pd
from joblib import load as load_joblib

from analyst_toolkit.m00_utils.export_utils import (
    export_dataframes,
    export_html_report,
    save_joblib,
)
from analyst_toolkit.m00_utils.report_generator import generate_outlier_handling_report

from analyst_toolkit.m06_outlier_handling.outlier_handler import handle_outliers


def configure_logging(notebook: bool = True, logging_mode: str = "auto"):
    if logging_mode == "off":
        logging.disable(logging.CRITICAL)
    else:
        level = logging.INFO if logging_mode == "on" or not notebook else logging.WARNING
        logging.basicConfig(
            level=level, format="%(asctime)s - %(levelname)s - %(message)s", force=True
        )


def run_outlier_handling_pipeline(
    config: dict,
    df: pd.DataFrame = None,
    detection_results: dict = None,
    notebook: bool = False,
    run_id: str = None,
):
    """Executes the outlier handling pipeline with robust configuration handling."""
    if "outlier_handling" in config:
        module_cfg = config.get("outlier_handling", {})
    else:
        module_cfg = config

    configure_logging(notebook=notebook, logging_mode=module_cfg.get("logging", "auto"))
    if not run_id:
        raise ValueError("A 'run_id' must be provided.")

    if df is None:
        df = load_joblib(module_cfg["input_df_path"].format(run_id=run_id))

    if detection_results is None:
        detection_results = load_joblib(module_cfg["detection_results_path"].format(run_id=run_id))

    df_original = df.copy()
    df_handled, handling_summary_log = handle_outliers(df, detection_results, module_cfg)

    # 2. Generate the detailed, evidence-based report
    handling_report = generate_outlier_handling_report(
        df_original, df_handled, handling_summary_log
    )

    settings = module_cfg.get("settings", {})

    # 3. Display the summary dashboard
    if settings.get("show_inline", True) and notebook:
        from analyst_toolkit.m06_outlier_handling.display_handling import display_handling_summary

        display_handling_summary(handling_report)

    # 4. Export the detailed report
    export_cfg = settings.get("export", {})
    if export_cfg.get("run", False) and handling_report:
        export_path = export_cfg.get(
            "export_path",
            f"exports/reports/outliers/handling/{run_id}_outlier_handling_report.xlsx",
        ).format(run_id=run_id)
        file_format = "csv" if export_cfg.get("as_csv", False) else "excel"
        export_dataframes(
            data_dict=handling_report,
            export_path=export_path,
            file_format=file_format,
            run_id=run_id,
        )
        if export_cfg.get("export_html", False):
            html_path = export_cfg.get(
                "export_html_path",
                f"exports/reports/outliers/handling/{run_id}_outlier_handling_report.html",
            ).format(run_id=run_id)
            export_html_report(handling_report, html_path, "Outlier Handling", run_id)

    # 5. Checkpoint the handled DataFrame
    checkpoint_cfg = settings.get("checkpoint", {})
    if isinstance(checkpoint_cfg, dict) and checkpoint_cfg.get("run", False):
        checkpoint_path = checkpoint_cfg.get(
            "checkpoint_path", f"exports/joblib/{run_id}_m06_df_handled.joblib"
        ).format(run_id=run_id)
        if not checkpoint_path:
            raise ValueError("Checkpoint enabled but 'checkpoint_path' is missing.")
        save_joblib(df_handled, path=checkpoint_path)

    return df_handled
