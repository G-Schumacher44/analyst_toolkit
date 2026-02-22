"""
🧩 Module: run_imputation_pipeline.py

Runner script for the M07 Imputation module in the Analyst Toolkit.

This pipeline executes column-level imputation strategies as defined in the YAML
configuration. It supports constant, mean, median, and mode imputations, then logs
and displays impact reports with before/after comparison plots.

Exports:
- Detailed changelog and report (XLSX or CSV)
- Inline notebook summary and plots (if enabled)
- Checkpointed imputed DataFrame (optional)

Example:
    >>> from m07_imputation.run_imputation_pipeline import run_imputation_pipeline
    >>> from m00_utils.config_loader import load_config
    >>> config = load_config("config/run_imputation_config.yaml")
    >>> diag_cfg = config.get("diagnostics", {})
    >>> notebook_mode = config.get("notebook", True)
    >>> run_id = config.get("run_id")
    >>> df_final = run_imputation_pipeline(config=config, run_id=run_id, df=df,  notebook=notebook_mode)
"""

import logging
from pathlib import Path  # <-- CORRECTED: Added missing import

import pandas as pd
from joblib import load as load_joblib

from analyst_toolkit.m00_utils.export_utils import (
    export_dataframes,
    export_html_report,
    save_joblib,
)
from analyst_toolkit.m00_utils.report_generator import generate_imputation_report
from analyst_toolkit.m07_imputation.display_imputation import display_imputation_summary
from analyst_toolkit.m07_imputation.impute_data import apply_imputation
from analyst_toolkit.m08_visuals.comparison_plots import (
    plot_categorical_imputation_comparison,
    plot_imputation_comparison,
)


def configure_logging(notebook: bool = True, logging_mode: str = "auto"):
    if logging_mode == "off":
        logging.disable(logging.CRITICAL)
    else:
        level = logging.INFO if logging_mode == "on" or not notebook else logging.WARNING
        logging.basicConfig(
            level=level, format="%(asctime)s - %(levelname)s - %(message)s", force=True
        )


def run_imputation_pipeline(
    config: dict, df: pd.DataFrame = None, notebook: bool = False, run_id: str = None
):
    """Executes the imputation pipeline with robust configuration handling."""
    if "imputation" in config:
        module_cfg = config.get("imputation", {})
    else:
        module_cfg = config

    if not module_cfg:
        raise ValueError("Configuration for 'imputation' module not found or is empty.")

    configure_logging(notebook=notebook, logging_mode=module_cfg.get("logging", "auto"))
    if not run_id:
        raise ValueError("A 'run_id' must be provided.")

    if df is None:
        # Correctly format path if run_id placeholder exists
        input_path = module_cfg["input_path"]
        if "{run_id}" in input_path:
            input_path = input_path.format(run_id=run_id)
        df = load_joblib(input_path)

    if not module_cfg.get("rules"):
        logging.warning("No imputation rules found. Returning original DataFrame.")
        return df

    df_original = df.copy()
    df_imputed, detailed_changelog = apply_imputation(df, module_cfg)

    imputation_report = generate_imputation_report(df_original, df_imputed, detailed_changelog)

    settings = module_cfg.get("settings", {})

    plot_paths = {}
    if settings.get("plotting", {}).get("run", True) and not detailed_changelog.empty:
        save_dir = (
            Path(settings.get("plotting", {}).get("save_dir", "exports/plots/imputation/")) / run_id
        )
        imputed_cols = detailed_changelog["Column"].tolist()

        comp_plots = []
        for col in imputed_cols:
            if pd.api.types.is_numeric_dtype(df_imputed[col]):
                plot_path = plot_imputation_comparison(
                    df_original[col], df_imputed[col], save_dir, run_id
                )
            else:
                plot_path = plot_categorical_imputation_comparison(
                    df_original[col], df_imputed[col], save_dir, run_id
                )
            if plot_path:
                comp_plots.append(plot_path)
        if comp_plots:
            plot_paths["Imputation Comparison"] = comp_plots

    if settings.get("show_inline", True) and notebook:
        display_imputation_summary(imputation_report, plot_paths)

    export_cfg = settings.get("export", {})
    if export_cfg.get("run") and imputation_report:
        export_path = export_cfg.get(
            "export_path", f"exports/reports/imputation/{run_id}_imputation_report.xlsx"
        )
        if "{run_id}" in export_path:
            export_path = export_path.format(run_id=run_id)
        file_format = "csv" if export_cfg.get("as_csv", False) else "excel"
        export_dataframes(
            data_dict=imputation_report,
            export_path=export_path,
            file_format=file_format,
            run_id=run_id,
        )
        if export_cfg.get("export_html", False):
            html_path = export_cfg.get(
                "export_html_path", f"exports/reports/imputation/{run_id}_imputation_report.html"
            )
            if "{run_id}" in html_path:
                html_path = html_path.format(run_id=run_id)
            export_html_report(imputation_report, html_path, "Imputation", run_id)

    checkpoint_cfg = settings.get("checkpoint", {})
    if isinstance(checkpoint_cfg, dict) and checkpoint_cfg.get("run", False):
        checkpoint_path = checkpoint_cfg.get("checkpoint_path", "").format(run_id=run_id)
        if not checkpoint_path:
            raise ValueError("Checkpoint enabled but 'checkpoint_path' is missing.")
        save_joblib(df_imputed, path=checkpoint_path)

    return df_imputed
