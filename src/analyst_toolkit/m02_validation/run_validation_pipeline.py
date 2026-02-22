"""
🚀 Module: run_validation_pipeline.py

Runner script for the M02 Validation module of the Analyst Toolkit.

This orchestrator handles configuration resolution, logging setup, optional
data loading, validation execution, export of rule-check results, and inline
dashboard display. It supports both standalone use in notebooks and integration
with the full pipeline runner.

Supports schema, dtype, categorical, and range-based rule validation.

Example:
    >>> from m02_validation.run_validation_pipeline import run_validation_pipeline
    >>> from m00_utils.config_loader import load_config
    >>> config = load_config("config/run_validation_config.yaml")
    >>> diag_cfg = config.get("diagnostics", {})
    >>> notebook_mode = config.get("notebook", True)
    >>> run_id = config.get("run_id")
    >>> df = run_validation_pipeline(config=config, df=df, notebook=notebook_mode, run_id=run_id)
"""

import logging

import pandas as pd

from analyst_toolkit.m00_utils.export_utils import (
    export_html_report,
    export_validation_results,
    save_joblib,
)
from analyst_toolkit.m00_utils.load_data import load_csv
from analyst_toolkit.m02_validation.validate_data import run_validation_suite



def configure_logging(notebook: bool = True, logging_mode: str = "auto"):
    """Configures logging based on execution mode."""
    if logging_mode == "off":
        logging.disable(logging.CRITICAL)
        return

    # Default to WARNING in notebooks unless 'on', and INFO in scripts unless 'off'.
    if logging_mode == "auto":
        level = logging.WARNING if notebook else logging.INFO
    else:  # 'on'
        level = logging.INFO

    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s", force=True)


def run_validation_pipeline(
    config: dict, notebook: bool = False, df: pd.DataFrame = None, run_id: str = None
):
    """
    Executes the validation pipeline with robust configuration handling
    for both standalone and master-runner execution.
    """
    # --- ROBUST CONFIGURATION HANDLING ---
    # If the 'validation' key exists, it means the full config was passed (from master runner).
    # Otherwise, assume the passed config is the correct module-specific block (from notebook).
    if "validation" in config:
        module_cfg = config.get("validation", {})
    else:
        module_cfg = config

    if not module_cfg:
        raise ValueError("Configuration for 'validation' module not found or is empty.")

    logging_mode = module_cfg.get("logging", "auto")
    configure_logging(notebook=notebook, logging_mode=logging_mode)

    if not run_id:
        raise ValueError("A 'run_id' must be provided.")

    if df is None:
        input_path = module_cfg.get("input_path")
        if not input_path:
            raise KeyError(
                "Missing 'input_path' in validation config and no DataFrame was provided."
            )
        df = load_csv(input_path)
        logging.info(f"Loaded data from CSV via config: {input_path}")
    else:
        logging.info("Using passed-in DataFrame (no reload).")

    schema_validation_cfg = module_cfg.get("schema_validation", {})
    if schema_validation_cfg.get("run", False):
        validation_results = run_validation_suite(df, config=module_cfg)

        fail_on_error = schema_validation_cfg.get("fail_on_error", False)
        if fail_on_error:
            failed_checks = [
                name
                for name, check in validation_results.items()
                if isinstance(check, dict) and "passed" in check and not check.get("passed")
            ]
            if failed_checks:
                error_message = (
                    "Validation Gatekeeper FAILED. The following checks did not pass:\n- "
                    + "\n- ".join(failed_checks)
                )
                logging.error(error_message)
                raise ValueError(error_message)
            else:
                logging.info("✅ Validation Gatekeeper PASSED. All checks are clean.")

        settings = module_cfg.get("settings", {})
        if settings.get("export", True):
            export_validation_results(validation_results, config=settings, run_id=run_id)
            if settings.get("export_html", False):
                html_path = settings.get(
                    "export_html_path", "exports/reports/validation/{run_id}_validation_report.html"
                ).format(run_id=run_id)
                checks = {
                    k: v
                    for k, v in validation_results.items()
                    if isinstance(v, dict) and "passed" in v
                }
                report_tables = {
                    k: pd.DataFrame(
                        [
                            {
                                "Rule": k,
                                "Passed": v.get("passed"),
                                "Description": v.get("rule_description", ""),
                            }
                        ]
                    )
                    for k, v in checks.items()
                }
                export_html_report(report_tables, html_path, "Validation", run_id)

        if settings.get("show_inline", True) and notebook:
            from analyst_toolkit.m02_validation.validation_display import display_validation_summary

            display_validation_summary(validation_results, notebook=notebook)

        if settings.get("checkpoint", False):
            checkpoint_path = settings.get("checkpoint_path", "").format(run_id=run_id)
            if not checkpoint_path:
                raise ValueError("Checkpoint enabled but 'checkpoint_path' is missing.")
            save_joblib(df, path=checkpoint_path)

    return df
