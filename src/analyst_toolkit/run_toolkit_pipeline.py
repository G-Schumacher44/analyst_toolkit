"""
🚀 run_toolkit_pipeline.py
✅ Module: Master Pipeline Orchestrator
This is the main entry point for running the entire Analyst Toolkit pipeline from start to finish.

Responsibilities:
- Loads a master YAML configuration file.
- Sequentially executes each enabled module (M01-M10).
- Passes the DataFrame from one module to the next.
- Handles global settings like `run_id` and `notebook_mode`.

Usage (Notebook):
-----------------
```python
from analyst_toolkit.run_toolkit_pipeline import run_full_pipeline

final_df = run_full_pipeline(config_path="config/run_toolkit_config.yaml")
```
Usage (CLI / Script):
---------------------
Ensure `notebook: false` and `run_id: "your_id"` are set in the config YAML.

```bash
python -m analyst_toolkit.run_toolkit_pipeline --config config/run_toolkit_config.yaml
```

"""

import argparse
import logging

import pandas as pd

from analyst_toolkit.m00_utils.config_loader import load_config
from analyst_toolkit.m00_utils.load_data import load_csv

# Import all module runners
from analyst_toolkit.m01_diagnostics.run_diag_pipeline import run_diag_pipeline
from analyst_toolkit.m02_validation.run_validation_pipeline import run_validation_pipeline
from analyst_toolkit.m03_normalization.run_normalization_pipeline import run_normalization_pipeline
from analyst_toolkit.m04_duplicates.run_dupes_pipeline import run_duplicates_pipeline
from analyst_toolkit.m05_detect_outliers.run_detection_pipeline import (
    run_outlier_detection_pipeline,
)
from analyst_toolkit.m06_outlier_handling.run_handling_pipeline import run_outlier_handling_pipeline
from analyst_toolkit.m07_imputation.run_imputation_pipeline import run_imputation_pipeline
from analyst_toolkit.m10_final_audit.final_audit_pipeline import run_final_audit_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def run_full_pipeline(config_path: str):
    """
    Executes the full data processing pipeline by chaining modules together.
    """
    logging.info(f"--- Loading Master Orchestration Config from {config_path} ---")
    master_config = load_config(config_path)

    run_id = master_config.get("run_id", "default_run")
    notebook_mode = master_config.get("notebook", False)
    modules_to_run = master_config.get("modules", {})

    # --- ROBUST INITIAL DATA LOAD ---
    entry_path = master_config.get("pipeline_entry_path")
    if not entry_path:
        raise ValueError("Master config is missing 'pipeline_entry_path'. Cannot start pipeline.")

    logging.info(f"--- 🚚 Loading initial data from {entry_path} ---")
    df: pd.DataFrame = load_csv(entry_path)

    # Initialize artifact placeholder for outlier detection
    detection_results = {}

    # --- MODULE EXECUTION CHAIN ---

    # M01: Diagnostics
    module_info = modules_to_run.get("diagnostics")
    if module_info and module_info.get("run"):
        logging.info("--- 🚀 Starting Module: DIAGNOSTICS ---")
        module_config = load_config(module_info["config_path"])
        # The runner function does not modify the df, so no reassignment needed
        run_diag_pipeline(config=module_config, df=df, notebook=notebook_mode, run_id=run_id)
        logging.info("--- ✅ Finished Module: DIAGNOSTICS ---")

    # M02: Initial Validation
    module_info = modules_to_run.get("validation")
    if module_info and module_info.get("run"):
        logging.info("--- 🚀 Starting Module: VALIDATION ---")
        module_config = load_config(module_info["config_path"])
        df = run_validation_pipeline(
            config=module_config, df=df, notebook=notebook_mode, run_id=run_id
        )
        logging.info("--- ✅ Finished Module: VALIDATION ---")

    # M03: Normalization
    module_info = modules_to_run.get("normalization")
    if module_info and module_info.get("run"):
        logging.info("--- 🚀 Starting Module: NORMALIZATION ---")
        module_config = load_config(module_info["config_path"])
        df = run_normalization_pipeline(
            config=module_config, df=df, notebook=notebook_mode, run_id=run_id
        )
        logging.info("--- ✅ Finished Module: NORMALIZATION ---")

    # M04: Validation Gatekeeper
    module_info = modules_to_run.get("validation_gatekeeper")
    if module_info and module_info.get("run"):
        logging.info("--- 🚀 Starting Module: VALIDATION_GATEKEEPER ---")
        module_config = load_config(module_info["config_path"])
        df = run_validation_pipeline(
            config=module_config, df=df, notebook=notebook_mode, run_id=run_id
        )
        logging.info("--- ✅ Finished Module: VALIDATION_GATEKEEPER ---")

    # M05: Duplicates Handling
    module_info = modules_to_run.get("duplicates")
    if module_info and module_info.get("run"):
        logging.info("--- 🚀 Starting Module: DUPLICATES ---")
        module_config = load_config(module_info["config_path"])
        df = run_duplicates_pipeline(
            config=module_config, df=df, notebook=notebook_mode, run_id=run_id
        )
        logging.info("--- ✅ Finished Module: DUPLICATES ---")

    # M06: Outlier Detection
    module_info = modules_to_run.get("outlier_detection")
    if module_info and module_info.get("run"):
        logging.info("--- 🚀 Starting Module: OUTLIER_DETECTION ---")
        module_config = load_config(module_info["config_path"])
        df, detection_results = run_outlier_detection_pipeline(
            config=module_config, df=df, notebook=notebook_mode, run_id=run_id
        )
        logging.info("--- ✅ Finished Module: OUTLIER_DETECTION ---")

    # M07: Outlier Handling
    module_info = modules_to_run.get("outlier_handling")
    if module_info and module_info.get("run"):
        if not detection_results:
            raise RuntimeError(
                "M07 Outlier Handling cannot run because M06 Outlier Detection did not return results."
            )
        logging.info("--- 🚀 Starting Module: OUTLIER_HANDLING ---")
        module_config = load_config(module_info["config_path"])
        df = run_outlier_handling_pipeline(
            config=module_config,
            df=df,
            detection_results=detection_results,
            notebook=notebook_mode,
            run_id=run_id,
        )
        logging.info("--- ✅ Finished Module: OUTLIER_HANDLING ---")

    # M08: Imputation
    module_info = modules_to_run.get("imputation")
    if module_info and module_info.get("run"):
        logging.info("--- 🚀 Starting Module: IMPUTATION ---")
        module_config = load_config(module_info["config_path"])
        df = run_imputation_pipeline(
            config=module_config, df=df, notebook=notebook_mode, run_id=run_id
        )
        logging.info("--- ✅ Finished Module: IMPUTATION ---")

    # M10: Final Audit
    module_info = modules_to_run.get("final_audit")
    if module_info and module_info.get("run"):
        logging.info("--- 🚀 Starting Module: FINAL_AUDIT ---")
        module_config = load_config(module_info["config_path"])
        df = run_final_audit_pipeline(
            config=module_config, df=df, run_id=run_id, notebook=notebook_mode
        )
        logging.info("--- ✅ Finished Module: FINAL_AUDIT ---")

    logging.info("--- 🎉 Full Pipeline Execution Complete ---")
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the full analyst_toolkit data processing pipeline."
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/run_toolkit_config.yaml",
        help="Path to the master run_toolkit_config.yaml file.",
    )
    args = parser.parse_args()

    run_full_pipeline(config_path=args.config)
