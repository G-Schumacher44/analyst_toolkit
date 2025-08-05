"""
✅ Module: final_audit_pipeline.py

This is the main runner script for the M10 Final Audit & Certification module.

Responsibilities:
- Loads both the final cleaned DataFrame and original raw input
- Applies final edits (drop, rename, dtype enforcement)
- Executes a strict certification pass (schema, types, null audit)
- Builds a capstone report with summary, edits log, and lifecycle metrics
- Optionally renders the report in notebook and exports it to disk

Usage (notebook cell):
```python
from m10_final_audit.final_audit_pipeline import run_final_audit_pipeline

notebook_mode = config.get("notebook", True)
run_id = config.get("run_id")

df_certified = run_final_audit_pipeline(
    config=final_audit_config,
    df=df_imputed,              # Pass the most recently cleaned DataFrame
    notebook=notebook_mode,    # Toggle inline display
    run_id=run_id             # Used for paths and output tagging
)
```
"""

import logging
import pandas as pd
from joblib import load as load_joblib
from analyst_toolkit.m00_utils.load_data import load_csv
from analyst_toolkit.m00_utils.export_utils import save_joblib, export_dataframes
from analyst_toolkit.m01_diagnostics.data_diag import run_data_profile
from analyst_toolkit.m10_final_audit.final_audit_producer import run_final_audit_producer
from analyst_toolkit.m10_final_audit.display_final_audit import display_final_audit_summary

def _generate_final_report(producer_results: dict, df_raw: pd.DataFrame, df_final: pd.DataFrame) -> dict:
    """Builds the final, comprehensive report dictionary for export and display."""
    report = {}
    
    cert_results = producer_results.get("certification_results", {})
    all_cert_checks_passed = all(check.get('passed', False) for check in cert_results.values() if isinstance(check, dict) and 'passed' in check)
    null_audit_passed = producer_results.get("null_audit_results", {}).get("passed", False)
    final_status_passed = all_cert_checks_passed and null_audit_passed
    final_status = "✅ PIPELINE CERTIFIED" if final_status_passed else "❌ CERTIFICATION FAILED"
    
    report["Pipeline_Summary"] = pd.DataFrame([
        {"Metric": "Final Pipeline Status", "Value": final_status},
        {"Metric": "Certification Rules Passed", "Value": all_cert_checks_passed},
        {"Metric": "Null Value Audit Passed", "Value": null_audit_passed},
    ])
    
    report["Data_Lifecycle"] = pd.DataFrame({
        "Metric": ["Initial Rows", "Final Rows", "Initial Columns", "Final Columns"],
        "Value": [len(df_raw), len(df_final), len(df_raw.columns), len(df_final.columns)]
    })
    
    report["Final_Edits_Log"] = producer_results.get("final_edits_log")
    
    if not final_status_passed:
        failed_details = {}
        if not null_audit_passed:
            failed_details["Null_Check_Failures"] = producer_results.get("null_audit_results", {}).get("details")
        
        failed_cert_details = {
            f"FAILURES_{k}": v.get('details') 
            for k, v in cert_results.items() 
            if isinstance(v, dict) and 'passed' in v and not v.get('passed')
        }
        failed_details.update(failed_cert_details)
        report.update(failed_details)
        
    final_profile = run_data_profile(df_final, config={})
    report["Final_Data_Profile"] = final_profile["for_export"].get("schema")
    report["Final_Descriptive_Stats"] = final_profile["for_export"].get("describe")
    report["Final_Data_Preview"] = df_final.head(5)

    def is_empty(value):
        if value is None: return True
        if isinstance(value, pd.DataFrame): return value.empty
        if isinstance(value, dict): return not bool(value)
        return False

    return {k: v for k, v in report.items() if not is_empty(v)}

# --- CORRECTED FUNCTION SIGNATURE ---
def run_final_audit_pipeline(config: dict, df: pd.DataFrame = None, notebook: bool = True, run_id: str = None):
    """Main orchestrator for the M10 Final Audit and Certification module.

    Args:
        config (dict): Configuration dictionary for the final audit module.
        df (pd.DataFrame, optional): DataFrame to audit. If None, will load from joblib path.
        notebook (bool): Whether to show inline report outputs.
        run_id (str): Run identifier used for paths and tagging.

    Returns:
        pd.DataFrame: The cleaned and certified DataFrame.
    """
    # This module now correctly parses its nested config AND uses the passed-in run_id.
    if 'final_audit' in config:
        module_cfg = config.get("final_audit", {})
    else:
        module_cfg = config

    if not module_cfg.get("run"):
        logging.info("M10 Final Audit skipped by config.")
        return df

    # The passed-in run_id from the master runner is the source of truth.
    if not run_id:
        raise ValueError("A 'run_id' must be provided to the final audit module.")
        
    paths = module_cfg.get("settings", {}).get("paths", {})

    if df is None:
        df = load_joblib(module_cfg["input_df_path"].format(run_id=run_id))
    df_raw = load_csv(module_cfg["raw_data_path"])

    df_certified, producer_results = run_final_audit_producer(df, module_cfg)
    final_report = _generate_final_report(producer_results, df_raw, df_certified)
    
    if module_cfg.get("settings", {}).get("show_inline") and notebook:
        display_final_audit_summary(final_report)
        
    if module_cfg.get("settings", {}).get("export_report"):
        logging.info("Exporting final audit artifacts...")
        export_dataframes(final_report, paths["report_excel"].format(run_id=run_id))
        save_joblib(final_report, paths["report_joblib"].format(run_id=run_id))
        df_certified.to_csv(paths["checkpoint_csv"].format(run_id=run_id), index=False)
        save_joblib(df_certified, paths["checkpoint_joblib"].format(run_id=run_id))
        logging.info("✅ Final artifacts exported successfully.")
        
    return df_certified