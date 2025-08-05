"""
📦 export_utils.py

Standardized export utilities for Analyst Toolkit pipeline modules.

Includes:
- Dictionary-to-Excel/CSV export (multi-sheet, MultiIndex-aware)
- Joblib-based checkpoint serialization
- Wrapper functions for exporting summaries and reports from diagnostics, validation, duplicates, etc.

All exports are configuration-driven and respect run-specific paths.

Part of the analyst_toolkit system.
"""
from pathlib import Path
import pandas as pd
from joblib import dump, load
import logging


def export_dataframes(data_dict: dict[str, pd.DataFrame], export_path: str, file_format: str = "excel", encoding: str = "utf-8", run_id: str = None, logging_mode: str = "on"):
    """
    Export a dictionary of DataFrames. (Updated to accept 'xlsx' as a valid format).
    """
    export_path = Path(export_path)
    normalized_format = file_format.lower()

    # Ensure the correct target directory exists based on the format.
    if normalized_format == "csv":
        export_path.mkdir(parents=True, exist_ok=True)
    elif normalized_format in ["excel", "xlsx"]:
        export_path.parent.mkdir(parents=True, exist_ok=True)

    if normalized_format == "csv":
        # The export_path for CSV is the directory.
        for name, df in data_dict.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                filename = f"{run_id}_{name}.csv" if run_id else f"{name}.csv"
                df.to_csv(export_path / filename, index=False, encoding=encoding)
        if logging_mode != "off":
            logging.info(f"📊 Exported {len(data_dict)} CSV files to {export_path}")

  
    # Accept both 'excel' and 'xlsx' as valid identifiers for an Excel file.
    elif normalized_format in ["excel", "xlsx"]:
    
        base_name = export_path.name
        path_with_run_id = export_path.with_name(f"{run_id}_{base_name}") if run_id else export_path
        with pd.ExcelWriter(path_with_run_id, engine="xlsxwriter") as writer:
            for name, df in data_dict.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Flatten MultiIndex columns for Excel compatibility
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['__'.join(map(str, col)).strip() for col in df.columns.values]
                        if logging_mode != "off":
                            logging.info(f"⚠️ Flattened MultiIndex columns in sheet '{name}' for Excel compatibility.")
                    df.to_excel(writer, sheet_name=name[:31], index=False)
        if logging_mode != "off":
            logging.info(f"📊 Exported {len(data_dict)} sheets to {path_with_run_id}")
    else:
        raise ValueError(f"Unsupported file format: {file_format}")



def export_validation_results(results: dict, config: dict, run_id: str = None):
    """
    Refactored wrapper to export structured validation results.
    Creates a summary sheet and individual sheets for failure details.
    """
    if not run_id:
        raise ValueError("A 'run_id' must be provided for export traceability.")

    export_payload = {}
    
    # --- Create the main summary DataFrame ---
    summary_data = []
    checks = {k: v for k, v in results.items() if isinstance(v, dict) and 'passed' in v}
    for name, check in checks.items():
        rule_name = name.replace('_', ' ').title()
        issue_count = len(check['details'])
        status = "Pass" if check['passed'] else f"Fail ({issue_count} issues)"
        summary_data.append({
            "Validation Rule": rule_name,
            "Description": check['rule_description'],
            "Status": status
        })
    export_payload['validation_summary'] = pd.DataFrame(summary_data)

    # --- Extract DataFrames from failure details ---
    for name, check in checks.items():
        if not check['passed']:
            details = check['details']
            if name == 'schema_conformity':
                df = pd.DataFrame([
                    {"type": "Missing", "columns": ', '.join(details.get('missing_columns', []))},
                    {"type": "Unexpected", "columns": ', '.join(details.get('unexpected_columns', []))}
                ])
                export_payload['schema_failures'] = df
            elif name == 'dtype_enforcement':
                export_payload['dtype_failures'] = pd.DataFrame.from_dict(details, orient='index')
            elif name in ['categorical_values', 'numeric_ranges']:
                for col, violation_info in details.items():
                    # Create a separate sheet for each column's violations
                    export_payload[f"failures_{col}"] = violation_info['violating_rows']

    export_dataframes(
        data_dict=export_payload,
        export_path=config.get("export_path", "exports/reports/validation/validation_report.xlsx"),
        file_format="csv" if config.get("as_csv", False) else "excel",
        run_id=run_id
    )

def export_profile_summary(profile: dict, config: dict, notebook: bool = True, run_id: str = None):
    """
    Wrapper to export profile summary data using the generic export utility.

    Args:
        profile (dict): Profile data containing DataFrames.
        config (dict): Configuration dict with export options.
        notebook (bool): Flag indicating if running in notebook context (unused).
        run_id (str): Unique identifier for this export run (required).
    """
    if not run_id:
        raise ValueError("A 'run_id' must be provided for export traceability.")
    export_payload = {}
    for k, v in profile.items():
        if isinstance(v, pd.DataFrame) and not v.empty:
            df_copy = v.copy()
            df_copy.insert(0, "run_id", run_id)
            export_payload[k] = df_copy
    export_dataframes(
        data_dict=export_payload,
        export_path=config.get("export_path", "exports/reports/diagnostics/profile_summary.xlsx"),
        file_format="csv" if config.get("as_csv", False) else "excel",
        run_id=run_id
    )


# --- Utility functions for joblib serialization ---
def save_joblib(obj, path: str):
    """
    Save a Python object to disk using joblib serialization.

    Args:
        obj: Python object to serialize.
        path (str): Destination file path (relative to project root).

    Raises:
        ValueError: If the path is not provided.
    """
    if not path:
        raise ValueError("An explicit 'path' is required to save a joblib checkpoint.")

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    dump(obj, path)
    logging.info(f"💾 Checkpoint saved to {path}")

def export_duplicates_summary(results: dict, config: dict, before_shape: tuple, after_shape: tuple, df_cleaned: pd.DataFrame, run_id: str = None):
    """
    Wrapper to export duplicates summary data using the generic export utility.

    Args:
        results (dict): Dictionary containing duplicates-related DataFrames.
        config (dict): Configuration dict with export options.
        before_shape (tuple): Shape of original DataFrame before cleaning.
        after_shape (tuple): Shape of DataFrame after cleaning.
        df_cleaned (pd.DataFrame): Cleaned DataFrame after duplicates removal.
        run_id (str): Unique identifier for this export run (required).
    """
    if not run_id:
        raise ValueError("A 'run_id' must be provided for export traceability.")
    # Prepare summary DataFrame and inject run_id
    summary_df = pd.DataFrame([
        {"metric": "original_shape", "value": str(before_shape)},
        {"metric": "cleaned_shape", "value": str(after_shape)},
        {"metric": "rows_removed", "value": before_shape[0] - after_shape[0]}
    ])
    if not summary_df.empty:
        summary_df = summary_df.copy()
        summary_df.insert(0, "run_id", run_id)
    # Prepare removed_rows DataFrame and inject run_id
    removed_rows = results.get("duplicated_rows")
    if isinstance(removed_rows, pd.DataFrame) and not removed_rows.empty:
        removed_rows = removed_rows.copy()
        removed_rows.insert(0, "run_id", run_id)
    # Prepare flagged_df DataFrame and inject run_id
    flagged_df = df_cleaned
    if isinstance(flagged_df, pd.DataFrame) and not flagged_df.empty:
        flagged_df = flagged_df.copy()
        flagged_df.insert(0, "run_id", run_id)
    export_payload = {
        "summary": summary_df,
        "removed_rows": removed_rows,
        "flagged_df": flagged_df
    }
    export_dataframes(
        data_dict=export_payload,
        export_path=config.get("export_path", "exports/reports/duplicates/duplicates_summary.xlsx"),
        file_format="csv" if config.get("as_csv", False) else "excel",
        run_id=run_id
    )

# ------- Normalization Reports ----------

def export_normalization_results(results: dict, config: dict, run_id: str = None):
    """
    Wrapper to export normalization results, including the detailed change log.
    """
    if not run_id:
        raise ValueError("A 'run_id' must be provided for export traceability.")
    
    # Prepare the main artifacts for export
    export_payload = {
        "change_log": results.get("change_log_df"),
        "null_value_audit": results.get("null_audit_summary")
    }

    # Add before/after diff previews as individual sheets
    for col, diff_df in results.get("preview_diffs", {}).items():
        export_payload[f"preview_{col}"] = diff_df
    
    export_dataframes(
        data_dict=export_payload,
        export_path=config.get("export_path", "exports/reports/normalization/normalization_report.xlsx"),
        file_format="csv" if config.get("as_csv", False) else "excel",
        run_id=run_id
    )
