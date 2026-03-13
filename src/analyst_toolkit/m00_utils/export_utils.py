"""
📦 export_utils.py

Standardized export utilities for Analyst Toolkit pipeline modules.
"""

# mypy: ignore-errors

import logging
from pathlib import Path

import pandas as pd
from joblib import dump

_HTML_SIZE_WARNING_THRESHOLD_MB = 25


def _format_export_path(export_path: str, run_id: str | None) -> Path:
    if run_id and "{run_id}" in export_path:
        export_path = export_path.format(run_id=run_id)
    return Path(export_path)


def _resolve_export_file_path(export_path: Path, run_id: str | None) -> Path:
    if not run_id or export_path.name == run_id or export_path.name.startswith(f"{run_id}_"):
        return export_path
    return export_path.with_name(f"{run_id}_{export_path.name}")


def export_dataframes(
    data_dict: dict[str, pd.DataFrame],
    export_path: str,
    file_format: str = "excel",
    encoding: str = "utf-8",
    run_id: str = None,
    logging_mode: str = "on",
):
    """
    Export a dictionary of DataFrames. (Updated to accept 'xlsx' as a valid format).
    """
    export_path = _format_export_path(export_path, run_id)
    normalized_format = file_format.lower()

    # The export path's parent directory should always exist.
    export_path.parent.mkdir(parents=True, exist_ok=True)

    if normalized_format == "csv":
        # When format is CSV, export_path is treated as a base name for multiple files.
        # We'll use its parent directory and stem.
        resolved_export_path = _resolve_export_file_path(export_path, run_id)
        base_dir = resolved_export_path.parent
        base_stem = resolved_export_path.stem
        for name, df in data_dict.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Construct a unique filename for each dataframe
                filename = f"{base_stem}_{name}.csv"
                df.to_csv(base_dir / filename, index=False, encoding=encoding)
        if logging_mode != "off":
            logging.info(f"📊 Exported {len(data_dict)} CSV files to directory {base_dir}")

    # Accept both 'excel' and 'xlsx' as valid identifiers for an Excel file.
    elif normalized_format in ["excel", "xlsx"]:
        path_with_run_id = _resolve_export_file_path(export_path, run_id)
        # Set explicit Excel number formats so spreadsheet apps render dates consistently
        with pd.ExcelWriter(
            path_with_run_id,
            engine="xlsxwriter",
            date_format="yyyy-mm-dd",
            datetime_format="yyyy-mm-dd hh:mm:ss",
        ) as writer:
            for name, df in data_dict.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Flatten MultiIndex columns for Excel compatibility
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ["__".join(map(str, col)).strip() for col in df.columns.values]
                        if logging_mode != "off":
                            logging.info(
                                f"⚠️ Flattened MultiIndex columns in sheet '{name}' for Excel compatibility."
                            )
                    df.to_excel(writer, sheet_name=name[:31], index=False)
        if logging_mode != "off":
            logging.info(f"📊 Exported {len(data_dict)} sheets to {path_with_run_id}")
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def export_html_report(
    report_tables: dict,
    export_path: str,
    module_name: str,
    run_id: str,
    plot_paths: dict | None = None,
) -> str:
    """
    Generate a self-contained HTML report and write it to disk.

    Calls generate_html_report() from report_generator and writes the result.
    Returns the absolute path written — used by MCP tools as artifact_path.

    Args:
        report_tables: dict[str, pd.DataFrame] keyed by section name.
        export_path: Destination file path (e.g. "exports/reports/outliers/run.html").
        module_name: Display name for the module.
        run_id: Pipeline run identifier.
        plot_paths: Optional dict[str, str] of plot name → local file path.

    Returns:
        str: Absolute path of the written HTML file.
    """
    from analyst_toolkit.m00_utils.report_generator import generate_html_report

    path = Path(export_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    html = generate_html_report(
        report_tables=report_tables,
        module_name=module_name,
        run_id=run_id,
        plot_paths=plot_paths,
    )
    html_size_bytes = len(html.encode("utf-8"))
    if html_size_bytes > _HTML_SIZE_WARNING_THRESHOLD_MB * 1024 * 1024:
        logging.warning(
            "Serialized HTML artifact exceeds %s MB. Consider reducing plot count or resolution.",
            _HTML_SIZE_WARNING_THRESHOLD_MB,
        )

    path.write_text(html, encoding="utf-8")
    logging.info(f"📄 HTML report written to {path.resolve()}")
    return str(path.resolve())


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
    checks = {k: v for k, v in results.items() if isinstance(v, dict) and "passed" in v}
    for name, check in checks.items():
        rule_name = name.replace("_", " ").title()
        issue_count = len(check["details"])
        status = "Pass" if check["passed"] else f"Fail ({issue_count} issues)"
        summary_data.append(
            {
                "Validation Rule": rule_name,
                "Description": check["rule_description"],
                "Status": status,
            }
        )
    export_payload["validation_summary"] = pd.DataFrame(summary_data)

    # --- Extract DataFrames from failure details ---
    for name, check in checks.items():
        if not check["passed"]:
            details = check["details"]
            if name == "schema_conformity":
                df = pd.DataFrame(
                    [
                        {
                            "type": "Missing",
                            "columns": ", ".join(details.get("missing_columns", [])),
                        },
                        {
                            "type": "Unexpected",
                            "columns": ", ".join(details.get("unexpected_columns", [])),
                        },
                    ]
                )
                export_payload["schema_failures"] = df
            elif name == "dtype_enforcement":
                export_payload["dtype_failures"] = pd.DataFrame.from_dict(details, orient="index")
            elif name in ["categorical_values", "numeric_ranges"]:
                for col, violation_info in details.items():
                    # Create a separate sheet for each column's violations
                    export_payload[f"failures_{col}"] = violation_info["violating_rows"]

    export_dataframes(
        data_dict=export_payload,
        export_path=config.get(
            "export_path", "exports/reports/validation/{run_id}_validation_report.xlsx"
        ),
        file_format="csv" if config.get("as_csv", False) else "excel",
        run_id=run_id,
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
        export_path=config.get(
            "export_path", "exports/reports/diagnostics/{run_id}_profile_summary.xlsx"
        ),
        file_format="csv" if config.get("as_csv", False) else "excel",
        run_id=run_id,
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


def export_duplicates_report(report: dict, config: dict, run_id: str):
    """
    Exports the report generated by the duplicates module.
    This function expects the dictionary created by `generate_duplicates_report`.

    Args:
        report (dict): The report dictionary from `generate_duplicates_report`.
        config (dict): Configuration dict with export options.
        run_id (str): Unique identifier for this export run (required).
    """
    if not run_id:
        raise ValueError("A 'run_id' must be provided for export traceability.")

    # The report dictionary contains all the DataFrames needed for the report.
    # We filter for DataFrames to be safe.
    export_payload = {k: v for k, v in report.items() if isinstance(v, pd.DataFrame)}

    if not export_payload:
        logging.info("Duplicates report is empty. Skipping export.")
        return

    export_dataframes(
        data_dict=export_payload,
        export_path=config.get(
            "export_path", "exports/reports/duplicates/{run_id}_duplicates_report.xlsx"
        ),
        file_format="csv" if config.get("as_csv") else "excel",
        run_id=run_id,
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
        "null_value_audit": results.get("null_audit_summary"),
    }

    # Add before/after diff previews as individual sheets
    for col, diff_df in results.get("preview_diffs", {}).items():
        export_payload[f"preview_{col}"] = diff_df

    export_dataframes(
        data_dict=export_payload,
        export_path=config.get(
            "export_path", "exports/reports/normalization/{run_id}_normalization_report.xlsx"
        ),
        file_format="csv" if config.get("as_csv", False) else "excel",
        run_id=run_id,
    )
