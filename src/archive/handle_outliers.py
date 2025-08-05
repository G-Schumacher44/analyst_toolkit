"""
handle_outliers.py

Detects and handles outliers in a DataFrame using IQR or Z-score methods. Supports multiple strategies
for handling (e.g., replace, clip, drop) and exports cleaned data and logs. Designed for EDA and preprocessing workflows.

Usage Example (Notebook):
-------------------------
from handle_outliers import handle_outliers

df_cleaned, flagged_cols, summary_df = handle_outliers(
    df=df,
    method="iqr",                    # or "zscore"
    iqr_multiplier=1.5,
    zscore_threshold=3.0,
    strategy="clip",                # Options: mean, median, constant, clip, drop, nan, none
    fill_value=None,
    append_flags=True,
    exclude_columns=["employee_id", "name", "group"],
    export_path="data/exports/df_with_flags.xlsx",
    show_markdown=True,
    verbose_output=True
)

Configurable Options:
---------------------
- method: outlier detection technique ("iqr" or "zscore")
- iqr_multiplier: multiplier for IQR bounds
- zscore_threshold: threshold for z-score method
- strategy: how to handle outliers ("mean", "median", "clip", "drop", "nan", "constant", "none")
- fill_value: replacement if strategy == "constant"
- clip_bounds: optional column-wise bounds
- append_flags: whether to add *_is_outlier columns
- exclude_columns: columns to skip
- show_markdown / verbose_output / show_report_table: notebook display toggles
- export_*: toggles for exporting summary, flag log, boolean mask, and final dataset
- base_export_dir: base path for saving outputs (default = "data/exports")

Returns:
--------
- df_handled (DataFrame): processed dataframe
- flagged_cols (list): columns where outliers were detected
- summary_df (DataFrame): detection summary
"""
import pandas as pd
import numpy as np
import os

def detect_outliers_only(
    df,
    method="iqr",
    iqr_multiplier=1.5,
    zscore_threshold=3.0,
    exclude_columns=None
):
    if exclude_columns is None:
        exclude_columns = []

    method = method.lower()
    numeric_cols = df.select_dtypes(include=np.number).columns.difference(exclude_columns)
    columns_flagged = []
    summary_rows = []

    for col in numeric_cols:
        series = df[col]
        lower_bound, upper_bound = None, None

        if method == "iqr":
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - iqr_multiplier * iqr
            upper_bound = q3 + iqr_multiplier * iqr
            outliers = (series < lower_bound) | (series > upper_bound)
        elif method == "zscore":
            mean = series.mean()
            std = series.std()
            if std == 0:
                outliers = pd.Series(False, index=series.index)
            else:
                z_scores = (series - mean) / std
                outliers = z_scores.abs() > zscore_threshold
                lower_bound = mean - zscore_threshold * std
                upper_bound = mean + zscore_threshold * std
        else:
            raise ValueError(f"Unsupported outlier detection method: {method}")

        if outliers.any():
            outlier_count = outliers.sum()
            percent_flagged = round((outlier_count / len(df)) * 100, 2)
            columns_flagged.append(col)

            summary_rows.append({
                "column": col,
                "method": method,
                "outlier_count": outlier_count,
                "percent_flagged": percent_flagged,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound
            })

    outlier_log = pd.DataFrame(summary_rows)

    return {
        "outlier_log": outlier_log,
        "columns_flagged": columns_flagged
    }


# New function: handle_outliers
def handle_outliers(
    df,
    method="iqr",
    iqr_multiplier=1.5,
    zscore_threshold=3.0,
    strategy="none",
    fill_value=None,
    clip_bounds=None,
    append_flags=True,
    exclude_columns=None,
    export_path=None,
    summary_export_path=None,
    show_markdown=False,
    # show_report_table (bool): Display full summary_df as a table in notebook.
    # verbose_output (bool): Print per-column replacement stats to stdout.
    show_report_table=False,
    verbose_output=False,
    export_csv=False,
    export_handling_log=False,
    export_final_xlsx=True,
    export_summary=True,
    export_flag_summary=True,
    export_log_matrix=True,
    base_export_dir=None,
):
    if exclude_columns is None:
        exclude_columns = []

    # Determine export directory (defaults to 'data/exports' if not provided)
    if base_export_dir is None:
        base_export_dir = "data/exports"

    # Ensure default export directories exist
    os.makedirs(base_export_dir, exist_ok=True)

    # Step 1: Detect outliers and collect summary statistics
    detection_result = detect_outliers_only(
        df=df,
        method=method,
        iqr_multiplier=iqr_multiplier,
        zscore_threshold=zscore_threshold,
        exclude_columns=exclude_columns
    )
    summary_df = detection_result["outlier_log"]
    flagged_cols = detection_result["columns_flagged"]

    # Step 2: Construct boolean mask for each flagged column
    outlier_mask = pd.DataFrame(False, index=df.index, columns=flagged_cols)
    for row in summary_df.itertuples():
        col = row.column
        if method == "iqr":
            mask = (df[col] < row.lower_bound) | (df[col] > row.upper_bound)
        elif method == "zscore":
            mask = (df[col] < row.lower_bound) | (df[col] > row.upper_bound)
        else:
            continue
        outlier_mask[col] = mask

    # Step 3: Apply handling strategy to flagged columns
    df_handled = df.copy()
    before_shape = df.shape
    handling_summary_rows = []
    total_rows = len(df)
    for col in flagged_cols:
        count = outlier_mask[col].sum()
        pct = round(100 * count / total_rows, 2)
        lower_clipped = None
        upper_clipped = None
        replacement = None

        if strategy == "mean":
            replacement = df[col].mean()
            df_handled.loc[outlier_mask[col], col] = replacement
        elif strategy == "median":
            replacement = df[col].median()
            df_handled.loc[outlier_mask[col], col] = replacement
        elif strategy == "nan":
            replacement = np.nan
            df_handled.loc[outlier_mask[col], col] = replacement
        elif strategy == "constant":
            if fill_value is None:
                raise ValueError("fill_value must be provided when strategy='constant'")
            replacement = fill_value
            df_handled.loc[outlier_mask[col], col] = replacement
        elif strategy == "clip":
            lower = summary_df.loc[summary_df["column"] == col, "lower_bound"].values[0]
            upper = summary_df.loc[summary_df["column"] == col, "upper_bound"].values[0]
            count_lower = (df[col] < lower).sum()
            count_upper = (df[col] > upper).sum()
            count_clipped = count_lower + count_upper
            df_handled[col] = df[col].clip(lower, upper)
            lower_clipped = int(count_lower)
            upper_clipped = int(count_upper)
            replacement = None
        elif strategy == "drop":
            replacement = None
        elif strategy == "none" and append_flags:
            replacement = "flags only"
            strategy = "flag_only"
        elif strategy == "none":
            replacement = None
        else:
            raise ValueError(f"Unsupported strategy: {strategy}")

        handling_summary_rows.append({
            "column": col,
            "strategy": strategy if strategy != "clip" else f"clipped [{lower:.2f}–{upper:.2f}]",
            "outlier_count": count if strategy != "clip" else count_clipped,
            "replacement_value": replacement,
            "percent_affected": pct if strategy != "clip" else round(100 * count_clipped / total_rows, 2),
            "lower_clipped": lower_clipped,
            "upper_clipped": upper_clipped
        })

    # Add special handling summary for "drop" strategy
    if strategy == "drop":
        before_rows = len(df)
        combined_mask = outlier_mask.any(axis=1)
        df_handled = df_handled.loc[~combined_mask]
        after_rows = len(df_handled)
        dropped_rows = before_rows - after_rows
        handling_summary_rows.append({
            "column": "ALL",
            "strategy": "drop",
            "outlier_count": dropped_rows,
            "replacement_value": "rows removed",
            "percent_affected": round(100 * dropped_rows / before_rows, 2),
            "lower_clipped": None,
            "upper_clipped": None
        })

    # Finalize handling summary table
    handling_summary_df = pd.DataFrame(handling_summary_rows)

    # Step 7: Export outputs (cleaned data, summaries, logs)
    base_filename = f"{method}_{strategy}"
    if export_path is None:
        export_path = os.path.join(base_export_dir, f"df_cleaned_{base_filename}.xlsx")
    if summary_export_path is None:
        summary_export_path = os.path.join(base_export_dir, f"outlier_summary_{base_filename}.csv")
    flag_summary_path = os.path.join(base_export_dir, f"outlier_flag_summary_{base_filename}.csv")

    if export_final_xlsx:
        os.makedirs(os.path.dirname(export_path), exist_ok=True)
        df_handled.to_excel(export_path, index=False)
    if export_csv:
        csv_export_path = export_path.replace(".xlsx", ".csv")
        os.makedirs(os.path.dirname(csv_export_path), exist_ok=True)
        df_handled.to_csv(csv_export_path, index=False)
    if export_summary:
        os.makedirs(os.path.dirname(summary_export_path), exist_ok=True)
        summary_df.to_csv(summary_export_path, index=False)
    if export_flag_summary:
        os.makedirs(os.path.dirname(flag_summary_path), exist_ok=True)
        handling_summary_df.to_csv(flag_summary_path, index=False)
    # Export the boolean outlier mask
    if export_log_matrix:
        outlier_log_path = os.path.join(base_export_dir, f"outlier_log_{base_filename}.xlsx")
        os.makedirs(os.path.dirname(outlier_log_path), exist_ok=True)
        outlier_mask.to_excel(outlier_log_path)

    if export_handling_log:
        handling_log_path = os.path.join(base_export_dir, f"handling_log_{base_filename}.csv")
        os.makedirs(os.path.dirname(handling_log_path), exist_ok=True)
        handling_summary_df.to_csv(handling_log_path, index=False)

    # Step 8: Render markdown summary (optional)
    if show_markdown:
        from IPython.display import display, Markdown
        md = "### 🧼 Outlier Handling Summary\n<details>\n<summary>Click to expand</summary>\n\n"
        md += handling_summary_df.to_markdown(index=False)
        md += f"\n\n**Data Shape Before Handling:** {before_shape}\n"
        md += f"**Data Shape After Handling:** {df_handled.shape}\n"
        md += "\n\n</details>"
        display(Markdown(md))

    # Step 9: Print verbose output to console (optional)
    if verbose_output:
        print(f"🧪 Data shape before: {before_shape}, after: {df_handled.shape}")
        for row in handling_summary_df.itertuples(index=False):
            col = row.column
            print(f"🔍 Column: {col}")
            print(f"   → Outliers Detected: {row.outlier_count} ({row.percent_affected}%)")
            print(f"   → Strategy: {row.strategy}")
            if row.replacement_value is not None:
                print(f"   → Replaced with: {row.replacement_value}")
            if row.lower_clipped is not None or row.upper_clipped is not None:
                print(f"   → Lower clipped count: {row.lower_clipped}")
                print(f"   → Upper clipped count: {row.upper_clipped}")
            print("-" * 50)

    # Step 10: Display full summary_df in notebook (optional)
    if show_report_table:
        from IPython.display import display
        display(summary_df)

    return df_handled, flagged_cols, summary_df
