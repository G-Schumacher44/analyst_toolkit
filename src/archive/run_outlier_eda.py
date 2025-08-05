import pandas as pd
import os


from data_cleaning.plot_outliers import run_outlier_plots
from data_cleaning.handle_outliers import detect_outliers_only

def run_outlier_eda_pipe(
    df,
    method="iqr",
    iqr_multiplier=1.5,
    zscore_threshold=3.0,
    exclude_columns=None,
    plot_kinds=["box", "hist"],
    hue=None,
    save_dir=None,
    inline_display=True,
    export_log_path=None,
    export_summary_path=None,
    show_plots_inline: bool = False,
    export_relative_paths: bool = False,
    show_fliers: bool = True,
    show_summary_markdown: bool = True,
    verbose_output: bool = True
):
    """
    Outlier detection and visualization pipeline.

    Parameters:
        df (pd.DataFrame): Raw input DataFrame.
        method (str): Outlier detection method ("iqr" or "zscore").
        iqr_multiplier (float): IQR bound multiplier.
        zscore_threshold (float): Z-score cutoff.
        exclude_columns (list[str] or None): Columns to exclude from detection.
        plot_kinds (list[str]): Plot types to generate.
        hue (str or None): Optional hue column for plots.
        save_dir (str or None): Where to save plots.
        inline_display (bool): Whether to print outlier summary inline.
        export_log_path (str or None): Path to export outlier log CSV.
        export_summary_path (str or None): Path to export outlier summary CSV.
        show_plots_inline (bool): Whether to display plots inline.
        export_relative_paths (bool): Whether to export relative plot paths in summary.
        show_fliers (bool): Whether to show fliers in plots.
        show_summary_markdown (bool): If True, display styled outlier summary as Markdown (not just print).
        verbose_output (bool): If True, print status messages for progress and exports.

    Returns:
        tuple:
            pd.DataFrame: Outlier log DataFrame.
            list[str]: List of columns flagged for outliers.
            pd.DataFrame: Summary DataFrame of outlier counts and percentages.
    """
    # Auto-detect columns to exclude from outlier detection
    if exclude_columns is None:
        exclude_columns = [
            col for col in df.columns
            if not pd.api.types.is_numeric_dtype(df[col])
            or col.lower() in ["churned", "target"]
            or "id" in col.lower()
            or col == hue
        ]

    # === Detect outliers
    outlier_result = detect_outliers_only(
        df,
        method=method,
        iqr_multiplier=iqr_multiplier,
        zscore_threshold=zscore_threshold,
        exclude_columns=exclude_columns
    )
    outlier_matrix = outlier_result["outlier_log"]
    columns_flagged = outlier_result["columns_flagged"]

    # === Use the outlier log as summary
    outlier_summary_df = outlier_matrix.copy()

    if inline_display and verbose_output:
        print("🔍 Outlier Detection Summary")
        print(outlier_summary_df)

    if export_log_path:
        outlier_matrix.to_csv(export_log_path, index=False)

    if export_summary_path:
        outlier_summary_df.to_csv(export_summary_path, index=False)

    plot_paths = run_outlier_plots(
        df=df,
        outlier_log=outlier_summary_df,
        columns=columns_flagged,
        kind=plot_kinds,
        hue=hue,
        save_dir=save_dir,
        show_inline=show_plots_inline,
        return_paths=export_relative_paths,
        show_fliers=show_fliers,
        verbose_output=verbose_output
    )

    if show_summary_markdown and not outlier_summary_df.empty:
        from IPython.display import display, Markdown
        summary_md = "### 📈 Outlier Diagnostics Summary\n<details>\n<summary>Click to expand</summary>\n\n"

        for kind in plot_kinds:
            status = "✅" if plot_paths and any(kind in str(v) for v in plot_paths.values()) else "❌"
            summary_md += f"- {status} **{kind.capitalize()} Plots**\n"

        summary_md += "\n\n#### 🔍 Outlier Detection Summary\n\n"
        summary_md += outlier_summary_df.to_markdown(index=False)
        summary_md += "\n\n</details>"

        display(Markdown(summary_md))

    if export_relative_paths and plot_paths:
        # Add joined relative paths as a new column in summary df
        paths_joined = []
        for col in columns_flagged:
            col_paths = plot_paths.get(col, [])
            if isinstance(col_paths, list):
                joined = ", ".join(col_paths)
            else:
                joined = str(col_paths)
            paths_joined.append(joined)
        outlier_summary_df["plot_paths"] = paths_joined

    return outlier_matrix, columns_flagged, outlier_summary_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the outlier detection pipeline.")
    parser.add_argument("--infile", type=str, required=True, help="Path to input CSV.")
    parser.add_argument("--outfile", type=str, required=True, help="Path to save cleaned CSV.")
    parser.add_argument("--save_dir", type=str, default=None, help="Optional directory to save plots.")
    parser.add_argument("--method", type=str, default="iqr", choices=["iqr", "zscore"])
    parser.add_argument("--iqr_multiplier", type=float, default=1.5)
    parser.add_argument("--zscore_threshold", type=float, default=3.0)
    parser.add_argument("--fill_value", type=float, default=None)
    parser.add_argument("--hue", type=str, default=None)
    parser.add_argument("--apply_handling", type=bool, default=True, help="Whether to apply outlier handling")
    # Note: clip_bounds argument is not exposed via CLI here; could be added if needed.

    args = parser.parse_args()

    df = pd.read_csv(args.infile)
    outlier_matrix, columns_flagged, outlier_summary_df = run_pipeline(
        df,
        method=args.method,
        iqr_multiplier=args.iqr_multiplier,
        zscore_threshold=args.zscore_threshold,
        fill_value=args.fill_value,
        hue=args.hue,
        save_dir=args.save_dir
    )
    df.to_csv(args.outfile, index=False)
