"""
plot_outliers.py

Generates diagnostic plots for outlier detection. Supports histograms, box plots, and violin plots
for columns flagged as having outliers. Designed to work with detection summary logs and 
support inline display or file export.

Usage:
    from plot_outliers import run_outlier_plots

Returns:
    Optionally returns plot file paths if return_paths=True.
"""
import os
import math
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import display, Markdown
import pandas as pd

def run_outlier_plots(
    df,
    outlier_log,
    columns,
    kind=["hist", "box", "violin"],
    hue=None,
    save_dir=None,
    show_kde=True,
    show_bounds=True,
    show_fliers=True,
    show_inline=True,
    show_summary=True,
    verbose_output=True,
    return_paths=False
):
    """
    Generate and optionally export plots for flagged outlier columns using a grid layout.
    """
    # If no columns are flagged, skip plotting
    if not columns:
        if verbose_output:
            print("No columns flagged for outlier plotting.")
        return None

    # Create save directory if specified
    if save_dir:
        os.makedirs(save_dir, exist_ok=True)

    plot_paths = {}
    completed_plots = set()
    # Filter columns to only include valid ones in the DataFrame
    valid_columns = [col for col in columns if col in df.columns]

    # Loop through each requested plot type (hist, box, violin)
    for plot_kind in kind:
        plot_kind = plot_kind.lower()
        if not valid_columns:
            if verbose_output:
                display(Markdown(f"⚠️ **{plot_kind.capitalize()} Plots:** Skipped, no valid columns provided."))
            continue
        try:
            ncols = min(3, len(valid_columns))
            nrows = math.ceil(len(valid_columns) / ncols)
            fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6 * ncols, 4 * nrows))
            axes = axes.flatten() if hasattr(axes, "flatten") else [axes]

            for i, col in enumerate(valid_columns):
                ax = axes[i]
                # Attempt to retrieve matching summary info for the column
                log_row = None
                if outlier_log is not None and "column" in outlier_log.columns:
                    matching_rows = outlier_log[outlier_log["column"] == col]
                    if not matching_rows.empty:
                        log_row = matching_rows.iloc[0]
                outlier_count = log_row['outlier_count'] if log_row is not None else 'N/A'

                is_discrete = pd.api.types.is_integer_dtype(df[col]) and df[col].nunique() < 25

                # Histogram with optional KDE and outlier bounds
                if plot_kind == "hist":
                    sns.histplot(data=df, x=col, hue=hue, kde=show_kde if not is_discrete else False,
                                 ax=ax, discrete=is_discrete, bins='auto' if is_discrete else None,
                                 fill=True, edgecolor="white")
                    ax.set_title(f"Histogram: {col} | Outliers: {outlier_count}")
                    if show_bounds and log_row is not None:
                        lower = log_row.get("lower_bound")
                        upper = log_row.get("upper_bound")
                        if lower is not None: ax.axvline(lower, color="r", linestyle="--")
                        if upper is not None: ax.axvline(upper, color="r", linestyle="--")

                # Boxplot with optional grouping and fliers toggle
                elif plot_kind == "box":
                    sns.boxplot(data=df, x=hue, y=col, showfliers=show_fliers, ax=ax)
                    if len(df) < 5000 and hue:
                        sns.swarmplot(data=df, x=hue, y=col, color=".25", size=2, alpha=0.6, ax=ax)
                    ax.set_title(f"Boxplot: {col}")

                # Violin plot, only if column has >5 unique values
                elif plot_kind == "violin":
                    if df[col].nunique() > 5:
                        if hue and df[hue].nunique() == 2:
                            sns.violinplot(data=df, x=hue, y=col, split=True, ax=ax)
                        else:
                            sns.violinplot(data=df, x=hue, y=col, ax=ax)
                        ax.set_title(f"Violin Plot: {col}")
                    else:
                        ax.set_visible(False)
                        continue

            # Hide unused subplots if fewer columns than grid cells
            for j in range(i + 1, len(axes)):
                axes[j].set_visible(False)

            fig.tight_layout(rect=[0, 0.03, 1, 0.97])
            plot_path = None
            # Save figure to file if save_dir is provided
            if save_dir:
                plot_path = os.path.join(save_dir, f"outlier_diagnostics_{plot_kind}.png")
                fig.savefig(plot_path)
                plot_paths[plot_kind] = plot_path

            if show_inline:
                plt.show()
            plt.close(fig)

            completed_plots.add(plot_kind)

            # Optionally print verbose message about completed plots
            if verbose_output:
                msg = f"**{plot_kind.capitalize()} plots completed for {len(valid_columns)} features.**"
                if plot_path: msg += f" Saved to `{os.path.basename(plot_path)}`."
                display(Markdown(msg))

        except Exception as e:
            if verbose_output:
                print(f"[Warning] Failed to generate {plot_kind} plots: {e}")


    if return_paths:
        return plot_paths