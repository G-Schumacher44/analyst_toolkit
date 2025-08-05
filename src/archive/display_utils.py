import pandas as pd
from IPython.display import HTML, Markdown, display

def display_markdown_summary(title: str, df: pd.DataFrame, max_rows: int = 10):
    """
    Display a DataFrame as a Markdown table in notebook environments.

    Args:
        title (str): Title to display above the table.
        df (pd.DataFrame): The DataFrame to display.
        max_rows (int): Max rows to display.
    """
    trimmed_df = df.head(max_rows)
    markdown = f"### {title}\n\n" + trimmed_df.to_markdown(index=False)
    display(Markdown(markdown))

def display_warnings(warning_list: list, title: str = "⚠️ Warnings"):
    """
    Display a list of warnings or messages in markdown block.

    Args:
        warning_list (list): List of warning strings.
        title (str): Section title.
    """
    if warning_list:
        markdown = f"### {title}\n\n"
        markdown += "\n".join(f"- {w}" for w in warning_list)
        display(Markdown(markdown))


def display_validation_summary_collapsible(results: dict, max_rows: int = 10, notebook: bool = True):
    if not notebook:
        return
    from IPython.display import display, HTML

    def to_html_table(df, title="", max_rows=10):
        if isinstance(df, pd.DataFrame) and not df.empty:
            if title in ["Sample Preview"]:
                return df.head(max_rows).to_html(index=False, border=1)
            else:
                return df.to_html(index=False, border=1)
        elif isinstance(df, list) and df:
            return "<ul>" + "".join(f"<li>{item}</li>" for item in df) + "</ul>"
        elif isinstance(df, dict) and df:
            return pd.DataFrame(df.items(), columns=["item", "count"]).to_html(index=False, border=1)
        return "<p><em>No issues found.</em></p>"

    def format_check_row(key, obj):
        key_display = key.replace('_', ' ').title()
        if key == "dtypes":
            count = "-"  # Informational only
            status = "✅"
        elif isinstance(obj, pd.DataFrame):
            count = len(obj)
            status = "✅" if obj.empty else "⚠️"
        elif isinstance(obj, list):
            count = len(obj)
            status = "✅" if not obj else "⚠️"
        elif isinstance(obj, dict):
            count = len(obj)
            status = "✅" if count == 0 else "⚠️"
        elif obj is None:
            count = 0
            status = "✅"
        else:
            count = "?"
            status = "⚠️"
        return f"<tr><td>{key_display}</td><td>{count}</td><td>{status}</td></tr>"


    required_keys = {
        "Schema": "schema",
        "Dtypes": "dtypes",
        "Nulls": "null_summary",
        "Cardinality": "high_cardinality",
        "Unexpected Columns": "unexpected_columns",
        "Missing Columns": "missing_columns",
        "Extra Columns": "extra_columns"
    }

    summary_map = {}
    for label, key in required_keys.items():
        val = results.get(key, None)
        if isinstance(val, (pd.DataFrame, list, dict)):
            summary_map[label] = val
        else:
            summary_map[label] = {}

    summary_rows = "".join(
        format_check_row(k.lower().replace(' ', '_'), summary_map[k]) for k in summary_map
    )

    html = f"""
    <details open>
    <summary><strong>📋 Validation Report</strong></summary>

    <details>
    <summary><strong>Summary & Cardinality Checks</strong></summary>
    <div style="display: flex; gap: 30px;">
        <div style="flex: 1;">
            <h3>Validation Summary Table</h3>
            <table border="1" style="border-collapse: collapse; margin-bottom: 1em;">
                <thead><tr><th>Check</th><th>Issues</th><th>Status</th></tr></thead>
                <tbody>{summary_rows}</tbody>
            </table>
        </div>
        <div style="flex: 1;">
            <h3>High Cardinality</h3>
            {to_html_table(summary_map.get("Cardinality", {}), title="High Cardinality")}
        </div>
    </div>
    </details>

    <details>
    <summary><strong> Dtypes & Missingness</strong></summary>
    <div style="display: flex; gap: 30px;">
        <div style="flex: 1;">
            <h3>Dtypes</h3>
            {to_html_table(summary_map.get("Dtypes", {}), title="Dtypes")}
        </div>
        <div style="flex: 1;">
            <h4>Null Summary</h4>
            {to_html_table(summary_map.get("Nulls", {}), title="Nulls")}
        </div>
    </div>
    </details>

    <details>
    <summary><strong> Sample Preview (First Rows)</strong></summary>
    <div style="margin-top: 10px;">
        {to_html_table(results.get("sample_preview", {}), title="Sample Preview")}
    </div>
    </details>

    </details>
    """
    display(HTML(html))
    
def display_profile_summary(profile: dict, max_rows: int = 10):
    """
    Render a structured HTML block summarizing the data profile output.

    Args:
        profile (dict): Output from generate_data_profile()
        max_rows (int): Max rows to show per table
    """
    def to_html_table(df, max_rows=None):
        if isinstance(df, pd.DataFrame) and not df.empty:
            drop_cols = [col for col in ["timestamp", "script_name", "user"] if col in df.columns]
            try:
                df_display = df.drop(columns=drop_cols)
            except Exception:
                df_display = df.copy()
            if max_rows is not None:
                df_display = df_display.head(max_rows)
            return df_display.to_html(index=False, border=1)
        elif isinstance(df, dict) and df:
            return pd.DataFrame(df.items(), columns=["item", "count"]).to_html(index=False, border=1)
        return "<p><em>No data available.</em></p>"

    mem_profile_df = profile.get("memory", pd.DataFrame())

    if not mem_profile_df.empty:
        mem_info = mem_profile_df.iloc[0]
        memory_kb = mem_info.get('memory_kb', 'N/A')
        shape_tuple = mem_info.get('shape', ('N/A', 'N/A'))
        rows, columns = shape_tuple if isinstance(shape_tuple, tuple) and len(shape_tuple) == 2 else ('N/A', 'N/A')
    else:
        memory_kb, rows, columns = 'N/A', 'N/A', 'N/A'

    mem_df = pd.DataFrame([["Memory (KB)", memory_kb]], columns=["Metric", "Value"])
    shape_df = pd.DataFrame([["Rows", rows], ["Columns", columns]], columns=["Metric", "Value"])

    duplicates_df = profile.get("duplicates", pd.DataFrame())
    duplicated_rows_df = profile.get("duplicated_rows", pd.DataFrame())
    missingness_df = profile.get("missingness", pd.DataFrame())

    html = f"""
    <details open>
    <summary><strong>📊 Data Profile Summary</strong></summary>

    <details>
    <summary><strong>Summary Metrics</strong></summary>
    <div style="display: flex; gap: 30px; justify-content: space-between; margin-bottom: 20px;">
        <div style="flex: 1;">
            <h4 style="margin-bottom: 8px;">Memory</h4>
            {to_html_table(mem_df, max_rows)}
            <h4 style="margin-bottom: 8px;">Shape</h4>
            {to_html_table(shape_df, max_rows)}
        </div>
        <div style="flex: 1;">
            <h4 style="margin-bottom: 8px;">Missingness by Column</h4>
            {to_html_table(missingness_df)}
        </div>
    </div>
    </details>

    <details>
    <summary><strong>Full Row Duplicate Check</strong></summary>
    <div style="margin-bottom: 20px;">
        <h4 style="margin-bottom: 8px;">Full Row Duplicates Summary</h4>
        {to_html_table(duplicates_df, max_rows)}
    </div>
    {to_html_table(duplicated_rows_df, max_rows) if not duplicated_rows_df.empty else "<p><em>No duplicated records available.</em></p>"}
    </details>

    <details>
    <summary><strong>First Rows Preview</strong></summary>
    <div style="display: flex; gap: 30px; justify-content: space-between;">
        <div style="flex: 1;">
            <h4 style="margin-bottom: 8px;">Sample Head</h4>
            {to_html_table(profile.get("sample_head", pd.DataFrame()))}
        </div>
    </div>
    </details>

    <details>
    <summary><strong>Descriptive Statistics Preview</strong></summary>
    <h4 style="margin-bottom: 8px;">Descriptive Statistics Preview</h4>
    {to_html_table(profile.get("describe", pd.DataFrame()), max_rows=max_rows)}
    </details>

    </details>
    """
    display(HTML(html))