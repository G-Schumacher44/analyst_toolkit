"""MCP tool: toolkit_imputation — missing value imputation via M07."""

from pathlib import Path
from analyst_toolkit.m00_utils.export_utils import export_dataframes, export_html_report
from analyst_toolkit.m00_utils.report_generator import generate_imputation_report
from analyst_toolkit.m07_imputation.impute_data import apply_imputation
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    default_run_id,
    get_session_metadata,
    load_input,
    save_to_session,
    should_export_html,
    upload_artifact,
)
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_imputation(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
) -> dict:
    """Run missing value imputation on the dataset at gcs_path or session_id."""
    run_id = run_id or default_run_id()
    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    # Accept config either as {"rules": ...} (flat) or {"imputation": {"rules": ...}} (nested)
    module_config = config.get("imputation", config)

    if not module_config.get("rules"):
        # Save to session even if no rules, to keep the pipeline alive
        if not session_id:
            session_id = save_to_session(df)
        metadata = get_session_metadata(session_id) or {}
        return {
            "status": "warn",
            "module": "imputation",
            "run_id": run_id,
            "session_id": session_id,
            "summary": {
                "message": "No imputation rules provided. Returning original DataFrame unchanged.",
                "row_count": metadata.get("row_count")
            },
            "columns_imputed": [],
            "nulls_filled": 0,
            "artifact_path": "",
            "artifact_url": "",
        }

    df_original = df.copy()
    module_cfg = {**module_config, "logging": "off"}
    df_imputed, detailed_changelog = apply_imputation(df, module_cfg)

    # Save to session
    session_id = save_to_session(df_imputed, session_id=session_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count")

    columns_imputed = (
        detailed_changelog["Column"].unique().tolist()
        if not detailed_changelog.empty and "Column" in detailed_changelog.columns
        else []
    )
    nulls_before = int(df_original[columns_imputed].isnull().sum().sum()) if columns_imputed else 0
    nulls_after = int(df_imputed[columns_imputed].isnull().sum().sum()) if columns_imputed else 0
    nulls_filled = nulls_before - nulls_after

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}
    if should_export_html(config):
        report_tables = generate_imputation_report(df_original, df_imputed, detailed_changelog)
        html_path = f"exports/reports/imputation/{run_id}_imputation_report.html"
        artifact_path = export_html_report(report_tables, html_path, "Imputation", run_id)
        artifact_url = upload_artifact(artifact_path, run_id, "imputation")

        export_dataframes(
            {"imputation_log": detailed_changelog},
            "exports/reports/imputation/imputation_report.xlsx",
            file_format="xlsx",
            run_id=run_id,
        )
        xlsx_path = f"exports/reports/imputation/{run_id}_imputation_report.xlsx"
        xlsx_url = upload_artifact(xlsx_path, run_id, "imputation")
        
        # Upload plots
        plot_dir = Path("exports/plots/imputation")
        if plot_dir.exists():
            for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                url = upload_artifact(str(plot_file), run_id, "imputation/plots")
                if url:
                    plot_urls[plot_file.name] = url

    res = {
        "status": "pass",
        "module": "imputation",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {
            "columns_imputed": columns_imputed, 
            "nulls_filled": nulls_filled,
            "row_count": row_count
        },
        "columns_imputed": columns_imputed,
        "nulls_filled": nulls_filled,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "plot_urls": plot_urls,
    }
    append_to_run_history(run_id, res)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="imputation",
    fn=_toolkit_imputation,
    description="Run missing value imputation on a dataset using configured rules.",
    input_schema=base_input_schema(),
)
