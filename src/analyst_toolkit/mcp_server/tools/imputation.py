"""MCP tool: toolkit_imputation — missing value imputation via M07."""

from pathlib import Path

from analyst_toolkit.m07_imputation.run_imputation_pipeline import run_imputation_pipeline
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

    # Build module config for the pipeline runner
    module_cfg = {
        "imputation": {
            **config,
            "logging": "off",
            "settings": {
                "export": {"run": True, "export_html": should_export_html(config)},
                "plotting": {"run": True},
            },
        }
    }

    # run_imputation_pipeline returns the imputed dataframe
    df_imputed = run_imputation_pipeline(config=module_cfg, df=df, notebook=False, run_id=run_id)

    # Save to session
    session_id = save_to_session(df_imputed, session_id=session_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count")

    # We need to compute these for the MCP response summary
    nulls_before = int(df.isnull().sum().sum())
    nulls_after = int(df_imputed.isnull().sum().sum())
    nulls_filled = nulls_before - nulls_after

    # Simple way to get columns imputed
    columns_imputed = [c for c in df.columns if df[c].isnull().sum() > df_imputed[c].isnull().sum()]

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}

    if should_export_html(config):
        artifact_path = f"exports/reports/imputation/{run_id}_imputation_report.html"
        artifact_url = upload_artifact(artifact_path, run_id, "imputation")

        xlsx_path = f"exports/reports/imputation/{run_id}_imputation_report.xlsx"
        xlsx_url = upload_artifact(xlsx_path, run_id, "imputation")

        # Upload plots - search both root and run_id subdir
        plot_dirs = [Path("exports/plots/imputation"), Path(f"exports/plots/imputation/{run_id}")]
        for plot_dir in plot_dirs:
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
            "row_count": row_count,
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
