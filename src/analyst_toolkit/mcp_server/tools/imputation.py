"""MCP tool: toolkit_imputation — missing value imputation via M07."""

from analyst_toolkit.m00_utils.export_utils import export_html_report
from analyst_toolkit.m00_utils.report_generator import generate_imputation_report
from analyst_toolkit.m07_imputation.impute_data import apply_imputation
from analyst_toolkit.mcp_server.io import load_input, should_export_html, upload_report
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_imputation(
    gcs_path: str, config: dict | None = None, run_id: str = "mcp_run"
) -> dict:
    """Run missing value imputation on the dataset at gcs_path."""
    config = config or {}
    df = load_input(gcs_path)

    if not config.get("rules"):
        return {
            "status": "warn",
            "module": "imputation",
            "run_id": run_id,
            "summary": {
                "message": "No imputation rules provided. Returning original DataFrame unchanged."
            },
            "columns_imputed": [],
            "nulls_filled": 0,
            "artifact_path": "",
            "artifact_url": "",
        }

    df_original = df.copy()
    module_cfg = {**config, "logging": "off"}
    df_imputed, detailed_changelog = apply_imputation(df, module_cfg)

    columns_imputed = (
        detailed_changelog["Column"].unique().tolist() if not detailed_changelog.empty else []
    )
    nulls_before = int(df_original[columns_imputed].isnull().sum().sum()) if columns_imputed else 0
    nulls_after = int(df_imputed[columns_imputed].isnull().sum().sum()) if columns_imputed else 0
    nulls_filled = nulls_before - nulls_after

    artifact_path = ""
    artifact_url = ""
    if should_export_html(config):
        report_tables = generate_imputation_report(df_original, df_imputed, detailed_changelog)
        html_path = f"exports/reports/imputation/{run_id}_imputation_report.html"
        artifact_path = export_html_report(report_tables, html_path, "Imputation", run_id)
        artifact_url = upload_report(artifact_path, run_id, "imputation")

    return {
        "status": "pass",
        "module": "imputation",
        "run_id": run_id,
        "summary": {"columns_imputed": columns_imputed, "nulls_filled": nulls_filled},
        "columns_imputed": columns_imputed,
        "nulls_filled": nulls_filled,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
    }


from analyst_toolkit.mcp_server.server import register_tool  # noqa: E402

register_tool(
    name="toolkit_imputation",
    fn=_toolkit_imputation,
    description="Run missing value imputation on a dataset using configured rules.",
    input_schema=base_input_schema(),
)
