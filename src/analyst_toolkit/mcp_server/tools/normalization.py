"""MCP tool: toolkit_normalization — data cleaning and standardization via M03."""

from analyst_toolkit.mcp_server.schemas import base_input_schema
from analyst_toolkit.mcp_server.io import load_input
from analyst_toolkit.m03_normalization.normalize_data import apply_normalization
from analyst_toolkit.m00_utils.report_generator import generate_transformation_report
from analyst_toolkit.m00_utils.export_utils import export_html_report


async def _toolkit_normalization(gcs_path: str, config: dict = {}, run_id: str = "mcp_run") -> dict:
    """Run normalization (rename, value mapping, type conversion) on the dataset at gcs_path."""
    df = load_input(gcs_path)

    module_cfg = {**config, "logging": "off"}
    # apply_normalization returns (df_normalized, change_log_df, changelog_dict)
    df_normalized, change_log_df, changelog = apply_normalization(df, config=module_cfg)

    changes_made = int(len(change_log_df)) if hasattr(change_log_df, "__len__") else 0
    changelog_rows = changes_made

    artifact_path = ""
    if config.get("export_html", False):
        report_tables = generate_transformation_report(
            df_original=df,
            df_transformed=df_normalized,
            changelog=changelog if isinstance(changelog, dict) else {},
            module_name="Normalization",
            run_id=run_id,
            export_config=config,
        )
        html_path = f"exports/reports/normalization/{run_id}_normalization_report.html"
        artifact_path = export_html_report(report_tables, html_path, "Normalization", run_id)

    return {
        "status": "pass",
        "module": "normalization",
        "run_id": run_id,
        "summary": {"changes_made": changes_made, "changelog_rows": changelog_rows},
        "changes_made": changes_made,
        "changelog_rows": changelog_rows,
        "artifact_path": artifact_path,
    }


from analyst_toolkit.mcp_server.server import register_tool  # noqa: E402

register_tool(
    name="toolkit_normalization",
    fn=_toolkit_normalization,
    description="Run data normalization (rename, value mapping, dtype conversion) on a dataset.",
    input_schema=base_input_schema(),
)
