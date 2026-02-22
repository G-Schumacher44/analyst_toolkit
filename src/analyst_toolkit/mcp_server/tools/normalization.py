"""MCP tool: toolkit_normalization — data cleaning and standardization via M03."""

from analyst_toolkit.m00_utils.export_utils import export_html_report, export_normalization_results
from analyst_toolkit.m00_utils.report_generator import generate_transformation_report
from analyst_toolkit.m03_normalization.normalize_data import apply_normalization
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    default_run_id,
    load_input,
    save_to_session,
    should_export_html,
    upload_artifact,
)
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_normalization(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
) -> dict:
    """Run normalization (rename, value mapping, dtype conversion) on the dataset at gcs_path or session_id."""
    run_id = run_id or default_run_id()
    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    module_cfg = {**config, "logging": "off"}
    # apply_normalization returns (df_normalized, change_log_df, changelog_dict)
    df_normalized, change_log_df, changelog = apply_normalization(df, config=module_cfg)

    # Save to session
    session_id = save_to_session(df_normalized, session_id=session_id)

    changes_made = int(len(change_log_df)) if hasattr(change_log_df, "__len__") else 0

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    if should_export_html(config):
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
        artifact_url = upload_artifact(artifact_path, run_id, "normalization")

        xlsx_cfg = {"export_path": "exports/reports/normalization/normalization_report.xlsx"}
        change_log_xlsx = (
            change_log_df.head(10_000) if hasattr(change_log_df, "head") else change_log_df
        )
        export_normalization_results(
            {"change_log_df": change_log_xlsx, "null_audit_summary": None, "preview_diffs": {}},
            xlsx_cfg,
            run_id=run_id,
        )
        xlsx_path = f"exports/reports/normalization/{run_id}_normalization_report.xlsx"
        xlsx_url = upload_artifact(xlsx_path, run_id, "normalization")

    res = {
        "status": "pass",
        "module": "normalization",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {"changes_made": changes_made},
        "changes_made": changes_made,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
    }
    append_to_run_history(run_id, res)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="toolkit_normalization",
    fn=_toolkit_normalization,
    description="Run data normalization (rename, value mapping, dtype conversion) on a dataset.",
    input_schema=base_input_schema(),
)
