"""MCP tool: toolkit_diagnostics — data profiling via M01."""

from analyst_toolkit.m00_utils.export_utils import export_html_report, export_profile_summary
from analyst_toolkit.m01_diagnostics.data_diag import run_data_profile
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    default_run_id,
    load_input,
    save_to_session,
    should_export_html,
    upload_artifact,
)
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_diagnostics(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
) -> dict:
    """Run data profiling and structural diagnostics on the dataset at gcs_path or session_id."""
    run_id = run_id or default_run_id()
    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    # If it came from a path, save it to a session so downstream tools can use it
    if not session_id:
        session_id = save_to_session(df)

    module_cfg = {**config, "logging": "off"}
    full_profile = run_data_profile(df, config=module_cfg)
    profile_export = full_profile.get("for_export", {})

    shape = [int(df.shape[0]), int(df.shape[1])]
    null_rate = round(float(df.isnull().mean().mean()), 4)

    # Base status on configurable or default threshold
    null_threshold = config.get("null_threshold", 0.1)
    status = "pass" if null_rate < null_threshold else "warn"

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    if should_export_html(config):
        html_path = f"exports/reports/diagnostics/{run_id}_diagnostics_report.html"
        artifact_path = export_html_report(profile_export, html_path, "Diagnostics", run_id)
        artifact_url = upload_artifact(artifact_path, run_id, "diagnostics")

        xlsx_cfg = {"export_path": "exports/reports/diagnostics/diagnostics_summary.xlsx"}
        export_profile_summary(profile_export, xlsx_cfg, run_id=run_id)
        xlsx_path = f"exports/reports/diagnostics/{run_id}_diagnostics_summary.xlsx"
        xlsx_url = upload_artifact(xlsx_path, run_id, "diagnostics")

    res = {
        "status": status,
        "module": "diagnostics",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {"shape": shape, "null_rate": null_rate, "column_count": shape[1]},
        "profile_shape": shape,
        "null_rate": null_rate,
        "column_count": shape[1],
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
    }
    append_to_run_history(run_id, res)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="toolkit_diagnostics",
    fn=_toolkit_diagnostics,
    description="Run data profiling on a dataset. Returns shape, null rate, and column summary.",
    input_schema=base_input_schema(),
)
