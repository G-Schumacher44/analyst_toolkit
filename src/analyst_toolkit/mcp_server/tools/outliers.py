"""MCP tool: toolkit_outliers — outlier detection via M05."""

from analyst_toolkit.m00_utils.export_utils import export_html_report
from analyst_toolkit.m00_utils.report_generator import generate_outlier_report
from analyst_toolkit.m05_detect_outliers.run_detection_pipeline import (
    run_outlier_detection_pipeline,
)
from analyst_toolkit.mcp_server.io import load_input, should_export_html, upload_report
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_outliers(
    gcs_path: str, config: dict | None = None, run_id: str = "mcp_run"
) -> dict:
    """Run outlier detection on the dataset at gcs_path."""
    config = config or {}
    df = load_input(gcs_path)

    # Build a minimal module config that won't trigger file-based IO
    module_cfg = {"outlier_detection": {**config, "logging": "off"}}

    df_out, detection_results = run_outlier_detection_pipeline(
        config=module_cfg, df=df, notebook=False, run_id=run_id
    )

    outlier_log = detection_results.get("outlier_log")
    outlier_count = (
        int(len(outlier_log)) if outlier_log is not None and not outlier_log.empty else 0
    )
    flagged_columns = (
        outlier_log["column"].unique().tolist()
        if outlier_log is not None and "column" in outlier_log.columns
        else []
    )

    artifact_path = ""
    artifact_url = ""
    if should_export_html(config):
        report_tables = generate_outlier_report(detection_results)
        html_path = f"exports/reports/outliers/detection/{run_id}_outlier_report.html"
        artifact_path = export_html_report(report_tables, html_path, "Outlier Detection", run_id)
        artifact_url = upload_report(artifact_path, run_id, "outliers")

    return {
        "status": "pass" if outlier_count == 0 else "warn",
        "module": "outliers",
        "run_id": run_id,
        "summary": {"outlier_count": outlier_count, "flagged_columns": flagged_columns},
        "flagged_columns": flagged_columns,
        "outlier_count": outlier_count,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
    }


# Self-register
from analyst_toolkit.mcp_server.server import register_tool  # noqa: E402

register_tool(
    name="toolkit_outliers",
    fn=_toolkit_outliers,
    description="Run IQR/z-score outlier detection on a dataset. Returns flagged columns and count.",
    input_schema=base_input_schema(),
)
