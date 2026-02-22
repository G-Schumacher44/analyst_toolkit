"""MCP tool: toolkit_duplicates — duplicate detection via M04."""

from analyst_toolkit.m00_utils.export_utils import export_html_report
from analyst_toolkit.m00_utils.report_generator import generate_duplicates_report
from analyst_toolkit.m04_duplicates.detect_dupes import detect_duplicates
from analyst_toolkit.mcp_server.io import load_input, should_export_html, upload_report
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_duplicates(
    gcs_path: str, config: dict | None = None, run_id: str = "mcp_run"
) -> dict:
    """Run duplicate detection on the dataset at gcs_path."""
    config = config or {}
    df = load_input(gcs_path)

    subset_cols = config.get("subset_columns")
    mode = config.get("mode", "flag")

    df_flagged, detection_results = detect_duplicates(df.copy(), subset_cols)
    duplicate_count = int(detection_results.get("duplicate_count", 0))

    artifact_path = ""
    artifact_url = ""
    if should_export_html(config):
        report_tables = generate_duplicates_report(
            df, df_flagged, detection_results, mode, df_flagged=df_flagged
        )
        html_path = f"exports/reports/duplicates/{run_id}_duplicates_report.html"
        artifact_path = export_html_report(report_tables, html_path, "Duplicates", run_id)
        artifact_url = upload_report(artifact_path, run_id, "duplicates")

    return {
        "status": "pass" if duplicate_count == 0 else "warn",
        "module": "duplicates",
        "run_id": run_id,
        "summary": {"duplicate_count": duplicate_count, "mode": mode},
        "duplicate_count": duplicate_count,
        "mode": mode,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
    }


from analyst_toolkit.mcp_server.server import register_tool  # noqa: E402

register_tool(
    name="toolkit_duplicates",
    fn=_toolkit_duplicates,
    description="Detect duplicate rows in a dataset. Returns count and optional clusters.",
    input_schema=base_input_schema(
        extra_props={
            "subset_columns": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Column subset to consider for duplicate detection. Defaults to all columns.",
            }
        }
    ),
)
