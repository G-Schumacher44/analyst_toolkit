"""MCP tool: toolkit_duplicates — duplicate detection via M04."""

from pathlib import Path

from analyst_toolkit.m00_utils.export_utils import export_dataframes, export_html_report
from analyst_toolkit.m00_utils.report_generator import generate_duplicates_report
from analyst_toolkit.m04_duplicates.detect_dupes import detect_duplicates
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


async def _toolkit_duplicates(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
    subset_columns: list[str] | None = None,
) -> dict:
    """Run duplicate detection on the dataset at gcs_path or session_id."""
    run_id = run_id or default_run_id()
    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    subset_cols = subset_columns or config.get("subset_columns")
    mode = config.get("mode", "flag")

    df_flagged, detection_results = detect_duplicates(df.copy(), subset_cols)
    duplicate_count = int(detection_results.get("duplicate_count", 0))

    # Save to session
    session_id = save_to_session(df_flagged, session_id=session_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count", len(df_flagged))

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}
    if should_export_html(config):
        report_tables = generate_duplicates_report(
            df, df_flagged, detection_results, mode, df_flagged=df_flagged
        )
        html_path = f"exports/reports/duplicates/{run_id}_duplicates_report.html"
        artifact_path = export_html_report(report_tables, html_path, "Duplicates", run_id)
        artifact_url = upload_artifact(artifact_path, run_id, "duplicates")

        xlsx_tables = {
            k: (v.head(10_000) if hasattr(v, "head") else v)
            for k, v in report_tables.items()
            if hasattr(v, "to_excel") and k != "flagged_dataset"
        }
        if xlsx_tables:
            export_dataframes(
                xlsx_tables,
                "exports/reports/duplicates/duplicates_report.xlsx",
                file_format="xlsx",
                run_id=run_id,
            )
            xlsx_path = f"exports/reports/duplicates/{run_id}_duplicates_report.xlsx"
            xlsx_url = upload_artifact(xlsx_path, run_id, "duplicates")

        # Upload plots
        plot_dir = Path("exports/plots/duplicates")
        if plot_dir.exists():
            for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                url = upload_artifact(str(plot_file), run_id, "duplicates/plots")
                if url:
                    plot_urls[plot_file.name] = url

    res = {
        "status": "pass" if duplicate_count == 0 else "warn",
        "module": "duplicates",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {
            "duplicate_count": duplicate_count,
            "mode": mode,
            "row_count": row_count,
        },
        "duplicate_count": duplicate_count,
        "mode": mode,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "plot_urls": plot_urls,
    }
    append_to_run_history(run_id, res)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="duplicates",
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
