"""MCP tool: toolkit_outliers — outlier detection via M05."""

from pathlib import Path

from analyst_toolkit.m00_utils.export_utils import export_dataframes, export_html_report
from analyst_toolkit.m00_utils.report_generator import generate_outlier_report
from analyst_toolkit.m05_detect_outliers.run_detection_pipeline import (
    run_outlier_detection_pipeline,
)
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


async def _toolkit_outliers(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
) -> dict:
    """Run outlier detection on the dataset at gcs_path or session_id."""
    run_id = run_id or default_run_id()
    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    # Build a minimal module config that won't trigger file-based IO
    module_cfg = {"outlier_detection": {**config, "logging": "off"}}

    df_out, detection_results = run_outlier_detection_pipeline(
        config=module_cfg, df=df, notebook=False, run_id=run_id
    )

    # Save to session
    session_id = save_to_session(df_out, session_id=session_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count", len(df_out))

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
    xlsx_url = ""
    plot_urls = {}
    if should_export_html(config):
        report_tables = generate_outlier_report(detection_results)
        html_path = f"exports/reports/outliers/detection/{run_id}_outlier_report.html"
        artifact_path = export_html_report(report_tables, html_path, "Outlier Detection", run_id)
        artifact_url = upload_artifact(artifact_path, run_id, "outliers")

        if outlier_log is not None and not outlier_log.empty:
            export_dataframes(
                {"outlier_log": outlier_log},
                "exports/reports/outliers/detection/outlier_report.xlsx",
                file_format="xlsx",
                run_id=run_id,
            )
            xlsx_path = f"exports/reports/outliers/detection/{run_id}_outlier_report.xlsx"
            xlsx_url = upload_artifact(xlsx_path, run_id, "outliers")

        # Upload plots - search both root and run_id subdir
        plot_dirs = [
            Path("exports/plots/outliers/detection"),
            Path(f"exports/plots/outliers/{run_id}")
        ]
        for plot_dir in plot_dirs:
            if plot_dir.exists():
                for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                    url = upload_artifact(str(plot_file), run_id, "outliers/plots")
                    if url:
                        plot_urls[plot_file.name] = url

    res = {
        "status": "pass" if outlier_count == 0 else "warn",
        "module": "outliers",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {
            "outlier_count": outlier_count,
            "flagged_columns": flagged_columns,
            "row_count": row_count,
        },
        "flagged_columns": flagged_columns,
        "outlier_count": outlier_count,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "plot_urls": plot_urls,
    }
    append_to_run_history(run_id, res)
    return res


# Self-register
from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="outliers",
    fn=_toolkit_outliers,
    description="Run IQR/z-score outlier detection on a dataset. Returns flagged columns and count.",
    input_schema=base_input_schema(),
)
