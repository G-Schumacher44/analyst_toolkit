"""MCP tool: toolkit_outliers — outlier detection via M05."""

from pathlib import Path

from analyst_toolkit.m05_detect_outliers.run_detection_pipeline import (
    run_outlier_detection_pipeline,
)
from analyst_toolkit.mcp_server.config_normalizers import normalize_outliers_config
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    check_upload,
    coerce_config,
    generate_default_export_path,
    get_session_metadata,
    load_input,
    resolve_run_context,
    save_output,
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
    **kwargs,
) -> dict:
    """Run outlier detection on the dataset at gcs_path or session_id."""
    run_id, lifecycle = resolve_run_context(run_id, session_id)

    config = coerce_config(config, "outlier_detection")
    df = load_input(gcs_path, session_id=session_id)

    base_cfg = normalize_outliers_config(config.get("outlier_detection", config))

    # Build a module config that ensures plotting and export are on
    module_cfg = {
        "outlier_detection": {
            **base_cfg,
            "logging": "off",
            "plotting": {"run": True},
            "export": {"run": True, "export_html": should_export_html(config)},
        }
    }

    df_out, detection_results = run_outlier_detection_pipeline(
        config=module_cfg, df=df, notebook=False, run_id=run_id
    )

    # Save to session
    session_id = save_to_session(df_out, session_id=session_id, run_id=run_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count", len(df_out))

    # Handle explicit or default export
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "outliers", session_id=session_id
    )
    export_url = save_output(df_out, export_path)

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
    warnings: list = []
    warnings.extend(lifecycle["warnings"])
    if should_export_html(config):
        # Path where the pipeline runner saves its report
        artifact_path = f"exports/reports/outliers/detection/{run_id}_outlier_report.html"
        artifact_url = check_upload(
            upload_artifact(
                artifact_path, run_id, "outliers", config=kwargs, session_id=session_id
            ),
            artifact_path,
            warnings,
        )

        xlsx_path = f"exports/reports/outliers/detection/{run_id}_outlier_report.xlsx"
        xlsx_url = check_upload(
            upload_artifact(xlsx_path, run_id, "outliers", config=kwargs, session_id=session_id),
            xlsx_path,
            warnings,
        )

        # Upload plots - search both root and run_id subdir
        plot_dirs = [
            Path("exports/plots/outliers/detection"),
            Path(f"exports/plots/outliers/{run_id}"),
        ]
        for plot_dir in plot_dirs:
            if plot_dir.exists():
                for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                    url = upload_artifact(
                        str(plot_file),
                        run_id,
                        "outliers/plots",
                        config=kwargs,
                        session_id=session_id,
                    )
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
        "export_url": export_url,
        "warnings": warnings,
        "lifecycle": {k: v for k, v in lifecycle.items() if k != "warnings"},
    }
    append_to_run_history(run_id, res, session_id=session_id)
    return res


# Self-register
from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="outliers",
    fn=_toolkit_outliers,
    description="Run IQR/z-score outlier detection on a dataset. Returns flagged columns and count.",
    input_schema=base_input_schema(),
)
