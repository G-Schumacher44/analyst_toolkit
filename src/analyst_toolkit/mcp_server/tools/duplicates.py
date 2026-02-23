"""MCP tool: toolkit_duplicates — duplicate detection via M04."""

from pathlib import Path

from analyst_toolkit.m04_duplicates.run_dupes_pipeline import run_duplicates_pipeline
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    default_run_id,
    generate_default_export_path,
    get_session_metadata,
    get_session_run_id,
    load_input,
    save_output,
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
    **kwargs
) -> dict:
    """Run duplicate detection on the dataset at gcs_path or session_id."""
    if not run_id and session_id:
        run_id = get_session_run_id(session_id)
    run_id = run_id or default_run_id()

    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    subset_cols = subset_columns or config.get("subset_columns")
    mode = config.get("mode", "flag")

    # Robustly handle config nesting
    base_cfg = config.get("duplicates", config) if isinstance(config, dict) else {}

    # Build module config for the pipeline runner
    module_cfg = {
        "duplicates": {
            **base_cfg,
            "subset_columns": subset_cols,
            "mode": mode,
            "logging": "off",
            "settings": {
                "export": True,
                "export_html": should_export_html(config),
                "plotting": {"run": True},
            },
        }
    }

    # run_duplicates_pipeline returns the processed dataframe (flagged or removed)
    df_processed = run_duplicates_pipeline(config=module_cfg, df=df, notebook=False, run_id=run_id)

    # We still need the duplicate count for the MCP response.
    # Since we don't have the results dict from the runner here easily,
    # we'll look at the difference in row counts if removed, or the 'is_duplicate' column if flagged.
    if mode == "remove":
        duplicate_count = len(df) - len(df_processed)
    else:
        duplicate_count = (
            int(df_processed["is_duplicate"].sum()) if "is_duplicate" in df_processed.columns else 0
        )

    # Save to session
    session_id = save_to_session(df_processed, session_id=session_id, run_id=run_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count", len(df_processed))

    # Handle explicit or default export
    export_path = kwargs.get("export_path") or generate_default_export_path(run_id, "duplicates", session_id=session_id)
    export_url = save_output(df_processed, export_path)

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}

    if should_export_html(config):
        artifact_path = f"exports/reports/duplicates/{run_id}_duplicates_report.html"
        artifact_url = upload_artifact(artifact_path, run_id, "duplicates", config=kwargs, session_id=session_id)

        xlsx_path = f"exports/reports/duplicates/{run_id}_duplicates_report.xlsx"
        xlsx_url = upload_artifact(xlsx_path, run_id, "duplicates", config=kwargs, session_id=session_id)

        # Upload plots - search both root and run_id subdir
        plot_dirs = [Path("exports/plots/duplicates"), Path(f"exports/plots/duplicates/{run_id}")]
        for plot_dir in plot_dirs:
            if plot_dir.exists():
                for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                    url = upload_artifact(str(plot_file), run_id, "duplicates/plots", config=kwargs, session_id=session_id)
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
        "export_url": export_url,
    }
    append_to_run_history(run_id, res, session_id=session_id)
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
