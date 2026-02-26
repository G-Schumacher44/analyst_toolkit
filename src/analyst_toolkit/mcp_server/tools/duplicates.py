"""MCP tool: toolkit_duplicates — duplicate detection via M04."""

from pathlib import Path

from analyst_toolkit.m04_duplicates.run_dupes_pipeline import run_duplicates_pipeline
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


def _normalize_mode(mode: str) -> str:
    normalized = str(mode).strip().lower()
    if normalized == "drop":
        return "remove"
    return normalized


async def _toolkit_duplicates(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
    subset_columns: list[str] | None = None,
    **kwargs,
) -> dict:
    """Run duplicate detection on the dataset at gcs_path or session_id."""
    run_id, lifecycle = resolve_run_context(run_id, session_id)

    config = coerce_config(config, "duplicates")
    df = load_input(gcs_path, session_id=session_id)

    base_cfg = config.get("duplicates", config)
    subset_cols = subset_columns or base_cfg.get("subset_columns")
    mode = _normalize_mode(base_cfg.get("mode", "flag"))

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
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "duplicates", session_id=session_id
    )
    export_url = save_output(df_processed, export_path)

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}
    warnings: list = []
    warnings.extend(lifecycle["warnings"])

    if should_export_html(config):
        artifact_path = f"exports/reports/duplicates/{run_id}_duplicates_report.html"
        artifact_url = check_upload(
            upload_artifact(
                artifact_path, run_id, "duplicates", config=kwargs, session_id=session_id
            ),
            artifact_path,
            warnings,
        )

        xlsx_path = f"exports/reports/duplicates/{run_id}_duplicates_report.xlsx"
        xlsx_url = check_upload(
            upload_artifact(xlsx_path, run_id, "duplicates", config=kwargs, session_id=session_id),
            xlsx_path,
            warnings,
        )

        # Upload plots - search both root and run_id subdir
        plot_dirs = [Path("exports/plots/duplicates"), Path(f"exports/plots/duplicates/{run_id}")]
        for plot_dir in plot_dirs:
            if plot_dir.exists():
                for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                    url = upload_artifact(
                        str(plot_file),
                        run_id,
                        "duplicates/plots",
                        config=kwargs,
                        session_id=session_id,
                    )
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
        "warnings": warnings,
        "lifecycle": {k: v for k, v in lifecycle.items() if k != "warnings"},
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
