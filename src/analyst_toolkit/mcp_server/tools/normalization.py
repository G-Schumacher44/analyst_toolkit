"""MCP tool: toolkit_normalization — data cleaning and standardization via M03."""

from analyst_toolkit.m03_normalization.run_normalization_pipeline import (
    run_normalization_pipeline,
)
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


async def _toolkit_normalization(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
    **kwargs
) -> dict:
    """Run normalization (rename, value mapping, dtype conversion) on the dataset at gcs_path or session_id."""
    if not run_id and session_id:
        run_id = get_session_run_id(session_id)
    run_id = run_id or default_run_id()

    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    # Robustly handle config nesting
    base_cfg = config.get("normalization", config) if isinstance(config, dict) else {}

    # Build module config for the pipeline runner
    module_cfg = {
        "normalization": {
            **base_cfg,
            "logging": "off",
            "settings": {
                "export": True,
                "export_html": should_export_html(config),
            },
        }
    }

    # run_normalization_pipeline handles transformation and reporting
    df_normalized = run_normalization_pipeline(
        config=module_cfg, df=df, notebook=False, run_id=run_id
    )

    # Save to session
    session_id = save_to_session(df_normalized, session_id=session_id, run_id=run_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count")

    # Handle explicit or default export
    export_path = kwargs.get("export_path") or generate_default_export_path(run_id, "normalization")
    export_url = save_output(df_normalized, export_path)

    changes_made = 0  # Placeholder

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""

    if should_export_html(config):
        artifact_path = f"exports/reports/normalization/{run_id}_normalization_report.html"
        artifact_url = upload_artifact(artifact_path, run_id, "normalization", config=kwargs)

        xlsx_path = f"exports/reports/normalization/normalization_report_{run_id}.xlsx"
        xlsx_url = upload_artifact(xlsx_path, run_id, "normalization", config=kwargs)

    res = {
        "status": "pass",
        "module": "normalization",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {"changes_made": changes_made, "row_count": row_count},
        "changes_made": changes_made,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "export_url": export_url,
    }
    append_to_run_history(run_id, res)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="normalization",
    fn=_toolkit_normalization,
    description="Run data normalization (rename, value mapping, dtype conversion) on a dataset.",
    input_schema=base_input_schema(),
)
