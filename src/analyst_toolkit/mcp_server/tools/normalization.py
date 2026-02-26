"""MCP tool: toolkit_normalization — data cleaning and standardization via M03."""

from analyst_toolkit.m03_normalization.normalize_data import apply_normalization
from analyst_toolkit.m03_normalization.run_normalization_pipeline import (
    run_normalization_pipeline,
)
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


async def _toolkit_normalization(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """Run normalization (rename, value mapping, dtype conversion) on the dataset at gcs_path or session_id."""
    run_id, lifecycle = resolve_run_context(run_id, session_id)

    config = coerce_config(config, "normalization")
    df = load_input(gcs_path, session_id=session_id)

    base_cfg = config.get("normalization", config)

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

    # Compute changes_made from changelog before running the full pipeline
    _rules_cfg = base_cfg if base_cfg else {}
    _, _, changelog = apply_normalization(df, _rules_cfg)

    def _count_changelog(cl: dict) -> int:
        total = 0
        for key, cdf in cl.items():
            if cdf is None or (hasattr(cdf, "empty") and cdf.empty):
                continue
            if key == "values_mapped" and "Mappings Applied" in cdf.columns:
                total += int(cdf["Mappings Applied"].sum())
            elif key == "types_coerced" and "Status" in cdf.columns:
                total += int((cdf["Status"].str.contains("Success", na=False)).sum())
            elif key == "datetimes_parsed" and "NaT Added" in cdf.columns:
                total += int(cdf["NaT Added"].sum())
            else:
                total += len(cdf)
        return total

    changes_made = _count_changelog(changelog)

    # run_normalization_pipeline handles transformation and reporting
    df_normalized = run_normalization_pipeline(
        config=module_cfg, df=df, notebook=False, run_id=run_id
    )

    # Save to session
    session_id = save_to_session(df_normalized, session_id=session_id, run_id=run_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count")

    # Handle explicit or default export
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "normalization", session_id=session_id
    )
    export_url = save_output(df_normalized, export_path)

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""

    warnings: list = []
    warnings.extend(lifecycle["warnings"])

    if should_export_html(config):
        artifact_path = f"exports/reports/normalization/{run_id}_normalization_report.html"
        artifact_url = check_upload(
            upload_artifact(
                artifact_path, run_id, "normalization", config=kwargs, session_id=session_id
            ),
            artifact_path,
            warnings,
        )

        xlsx_path = f"exports/reports/normalization/normalization_report_{run_id}.xlsx"
        xlsx_url = check_upload(
            upload_artifact(
                xlsx_path, run_id, "normalization", config=kwargs, session_id=session_id
            ),
            xlsx_path,
            warnings,
        )

    res = {
        "status": "warn" if warnings else "pass",
        "module": "normalization",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {"changes_made": changes_made, "row_count": row_count},
        "changes_made": changes_made,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "export_url": export_url,
        "warnings": warnings,
        "lifecycle": {k: v for k, v in lifecycle.items() if k != "warnings"},
    }
    append_to_run_history(run_id, res, session_id=session_id)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="normalization",
    fn=_toolkit_normalization,
    description="Run data normalization (rename, value mapping, dtype conversion) on a dataset.",
    input_schema=base_input_schema(),
)
