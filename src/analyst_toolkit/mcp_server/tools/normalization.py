"""MCP tool: toolkit_normalization — data cleaning and standardization via M03."""

from typing import Any

from analyst_toolkit.m03_normalization.run_normalization_pipeline import (
    count_normalization_changes,
    run_normalization_pipeline,
)
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    build_artifact_contract,
    coerce_config,
    compact_destination_metadata,
    deliver_artifact,
    empty_delivery_state,
    fold_status_with_artifacts,
    generate_default_export_path,
    get_session_metadata,
    load_input,
    resolve_run_context,
    save_output,
    save_to_session,
    should_export_html,
    split_artifact_reference,
)
from analyst_toolkit.mcp_server.response_utils import with_dashboard_artifact
from analyst_toolkit.mcp_server.runtime_overlay import (
    normalize_runtime_overlay,
    resolve_layered_config,
    runtime_to_config_overlay,
    runtime_to_tool_overrides,
)
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_normalization(
    gcs_path: str | None = None,
    session_id: str | None = None,
    input_id: str | None = None,
    config: dict | None = None,
    runtime: dict | str | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """Run normalization (rename, value mapping, dtype conversion) on the dataset at gcs_path or session_id."""
    runtime_cfg, runtime_warnings = normalize_runtime_overlay(runtime)
    runtime_overrides = runtime_to_tool_overrides(runtime_cfg)
    runtime_applied = bool(runtime_cfg)
    gcs_path = gcs_path or runtime_overrides.get("gcs_path")
    session_id = session_id or runtime_overrides.get("session_id")
    input_id = input_id or runtime_overrides.get("input_id")
    run_id = run_id or runtime_overrides.get("run_id")
    for key in (
        "output_bucket",
        "output_prefix",
        "local_output_root",
        "drive_folder_id",
        "upload_artifacts",
    ):
        kwargs.setdefault(key, runtime_overrides.get(key))

    run_id, lifecycle = resolve_run_context(run_id, session_id)

    config = coerce_config(config, "normalization")
    config, runtime_meta = resolve_layered_config(
        provided=config,
        explicit=runtime_to_config_overlay(runtime_cfg),
    )
    df = load_input(gcs_path, session_id=session_id, input_id=input_id)

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

    # run_normalization_pipeline handles transformation and reporting
    pipeline_result = run_normalization_pipeline(
        config=module_cfg,
        df=df,
        notebook=False,
        run_id=run_id,
        return_metadata=True,
    )
    if isinstance(pipeline_result, tuple):
        df_normalized, metadata = pipeline_result
    else:
        df_normalized, metadata = pipeline_result, {}
    changelog = metadata.get("changelog", {})
    changes_made = metadata.get("changes_made", count_normalization_changes(changelog))

    # Save to session
    session_id = save_to_session(df_normalized, session_id=session_id, run_id=run_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count")

    # Handle explicit or default export
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "normalization", session_id=session_id
    )
    export_url = save_output(df_normalized, export_path)
    export_local_path, export_remote_url = split_artifact_reference(export_url)
    export_delivery: dict[str, Any] = {
        "reference": export_url,
        "local_path": export_local_path,
        "url": export_remote_url,
        "warnings": [],
        "destinations": {},
    }
    if export_delivery["local_path"]:
        export_delivery = deliver_artifact(
            export_delivery["local_path"],
            run_id,
            "normalization/data",
            config=kwargs,
            session_id=session_id,
        )
        export_url = export_delivery["reference"]

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    artifact_delivery: dict[str, Any] = empty_delivery_state()
    xlsx_delivery: dict[str, Any] = empty_delivery_state()

    status_warnings: list = []
    status_warnings.extend(lifecycle["warnings"])
    status_warnings.extend(runtime_warnings)
    status_warnings.extend(runtime_meta["runtime_warnings"])
    status_warnings.extend(export_delivery["warnings"])

    # Artifact delivery warnings are informational — collected separately so they
    # appear in the response but do not escalate base_status when the artifacts
    # are non-required.
    artifact_warnings: list = []

    if should_export_html(config):
        artifact_path = f"exports/reports/normalization/{run_id}_normalization_report.html"
        artifact_delivery = deliver_artifact(
            artifact_path,
            run_id,
            "normalization",
            config=kwargs,
            session_id=session_id,
        )
        artifact_path = artifact_delivery["local_path"]
        artifact_url = artifact_delivery["url"]
        artifact_warnings.extend(artifact_delivery["warnings"])

        xlsx_path = f"exports/reports/normalization/{run_id}_normalization_report.xlsx"
        xlsx_delivery = deliver_artifact(
            xlsx_path,
            run_id,
            "normalization",
            config=kwargs,
            session_id=session_id,
        )
        xlsx_url = xlsx_delivery["url"]
        artifact_warnings.extend(xlsx_delivery["warnings"])

    artifact_contract = build_artifact_contract(
        export_url,
        export_path=export_delivery["local_path"],
        artifact_path=artifact_path,
        artifact_url=artifact_url,
        xlsx_path=xlsx_delivery["local_path"],
        xlsx_url=xlsx_url,
        expect_html=should_export_html(config),
        expect_xlsx=should_export_html(config),
        required_html=False,
        probe_local_paths=True,
    )
    artifact_warnings.extend(artifact_contract["artifact_warnings"])
    warnings = status_warnings + artifact_warnings
    base_status = "warn" if status_warnings else "pass"
    status = fold_status_with_artifacts(
        base_status, artifact_contract["missing_required_artifacts"]
    )

    res = {
        "status": status,
        "module": "normalization",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {"changes_made": changes_made, "row_count": row_count},
        "changes_made": changes_made,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "export_url": export_url,
        "destination_delivery": {
            "data_export": compact_destination_metadata(export_delivery["destinations"]),
            "html_report": compact_destination_metadata(artifact_delivery["destinations"]),
            "xlsx_report": compact_destination_metadata(xlsx_delivery["destinations"]),
        },
        "warnings": warnings,
        "lifecycle": {k: v for k, v in lifecycle.items() if k != "warnings"},
        "runtime_applied": runtime_applied,
        "artifact_matrix": artifact_contract["artifact_matrix"],
        "expected_artifacts": artifact_contract["expected_artifacts"],
        "uploaded_artifacts": artifact_contract["uploaded_artifacts"],
        "missing_required_artifacts": artifact_contract["missing_required_artifacts"],
    }
    res = with_dashboard_artifact(
        res,
        artifact_path=artifact_path,
        artifact_url=artifact_url,
        label="Normalization dashboard",
    )
    append_to_run_history(run_id, res, session_id=session_id)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="normalization",
    fn=_toolkit_normalization,
    description="Run data normalization (rename, value mapping, dtype conversion) on a dataset and return a standalone normalization dashboard artifact.",
    input_schema=base_input_schema(),
)
