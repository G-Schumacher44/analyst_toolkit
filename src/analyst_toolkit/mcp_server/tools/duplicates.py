"""MCP tool: toolkit_duplicates — duplicate detection via M04."""

from pathlib import Path
from typing import Any

from analyst_toolkit.m04_duplicates.run_dupes_pipeline import run_duplicates_pipeline
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    build_artifact_contract,
    coerce_config,
    compact_destination_metadata,
    deliver_artifact,
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


def _normalize_mode(mode: str) -> str:
    normalized = str(mode).strip().lower()
    if normalized == "drop":
        return "remove"
    return normalized


async def _toolkit_duplicates(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    runtime: dict | str | None = None,
    run_id: str | None = None,
    subset_columns: list[str] | None = None,
    **kwargs,
) -> dict:
    """Run duplicate detection on the dataset at gcs_path or session_id."""
    runtime_cfg, runtime_warnings = normalize_runtime_overlay(runtime)
    runtime_overrides = runtime_to_tool_overrides(runtime_cfg)
    runtime_applied = bool(runtime_cfg)
    gcs_path = gcs_path or runtime_overrides.get("gcs_path")
    session_id = session_id or runtime_overrides.get("session_id")
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

    config = coerce_config(config, "duplicates")
    config, runtime_meta = resolve_layered_config(
        provided=config,
        explicit=runtime_to_config_overlay(runtime_cfg),
    )
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
            "duplicates/data",
            config=kwargs,
            session_id=session_id,
        )
        export_url = export_delivery["reference"]

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}
    artifact_delivery: dict[str, Any] = {
        "local_path": "",
        "url": "",
        "warnings": [],
        "destinations": {},
    }
    xlsx_delivery: dict[str, Any] = {
        "local_path": "",
        "url": "",
        "warnings": [],
        "destinations": {},
    }
    plot_delivery: dict[str, dict] = {}
    warnings: list = []
    warnings.extend(lifecycle["warnings"])
    warnings.extend(runtime_warnings)
    warnings.extend(runtime_meta["runtime_warnings"])
    warnings.extend(export_delivery["warnings"])

    if should_export_html(config):
        artifact_path = f"exports/reports/duplicates/{run_id}_duplicates_report.html"
        artifact_delivery = deliver_artifact(
            artifact_path,
            run_id,
            "duplicates",
            config=kwargs,
            session_id=session_id,
        )
        artifact_path = artifact_delivery["local_path"]
        artifact_url = artifact_delivery["url"]
        warnings.extend(artifact_delivery["warnings"])

        xlsx_path = f"exports/reports/duplicates/{run_id}_duplicates_report.xlsx"
        xlsx_delivery = deliver_artifact(
            xlsx_path,
            run_id,
            "duplicates",
            config=kwargs,
            session_id=session_id,
        )
        xlsx_url = xlsx_delivery["url"]
        warnings.extend(xlsx_delivery["warnings"])

        # Upload plots - search both root and run_id subdir
        plot_dirs = [Path("exports/plots/duplicates"), Path(f"exports/plots/duplicates/{run_id}")]
        for plot_dir in plot_dirs:
            if plot_dir.exists():
                for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                    delivered = deliver_artifact(
                        str(plot_file),
                        run_id,
                        "duplicates/plots",
                        config=kwargs,
                        session_id=session_id,
                    )
                    plot_delivery[plot_file.name] = delivered
                    warnings.extend(delivered["warnings"])
                    if delivered["url"]:
                        plot_urls[plot_file.name] = delivered["url"]

    artifact_contract = build_artifact_contract(
        export_url,
        export_path=export_delivery["local_path"],
        artifact_path=artifact_path,
        artifact_url=artifact_url,
        xlsx_path=xlsx_delivery["local_path"],
        xlsx_url=xlsx_url,
        plot_paths={
            name: item["local_path"] for name, item in plot_delivery.items() if item["local_path"]
        },
        plot_urls=plot_urls,
        expect_html=should_export_html(config),
        expect_xlsx=should_export_html(config),
        expect_plots=should_export_html(config),
        required_html=should_export_html(config),
        probe_local_paths=True,
    )
    warnings.extend(artifact_contract["artifact_warnings"])
    base_status = "pass" if duplicate_count == 0 else "warn"
    if warnings and base_status == "pass":
        base_status = "warn"
    status = fold_status_with_artifacts(
        base_status, artifact_contract["missing_required_artifacts"]
    )

    res = {
        "status": status,
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
        "destination_delivery": {
            "data_export": compact_destination_metadata(export_delivery["destinations"]),
            "html_report": compact_destination_metadata(artifact_delivery["destinations"]),
            "xlsx_report": compact_destination_metadata(xlsx_delivery["destinations"]),
            "plots": {
                name: compact_destination_metadata(delivery["destinations"])
                for name, delivery in plot_delivery.items()
            },
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
        label="Duplicates dashboard",
    )
    append_to_run_history(run_id, res, session_id=session_id)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="duplicates",
    fn=_toolkit_duplicates,
    description="Detect duplicate rows in a dataset and return a standalone duplicates dashboard artifact.",
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
