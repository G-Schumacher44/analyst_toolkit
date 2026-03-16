"""MCP tool: toolkit_outliers — outlier detection via M05."""

from pathlib import Path
from typing import Any

from analyst_toolkit.m05_detect_outliers.run_detection_pipeline import (
    run_outlier_detection_pipeline,
)
from analyst_toolkit.mcp_server.config_normalizers import normalize_outliers_config
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


async def _toolkit_outliers(
    gcs_path: str | None = None,
    session_id: str | None = None,
    input_id: str | None = None,
    config: dict | None = None,
    runtime: dict | str | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """Run outlier detection on the dataset at gcs_path or session_id."""
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
        value = runtime_overrides.get(key)
        if value is not None:
            kwargs.setdefault(key, value)

    run_id, lifecycle = resolve_run_context(run_id, session_id)

    config = coerce_config(config, "outlier_detection")
    config, runtime_meta = resolve_layered_config(
        provided=config,
        explicit=runtime_to_config_overlay(runtime_cfg),
    )
    df = load_input(gcs_path, session_id=session_id, input_id=input_id)

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
            "outliers/data",
            config=kwargs,
            session_id=session_id,
        )
        export_url = export_delivery["reference"]

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
    artifact_delivery: dict[str, Any] = empty_delivery_state()
    xlsx_delivery: dict[str, Any] = empty_delivery_state()
    plot_delivery: dict[str, dict] = {}
    status_warnings: list = []
    status_warnings.extend(lifecycle["warnings"])
    status_warnings.extend(runtime_warnings)
    status_warnings.extend(runtime_meta["runtime_warnings"])
    status_warnings.extend(export_delivery["warnings"])

    artifact_warnings: list = []
    # Only expect report artifacts when outliers were actually detected
    html_requested = should_export_html(config)
    expect_reports = html_requested and outlier_count > 0

    if expect_reports:
        # Path where the pipeline runner saves its report
        artifact_path = f"exports/reports/outliers/detection/{run_id}_outlier_report.html"
        artifact_delivery = deliver_artifact(
            artifact_path,
            run_id,
            "outliers",
            config=kwargs,
            session_id=session_id,
        )
        artifact_path = artifact_delivery["local_path"]
        artifact_url = artifact_delivery["url"]
        artifact_warnings.extend(artifact_delivery["warnings"])

        xlsx_path = f"exports/reports/outliers/detection/{run_id}_outlier_report.xlsx"
        xlsx_delivery = deliver_artifact(
            xlsx_path,
            run_id,
            "outliers",
            config=kwargs,
            session_id=session_id,
        )
        xlsx_url = xlsx_delivery["url"]
        artifact_warnings.extend(xlsx_delivery["warnings"])

        # Upload plots - search both root and run_id subdir
        plot_dirs = [
            Path("exports/plots/outliers/detection"),
            Path(f"exports/plots/outliers/{run_id}"),
        ]
        for plot_dir in plot_dirs:
            if plot_dir.exists():
                for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                    delivered = deliver_artifact(
                        str(plot_file),
                        run_id,
                        "outliers/plots",
                        config=kwargs,
                        session_id=session_id,
                    )
                    plot_delivery[plot_file.name] = delivered
                    artifact_warnings.extend(delivered["warnings"])
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
        expect_html=expect_reports,
        expect_xlsx=expect_reports,
        expect_plots=expect_reports,
        required_html=False,
        probe_local_paths=True,
    )
    artifact_warnings.extend(artifact_contract["artifact_warnings"])
    warnings = status_warnings + artifact_warnings
    base_status = "pass" if outlier_count == 0 else "warn"
    if status_warnings and base_status == "pass":
        base_status = "warn"
    status = fold_status_with_artifacts(
        base_status, artifact_contract["missing_required_artifacts"]
    )

    res = {
        "status": status,
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
        label="Outlier detection dashboard",
    )
    append_to_run_history(run_id, res, session_id=session_id)
    return res


# Self-register
from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="outliers",
    fn=_toolkit_outliers,
    description="Run IQR/z-score outlier detection on a dataset and return a standalone outlier detection dashboard artifact.",
    input_schema=base_input_schema(),
)
