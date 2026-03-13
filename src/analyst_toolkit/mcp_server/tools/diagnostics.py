"""MCP tool: toolkit_diagnostics — data profiling via M01."""

from pathlib import Path
from typing import Any

from analyst_toolkit.m01_diagnostics.run_diag_pipeline import run_diag_pipeline
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    build_artifact_contract,
    coerce_config,
    compact_destination_metadata,
    deliver_artifact,
    empty_delivery_state,
    fold_status_with_artifacts,
    generate_default_export_path,
    load_input,
    resolve_run_context,
    save_output,
    save_to_session,
    should_export_html,
    split_artifact_reference,
)
from analyst_toolkit.mcp_server.response_utils import (
    next_action,
    with_dashboard_artifact,
    with_next_actions,
)
from analyst_toolkit.mcp_server.runtime_overlay import (
    normalize_runtime_overlay,
    resolve_layered_config,
    runtime_to_config_overlay,
    runtime_to_tool_overrides,
)
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_diagnostics(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    runtime: dict | str | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """Run data profiling and structural diagnostics on the dataset."""
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

    config = coerce_config(config, "diagnostics")
    config, runtime_meta = resolve_layered_config(
        provided=config,
        explicit=runtime_to_config_overlay(runtime_cfg),
    )
    df = load_input(gcs_path, session_id=session_id)

    # If it came from a path, save it to a session
    if not session_id:
        session_id = save_to_session(df, run_id=run_id)
    else:
        save_to_session(df, session_id=session_id, run_id=run_id)

    # Resolve Plotting Settings (Opt-in by default)
    base_cfg = config.get("diagnostics", config) if isinstance(config, dict) else {}
    plotting_cfg = base_cfg.get("plotting", {})

    # Check if user explicitly asked for plotting in config OR tool arguments
    run_plots = plotting_cfg.get("run", False) or kwargs.get("plotting", False)

    # Build module config
    module_cfg = {
        "diagnostics": {
            **base_cfg,
            "logging": "off",
            "profile": {
                "run": True,
                "settings": {"export": True, "export_html": should_export_html(config)},
            },
            "plotting": {
                "run": run_plots,
                "include_distributions": run_plots,  # Only draw if requested
                "max_distribution_plots": kwargs.get("max_plots", 50),
            },
        }
    }

    run_diag_pipeline(config=module_cfg, df=df, notebook=False, run_id=run_id)

    # Handle export
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "diagnostics", session_id=session_id
    )
    export_url = save_output(df, export_path)
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
            "diagnostics/data",
            config=kwargs,
            session_id=session_id,
        )
        export_url = export_delivery["reference"]

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}
    artifact_delivery: dict[str, Any] = empty_delivery_state()
    xlsx_delivery: dict[str, Any] = empty_delivery_state()
    plot_delivery: dict[str, dict] = {}

    warnings: list = []
    warnings.extend(lifecycle["warnings"])
    warnings.extend(runtime_warnings)
    warnings.extend(runtime_meta["runtime_warnings"])
    warnings.extend(export_delivery["warnings"])

    if should_export_html(config):
        artifact_path = f"exports/reports/diagnostics/{run_id}_diagnostics_report.html"
        artifact_delivery = deliver_artifact(
            artifact_path,
            run_id,
            "diagnostics",
            config=kwargs,
            session_id=session_id,
        )
        artifact_path = artifact_delivery["local_path"]
        artifact_url = artifact_delivery["url"]
        warnings.extend(artifact_delivery["warnings"])

        xlsx_path = f"exports/reports/diagnostics/{run_id}_diagnostics_report.xlsx"
        xlsx_delivery = deliver_artifact(
            xlsx_path,
            run_id,
            "diagnostics",
            config=kwargs,
            session_id=session_id,
        )
        xlsx_url = xlsx_delivery["url"]
        warnings.extend(xlsx_delivery["warnings"])

        if run_plots:
            plot_dirs = [
                Path("exports/plots/diagnostics"),
                Path(f"exports/plots/diagnostics/{run_id}"),
            ]
            for plot_dir in plot_dirs:
                if plot_dir.exists():
                    for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                        delivered = deliver_artifact(
                            str(plot_file),
                            run_id,
                            "diagnostics/plots",
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
        expect_plots=run_plots and should_export_html(config),
        required_html=should_export_html(config),
        required_xlsx=False,
        probe_local_paths=True,
    )
    warnings.extend(artifact_contract["artifact_warnings"])

    base_status = "warn" if warnings else "pass"
    status = fold_status_with_artifacts(
        base_status, artifact_contract["missing_required_artifacts"]
    )

    res = {
        "status": status,
        "module": "diagnostics",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {
            "null_rate": round(float(df.isnull().mean().mean()), 4),
            "column_count": len(df.columns),
            "row_count": len(df),
        },
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
        label="Diagnostics dashboard",
    )
    res = with_next_actions(
        res,
        [
            next_action(
                "infer_configs",
                "Generate module-ready YAML configs from this profiled dataset.",
                {"session_id": session_id},
            ),
            next_action(
                "get_capability_catalog",
                "Review editable knobs before applying inferred configs.",
                {},
            ),
            next_action(
                "auto_heal",
                "Run one-shot normalization + imputation from inferred configs.",
                {"session_id": session_id, "run_id": run_id},
            ),
        ],
    )
    append_to_run_history(run_id, res, session_id=session_id)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="diagnostics",
    fn=_toolkit_diagnostics,
    description="Run data profiling on a dataset and return a standalone diagnostics dashboard artifact. Plotting is opt-in (pass plotting=true).",
    input_schema=base_input_schema(
        extra_props={
            "plotting": {
                "type": "boolean",
                "description": "Opt-in to generate visualization plots. Default: false.",
            },
            "max_plots": {
                "type": "integer",
                "description": "Max number of column distributions to plot. Default: 50.",
            },
        }
    ),
)
