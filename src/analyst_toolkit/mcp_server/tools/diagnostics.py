"""MCP tool: toolkit_diagnostics — data profiling via M01."""

from pathlib import Path

from analyst_toolkit.m01_diagnostics.run_diag_pipeline import run_diag_pipeline
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    check_upload,
    coerce_config,
    default_run_id,
    generate_default_export_path,
    get_session_run_id,
    load_input,
    save_output,
    save_to_session,
    should_export_html,
    upload_artifact,
)
from analyst_toolkit.mcp_server.response_utils import next_action, with_next_actions
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_diagnostics(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """Run data profiling and structural diagnostics on the dataset."""
    if not run_id and session_id:
        run_id = get_session_run_id(session_id)
    run_id = run_id or default_run_id()

    config = coerce_config(config, "diagnostics")
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

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}

    warnings: list = []

    if should_export_html(config):
        artifact_path = f"exports/reports/diagnostics/{run_id}_diagnostics_report.html"
        artifact_url = check_upload(
            upload_artifact(
                artifact_path, run_id, "diagnostics", config=kwargs, session_id=session_id
            ),
            artifact_path,
            warnings,
        )

        xlsx_path = f"exports/reports/diagnostics/{run_id}_diagnostics_report.xlsx"
        xlsx_url = check_upload(
            upload_artifact(xlsx_path, run_id, "diagnostics", config=kwargs, session_id=session_id),
            xlsx_path,
            warnings,
        )

        if run_plots:
            plot_dirs = [
                Path("exports/plots/diagnostics"),
                Path(f"exports/plots/diagnostics/{run_id}"),
            ]
            for plot_dir in plot_dirs:
                if plot_dir.exists():
                    for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                        url = upload_artifact(
                            str(plot_file),
                            run_id,
                            "diagnostics/plots",
                            config=kwargs,
                            session_id=session_id,
                        )
                        if url:
                            plot_urls[plot_file.name] = url

    res = {
        "status": "warn" if warnings else "pass",
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
        "warnings": warnings,
    }
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
    description="Run data profiling on a dataset. Plotting is opt-in (pass plotting=true).",
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
