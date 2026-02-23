"""MCP tool: toolkit_diagnostics — data profiling via M01."""

from pathlib import Path

from analyst_toolkit.m01_diagnostics.run_diag_pipeline import run_diag_pipeline
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    default_run_id,
    generate_default_export_path,
    get_session_run_id,
    load_input,
    save_output,
    save_to_session,
    should_export_html,
    upload_artifact,
)
from analyst_toolkit.mcp_server.schemas import base_input_schema


async def _toolkit_diagnostics(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """Run data profiling and structural diagnostics on the dataset at gcs_path or session_id."""
    # Resolve run_id: 1. Explicitly provided, 2. Existing session run_id, 3. Default timestamp
    if not run_id and session_id:
        run_id = get_session_run_id(session_id)
    run_id = run_id or default_run_id()

    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    # If it came from a path, save it to a session so downstream tools can use it
    if not session_id:
        session_id = save_to_session(df, run_id=run_id)
    else:
        # Update session with current run_id if it changed or was missing
        save_to_session(df, session_id=session_id, run_id=run_id)

    # Handle explicit or default export
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "diagnostics", session_id=session_id
    )
    export_url = save_output(df, export_path)

    # Robustly handle config nesting
    base_cfg = config.get("diagnostics", config) if isinstance(config, dict) else {}

    module_cfg = {
        "diagnostics": {
            **base_cfg,
            "logging": "off",
            "profile": {
                "run": True,
                "settings": {"export": True, "export_html": should_export_html(config)},
            },
            "plotting": {"run": True},
        }
    }

    # run_diag_pipeline handles profiling, plotting and report generation
    run_diag_pipeline(config=module_cfg, df=df, notebook=False, run_id=run_id)

    shape = [int(df.shape[0]), int(df.shape[1])]
    null_rate = round(float(df.isnull().mean().mean()), 4)

    # Base status on configurable or default threshold
    null_threshold = config.get("null_threshold", 0.1)
    status = "pass" if null_rate < null_threshold else "warn"

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}

    if should_export_html(config):
        # Paths where run_diag_pipeline saves its reports
        artifact_path = f"exports/reports/diagnostics/{run_id}_diagnostics_report.html"
        artifact_url = upload_artifact(
            artifact_path, run_id, "diagnostics", config=kwargs, session_id=session_id
        )

        xlsx_path = f"exports/reports/diagnostics/{run_id}_diagnostics_report.xlsx"
        xlsx_url = upload_artifact(
            xlsx_path, run_id, "diagnostics", config=kwargs, session_id=session_id
        )

        # Upload plots - search both root and run_id subdir
        plot_dirs = [Path("exports/plots/diagnostics"), Path(f"exports/plots/diagnostics/{run_id}")]
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
        "status": status,
        "module": "diagnostics",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {
            "shape": shape,
            "null_rate": null_rate,
            "column_count": shape[1],
            "row_count": shape[0],
        },
        "profile_shape": shape,
        "null_rate": null_rate,
        "column_count": shape[1],
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
    name="diagnostics",
    fn=_toolkit_diagnostics,
    description="Run data profiling on a dataset. Returns shape, null rate, and column summary.",
    input_schema=base_input_schema(),
)
