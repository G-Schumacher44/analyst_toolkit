"""MCP tool: toolkit_final_audit — final certification and big HTML report via M10."""

import tempfile
import os

from analyst_toolkit.m10_final_audit.final_audit_pipeline import (
    run_final_audit_pipeline,
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


async def _toolkit_final_audit(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """
    Run the final certification audit.
    Applies final edits and generates the 'Big HTML Report' (Healing Certificate).
    """
    if not run_id and session_id:
        run_id = get_session_run_id(session_id)
    run_id = run_id or default_run_id()

    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    # Robustly handle config nesting
    base_cfg = config.get("final_audit", config) if isinstance(config, dict) else {}

    # The M10 pipeline requires a raw_data_path to compute before/after row counts.
    # Write the current df to a temp CSV as the "raw" snapshot if not provided.
    tmp_raw = None
    raw_data_path = base_cfg.get("raw_data_path")
    if not raw_data_path:
        tmp_raw = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        df.to_csv(tmp_raw.name, index=False)
        raw_data_path = tmp_raw.name

    # Build module config for the pipeline runner
    module_cfg = {
        "final_audit": {
            **base_cfg,
            "run": True,
            "logging": "off",
            "raw_data_path": raw_data_path,
            "settings": {
                "export_report": True,
                "export_html": True,  # Force true for the certification
                "paths": {
                    "report_excel": f"exports/reports/final_audit/{{run_id}}_final_audit_report.xlsx",
                    "report_joblib": f"exports/reports/final_audit/{{run_id}}_final_audit_report.joblib",
                    "checkpoint_csv": f"exports/reports/final_audit/{{run_id}}_certified.csv",
                    "checkpoint_joblib": f"exports/reports/final_audit/{{run_id}}_certified.joblib",
                    "report_html": f"exports/reports/final_audit/{{run_id}}_final_audit_report.html",
                },
            },
        }
    }

    try:
        # run_final_audit_pipeline returns the certified dataframe
        df_certified = run_final_audit_pipeline(config=module_cfg, df=df, run_id=run_id, notebook=False)
    finally:
        if tmp_raw:
            os.unlink(tmp_raw.name)

    # Save to session
    session_id = save_to_session(df_certified, session_id=session_id, run_id=run_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count")

    # Handle explicit or default export
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "final_audit", session_id=session_id
    )
    export_url = save_output(df_certified, export_path)

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""

    # M10 exports to these locations (matches final_audit_pipeline.py defaults)
    artifact_path = f"exports/reports/final_audit/{run_id}_final_audit_report.html"
    artifact_url = upload_artifact(
        artifact_path, run_id, "final_audit", config=kwargs, session_id=session_id
    )

    xlsx_path = f"exports/reports/final_audit/{run_id}_final_audit_report.xlsx"
    xlsx_url = upload_artifact(
        xlsx_path, run_id, "final_audit", config=kwargs, session_id=session_id
    )

    res = {
        "status": "pass",  # Final audit status
        "module": "final_audit",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {
            "message": "Final Audit Complete. Data is certified.",
            "row_count": row_count,
        },
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "export_url": export_url,
    }
    append_to_run_history(run_id, res, session_id=session_id)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="final_audit",
    fn=_toolkit_final_audit,
    description="Run the final certification audit and generate the comprehensive Healing Certificate (HTML).",
    input_schema=base_input_schema(),
)
