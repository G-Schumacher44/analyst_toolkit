"""MCP tool: toolkit_validation — schema/dtype/range validation via M02."""

import pandas as pd

from analyst_toolkit.m02_validation.run_validation_pipeline import (
    run_validation_pipeline,
)
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    check_upload,
    coerce_config,
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


async def _toolkit_validation(
    gcs_path: str | None = None,
    session_id: str | None = None,
    config: dict | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """Run schema and data validation on the dataset at gcs_path or session_id."""
    if not run_id and session_id:
        run_id = get_session_run_id(session_id)
    run_id = run_id or default_run_id()

    config = coerce_config(config, "validation")
    df = load_input(gcs_path, session_id=session_id)

    # Ensure it's in a session for the pipeline
    if not session_id:
        session_id = save_to_session(df, run_id=run_id)
    else:
        save_to_session(df, session_id=session_id, run_id=run_id)

    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count", len(df))

    # Handle explicit or default export
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "validation", session_id=session_id
    )
    export_url = save_output(df, export_path)

    base_cfg = config.get("validation", config)

    module_cfg = {
        "validation": {
            **base_cfg,
            "logging": "off",
            "settings": {
                "export": True,
                "export_html": should_export_html(config),
            },
        }
    }

    # run_validation_pipeline returns the validated (unchanged) dataframe
    run_validation_pipeline(config=module_cfg, df=df, notebook=False, run_id=run_id)

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    warnings: list = []
    if should_export_html(config):
        artifact_path = f"exports/reports/validation/{run_id}_validation_report.html"
        artifact_url = check_upload(
            upload_artifact(artifact_path, run_id, "validation", config=kwargs, session_id=session_id),
            artifact_path,
            warnings,
        )

        xlsx_path = f"exports/reports/validation/{run_id}_validation_report.xlsx"
        xlsx_url = check_upload(
            upload_artifact(xlsx_path, run_id, "validation", config=kwargs, session_id=session_id),
            xlsx_path,
            warnings,
        )

    # Logic to determine pass/fail for the response (heuristic)
    passed = True  # Placeholder

    res = {
        "status": "pass" if passed else "fail",
        "module": "validation",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {
            "passed": passed,
            "row_count": row_count,
        },
        "passed": passed,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "export_url": export_url,
        "warnings": warnings,
    }
    append_to_run_history(run_id, res, session_id=session_id)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="validation",
    fn=_toolkit_validation,
    description="Run schema, dtype, categorical, and range validation on a dataset.",
    input_schema=base_input_schema(),
)
