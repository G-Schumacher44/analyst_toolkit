"""MCP tool: toolkit_validation — schema/dtype/range validation via M02."""

import pandas as pd

from analyst_toolkit.m02_validation.run_validation_pipeline import (
    run_validation_pipeline,
)
from analyst_toolkit.m02_validation.validate_data import run_validation_suite
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

    # run_validation_pipeline handles export/reporting
    run_validation_pipeline(config=module_cfg, df=df, notebook=False, run_id=run_id)

    # Run suite directly to get structured pass/fail results for the MCP response
    schema_cfg = base_cfg.get("schema_validation", {})
    violations_found: list[str] = []
    checks_run = 0
    if schema_cfg.get("run", False):
        validation_results = run_validation_suite(df, config=base_cfg)
        for check_name, check in validation_results.items():
            if isinstance(check, dict) and "passed" in check:
                checks_run += 1
                if not check["passed"]:
                    violations_found.append(check_name)

    passed = len(violations_found) == 0

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    warnings: list = []
    if should_export_html(config):
        artifact_path = f"exports/reports/validation/{run_id}_validation_report.html"
        artifact_url = check_upload(
            upload_artifact(
                artifact_path, run_id, "validation", config=kwargs, session_id=session_id
            ),
            artifact_path,
            warnings,
        )

        xlsx_path = f"exports/reports/validation/{run_id}_validation_report.xlsx"
        xlsx_url = check_upload(
            upload_artifact(xlsx_path, run_id, "validation", config=kwargs, session_id=session_id),
            xlsx_path,
            warnings,
        )

    status = "warn" if warnings else ("fail" if violations_found else "pass")

    res = {
        "status": status,
        "module": "validation",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {
            "passed": passed,
            "checks_run": checks_run,
            "violations_found": violations_found,
            "row_count": row_count,
        },
        "passed": passed,
        "violations_found": violations_found,
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
