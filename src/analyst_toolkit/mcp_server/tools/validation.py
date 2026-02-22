"""MCP tool: toolkit_validation — schema/dtype/range validation via M02."""

import pandas as pd

from analyst_toolkit.m00_utils.export_utils import export_html_report, export_validation_results
from analyst_toolkit.m02_validation.validate_data import run_validation_suite
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    default_run_id,
    load_input,
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
) -> dict:
    """Run schema and data validation on the dataset at gcs_path or session_id."""
    run_id = run_id or default_run_id()
    config = config or {}
    df = load_input(gcs_path, session_id=session_id)

    # Ensure it's in a session for the pipeline
    if not session_id:
        session_id = save_to_session(df)

    module_cfg = {**config, "logging": "off"}
    validation_results = run_validation_suite(df, config=module_cfg)

    checks = {k: v for k, v in validation_results.items() if isinstance(v, dict) and "passed" in v}
    failed_rules = [k for k, v in checks.items() if not v.get("passed")]

    # Robust issue count: number of failing columns or rules
    issue_count = len(failed_rules)
    passed = len(failed_rules) == 0

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    if should_export_html(config):
        report_tables = {
            k: pd.DataFrame(
                [
                    {
                        "Rule": k,
                        "Passed": v.get("passed"),
                        "Description": v.get("rule_description", ""),
                    }
                ]
            )
            for k, v in checks.items()
        }
        html_path = f"exports/reports/validation/{run_id}_validation_report.html"
        artifact_path = export_html_report(report_tables, html_path, "Validation", run_id)
        artifact_url = upload_artifact(artifact_path, run_id, "validation")

        xlsx_cfg = {"export_path": "exports/reports/validation/validation_report.xlsx"}
        export_validation_results(validation_results, xlsx_cfg, run_id=run_id)
        xlsx_path = f"exports/reports/validation/{run_id}_validation_report.xlsx"
        xlsx_url = upload_artifact(xlsx_path, run_id, "validation")

    res = {
        "status": "pass" if passed else "fail",
        "module": "validation",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {"passed": passed, "failed_rules": failed_rules, "issue_count": issue_count},
        "passed": passed,
        "failed_rules": failed_rules,
        "issue_count": issue_count,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
    }
    append_to_run_history(run_id, res)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="toolkit_validation",
    fn=_toolkit_validation,
    description="Run schema, dtype, categorical, and range validation on a dataset.",
    input_schema=base_input_schema(),
)
