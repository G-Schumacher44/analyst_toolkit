"""MCP tool: toolkit_validation — schema/dtype/range validation via M02."""

import pandas as pd
from analyst_toolkit.mcp_server.schemas import base_input_schema
from analyst_toolkit.mcp_server.io import load_input
from analyst_toolkit.m02_validation.validate_data import run_validation_suite
from analyst_toolkit.m00_utils.export_utils import export_html_report


async def _toolkit_validation(gcs_path: str, config: dict = {}, run_id: str = "mcp_run") -> dict:
    """Run schema and data validation on the dataset at gcs_path."""
    df = load_input(gcs_path)

    module_cfg = {**config, "logging": "off"}
    validation_results = run_validation_suite(df, config=module_cfg)

    checks = {k: v for k, v in validation_results.items() if isinstance(v, dict) and "passed" in v}
    failed_rules = [k for k, v in checks.items() if not v.get("passed")]
    issue_count = sum(len(v.get("details", {})) for k, v in checks.items() if not v.get("passed"))
    passed = len(failed_rules) == 0

    artifact_path = ""
    if config.get("export_html", False):
        report_tables = {
            k: pd.DataFrame([{"Rule": k, "Passed": v.get("passed"), "Description": v.get("rule_description", "")}])
            for k, v in checks.items()
        }
        html_path = f"exports/reports/validation/{run_id}_validation_report.html"
        artifact_path = export_html_report(report_tables, html_path, "Validation", run_id)

    return {
        "status": "pass" if passed else "fail",
        "module": "validation",
        "run_id": run_id,
        "summary": {"passed": passed, "failed_rules": failed_rules, "issue_count": issue_count},
        "passed": passed,
        "failed_rules": failed_rules,
        "issue_count": issue_count,
        "artifact_path": artifact_path,
    }


from analyst_toolkit.mcp_server.server import register_tool  # noqa: E402

register_tool(
    name="toolkit_validation",
    fn=_toolkit_validation,
    description="Run schema, dtype, categorical, and range validation on a dataset.",
    input_schema=base_input_schema(),
)
