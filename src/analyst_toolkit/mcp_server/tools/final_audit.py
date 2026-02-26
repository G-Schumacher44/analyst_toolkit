"""MCP tool: toolkit_final_audit — final certification and big HTML report via M10."""

import os
import tempfile

from analyst_toolkit.m02_validation.validate_data import run_validation_suite
from analyst_toolkit.m10_final_audit.final_audit_pipeline import (
    run_final_audit_pipeline,
)
from analyst_toolkit.mcp_server.config_normalizers import normalize_final_audit_config
from analyst_toolkit.mcp_server.io import (
    ALLOW_EMPTY_CERT_RULES,
    append_to_run_history,
    build_artifact_contract,
    check_upload,
    coerce_config,
    fold_status_with_artifacts,
    generate_default_export_path,
    get_session_metadata,
    load_input,
    make_json_safe,
    resolve_run_context,
    save_output,
    save_to_session,
    upload_artifact,
)
from analyst_toolkit.mcp_server.response_utils import next_action, with_next_actions
from analyst_toolkit.mcp_server.schemas import base_input_schema


def _has_effective_certification_rules(rules: dict) -> bool:
    if not isinstance(rules, dict):
        return False
    checks = [
        rules.get("expected_columns"),
        rules.get("expected_types"),
        rules.get("categorical_values"),
        rules.get("numeric_ranges"),
        rules.get("disallowed_null_columns"),
    ]
    return any(bool(item) for item in checks)


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
    run_id, lifecycle = resolve_run_context(run_id, session_id)

    config = coerce_config(config, "final_audit")
    base_cfg = normalize_final_audit_config(config)
    df = load_input(gcs_path, session_id=session_id)

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
                **base_cfg.get("settings", {}),
                "export_report": True,
                "export_html": True,  # Force true for the certification
                "paths": {
                    "report_excel": "exports/reports/final_audit/{run_id}_final_audit_report.xlsx",
                    "report_joblib": "exports/reports/final_audit/{run_id}_final_audit_report.joblib",
                    "checkpoint_csv": "exports/reports/final_audit/{run_id}_certified.csv",
                    "checkpoint_joblib": "exports/reports/final_audit/{run_id}_certified.joblib",
                    "report_html": "exports/reports/final_audit/{run_id}_final_audit_report.html",
                    **base_cfg.get("settings", {}).get("paths", {}),
                },
            },
        }
    }

    try:
        # run_final_audit_pipeline returns the certified dataframe
        df_certified = run_final_audit_pipeline(
            config=module_cfg, df=df, run_id=run_id, notebook=False
        )
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

    warnings: list = []
    warnings.extend(lifecycle["warnings"])

    # M10 exports to these locations (matches final_audit_pipeline.py defaults)
    artifact_path = f"exports/reports/final_audit/{run_id}_final_audit_report.html"
    artifact_url = check_upload(
        upload_artifact(artifact_path, run_id, "final_audit", config=kwargs, session_id=session_id),
        artifact_path,
        warnings,
    )

    xlsx_path = f"exports/reports/final_audit/{run_id}_final_audit_report.xlsx"
    xlsx_url = check_upload(
        upload_artifact(xlsx_path, run_id, "final_audit", config=kwargs, session_id=session_id),
        xlsx_path,
        warnings,
    )

    cert_cfg = base_cfg.get("certification", {})
    schema_cfg = cert_cfg.get("schema_validation", {})
    rules = schema_cfg.get("rules", {}) if isinstance(schema_cfg, dict) else {}
    has_effective_rules = _has_effective_certification_rules(rules)

    validation_results = run_validation_suite(df_certified, cert_cfg)
    violations_found = [
        name
        for name, check in validation_results.items()
        if isinstance(check, dict) and "passed" in check and not check["passed"]
    ]
    violations_detail = {
        name: make_json_safe(check.get("details", {}))
        for name, check in validation_results.items()
        if isinstance(check, dict) and "passed" in check and not check["passed"]
    }
    checks_run = sum(
        1 for check in validation_results.values() if isinstance(check, dict) and "passed" in check
    )
    disallowed_null_cols = rules.get("disallowed_null_columns", [])
    null_violation_columns = [
        col
        for col in disallowed_null_cols
        if col in df_certified.columns and df_certified[col].isnull().any()
    ]
    passed = not violations_found and not null_violation_columns
    if not has_effective_rules and not ALLOW_EMPTY_CERT_RULES:
        passed = False
        violations_found.append("rule_contract_missing")
        violations_detail["rule_contract_missing"] = {
            "reason": "Certification rules are empty or ineffective.",
            "remediation": (
                "Provide expected_columns/expected_types/categorical_values/numeric_ranges "
                "or disallowed_null_columns, or set ANALYST_MCP_ALLOW_EMPTY_CERT_RULES=1."
            ),
        }
        warnings.append("Certification rule contract is empty; failing closed for final_audit.")

    artifact_contract = build_artifact_contract(
        export_url,
        artifact_url=artifact_url,
        xlsx_url=xlsx_url,
        expect_html=True,
        expect_xlsx=True,
        required_html=True,
    )
    warnings.extend(artifact_contract["artifact_warnings"])
    base_status = "fail" if not passed else ("warn" if warnings else "pass")
    status = fold_status_with_artifacts(
        base_status, artifact_contract["missing_required_artifacts"]
    )

    res = {
        "status": status,
        "module": "final_audit",
        "run_id": run_id,
        "session_id": session_id,
        "effective_config": base_cfg,
        "summary": {
            "message": (
                "Final Audit Complete. Data is certified."
                if passed
                else "Final Audit completed with certification failures."
            ),
            "row_count": row_count,
            "passed": passed,
            "checks_run": checks_run,
            "violations_found": violations_found,
            "violations_detail": violations_detail,
            "null_violation_columns": null_violation_columns,
        },
        "passed": passed,
        "violations_found": violations_found,
        "violations_detail": violations_detail,
        "null_violation_columns": null_violation_columns,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "export_url": export_url,
        "warnings": warnings,
        "lifecycle": {k: v for k, v in lifecycle.items() if k != "warnings"},
        "artifact_matrix": artifact_contract["artifact_matrix"],
        "expected_artifacts": artifact_contract["expected_artifacts"],
        "uploaded_artifacts": artifact_contract["uploaded_artifacts"],
        "missing_required_artifacts": artifact_contract["missing_required_artifacts"],
    }
    res = with_next_actions(
        res,
        [
            next_action(
                "get_run_history",
                "Review the full healing ledger and module summaries for this run.",
                {"run_id": run_id, "session_id": session_id},
            ),
            next_action(
                "get_data_health_report",
                "Compute consolidated health score after certification.",
                {"run_id": run_id, "session_id": session_id},
            ),
        ],
    )
    append_to_run_history(run_id, res, session_id=session_id)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="final_audit",
    fn=_toolkit_final_audit,
    description="Run the final certification audit and generate the comprehensive Healing Certificate (HTML).",
    input_schema=base_input_schema(),
)
