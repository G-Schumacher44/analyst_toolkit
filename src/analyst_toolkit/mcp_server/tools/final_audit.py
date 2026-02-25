"""MCP tool: toolkit_final_audit — final certification and big HTML report via M10."""

import os
import tempfile
from copy import deepcopy

from analyst_toolkit.m02_validation.validate_data import run_validation_suite
from analyst_toolkit.m10_final_audit.final_audit_pipeline import (
    run_final_audit_pipeline,
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
    upload_artifact,
)
from analyst_toolkit.mcp_server.schemas import base_input_schema


def _normalize_final_audit_config(config: dict) -> dict:
    """
    Accept both full module config and MCP shorthand config.

    MCP callers often pass top-level `rules`, while M10 expects:
      final_audit.certification.schema_validation.rules
    """
    if "final_audit" in config and isinstance(config.get("final_audit"), dict):
        base_cfg = deepcopy(config["final_audit"])
    else:
        base_cfg = deepcopy(config)

    if not isinstance(base_cfg, dict):
        base_cfg = {}

    cert_cfg = base_cfg.get("certification", {})
    if not isinstance(cert_cfg, dict):
        cert_cfg = {}
    else:
        cert_cfg = deepcopy(cert_cfg)

    schema_cfg = cert_cfg.get("schema_validation", {})
    if not isinstance(schema_cfg, dict):
        schema_cfg = {}
    else:
        schema_cfg = deepcopy(schema_cfg)

    nested_rules = schema_cfg.get("rules", {})
    if not isinstance(nested_rules, dict):
        nested_rules = {}

    shorthand_rules = base_cfg.get("rules", {})
    if isinstance(shorthand_rules, dict) and shorthand_rules:
        nested_rules = {**nested_rules, **shorthand_rules}

    if "disallowed_null_columns" in base_cfg and isinstance(
        base_cfg.get("disallowed_null_columns"), list
    ):
        nested_rules["disallowed_null_columns"] = base_cfg["disallowed_null_columns"]

    schema_cfg["rules"] = nested_rules
    schema_cfg.setdefault("run", True)
    if "fail_on_error" in base_cfg and "fail_on_error" not in schema_cfg:
        schema_cfg["fail_on_error"] = bool(base_cfg["fail_on_error"])
    schema_cfg.setdefault("fail_on_error", True)

    cert_cfg.setdefault("run", True)
    cert_cfg["schema_validation"] = schema_cfg

    base_cfg["certification"] = cert_cfg
    base_cfg.pop("rules", None)
    base_cfg.pop("schema_validation", None)
    base_cfg.pop("disallowed_null_columns", None)
    base_cfg.pop("fail_on_error", None)

    return base_cfg


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

    config = coerce_config(config, "final_audit")
    base_cfg = _normalize_final_audit_config(config)
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
    validation_results = run_validation_suite(df_certified, cert_cfg)
    violations_found = [
        name
        for name, check in validation_results.items()
        if isinstance(check, dict) and "passed" in check and not check["passed"]
    ]
    checks_run = sum(
        1 for check in validation_results.values() if isinstance(check, dict) and "passed" in check
    )
    rules = cert_cfg.get("schema_validation", {}).get("rules", {})
    disallowed_null_cols = rules.get("disallowed_null_columns", [])
    null_violation_columns = [
        col
        for col in disallowed_null_cols
        if col in df_certified.columns and df_certified[col].isnull().any()
    ]
    passed = not violations_found and not null_violation_columns

    res = {
        "status": "fail" if not passed else ("warn" if warnings else "pass"),
        "module": "final_audit",
        "run_id": run_id,
        "session_id": session_id,
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
            "null_violation_columns": null_violation_columns,
        },
        "passed": passed,
        "violations_found": violations_found,
        "null_violation_columns": null_violation_columns,
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
    name="final_audit",
    fn=_toolkit_final_audit,
    description="Run the final certification audit and generate the comprehensive Healing Certificate (HTML).",
    input_schema=base_input_schema(),
)
