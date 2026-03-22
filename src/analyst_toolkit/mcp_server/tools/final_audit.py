"""MCP tool: toolkit_final_audit — final certification and big HTML report via M10."""

import logging
import os
import tempfile
from pathlib import Path
from typing import Any

import yaml

from analyst_toolkit.m02_validation.validate_data import run_validation_suite
from analyst_toolkit.m10_final_audit.final_audit_pipeline import (
    run_final_audit_pipeline,
)
from analyst_toolkit.mcp_server.config_normalizers import normalize_final_audit_config
from analyst_toolkit.mcp_server.input.ingest import get_input_descriptor
from analyst_toolkit.mcp_server.io import (
    ALLOW_EMPTY_CERT_RULES,
    append_to_run_history,
    build_artifact_contract,
    coerce_config,
    compact_destination_metadata,
    deliver_artifact,
    empty_delivery_state,
    fold_status_with_artifacts,
    generate_default_export_path,
    get_session_config,
    get_session_metadata,
    load_input,
    make_json_safe,
    resolve_run_context,
    save_output,
    save_to_session,
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

_TRANSIENT_PATH_KEYS = ("raw_data_path", "input_path", "input_df_path")


def _is_transient_path(value: str | None) -> bool:
    """Return True if the path looks like a temp file that won't survive across tool calls."""
    if not value or not isinstance(value, str):
        return False
    return value.startswith("/tmp/") or value.startswith(tempfile.gettempdir())


def _strip_transient_paths(cfg: dict) -> list[str]:
    """Remove transient filesystem paths from a config dict. Returns list of stripped keys."""
    stripped = []
    for key in _TRANSIENT_PATH_KEYS:
        if key in cfg and _is_transient_path(cfg[key]):
            del cfg[key]
            stripped.append(key)
    return stripped


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
    input_id: str | None = None,
    config: dict | None = None,
    runtime: dict | str | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """
    Run the final certification audit.
    Applies final edits and generates the 'Big HTML Report' (Healing Certificate).
    """
    runtime_cfg, runtime_warnings = normalize_runtime_overlay(runtime)
    runtime_overrides = runtime_to_tool_overrides(runtime_cfg)
    runtime_applied = bool(runtime_cfg)
    gcs_path = gcs_path or runtime_overrides.get("gcs_path")
    session_id = session_id or runtime_overrides.get("session_id")
    input_id = input_id or runtime_overrides.get("input_id")
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

    config = coerce_config(config, "final_audit")

    # Resolve session_id from input_id so config discovery can find inferred configs
    if not session_id and input_id:
        descriptor = get_input_descriptor(input_id)
        if descriptor and descriptor.session_id:
            session_id = descriptor.session_id

    # Auto-discover inferred configs from session when no explicit config is provided
    inferred_config: dict = {}
    if session_id:
        for config_key in ("final_audit", "certification"):
            raw_yaml = get_session_config(session_id, config_key)
            if not raw_yaml:
                continue
            try:
                parsed = yaml.safe_load(raw_yaml)
            except yaml.YAMLError:
                logging.getLogger(__name__).warning(
                    "Failed to parse inferred %s config from session %s",
                    config_key,
                    session_id,
                )
                continue
            if not isinstance(parsed, dict):
                continue
            # Unwrap module-level key so the structure matches provided config
            if "final_audit" in parsed and isinstance(parsed["final_audit"], dict):
                parsed = parsed["final_audit"]
            # Merge each discovered config layer (final_audit first, certification second)
            for key, value in parsed.items():
                if isinstance(value, dict) and isinstance(inferred_config.get(key), dict):
                    inferred_config[key] = {**inferred_config[key], **value}
                else:
                    inferred_config.setdefault(key, value)

    # Strip transient filesystem paths from both inferred and provided configs.
    # infer_configs embeds /tmp paths that expire after the inference call returns;
    # agents often pass those same stale paths back as explicit config.
    _strip_transient_paths(inferred_config)
    if isinstance(config, dict):
        _strip_transient_paths(config)

    config, runtime_meta = resolve_layered_config(
        inferred=inferred_config,
        provided=config,
        explicit=runtime_to_config_overlay(runtime_cfg),
    )
    base_cfg = normalize_final_audit_config(config)
    df = load_input(gcs_path, session_id=session_id, input_id=input_id)

    # The M10 pipeline requires a raw_data_path to compute before/after row counts.
    # Write the current df to a temp CSV as the "raw" snapshot if not provided.
    tmp_raw = None
    raw_data_path = base_cfg.get("raw_data_path")
    if _is_transient_path(raw_data_path):
        raw_data_path = None
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

    # Ensure output directories exist — the M10 pipeline does not create them.
    # Also strip any paths that resolve outside the project root to prevent
    # the downstream pipeline from writing to untrusted locations.
    project_root = Path(os.getcwd()).resolve()
    paths_cfg = module_cfg["final_audit"].get("settings", {}).get("paths", {})
    for path_key, path_value in list(paths_cfg.items()):
        if not isinstance(path_value, str) or not path_value:
            continue
        resolved = path_value.replace("{run_id}", run_id)
        target = (project_root / resolved).resolve()
        try:
            target.relative_to(project_root)
            target.parent.mkdir(parents=True, exist_ok=True)
        except ValueError:
            logging.getLogger(__name__).warning(
                "Refusing to use output path outside project root (%s): %s",
                path_key,
                target,
            )
            del paths_cfg[path_key]

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
            "final_audit/data",
            config=kwargs,
            session_id=session_id,
        )
        export_url = export_delivery["reference"]

    warnings: list = []
    warnings.extend(lifecycle["warnings"])
    warnings.extend(runtime_warnings)
    warnings.extend(runtime_meta["runtime_warnings"])
    warnings.extend(export_delivery["warnings"])
    artifact_delivery: dict[str, Any] = empty_delivery_state()
    xlsx_delivery: dict[str, Any] = empty_delivery_state()

    # M10 exports to these locations (matches final_audit_pipeline.py defaults)
    artifact_path = f"exports/reports/final_audit/{run_id}_final_audit_report.html"
    artifact_delivery = deliver_artifact(
        artifact_path,
        run_id,
        "final_audit",
        config=kwargs,
        session_id=session_id,
    )
    artifact_path = artifact_delivery["local_path"]
    artifact_url = artifact_delivery["url"]
    warnings.extend(artifact_delivery["warnings"])

    xlsx_path = f"exports/reports/final_audit/{run_id}_final_audit_report.xlsx"
    xlsx_delivery = deliver_artifact(
        xlsx_path,
        run_id,
        "final_audit",
        config=kwargs,
        session_id=session_id,
    )
    xlsx_url = xlsx_delivery["url"]
    warnings.extend(xlsx_delivery["warnings"])
    xlsx_expected = bool(xlsx_delivery.get("local_path")) or Path(xlsx_path).exists()

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
        export_path=export_delivery["local_path"],
        artifact_path=artifact_path,
        artifact_url=artifact_url,
        xlsx_path=xlsx_delivery["local_path"],
        xlsx_url=xlsx_url,
        expect_html=True,
        expect_xlsx=xlsx_expected,
        required_html=True,
        probe_local_paths=True,
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
        "destination_delivery": {
            "data_export": compact_destination_metadata(export_delivery["destinations"]),
            "html_report": compact_destination_metadata(artifact_delivery["destinations"]),
            "xlsx_report": compact_destination_metadata(xlsx_delivery["destinations"]),
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
        label="Final audit dashboard",
    )
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
    description="Run the final certification audit and return the Healing Certificate as a standalone dashboard artifact.",
    input_schema=base_input_schema(),
)
