"""MCP tool: toolkit_validation — schema/dtype/range validation via M02."""

from typing import Any

from analyst_toolkit.m02_validation.run_validation_pipeline import (
    run_validation_pipeline,
)
from analyst_toolkit.m02_validation.validate_data import run_validation_suite
from analyst_toolkit.mcp_server.config_normalizers import (
    INFER_CONFIG_REQUIRED_WARNING,
    adapt_validation_config_to_dataframe,
    has_actionable_validation_config,
    normalize_validation_config,
)
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    build_artifact_contract,
    coerce_config,
    compact_destination_metadata,
    deliver_artifact,
    empty_delivery_state,
    fold_status_with_artifacts,
    generate_default_export_path,
    get_inferred_config,
    get_session_metadata,
    load_input,
    make_json_safe,
    resolve_run_context,
    save_output,
    save_to_session,
    should_export_html,
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


async def _toolkit_validation(
    gcs_path: str | None = None,
    session_id: str | None = None,
    input_id: str | None = None,
    config: dict | None = None,
    runtime: dict | str | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """Run schema and data validation on the dataset at gcs_path or session_id."""
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

    config = coerce_config(config, "validation")
    config, runtime_meta = resolve_layered_config(
        inferred=get_inferred_config(session_id, "validation"),
        provided=config,
        explicit=runtime_to_config_overlay(runtime_cfg),
    )
    base_cfg = normalize_validation_config(config)
    df = load_input(gcs_path, session_id=session_id, input_id=input_id)
    base_cfg = adapt_validation_config_to_dataframe(base_cfg, df)

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
            "validation/data",
            config=kwargs,
            session_id=session_id,
        )
        export_url = export_delivery["reference"]

    module_cfg = {
        "validation": {
            **base_cfg,
            "logging": "off",
            "settings": {
                **base_cfg.get("settings", {}),
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
    violations_detail: dict = {}
    checks_run = 0
    if schema_cfg.get("run", False):
        validation_results = run_validation_suite(df, config=base_cfg)
        for check_name, check in validation_results.items():
            if isinstance(check, dict) and "passed" in check:
                checks_run += 1
                if not check["passed"]:
                    violations_found.append(check_name)
                    violations_detail[check_name] = make_json_safe(check.get("details", {}))

    passed = len(violations_found) == 0

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    artifact_delivery: dict[str, Any] = empty_delivery_state()
    xlsx_delivery: dict[str, Any] = empty_delivery_state()
    status_warnings: list = []
    advisory_warnings: list = []
    status_warnings.extend(lifecycle["warnings"])
    status_warnings.extend(runtime_warnings)
    status_warnings.extend(runtime_meta["runtime_warnings"])
    if not has_actionable_validation_config(base_cfg):
        advisory_warnings.append(INFER_CONFIG_REQUIRED_WARNING)
    status_warnings.extend(export_delivery["warnings"])
    if should_export_html(config):
        artifact_path = f"exports/reports/validation/{run_id}_validation_report.html"
        artifact_delivery = deliver_artifact(
            artifact_path,
            run_id,
            "validation",
            config=kwargs,
            session_id=session_id,
        )
        artifact_path = artifact_delivery["local_path"]
        artifact_url = artifact_delivery["url"]
        status_warnings.extend(artifact_delivery["warnings"])

        xlsx_path = f"exports/reports/validation/{run_id}_validation_report.xlsx"
        xlsx_delivery = deliver_artifact(
            xlsx_path,
            run_id,
            "validation",
            config=kwargs,
            session_id=session_id,
        )
        xlsx_url = xlsx_delivery["url"]
        status_warnings.extend(xlsx_delivery["warnings"])

    artifact_contract = build_artifact_contract(
        export_url,
        export_path=export_delivery["local_path"],
        artifact_path=artifact_path,
        artifact_url=artifact_url,
        xlsx_path=xlsx_delivery["local_path"],
        xlsx_url=xlsx_url,
        expect_html=should_export_html(config),
        expect_xlsx=should_export_html(config),
        required_html=should_export_html(config),
        probe_local_paths=True,
    )
    warnings = status_warnings + advisory_warnings + artifact_contract["artifact_warnings"]
    base_status = "fail" if violations_found else ("warn" if status_warnings else "pass")
    status = fold_status_with_artifacts(
        base_status, artifact_contract["missing_required_artifacts"]
    )

    res = {
        "status": status,
        "module": "validation",
        "run_id": run_id,
        "session_id": session_id,
        "effective_config": base_cfg,
        "summary": {
            "passed": passed,
            "checks_run": checks_run,
            "violations_found": violations_found,
            "violations_detail": violations_detail,
            "row_count": row_count,
        },
        "passed": passed,
        "violations_found": violations_found,
        "violations_detail": violations_detail,
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
    if passed:
        next_steps = [
            next_action(
                "final_audit",
                "Validation passed; run final certification and generate final artifacts.",
                {"session_id": session_id, "run_id": run_id},
            ),
            next_action(
                "get_run_history",
                "Inspect module-level execution ledger for this run.",
                {"run_id": run_id, "session_id": session_id},
            ),
        ]
    else:
        next_steps = [
            next_action(
                "infer_configs",
                "Generate config updates to resolve validation failures.",
                {"session_id": session_id, "modules": ["validation"]},
            ),
            next_action(
                "get_capability_catalog",
                "Review validation rule knobs and acceptable config paths.",
                {},
            ),
            next_action(
                "validation",
                "Re-run validation after adjusting rules/config.",
                {"session_id": session_id, "run_id": run_id},
            ),
        ]

    res = with_dashboard_artifact(
        res,
        artifact_path=artifact_path,
        artifact_url=artifact_url,
        label="Validation dashboard",
    )
    res = with_next_actions(res, next_steps)
    append_to_run_history(run_id, res, session_id=session_id)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="validation",
    fn=_toolkit_validation,
    description="Run schema, dtype, categorical, and range validation on a dataset and return a standalone validation dashboard artifact.",
    input_schema=base_input_schema(),
)
