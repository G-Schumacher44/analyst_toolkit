"""MCP tool: toolkit_auto_heal — infer and apply cleaning rules in one go."""

import asyncio
import logging

import yaml

from analyst_toolkit.m00_utils.export_utils import export_html_report
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    build_artifact_contract,
    compact_destination_metadata,
    deliver_artifact,
    fold_status_with_artifacts,
    get_session_metadata,
    resolve_run_context,
)
from analyst_toolkit.mcp_server.job_state import JobStore
from analyst_toolkit.mcp_server.response_utils import (
    next_action,
    with_dashboard_artifact,
    with_next_actions,
)
from analyst_toolkit.mcp_server.runtime_overlay import (
    normalize_runtime_overlay,
    runtime_to_tool_overrides,
)
from analyst_toolkit.mcp_server.tools.imputation import _toolkit_imputation
from analyst_toolkit.mcp_server.tools.infer_configs import _toolkit_infer_configs
from analyst_toolkit.mcp_server.tools.normalization import _toolkit_normalization

logger = logging.getLogger("analyst_toolkit.mcp_server.auto_heal")


def _is_terminal_failure(status: str | None) -> bool:
    return status in {"error", "fail"}


async def _run_auto_heal_pipeline(
    gcs_path: str | None = None,
    session_id: str | None = None,
    runtime: dict | str | None = None,
    run_id: str | None = None,
) -> dict:
    """
    Run inference, then automatically apply recommended normalization and imputation.
    Returns a cleaned session_id.
    """
    runtime_cfg, runtime_warnings = normalize_runtime_overlay(runtime)
    runtime_overrides = runtime_to_tool_overrides(runtime_cfg)
    runtime_applied = bool(runtime_cfg)
    gcs_path = gcs_path or runtime_overrides.get("gcs_path")
    session_id = session_id or runtime_overrides.get("session_id")
    run_id = run_id or runtime_overrides.get("run_id")
    run_id, lifecycle = resolve_run_context(run_id, session_id)

    # 1. Infer configs
    infer_res = await _toolkit_infer_configs(
        gcs_path=gcs_path,
        session_id=session_id,
        runtime=runtime_cfg,
        run_id=run_id,
        modules=["normalization", "imputation"],
    )

    if infer_res["status"] == "error":
        return infer_res

    configs = infer_res.get("configs", {})
    current_session_id = infer_res.get("session_id")

    summary = {}
    failed_steps: list[str] = []
    norm_res: dict = {}
    imp_res: dict = {}

    # 2. Apply Normalization if inferred
    if "normalization" in configs:
        try:
            norm_cfg_str = configs["normalization"]
            norm_cfg = yaml.safe_load(norm_cfg_str)
            # The inferred config usually has a top-level 'normalization' key
            actual_cfg = norm_cfg.get("normalization", norm_cfg)

            norm_res = await _toolkit_normalization(
                session_id=current_session_id,
                config=actual_cfg,
                runtime=runtime_cfg,
                run_id=run_id,
            )
            current_session_id = norm_res.get("session_id")
            summary["normalization"] = norm_res.get("summary")
            if _is_terminal_failure(norm_res.get("status")):
                failed_steps.append("normalization")
        except Exception as e:
            summary["normalization"] = {"error": str(e)}
            failed_steps.append("normalization")

    # 3. Apply Imputation if inferred
    if "imputation" in configs:
        try:
            imp_cfg_str = configs["imputation"]
            imp_cfg = yaml.safe_load(imp_cfg_str)
            actual_cfg = imp_cfg.get("imputation", imp_cfg)
            rules = actual_cfg.get("rules", {}) if isinstance(actual_cfg, dict) else {}
            strategies = rules.get("strategies") if isinstance(rules, dict) else None

            if not strategies:
                summary["imputation"] = {
                    "skipped": True,
                    "reason": "No inferred imputation strategies.",
                }
            else:
                imp_res = await _toolkit_imputation(
                    session_id=current_session_id,
                    config=actual_cfg,
                    runtime=runtime_cfg,
                    run_id=run_id,
                )
                current_session_id = imp_res.get("session_id")
                summary["imputation"] = imp_res.get("summary")
                if _is_terminal_failure(imp_res.get("status")):
                    failed_steps.append("imputation")

        except Exception as e:
            summary["imputation"] = {"error": str(e)}
            failed_steps.append("imputation")

    # Final Metadata
    metadata = get_session_metadata(current_session_id) or {}
    row_count = metadata.get("row_count")

    child_statuses = [s for s in [norm_res.get("status"), imp_res.get("status")] if s]
    if failed_steps or "error" in child_statuses:
        status = "error"
    elif "fail" in child_statuses:
        status = "fail"
    elif "warn" in child_statuses:
        status = "warn"
    elif not child_statuses:
        status = "warn"
    else:
        status = "pass"

    message = "Auto-healing completed successfully."
    if status == "warn":
        message = "Auto-healing completed with warnings."
    elif status in {"fail", "error"}:
        message = "Auto-healing completed with failures. Review failed_steps and summaries."

    artifact_path = f"exports/reports/auto_heal/{run_id}_auto_heal_report.html"
    export_html = True
    artifacts_cfg = runtime_cfg.get("artifacts", {}) if isinstance(runtime_cfg, dict) else {}
    if isinstance(artifacts_cfg, dict) and "export_html" in artifacts_cfg:
        export_html = bool(artifacts_cfg.get("export_html"))
    auto_heal_report = {
        "status": status,
        "message": message,
        "row_count": row_count,
        "final_session_id": current_session_id,
        "final_export_url": imp_res.get("export_url") or norm_res.get("export_url", ""),
        "final_dashboard_url": imp_res.get("artifact_url") or norm_res.get("artifact_url", ""),
        "final_dashboard_path": imp_res.get("artifact_path") or norm_res.get("artifact_path", ""),
        "inferred_modules": sorted(configs.keys()),
        "failed_steps": failed_steps,
        "steps": {
            "normalization": {
                "status": norm_res.get("status", "skipped"),
                "summary": norm_res.get("summary", {}),
                "artifact_path": norm_res.get("artifact_path", ""),
                "artifact_url": norm_res.get("artifact_url", ""),
                "export_url": norm_res.get("export_url", ""),
            },
            "imputation": {
                "status": imp_res.get("status", "skipped"),
                "summary": imp_res.get("summary", {}),
                "artifact_path": imp_res.get("artifact_path", ""),
                "artifact_url": imp_res.get("artifact_url", ""),
                "export_url": imp_res.get("export_url", ""),
            },
        },
    }
    artifact_delivery = {
        "reference": "",
        "local_path": "",
        "url": "",
        "warnings": [],
        "destinations": {},
    }
    artifact_url = ""
    warnings = list(lifecycle["warnings"]) + list(runtime_warnings)
    if export_html:
        try:
            artifact_path = export_html_report(auto_heal_report, artifact_path, "Auto Heal", run_id)
            artifact_delivery = deliver_artifact(
                artifact_path,
                run_id=run_id,
                module="auto_heal",
                config=runtime_overrides,
                session_id=current_session_id,
            )
            artifact_path = str(artifact_delivery.get("local_path", ""))
            artifact_url = str(artifact_delivery.get("url", ""))
            warnings.extend(artifact_delivery["warnings"])
        except Exception as exc:
            logger.exception("Auto-heal dashboard export failed for run_id=%s", run_id, exc_info=exc)
            warnings.append("AUTO_HEAL_EXPORT_FAILED")
            artifact_path = ""
    else:
        artifact_path = ""

    artifact_contract = build_artifact_contract(
        imp_res.get("export_url") or norm_res.get("export_url", ""),
        artifact_path=artifact_path,
        artifact_url=artifact_url,
        expect_html=export_html,
        required_html=False,
        probe_local_paths=True,
    )
    warnings.extend(artifact_contract["artifact_warnings"])
    status = fold_status_with_artifacts(status, artifact_contract["missing_required_artifacts"])

    res = {
        "status": status,
        "module": "auto_heal",
        "run_id": run_id,
        "session_id": current_session_id,
        "summary": {**summary, "row_count": row_count},
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "export_url": imp_res.get("export_url") or norm_res.get("export_url", ""),
        "plot_urls": imp_res.get("plot_urls") or norm_res.get("plot_urls", {}),
        "failed_steps": failed_steps,
        "destination_delivery": {
            "html_report": compact_destination_metadata(artifact_delivery["destinations"]),
        },
        "warnings": warnings,
        "lifecycle": {k: v for k, v in lifecycle.items() if k != "warnings"},
        "message": message,
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
        label="Auto-heal dashboard",
    )
    res = with_next_actions(
        res,
        [
            next_action(
                "get_run_history",
                "Review the full auto-heal ledger and child tool outputs for this run.",
                {"run_id": run_id, "session_id": current_session_id},
            ),
            next_action(
                "final_audit",
                "Run final certification on the healed dataset.",
                {"session_id": current_session_id, "run_id": run_id},
            ),
        ],
    )
    append_to_run_history(run_id, res, session_id=current_session_id)
    return res


async def _auto_heal_worker(
    job_id: str,
    gcs_path: str | None,
    session_id: str | None,
    runtime: dict | str | None,
    run_id: str,
):
    JobStore.mark_running(job_id)
    try:
        result = await _run_auto_heal_pipeline(
            gcs_path=gcs_path,
            session_id=session_id,
            runtime=runtime,
            run_id=run_id,
        )
        if _is_terminal_failure(result.get("status")):
            JobStore.mark_failed(
                job_id,
                {
                    "error_type": "ToolResultError",
                    "message": "auto_heal completed with failure status.",
                    "terminal_result_status": result.get("status"),
                    "result": result,
                },
            )
        else:
            JobStore.mark_succeeded(job_id, result=result)
    except Exception as exc:
        JobStore.mark_failed(
            job_id,
            {
                "error_type": type(exc).__name__,
                "message": str(exc),
            },
        )


async def _toolkit_auto_heal(
    gcs_path: str | None = None,
    session_id: str | None = None,
    runtime: dict | str | None = None,
    run_id: str | None = None,
    async_mode: bool = False,
) -> dict:
    """
    Run inference + healing in sync mode (default) or queue a background async job.
    """
    runtime_cfg, _runtime_warnings = normalize_runtime_overlay(runtime)
    runtime_overrides = runtime_to_tool_overrides(runtime_cfg)
    normalized_runtime = runtime_cfg if runtime_cfg else runtime
    gcs_path = gcs_path or runtime_overrides.get("gcs_path")
    session_id = session_id or runtime_overrides.get("session_id")
    run_id = run_id or runtime_overrides.get("run_id")
    run_id, lifecycle = resolve_run_context(run_id, session_id)

    if async_mode:
        job_id = JobStore.create(
            module="auto_heal",
            run_id=run_id,
            inputs={
                "gcs_path": gcs_path,
                "session_id": session_id,
                "runtime": normalized_runtime,
                "run_id": run_id,
            },
        )
        try:
            asyncio.create_task(
                _auto_heal_worker(job_id, gcs_path, session_id, normalized_runtime, run_id)
            )
        except Exception as exc:
            JobStore.mark_failed(
                job_id,
                {
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
            )
            return {
                "status": "error",
                "module": "auto_heal",
                "run_id": run_id,
                "job_id": job_id,
                "message": "Failed to start async auto_heal job.",
            }

        return {
            "status": "accepted",
            "module": "auto_heal",
            "run_id": run_id,
            "job_id": job_id,
            "summary": {"state": "queued"},
            "warnings": list(lifecycle["warnings"]),
            "lifecycle": {k: v for k, v in lifecycle.items() if k != "warnings"},
            "message": "Auto-heal job accepted. Poll get_job_status(job_id).",
            "runtime_applied": bool(runtime_cfg),
        }

    return await _run_auto_heal_pipeline(
        gcs_path=gcs_path,
        session_id=session_id,
        runtime=normalized_runtime,
        run_id=run_id,
    )


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "gcs_path": {
            "type": "string",
            "description": "Local file path or GCS URI to load data from. Optional if session_id is used.",
        },
        "session_id": {
            "type": "string",
            "description": "Optional: In-memory session identifier.",
        },
        "run_id": {
            "type": "string",
            "description": "Optional run identifier.",
        },
        "runtime": {
            "type": ["object", "string"],
            "description": (
                "Optional runtime overlay dict or YAML string for shared run-scoped "
                "settings such as run_id, input_path, export_html, plotting, and destinations."
            ),
            "default": {},
        },
        "async_mode": {
            "type": "boolean",
            "description": "If true, queue background execution and return job_id immediately.",
            "default": False,
        },
    },
    "anyOf": [
        {"required": ["gcs_path"]},
        {"required": ["session_id"]},
    ],
}

from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="auto_heal",
    fn=_toolkit_auto_heal,
    description="Automatically infer and apply cleaning rules (normalization, imputation) in one step.",
    input_schema=_INPUT_SCHEMA,
)
