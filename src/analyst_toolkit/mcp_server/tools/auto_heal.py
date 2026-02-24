"""MCP tool: toolkit_auto_heal — infer and apply cleaning rules in one go."""

import yaml

from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    default_run_id,
    get_session_metadata,
)
from analyst_toolkit.mcp_server.tools.imputation import _toolkit_imputation
from analyst_toolkit.mcp_server.tools.infer_configs import _toolkit_infer_configs
from analyst_toolkit.mcp_server.tools.normalization import _toolkit_normalization


async def _toolkit_auto_heal(
    gcs_path: str | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
) -> dict:
    """
    Run inference, then automatically apply recommended normalization and imputation.
    Returns a cleaned session_id.
    """
    run_id = run_id or default_run_id()

    # 1. Infer configs
    infer_res = await _toolkit_infer_configs(
        gcs_path=gcs_path, session_id=session_id, modules=["normalization", "imputation"]
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
                session_id=current_session_id, config=actual_cfg, run_id=run_id
            )
            current_session_id = norm_res.get("session_id")
            summary["normalization"] = norm_res.get("summary")
        except Exception as e:
            summary["normalization"] = {"error": str(e)}
            failed_steps.append("normalization")

    # 3. Apply Imputation if inferred
    if "imputation" in configs:
        try:
            imp_cfg_str = configs["imputation"]
            imp_cfg = yaml.safe_load(imp_cfg_str)
            actual_cfg = imp_cfg.get("imputation", imp_cfg)

            imp_res = await _toolkit_imputation(
                session_id=current_session_id, config=actual_cfg, run_id=run_id
            )
            current_session_id = imp_res.get("session_id")
            summary["imputation"] = imp_res.get("summary")
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

    res = {
        "status": status,
        "module": "auto_heal",
        "run_id": run_id,
        "session_id": current_session_id,
        "summary": {**summary, "row_count": row_count},
        "artifact_path": imp_res.get("artifact_path") or norm_res.get("artifact_path", ""),
        "artifact_url": imp_res.get("artifact_url") or norm_res.get("artifact_url", ""),
        "export_url": imp_res.get("export_url") or norm_res.get("export_url", ""),
        "plot_urls": imp_res.get("plot_urls") or norm_res.get("plot_urls", {}),
        "failed_steps": failed_steps,
        "message": "Auto-healing completed. Normalization and Imputation applied based on inference.",
    }
    append_to_run_history(run_id, res, session_id=current_session_id)
    return res


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
