"""MCP tool: toolkit_auto_heal — infer and apply cleaning rules in one go."""

import yaml

from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    default_run_id,
    get_session_metadata,
    load_input,
    save_to_session,
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

    # Final Metadata
    metadata = get_session_metadata(current_session_id) or {}
    row_count = metadata.get("row_count")

    res = {
        "status": "pass",
        "module": "auto_heal",
        "run_id": run_id,
        "session_id": current_session_id,
        "summary": {**summary, "row_count": row_count},
        "artifact_path": summary.get("imputation", {}).get("artifact_path") or summary.get("normalization", {}).get("artifact_path", ""),
        "artifact_url": summary.get("imputation", {}).get("artifact_url") or summary.get("normalization", {}).get("artifact_url", ""),
        "export_url": summary.get("imputation", {}).get("export_url") or summary.get("normalization", {}).get("export_url", ""),
        "plot_urls": summary.get("imputation", {}).get("plot_urls") or summary.get("normalization", {}).get("plot_urls", {}),
        "message": "Auto-healing completed. Normalization and Imputation applied based on inference.",
    }
    append_to_run_history(run_id, res)
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
