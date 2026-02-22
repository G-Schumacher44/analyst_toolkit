"""MCP tool: toolkit_drift_detection — compare two datasets for schema or distribution changes."""

import pandas as pd
import numpy as np
from analyst_toolkit.mcp_server.io import load_input, default_run_id


async def _toolkit_drift_detection(
    base_path: str | None = None,
    base_session_id: str | None = None,
    target_path: str | None = None,
    target_session_id: str | None = None,
    run_id: str | None = None,
) -> dict:
    """
    Compare two datasets (base vs target) to detect schema drift or distribution changes.
    """
    run_id = run_id or default_run_id()
    
    df_base = load_input(base_path, session_id=base_session_id)
    df_target = load_input(target_path, session_id=target_session_id)
    
    # 1. Schema comparison
    base_cols = set(df_base.columns)
    target_cols = set(df_target.columns)
    
    added_cols = list(target_cols - base_cols)
    removed_cols = list(base_cols - target_cols)
    common_cols = list(base_cols & target_cols)
    
    dtype_changes = {}
    for col in common_cols:
        if df_base[col].dtype != df_target[col].dtype:
            dtype_changes[col] = {
                "base": str(df_base[col].dtype),
                "target": str(df_target[col].dtype)
            }
            
    # 2. Simple distribution comparison for numeric columns
    drift_metrics = {}
    numeric_cols = [c for c in common_cols if pd.api.types.is_numeric_dtype(df_base[c]) and pd.api.types.is_numeric_dtype(df_target[c])]
    
    for col in numeric_cols:
        base_mean = float(df_base[col].mean())
        target_mean = float(df_target[col].mean())
        mean_diff_pct = abs(target_mean - base_mean) / (abs(base_mean) + 1e-9)
        
        drift_metrics[col] = {
            "base_mean": round(base_mean, 4),
            "target_mean": round(target_mean, 4),
            "diff_pct": round(mean_diff_pct, 4)
        }

    summary = {
        "added_columns": len(added_cols),
        "removed_columns": len(removed_cols),
        "dtype_changes": len(dtype_changes),
        "drift_detected": len(dtype_changes) > 0 or any(m["diff_pct"] > 0.1 for m in drift_metrics.values())
    }
    
    return {
        "status": "warn" if summary["drift_detected"] else "pass",
        "module": "drift_detection",
        "run_id": run_id,
        "summary": summary,
        "added_columns": added_cols,
        "removed_columns": removed_cols,
        "dtype_changes": dtype_changes,
        "numeric_drift": drift_metrics
    }


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "base_path": {"type": "string", "description": "Path to the base (reference) dataset."},
        "base_session_id": {"type": "string", "description": "Session ID for the base dataset."},
        "target_path": {"type": "string", "description": "Path to the target (new) dataset."},
        "target_session_id": {"type": "string", "description": "Session ID for the target dataset."},
        "run_id": {"type": "string", "description": "Optional run identifier."},
    },
    "anyOf": [
        {"required": ["base_path", "target_path"]},
        {"required": ["base_session_id", "target_session_id"]},
        {"required": ["base_path", "target_session_id"]},
        {"required": ["base_session_id", "target_path"]},
    ],
}

from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="toolkit_drift_detection",
    fn=_toolkit_drift_detection,
    description="Compare two datasets to detect schema drift and statistical changes.",
    input_schema=_INPUT_SCHEMA,
)
