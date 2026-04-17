"""MCP tool: toolkit_drift_detection — compare two datasets for schema or distribution changes."""

from typing import Any

import numpy as np
import pandas as pd

from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    build_artifact_contract,
    compact_destination_metadata,
    default_run_id,
    deliver_artifact,
    fold_status_with_artifacts,
    generate_default_export_path,
    load_input,
    save_output,
    split_artifact_reference,
)


async def _toolkit_drift_detection(
    base_path: str | None = None,
    base_session_id: str | None = None,
    target_path: str | None = None,
    target_session_id: str | None = None,
    run_id: str | None = None,
    **kwargs,
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

    base_row_count = len(df_base)
    target_row_count = len(df_target)

    added_cols = list(target_cols - base_cols)
    removed_cols = list(base_cols - target_cols)
    common_cols = list(base_cols & target_cols)

    dtype_changes = {}
    for col in common_cols:
        if df_base[col].dtype != df_target[col].dtype:
            dtype_changes[col] = {
                "base": str(df_base[col].dtype),
                "target": str(df_target[col].dtype),
            }

    # 2. Simple distribution comparison for numeric columns
    drift_metrics = {}
    numeric_cols = [
        c
        for c in common_cols
        if pd.api.types.is_numeric_dtype(df_base[c]) and pd.api.types.is_numeric_dtype(df_target[c])
    ]

    for col in numeric_cols:
        base_mean = float(df_base[col].mean())
        target_mean = float(df_target[col].mean())
        mean_diff_pct = abs(target_mean - base_mean) / (abs(base_mean) + 1e-9)

        drift_metrics[col] = {
            "base_mean": round(base_mean, 4),
            "target_mean": round(target_mean, 4),
            "diff_pct": round(mean_diff_pct, 4),
        }

    summary = {
        "added_columns": len(added_cols),
        "removed_columns": len(removed_cols),
        "dtype_changes": len(dtype_changes),
        "drift_detected": len(dtype_changes) > 0
        or any(m["diff_pct"] > 0.1 for m in drift_metrics.values()),
        "base_row_count": base_row_count,
        "target_row_count": target_row_count,
    }

    # Save drift results to a physical file for persistence/audit
    drift_df = pd.DataFrame([summary])
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "drift_detection"
    )
    export_url = save_output(drift_df, export_path)
    export_local_path, export_remote_url = split_artifact_reference(export_url)
    export_delivery: dict[str, Any] = {
        "reference": export_url,
        "local_path": export_local_path,
        "url": export_remote_url,
        "warnings": [],
        "destinations": {},
    }
    if export_local_path:
        export_delivery = deliver_artifact(
            export_local_path,
            run_id,
            "drift_detection/data",
            config=kwargs,
            session_id=None,
        )
        export_url = str(export_delivery["reference"])

    artifact_contract = build_artifact_contract(
        export_url,
        export_path=str(export_delivery["local_path"]),
        expect_html=False,
        expect_xlsx=False,
        probe_local_paths=True,
    )
    warnings = list(export_delivery["warnings"]) + artifact_contract["artifact_warnings"]
    base_status = "warn" if summary["drift_detected"] else ("warn" if warnings else "pass")
    status = fold_status_with_artifacts(
        base_status, artifact_contract["missing_required_artifacts"]
    )

    res = {
        "status": status,
        "module": "drift_detection",
        "run_id": run_id,
        "summary": summary,
        "added_columns": added_cols,
        "removed_columns": removed_cols,
        "dtype_changes": dtype_changes,
        "numeric_drift": drift_metrics,
        "export_url": export_url,
        "destination_delivery": {
            "data_export": compact_destination_metadata(export_delivery["destinations"]),
        },
        "warnings": warnings,
        "artifact_matrix": artifact_contract["artifact_matrix"],
        "expected_artifacts": artifact_contract["expected_artifacts"],
        "uploaded_artifacts": artifact_contract["uploaded_artifacts"],
        "missing_required_artifacts": artifact_contract["missing_required_artifacts"],
    }
    append_to_run_history(run_id, res)
    return res


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "base_path": {"type": "string", "description": "Path to the base (reference) dataset."},
        "base_session_id": {"type": "string", "description": "Session ID for the base dataset."},
        "target_path": {"type": "string", "description": "Path to the target (new) dataset."},
        "target_session_id": {
            "type": "string",
            "description": "Session ID for the target dataset.",
        },
        "run_id": {"type": "string", "description": "Optional run identifier."},
    },
}

from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="drift_detection",
    fn=_toolkit_drift_detection,
    description="Compare two datasets to detect schema drift and statistical changes.",
    input_schema=_INPUT_SCHEMA,
)
