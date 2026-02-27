"""Serialization and artifact contract helpers for MCP IO."""

import json
import math
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import pandas as pd


def build_artifact_contract(
    export_url: str,
    *,
    artifact_url: str = "",
    xlsx_url: str = "",
    plot_urls: dict[str, str] | None = None,
    expect_html: bool = False,
    expect_xlsx: bool = False,
    expect_plots: bool = False,
    required_html: bool = False,
    required_xlsx: bool = False,
    required_data_export: bool = True,
) -> dict[str, Any]:
    plots = plot_urls or {}
    data_export_status, data_export_reason = _resolve_data_export_status(export_url)
    matrix: dict[str, dict[str, Any]] = {
        "data_export": {
            "expected": True,
            "required": required_data_export,
            "status": data_export_status,
            "url": export_url,
            "reason": data_export_reason,
        },
        "html_report": {
            "expected": expect_html,
            "required": required_html and expect_html,
            "status": (
                "disabled"
                if not expect_html
                else ("available" if bool(artifact_url) else "missing")
            ),
            "url": artifact_url if expect_html else "",
            "reason": (
                "disabled"
                if not expect_html
                else ("" if artifact_url else "upload_failed_or_not_generated")
            ),
        },
        "xlsx_report": {
            "expected": expect_xlsx,
            "required": required_xlsx and expect_xlsx,
            "status": (
                "disabled" if not expect_xlsx else ("available" if bool(xlsx_url) else "missing")
            ),
            "url": xlsx_url if expect_xlsx else "",
            "reason": (
                "disabled"
                if not expect_xlsx
                else ("" if xlsx_url else "upload_failed_or_not_generated")
            ),
        },
        "plots": {
            "expected": expect_plots,
            "required": False,
            "status": (
                "disabled" if not expect_plots else ("available" if len(plots) > 0 else "missing")
            ),
            "count": len(plots) if expect_plots else 0,
            "urls": plots if expect_plots else {},
            "reason": (
                "disabled"
                if not expect_plots
                else ("" if len(plots) > 0 else "not_generated_or_upload_failed")
            ),
        },
    }

    expected = [name for name, item in matrix.items() if bool(item.get("expected"))]
    uploaded = [
        name
        for name, item in matrix.items()
        if item.get("status") == "available"
        and (bool(item.get("url")) or (name == "plots" and int(item.get("count", 0)) > 0))
    ]
    missing_required = [
        name
        for name, item in matrix.items()
        if bool(item.get("required")) and item.get("status") != "available"
    ]
    warnings = [
        f"Missing required artifact: {name} ({matrix[name].get('reason', 'missing')})"
        for name in missing_required
    ]
    if data_export_reason == "server_local_path":
        warnings.append(
            "Data export path is local to MCP server runtime filesystem and may not be "
            "directly accessible from the client host."
        )
    return {
        "artifact_matrix": matrix,
        "expected_artifacts": expected,
        "uploaded_artifacts": uploaded,
        "missing_required_artifacts": missing_required,
        "artifact_warnings": warnings,
    }


def fold_status_with_artifacts(status: str, missing_required_artifacts: list[str]) -> str:
    if status in {"error", "fail"}:
        return status
    if missing_required_artifacts:
        return "warn"
    return status


def make_json_safe(value: Any) -> Any:
    """Recursively convert values into JSON-serializable primitives."""
    if value is None or isinstance(value, (str, bool, int)):
        return value

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, (datetime, date, time)):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if isinstance(value, pd.Timedelta):
        return str(value)

    if isinstance(value, pd.DataFrame):
        return {
            "_type": "dataframe",
            "row_count": int(value.shape[0]),
            "column_count": int(value.shape[1]),
            "columns": [str(c) for c in value.columns.tolist()],
        }

    if isinstance(value, pd.Series):
        return {
            "_type": "series",
            "name": str(value.name) if value.name is not None else "",
            "length": int(len(value)),
            "dtype": str(value.dtype),
        }

    if isinstance(value, dict):
        return {str(k): make_json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(v) for v in value]

    if hasattr(value, "item") and callable(value.item):
        try:
            return make_json_safe(value.item())
        except Exception:
            pass

    try:
        json.dumps(value, allow_nan=False)
        return value
    except Exception:
        return str(value)


def _resolve_data_export_status(export_url: str) -> tuple[str, str]:
    if not export_url:
        return "missing", "upload_failed"

    if export_url.startswith("gs://"):
        return "available", ""

    local_path = Path(export_url)
    if local_path.exists():
        return "available", "server_local_path"
    return "missing", "local_path_not_found"
