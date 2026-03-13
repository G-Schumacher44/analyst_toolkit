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
    export_path: str = "",
    artifact_path: str = "",
    artifact_url: str = "",
    xlsx_path: str = "",
    xlsx_url: str = "",
    plot_paths: dict[str, str] | None = None,
    plot_urls: dict[str, str] | None = None,
    expect_html: bool = False,
    expect_xlsx: bool = False,
    expect_plots: bool = False,
    required_html: bool = False,
    required_xlsx: bool = False,
    required_data_export: bool = True,
) -> dict[str, Any]:
    plots = plot_urls or {}
    plot_refs = plot_paths or {}
    data_export_status, data_export_reason = _resolve_reference_status(export_url, export_path)
    html_status, html_reason = _resolve_reference_status(artifact_url, artifact_path)
    xlsx_status, xlsx_reason = _resolve_reference_status(xlsx_url, xlsx_path)
    plots_available = bool(plots) or bool(plot_refs)
    plot_reason = "" if plots_available else "not_generated_or_upload_failed"
    plot_status = "available" if plots_available else "missing"
    matrix: dict[str, dict[str, Any]] = {
        "data_export": {
            "expected": True,
            "required": required_data_export,
            "status": data_export_status,
            "url": export_url,
            "path": export_path,
            "reason": data_export_reason,
        },
        "html_report": {
            "expected": expect_html,
            "required": required_html and expect_html,
            "status": "disabled" if not expect_html else html_status,
            "url": artifact_url if expect_html else "",
            "path": artifact_path if expect_html else "",
            "reason": "disabled" if not expect_html else html_reason,
        },
        "xlsx_report": {
            "expected": expect_xlsx,
            "required": required_xlsx and expect_xlsx,
            "status": "disabled" if not expect_xlsx else xlsx_status,
            "url": xlsx_url if expect_xlsx else "",
            "path": xlsx_path if expect_xlsx else "",
            "reason": "disabled" if not expect_xlsx else xlsx_reason,
        },
        "plots": {
            "expected": expect_plots,
            "required": False,
            "status": "disabled" if not expect_plots else plot_status,
            "count": max(len(plots), len(plot_refs)) if expect_plots else 0,
            "urls": plots if expect_plots else {},
            "paths": plot_refs if expect_plots else {},
            "reason": "disabled" if not expect_plots else plot_reason,
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
    for artifact_name in ("html_report", "xlsx_report"):
        if matrix[artifact_name].get("reason") == "server_local_path":
            warnings.append(
                f"{artifact_name} path is local to MCP server runtime filesystem and may not be "
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


def _resolve_reference_status(url: str, path: str = "") -> tuple[str, str]:
    if url:
        if url.startswith("gs://") or url.startswith("http://") or url.startswith("https://"):
            return "available", ""

        local_url_path = Path(url)
        if local_url_path.exists():
            return "available", "server_local_path"
        return "missing", "local_path_not_found"

    if not path:
        return "missing", "upload_failed"

    local_path = Path(path)
    if local_path.exists():
        return "available", "server_local_path"
    return "missing", "local_path_not_found"
