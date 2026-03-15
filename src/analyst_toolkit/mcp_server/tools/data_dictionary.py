"""MCP tool: data_dictionary — artifact-first data dictionary and prelaunch report flow."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from analyst_toolkit.m00_utils.data_dictionary_builder import build_data_dictionary_report
from analyst_toolkit.m00_utils.export_utils import export_dataframes, export_html_report
from analyst_toolkit.mcp_server.io import (
    append_to_run_history,
    compact_destination_metadata,
    deliver_artifact,
    empty_delivery_state,
    load_input,
    resolve_run_context,
    save_to_session,
)
from analyst_toolkit.mcp_server.registry import register_tool
from analyst_toolkit.mcp_server.response_utils import (
    attach_trace_id,
    new_trace_id,
    next_action,
    with_dashboard_artifact,
    with_next_actions,
)
from analyst_toolkit.mcp_server.runtime_overlay import (
    normalize_runtime_overlay,
    runtime_to_tool_overrides,
)
from analyst_toolkit.mcp_server.tools.cockpit_schemas import DATA_DICTIONARY_INPUT_SCHEMA
from analyst_toolkit.mcp_server.tools.infer_configs import _toolkit_infer_configs

logger = logging.getLogger("analyst_toolkit.mcp_server.data_dictionary")


def _should_export_dictionary_html(runtime_cfg: dict[str, Any]) -> bool:
    raw = runtime_cfg.get("artifacts", {}).get("export_html")
    if isinstance(raw, bool):
        return raw
    return True


def _preview_rows(frame: Any, *, limit: int) -> list[dict[str, str]]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return []
    preview = frame.head(limit).copy()
    preview = preview.fillna("")
    return [
        {str(column): str(value) for column, value in row.items()}
        for row in preview.to_dict(orient="records")
    ]


def _build_cockpit_preview(report: dict[str, Any]) -> dict[str, Any]:
    overview_df = report.get("overview")
    overview_row = (
        overview_df.iloc[0].to_dict()
        if isinstance(overview_df, pd.DataFrame) and not overview_df.empty
        else {}
    )
    readiness_df = report.get("prelaunch_readiness")
    expected_df = report.get("expected_schema")
    return {
        "overview": {
            "rows": int(overview_row.get("Rows", 0) or 0),
            "columns": int(overview_row.get("Columns", 0) or 0),
            "expected_columns": int(overview_row.get("Expected Columns", 0) or 0),
            "metadata_gaps": int(overview_row.get("Metadata Gaps", 0) or 0),
            "inference_seeded": str(overview_row.get("Inference Seeded", "False")),
            "profile_depth": str(overview_row.get("Profile Depth", "")),
        },
        "expected_schema_preview": _preview_rows(expected_df, limit=5),
        "readiness_preview": _preview_rows(readiness_df, limit=5),
    }


async def _toolkit_data_dictionary(
    gcs_path: str | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    runtime: dict | str | None = None,
    profile_depth: str = "standard",
    include_examples: bool = True,
    prelaunch_report: bool = True,
) -> dict[str, Any]:
    trace_id = new_trace_id()
    try:
        runtime_cfg, runtime_warnings = normalize_runtime_overlay(runtime)
        runtime_overrides = runtime_to_tool_overrides(runtime_cfg)
        runtime_applied = bool(runtime_cfg)
        gcs_path = gcs_path or runtime_overrides.get("gcs_path")
        session_id = session_id or runtime_overrides.get("session_id")
        run_id = run_id or runtime_overrides.get("run_id") or "data_dictionary_prelaunch"

        delivery_config = {
            key: runtime_overrides.get(key)
            for key in (
                "output_bucket",
                "output_prefix",
                "local_output_root",
                "drive_folder_id",
                "upload_artifacts",
            )
            if runtime_overrides.get(key) is not None
        }

        run_id, lifecycle = resolve_run_context(run_id, session_id)
        df = load_input(gcs_path, session_id=session_id)

        if not session_id:
            session_id = save_to_session(df, run_id=run_id)
        else:
            save_to_session(df, session_id=session_id, run_id=run_id)

        infer_result = await _toolkit_infer_configs(
            gcs_path=gcs_path,
            session_id=session_id,
            runtime=runtime,
            run_id=run_id,
        )
        inferred_configs = (
            infer_result.get("configs", {}) if infer_result.get("status") == "pass" else {}
        )
        warnings: list[str] = []
        warnings.extend(runtime_warnings)
        warnings.extend(lifecycle["warnings"])
        if infer_result.get("status") != "pass":
            warnings.append(
                "infer_configs did not return a full config contract; data dictionary is based on observed profiling plus partial inferred context."
            )
            infer_status = "warn"
        else:
            infer_status = "pass"
            warnings.extend(infer_result.get("warnings", []))

        report = build_data_dictionary_report(
            df,
            inferred_configs=inferred_configs,
            profile_depth=profile_depth,
            include_examples=include_examples,
            prelaunch_report=prelaunch_report,
        )

        xlsx_path = f"exports/reports/data_dictionary/{run_id}_data_dictionary_report.xlsx"
        export_dataframes(
            {
                key: value
                for key, value in report.items()
                if not str(key).startswith("__") and isinstance(value, pd.DataFrame)
            },
            xlsx_path,
            file_format="xlsx",
            run_id=run_id,
            logging_mode="off",
        )
        xlsx_delivery = deliver_artifact(
            xlsx_path,
            run_id,
            "data_dictionary",
            config=delivery_config,
            session_id=session_id,
        )
        warnings.extend(xlsx_delivery["warnings"])

        artifact_delivery = empty_delivery_state()
        artifact_path = ""
        artifact_url = ""
        if _should_export_dictionary_html(runtime_cfg):
            artifact_path = f"exports/reports/data_dictionary/{run_id}_data_dictionary_report.html"
            output_path = export_html_report(report, artifact_path, "Data Dictionary", run_id)
            artifact_delivery = deliver_artifact(
                output_path,
                run_id,
                "data_dictionary",
                config=delivery_config,
                session_id=session_id,
            )
            artifact_path = str(artifact_delivery.get("local_path", output_path))
            artifact_url = str(artifact_delivery.get("url", ""))
            warnings.extend(artifact_delivery["warnings"])

        readiness_df = report.get("prelaunch_readiness")
        gap_count = len(readiness_df) if isinstance(readiness_df, pd.DataFrame) else 0
        status = str(report.get("__dashboard_meta__", {}).get("status", "warn"))
        if infer_status == "warn" and status == "pass":
            status = "warn"

        result = {
            "status": status,
            "module": "data_dictionary",
            "run_id": run_id,
            "session_id": session_id,
            "summary": {
                "row_count": len(df),
                "column_count": len(df.columns),
                "metadata_gap_count": gap_count,
                "profile_depth": profile_depth,
                "include_examples": include_examples,
                "prelaunch_report": prelaunch_report,
                "inference_status": infer_status,
            },
            "artifact_path": artifact_path,
            "artifact_url": artifact_url,
            "xlsx_path": str(xlsx_delivery.get("local_path", "")),
            "xlsx_url": str(xlsx_delivery.get("url", "")),
            "template_path": "config/data_dictionary_request_template.yaml",
            "cockpit_preview": _build_cockpit_preview(report),
            "input_echo": {
                "gcs_path": gcs_path or "",
                "runtime_present": runtime_applied,
                "prelaunch_report": prelaunch_report,
            },
            "destination_delivery": {
                "html_report": compact_destination_metadata(artifact_delivery["destinations"]),
                "xlsx_report": compact_destination_metadata(xlsx_delivery["destinations"]),
            },
            "warnings": warnings,
            "runtime_applied": runtime_applied,
        }
        result = with_dashboard_artifact(
            result,
            artifact_path=artifact_path,
            artifact_url=artifact_url,
            label="Data dictionary dashboard",
        )
        result = with_next_actions(
            result,
            [
                next_action(
                    "get_cockpit_dashboard",
                    "Open the cockpit to review the latest dictionary surface beside other operator-facing artifacts.",
                    {},
                ),
                next_action(
                    "get_capability_catalog",
                    "Cross-check inferred config knobs and template paths before turning the prelaunch contract into executable module configs.",
                    {},
                ),
                next_action(
                    "validation",
                    "Use the inferred validation contract as the next executable quality gate after prelaunch review.",
                    {"session_id": session_id, "run_id": run_id},
                ),
            ],
        )
        append_to_run_history(run_id, result, session_id=session_id)
        return attach_trace_id(result, trace_id=trace_id)
    except FileNotFoundError:
        return {
            "status": "error",
            "module": "data_dictionary",
            "error_code": "INPUT_NOT_FOUND",
            "trace_id": trace_id,
            "message": "Input file could not be located.",
        }
    except Exception:
        logger.exception("data_dictionary failed (trace_id=%s)", trace_id)
        return {
            "status": "error",
            "module": "data_dictionary",
            "error_code": "INTERNAL_ERROR",
            "trace_id": trace_id,
            "message": "An unexpected error occurred while building the data dictionary.",
        }


register_tool(
    name="data_dictionary",
    fn=_toolkit_data_dictionary,
    description=(
        "Build a compact, artifact-first data dictionary and prelaunch readiness dashboard seeded "
        "from profiling plus infer_configs output."
    ),
    input_schema=DATA_DICTIONARY_INPUT_SCHEMA,
)
