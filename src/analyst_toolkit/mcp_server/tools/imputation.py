"""MCP tool: toolkit_imputation — missing value imputation via M07."""

from pathlib import Path
from typing import Any

import pandas as pd

from analyst_toolkit.m07_imputation.run_imputation_pipeline import (
    run_imputation_pipeline,
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
    get_session_metadata,
    load_input,
    resolve_run_context,
    save_output,
    save_to_session,
    should_export_html,
    split_artifact_reference,
)
from analyst_toolkit.mcp_server.response_utils import with_dashboard_artifact
from analyst_toolkit.mcp_server.runtime_overlay import (
    normalize_runtime_overlay,
    resolve_layered_config,
    runtime_to_config_overlay,
    runtime_to_tool_overrides,
)
from analyst_toolkit.mcp_server.schemas import base_input_schema


def _column_null_count(df: pd.DataFrame, column: str) -> int:
    values = df[column]
    if isinstance(values, pd.DataFrame):
        return int(values.isnull().sum().sum())
    return int(values.isnull().sum())


async def _toolkit_imputation(
    gcs_path: str | None = None,
    session_id: str | None = None,
    input_id: str | None = None,
    config: dict | None = None,
    runtime: dict | str | None = None,
    run_id: str | None = None,
    **kwargs,
) -> dict:
    """Run missing value imputation on the dataset at gcs_path or session_id."""
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

    config = coerce_config(config, "imputation")
    config, runtime_meta = resolve_layered_config(
        provided=config,
        explicit=runtime_to_config_overlay(runtime_cfg),
    )
    df = load_input(gcs_path, session_id=session_id, input_id=input_id)

    base_cfg = config.get("imputation", config)

    # Build module config for the pipeline runner
    module_cfg = {
        "imputation": {
            **base_cfg,
            "logging": "off",
            "settings": {
                "export": {"run": True, "export_html": should_export_html(config)},
                "plotting": {"run": True},
            },
        }
    }

    # run_imputation_pipeline returns the imputed dataframe
    df_imputed = run_imputation_pipeline(config=module_cfg, df=df, notebook=False, run_id=run_id)

    # Save to session
    session_id = save_to_session(df_imputed, session_id=session_id, run_id=run_id)
    metadata = get_session_metadata(session_id) or {}
    row_count = metadata.get("row_count")

    # Handle explicit or default export
    export_path = kwargs.get("export_path") or generate_default_export_path(
        run_id, "imputation", session_id=session_id
    )
    export_url = save_output(df_imputed, export_path)
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
            "imputation/data",
            config=kwargs,
            session_id=session_id,
        )
        export_url = export_delivery["reference"]

    # We need to compute these for the MCP response summary
    nulls_before = int(df.isnull().sum().sum())
    nulls_after = int(df_imputed.isnull().sum().sum())
    nulls_filled = nulls_before - nulls_after

    # Simple way to get columns imputed
    unique_columns = list(dict.fromkeys(df.columns))
    columns_imputed = [
        c for c in unique_columns if _column_null_count(df, c) > _column_null_count(df_imputed, c)
    ]

    artifact_path = ""
    artifact_url = ""
    xlsx_url = ""
    plot_urls = {}
    artifact_delivery: dict[str, Any] = empty_delivery_state()
    xlsx_delivery: dict[str, Any] = empty_delivery_state()
    plot_delivery: dict[str, dict] = {}

    warnings: list = []
    warnings.extend(lifecycle["warnings"])
    warnings.extend(runtime_warnings)
    warnings.extend(runtime_meta["runtime_warnings"])
    warnings.extend(export_delivery["warnings"])

    if should_export_html(config):
        artifact_path = f"exports/reports/imputation/{run_id}_imputation_report.html"
        artifact_delivery = deliver_artifact(
            artifact_path,
            run_id,
            "imputation",
            config=kwargs,
            session_id=session_id,
        )
        artifact_path = artifact_delivery["local_path"]
        artifact_url = artifact_delivery["url"]
        warnings.extend(artifact_delivery["warnings"])

        xlsx_path = f"exports/reports/imputation/{run_id}_imputation_report.xlsx"
        xlsx_delivery = deliver_artifact(
            xlsx_path,
            run_id,
            "imputation",
            config=kwargs,
            session_id=session_id,
        )
        xlsx_url = xlsx_delivery["url"]
        warnings.extend(xlsx_delivery["warnings"])

        # Upload plots - search both root and run_id subdir
        plot_dirs = [
            Path("exports/plots/imputation"),
            Path(f"exports/plots/imputation/{run_id}"),
        ]
        for plot_dir in plot_dirs:
            if plot_dir.exists():
                for plot_file in plot_dir.glob(f"*{run_id}*.png"):
                    delivered = deliver_artifact(
                        str(plot_file),
                        run_id,
                        "imputation/plots",
                        config=kwargs,
                        session_id=session_id,
                    )
                    plot_delivery[plot_file.name] = delivered
                    warnings.extend(delivered["warnings"])
                    if delivered["url"]:
                        plot_urls[plot_file.name] = delivered["url"]

    artifact_contract = build_artifact_contract(
        export_url,
        export_path=export_delivery["local_path"],
        artifact_path=artifact_path,
        artifact_url=artifact_url,
        xlsx_path=xlsx_delivery["local_path"],
        xlsx_url=xlsx_url,
        plot_paths={
            name: item["local_path"] for name, item in plot_delivery.items() if item["local_path"]
        },
        plot_urls=plot_urls,
        expect_html=should_export_html(config),
        expect_xlsx=should_export_html(config),
        expect_plots=should_export_html(config),
        required_html=False,
        probe_local_paths=True,
    )
    warnings.extend(artifact_contract["artifact_warnings"])
    base_status = "warn" if warnings else "pass"
    status = fold_status_with_artifacts(
        base_status, artifact_contract["missing_required_artifacts"]
    )

    res = {
        "status": status,
        "module": "imputation",
        "run_id": run_id,
        "session_id": session_id,
        "summary": {
            "columns_imputed": columns_imputed,
            "nulls_filled": nulls_filled,
            "row_count": row_count,
        },
        "columns_imputed": columns_imputed,
        "nulls_filled": nulls_filled,
        "artifact_path": artifact_path,
        "artifact_url": artifact_url,
        "xlsx_url": xlsx_url,
        "plot_urls": plot_urls,
        "export_url": export_url,
        "destination_delivery": {
            "data_export": compact_destination_metadata(export_delivery["destinations"]),
            "html_report": compact_destination_metadata(artifact_delivery["destinations"]),
            "xlsx_report": compact_destination_metadata(xlsx_delivery["destinations"]),
            "plots": {
                name: compact_destination_metadata(delivery["destinations"])
                for name, delivery in plot_delivery.items()
            },
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
        label="Imputation dashboard",
    )
    append_to_run_history(run_id, res, session_id=session_id)
    return res


from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="imputation",
    fn=_toolkit_imputation,
    description="Run missing value imputation on a dataset using configured rules and return a standalone imputation dashboard artifact.",
    input_schema=base_input_schema(),
)
