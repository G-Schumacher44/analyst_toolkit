"""MCP tool: cockpit — user/agent guidance, capability catalog, history, and health scoring."""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

import yaml

from analyst_toolkit.m00_utils.scoring import calculate_health_score
from analyst_toolkit.mcp_server.io import get_run_history
from analyst_toolkit.mcp_server.registry import register_tool
from analyst_toolkit.mcp_server.templates import get_golden_configs

logger = logging.getLogger("analyst_toolkit.mcp_server.cockpit")


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


TEMPLATE_IO_TIMEOUT_SEC = _env_float("ANALYST_MCP_TEMPLATE_IO_TIMEOUT_SEC", 8.0)

_TEMPLATE_SPECS = [
    ("diagnostics", "diagnostics", "config/diag_config_template.yaml"),
    ("validation", "validation", "config/validation_config_template.yaml"),
    ("normalization", "normalization", "config/normalization_config_template.yaml"),
    ("duplicates", "duplicates", "config/dups_config_template.yaml"),
    ("outliers", "outlier_detection", "config/outlier_config_template.yaml"),
    ("imputation", "imputation", "config/imputation_config_template.yaml"),
    ("final_audit", "final_audit", "config/final_audit_config_template.yaml"),
]

_KEY_KNOBS: dict[str, list[dict[str, str]]] = {
    "diagnostics": [
        {"path": "profile.run", "description": "Master diagnostics profile toggle."},
        {"path": "plotting.run", "description": "Enable/disable diagnostics plots."},
        {
            "path": "profile.settings.max_rows",
            "description": "Preview row count in profile output.",
        },
        {
            "path": "profile.settings.quality_checks.skew_threshold",
            "description": "Threshold for skewness warnings.",
        },
        {"path": "profile.settings.export_html", "description": "Export diagnostics HTML report."},
    ],
    "validation": [
        {"path": "schema_validation.run", "description": "Master validation toggle."},
        {
            "path": "schema_validation.rules.expected_columns",
            "description": "Enforce expected schema columns.",
        },
        {
            "path": "schema_validation.rules.categorical_values",
            "description": "Allowed values for categorical fields.",
        },
        {
            "path": "schema_validation.rules.numeric_ranges",
            "description": "Numeric min/max constraints.",
        },
        {"path": "settings.export_html", "description": "Export validation HTML report."},
    ],
    "normalization": [
        {"path": "run", "description": "Master normalization toggle."},
        {
            "path": "rules.standardize_text_columns",
            "description": "Columns to trim/normalize casing.",
        },
        {"path": "rules.value_mappings", "description": "Explicit remapping dictionary."},
        {
            "path": "rules.fuzzy_matching.run",
            "description": "Enable fuzzy typo correction for configured columns.",
        },
        {
            "path": "rules.fuzzy_matching.settings.<column>.master_list",
            "description": "Canonical values for fuzzy matching.",
        },
        {
            "path": "rules.fuzzy_matching.settings.<column>.score_cutoff",
            "description": "Minimum fuzzy score to apply corrections.",
        },
        {
            "path": "rules.parse_datetimes",
            "description": "Datetime parsing formats and strictness per column.",
        },
        {"path": "rules.coerce_dtypes", "description": "Explicit dtype coercions."},
        {"path": "settings.export_html", "description": "Export normalization HTML report."},
    ],
    "duplicates": [
        {"path": "run", "description": "Master duplicates module toggle."},
        {
            "path": "subset_columns",
            "description": "Columns used for duplicate detection identity.",
        },
        {
            "path": "mode",
            "description": "Duplicate handling mode (e.g., remove/flag).",
        },
        {"path": "settings.plotting.run", "description": "Enable duplicate summary plotting."},
        {"path": "settings.export_html", "description": "Export duplicates HTML report."},
    ],
    "outliers": [
        {"path": "run", "description": "Master outlier detection toggle."},
        {
            "path": "detection_specs.__default__.method",
            "description": "Default outlier method (`iqr` or `zscore`).",
        },
        {
            "path": "detection_specs.<column>.iqr_multiplier",
            "description": "Column-level IQR sensitivity tuning.",
        },
        {
            "path": "detection_specs.<column>.zscore_threshold",
            "description": "Column-level z-score sensitivity tuning.",
        },
        {"path": "plotting.run", "description": "Enable outlier visualization output."},
        {"path": "export.export_html", "description": "Export outlier HTML report."},
    ],
    "imputation": [
        {"path": "run", "description": "Master imputation module toggle."},
        {
            "path": "rules.strategies",
            "description": "Column-wise fill strategies (mean/median/mode/constant).",
        },
        {"path": "settings.plotting.run", "description": "Enable before/after imputation plots."},
        {"path": "settings.export.export_html", "description": "Export imputation HTML report."},
    ],
    "final_audit": [
        {"path": "run", "description": "Master final audit/certification toggle."},
        {"path": "final_edits.run", "description": "Enable final edit pass before certification."},
        {
            "path": "certification.run",
            "description": "Run strict final certification checks.",
        },
        {"path": "settings.export_html", "description": "Export final audit HTML certificate."},
    ],
}


def _load_template_root(root_key: str, template_path: str) -> dict[str, Any]:
    path = Path(template_path)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}
    root = data.get(root_key, {})
    return root if isinstance(root, dict) else {}


def _extract_value(root: dict[str, Any], path: str) -> Any:
    """
    Resolve dot-path values from a template.
    Supports placeholder segments like <column> by stopping at that level.
    """
    current: Any = root
    for segment in path.split("."):
        if segment.startswith("<") and segment.endswith(">"):
            return "<per-column>"
        if not isinstance(current, dict) or segment not in current:
            return "<not set>"
        current = current[segment]
    return current


def _build_capability_catalog() -> dict[str, Any]:
    modules: list[dict[str, Any]] = []
    for tool_name, root_key, template_path in _TEMPLATE_SPECS:
        root = _load_template_root(root_key, template_path)
        knobs = []
        for knob in _KEY_KNOBS.get(tool_name, []):
            default_value = _extract_value(root, knob["path"])
            knobs.append(
                {
                    "path": knob["path"],
                    "default": default_value,
                    "description": knob["description"],
                    "user_editable": True,
                }
            )
        modules.append(
            {
                "tool": tool_name,
                "template_path": template_path,
                "config_root": root_key,
                "user_editable": True,
                "key_knobs": knobs,
            }
        )

    return {
        "status": "pass",
        "summary": {
            "editable_configs": True,
            "inference_available": True,
            "inference_tool": "infer_configs",
            "manual_override_recommended": True,
        },
        "global_controls": [
            {
                "path": "<module>.run",
                "description": "Enable/disable individual module execution.",
                "user_editable": True,
            },
            {
                "path": "module-specific",
                "description": "Plotting toggles are module-specific; use per-module key_knobs for exact paths.",
                "example_paths": [
                    "diagnostics.plotting.run",
                    "duplicates.settings.plotting.run",
                    "outlier_detection.plotting.run",
                    "imputation.settings.plotting.run",
                ],
                "user_editable": True,
            },
            {
                "path": "module-specific",
                "description": "HTML export toggles are module-specific; use per-module key_knobs for exact paths.",
                "example_paths": [
                    "diagnostics.profile.settings.export_html",
                    "validation.settings.export_html",
                    "normalization.settings.export_html",
                    "duplicates.settings.export_html",
                    "outlier_detection.export.export_html",
                    "imputation.settings.export.export_html",
                    "final_audit.settings.export_html",
                ],
                "user_editable": True,
            },
        ],
        "highlight_examples": [
            {
                "feature": "Normalization fuzzy matching",
                "paths": [
                    "normalization.rules.fuzzy_matching.run",
                    "normalization.rules.fuzzy_matching.settings.<column>.master_list",
                    "normalization.rules.fuzzy_matching.settings.<column>.score_cutoff",
                ],
            },
            {
                "feature": "Plotting controls",
                "paths": [
                    "diagnostics.plotting.run",
                    "outlier_detection.plotting.run",
                    "imputation.settings.plotting.run",
                ],
            },
        ],
        "golden_templates": sorted(get_golden_configs().keys()),
        "modules": modules,
    }


async def _toolkit_get_user_quickstart() -> dict:
    """Returns a concise, human-readable guide for operating the toolkit."""
    guide = """
# Analyst Toolkit Quickstart (Human)

## What You Can Control
- You can edit module YAML configs directly before runs.
- You can keep automation (`infer_configs`) and still override any field.
- You can enable/disable plotting and HTML export per module.

## Recommended Order (Manual Pipeline)
1. `diagnostics`
2. `infer_configs`
3. Review/edit configs (normalization, duplicates, outliers, imputation, validation)
4. `normalization` -> `duplicates` -> `outliers` -> `imputation` -> `validation`
5. `final_audit`
6. `get_run_history` + `get_data_health_report`

## Key Example: Fuzzy Matching
In normalization config:
- `normalization.rules.fuzzy_matching.run`
- `normalization.rules.fuzzy_matching.settings.<column>.master_list`
- `normalization.rules.fuzzy_matching.settings.<column>.score_cutoff`

Use this to auto-correct typos while controlling aggressiveness via score cutoff.

## Key Example: Plotting Controls
- `diagnostics.plotting.run`
- `outlier_detection.plotting.run`
- `imputation.settings.plotting.run`

Turn plotting off for speed on large datasets, on for exploratory analysis.
"""
    machine_guide = {
        "ordered_steps": [
            {
                "step": 1,
                "tool": "diagnostics",
                "required_inputs": ["gcs_path|session_id", "run_id"],
            },
            {
                "step": 2,
                "tool": "infer_configs",
                "required_inputs": ["gcs_path|session_id"],
            },
            {
                "step": 3,
                "tool_chain": [
                    "normalization",
                    "duplicates",
                    "outliers",
                    "imputation",
                    "validation",
                ],
                "required_inputs": ["session_id", "run_id", "config"],
            },
            {
                "step": 4,
                "tool": "final_audit",
                "required_inputs": ["session_id", "run_id"],
            },
        ],
        "example_calls": [
            {
                "tool": "diagnostics",
                "arguments": {"gcs_path": "gs://bucket/data.csv", "run_id": "run_001"},
            },
            {
                "tool": "infer_configs",
                "arguments": {"session_id": "<session_id_from_diagnostics>"},
            },
        ],
    }
    return {
        "status": "pass",
        "content": {
            "format": "markdown",
            "title": "Analyst Toolkit Quickstart",
            "markdown": guide.strip(),
        },
        "machine_guide": machine_guide,
        "quick_actions": [
            {
                "label": "Run diagnostics",
                "tool": "diagnostics",
                "arguments_schema_hint": {"required": ["gcs_path|session_id", "run_id"]},
            },
            {
                "label": "Infer configs",
                "tool": "infer_configs",
                "arguments_schema_hint": {"required": ["gcs_path|session_id"]},
            },
            {
                "label": "Open capabilities",
                "tool": "get_capability_catalog",
                "arguments_schema_hint": {"required": []},
            },
        ],
    }


async def _toolkit_get_agent_playbook() -> dict:
    """Returns strict, step-by-step playbook data for client agents."""
    return {
        "status": "pass",
        "version": "1.0",
        "goal": "Audit, clean, and certify a dataset with controlled user-editable configs.",
        "prerequisites": [
            "Input data path (local csv/parquet or gs:// URI) or existing session_id",
            "Stable run_id used across calls",
            "Optional output bucket/prefix overrides",
        ],
        "ordered_steps": [
            {
                "step": 1,
                "tool": "diagnostics",
                "required_inputs": ["gcs_path|session_id", "run_id"],
                "outputs": ["session_id", "summary", "artifact_url?", "plot_urls?"],
                "next": [2],
            },
            {
                "step": 2,
                "tool": "get_data_health_report",
                "required_inputs": ["run_id", "session_id?"],
                "outputs": ["health_score", "breakdown"],
                "next": [3],
            },
            {
                "step": 3,
                "tool": "infer_configs",
                "required_inputs": ["gcs_path|session_id"],
                "outputs": ["configs (YAML strings by module)"],
                "next": [4],
            },
            {
                "step": 4,
                "tool": "get_capability_catalog",
                "required_inputs": [],
                "outputs": ["editable knobs + defaults + example paths"],
                "next": [5],
            },
            {
                "step": 5,
                "tool": "manual config review",
                "required_inputs": ["inferred configs", "capability catalog", "user intent"],
                "outputs": ["confirmed config per module"],
                "notes": [
                    "Do not flatten nested keys.",
                    "User can override inferred config fields before execution.",
                ],
                "next": [6],
            },
            {
                "step": 6,
                "tool_chain": [
                    "normalization",
                    "duplicates",
                    "outliers",
                    "imputation",
                    "validation",
                ],
                "required_inputs": ["session_id", "run_id", "config per tool"],
                "outputs": ["updated session_id", "module summaries", "artifacts"],
                "next": [7],
            },
            {
                "step": 7,
                "tool_chain": ["final_audit", "get_run_history"],
                "required_inputs": ["session_id", "run_id"],
                "outputs": ["final certificate artifacts", "healing ledger"],
                "next": [],
            },
        ],
        "decision_gates": [
            {
                "name": "automation_mode",
                "rule": "Use auto_heal only when user explicitly requests one-shot automation.",
            },
            {
                "name": "plotting_mode",
                "rule": "Default plotting to off for large data; enable only when visual review is requested.",
            },
            {
                "name": "fuzzy_matching_mode",
                "rule": "If fuzzy matching is enabled, require explicit master_list and cutoff review.",
            },
        ],
    }


async def _toolkit_get_capability_catalog() -> dict:
    """Returns user-editable configuration capabilities by module/template."""
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_build_capability_catalog),
            timeout=TEMPLATE_IO_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        logger.error(
            "Capability catalog build timed out after %.1fs",
            TEMPLATE_IO_TIMEOUT_SEC,
        )
        return {
            "status": "error",
            "error": (
                "Capability catalog generation timed out. "
                f"Try increasing ANALYST_MCP_TEMPLATE_IO_TIMEOUT_SEC (current={TEMPLATE_IO_TIMEOUT_SEC}s)."
            ),
        }


async def _toolkit_get_golden_templates() -> dict:
    """Returns a library of 'Golden Config' templates."""
    try:
        templates = await asyncio.wait_for(
            asyncio.to_thread(get_golden_configs),
            timeout=TEMPLATE_IO_TIMEOUT_SEC,
        )
        return {"status": "pass", "templates": templates}
    except asyncio.TimeoutError:
        logger.error(
            "Golden templates read timed out after %.1fs",
            TEMPLATE_IO_TIMEOUT_SEC,
        )
        return {
            "status": "error",
            "error": (
                "Golden template loading timed out. "
                f"Try increasing ANALYST_MCP_TEMPLATE_IO_TIMEOUT_SEC (current={TEMPLATE_IO_TIMEOUT_SEC}s)."
            ),
        }


async def _toolkit_get_run_history(run_id: str, session_id: str | None = None) -> dict:
    """Returns the 'Prescription & Healing Ledger'."""
    history = get_run_history(run_id, session_id=session_id)
    return {
        "status": "pass",
        "run_id": run_id,
        "session_id": session_id,
        "history_count": len(history),
        "ledger": history,
    }


async def _toolkit_get_data_health_report(run_id: str, session_id: str | None = None) -> dict:
    """Calculates a Red/Yellow/Green Data Health Score (0-100)."""
    history = get_run_history(run_id, session_id=session_id)
    metrics = {
        "null_rate": 0.0,
        "validation_pass_rate": 1.0,
        "outlier_ratio": 0.0,
        "duplicate_ratio": 0.0,
    }

    for entry in history:
        module = entry.get("module")
        summary = entry.get("summary", {})
        row_count = summary.get("row_count")

        if module == "diagnostics":
            metrics["null_rate"] = summary.get("null_rate", 0.0)
        elif module == "validation":
            metrics["validation_pass_rate"] = 1.0 if summary.get("passed", True) else 0.5
        elif module == "duplicates":
            count = summary.get("duplicate_count", 0)
            metrics["duplicate_ratio"] = count / row_count if row_count else min(0.2, count / 1000)
        elif module == "outliers":
            count = summary.get("outlier_count", 0)
            metrics["outlier_ratio"] = count / row_count if row_count else min(0.2, count / 1000)

    score_res = calculate_health_score(metrics)
    return {
        "status": "pass",
        "run_id": run_id,
        "session_id": session_id,
        "health_score": score_res["overall_score"],
        "health_status": score_res["status"],
        "breakdown": score_res["breakdown"],
        "message": f"Data Health Score is {score_res['overall_score']}/100 ({score_res['status'].upper()})",
    }


register_tool(
    name="get_agent_playbook",
    fn=_toolkit_get_agent_playbook,
    description="Returns structured, ordered execution guidance for client agents.",
    input_schema={"type": "object", "properties": {}},
)

register_tool(
    name="get_user_quickstart",
    fn=_toolkit_get_user_quickstart,
    description="Returns concise, human-readable usage guidance and config examples.",
    input_schema={"type": "object", "properties": {}},
)

register_tool(
    name="get_capability_catalog",
    fn=_toolkit_get_capability_catalog,
    description="Returns module capability knobs sourced from YAML templates, including defaults.",
    input_schema={"type": "object", "properties": {}},
)

register_tool(
    name="get_golden_templates",
    fn=_toolkit_get_golden_templates,
    description="Returns a library of 'Golden Config' templates for common use cases.",
    input_schema={"type": "object", "properties": {}},
)

register_tool(
    name="get_run_history",
    fn=_toolkit_get_run_history,
    description="Returns the 'Prescription & Healing Ledger' for a run.",
    input_schema={
        "type": "object",
        "properties": {
            "run_id": {"type": "string"},
            "session_id": {
                "type": "string",
                "description": "Optional session scope. Recommended when reusing run_id values.",
            },
        },
        "required": ["run_id"],
    },
)

register_tool(
    name="get_data_health_report",
    fn=_toolkit_get_data_health_report,
    description="Returns a Visual Data Health Score (0-100) for a run.",
    input_schema={
        "type": "object",
        "properties": {
            "run_id": {"type": "string"},
            "session_id": {
                "type": "string",
                "description": "Optional session scope. Recommended when reusing run_id values.",
            },
        },
        "required": ["run_id"],
    },
)
