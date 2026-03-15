"""Capability catalog builders for cockpit tools."""

from pathlib import Path
from typing import Any

import yaml

from analyst_toolkit.mcp_server.templates import (
    list_module_template_specs,
    list_runtime_template_specs,
    list_workflow_template_specs,
)

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
    """Resolve dot-path values from a template."""
    current: Any = root
    for segment in path.split("."):
        if segment.startswith("<") and segment.endswith(">"):
            return "<per-column>"
        if not isinstance(current, dict) or segment not in current:
            return "<not set>"
        current = current[segment]
    return current


def build_capability_catalog(*, golden_configs: dict[str, Any]) -> dict[str, Any]:
    module_template_specs = list_module_template_specs()
    workflow_template_specs = list_workflow_template_specs()
    runtime_template_specs = list_runtime_template_specs()
    runtime_template_path = runtime_template_specs[0].path.as_posix() if runtime_template_specs else ""
    workflow_template_paths = {
        spec.tool: spec.path.as_posix() for spec in workflow_template_specs if spec.tool
    }

    modules: list[dict[str, Any]] = []
    for spec in module_template_specs:
        if not spec.tool or not spec.config_root:
            continue
        tool_name = spec.tool
        root_key = spec.config_root
        template_path = spec.path.as_posix()
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
            "runtime_overlay_available": True,
            "runtime_template_path": runtime_template_path,
            "auto_heal_template_path": workflow_template_paths.get("auto_heal", ""),
            "data_dictionary_template_path": workflow_template_paths.get("data_dictionary", ""),
        },
        "global_controls": [
            {
                "path": "runtime.run.run_id",
                "description": "Set one run_id across the active tool chain without editing each module config.",
                "user_editable": True,
            },
            {
                "path": "runtime.run.input_path",
                "description": "Set the input path once for the active run.",
                "user_editable": True,
            },
            {
                "path": "runtime.artifacts.export_html",
                "description": "Enable or disable HTML dashboard export across runtime-aware tools.",
                "user_editable": True,
            },
            {
                "path": "runtime.artifacts.plotting",
                "description": "Use one plotting toggle for the active run instead of editing each module config.",
                "user_editable": True,
            },
            {
                "path": "runtime.destinations.gcs.*",
                "description": "Set shared GCS upload destination overrides for runtime-aware tools.",
                "user_editable": True,
            },
            {
                "path": "runtime.destinations.gcs.enabled",
                "description": "Explicitly opt in to remote GCS artifact uploads.",
                "user_editable": True,
            },
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
                "feature": "Auto-heal one-shot remediation",
                "paths": [
                    "config/auto_heal_request_template.yaml",
                    "runtime.run.run_id",
                    "runtime.run.input_path",
                    "runtime.artifacts.export_html",
                    "runtime.destinations.gcs.enabled",
                ],
            },
            {
                "feature": "Runtime overlay controls",
                "paths": [
                    "runtime.run.run_id",
                    "runtime.run.input_path",
                    "runtime.artifacts.export_html",
                    "runtime.artifacts.plotting",
                    "runtime.destinations.gcs.enabled",
                    "runtime.destinations.gcs.bucket_uri",
                    "runtime.destinations.gcs.prefix",
                ],
            },
            {
                "feature": "Data dictionary prelaunch planning",
                "paths": [
                    "config/data_dictionary_request_template.yaml",
                    "infer_configs",
                    "runtime.run.input_path",
                    "runtime.run.run_id",
                    "runtime.artifacts.export_html",
                ],
            },
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
        "workflow_templates": [
            {
                "tool": spec.tool,
                "template_path": spec.path.as_posix(),
                "description": (
                    "One-shot automated cleaning request with runtime-scoped controls and dashboard output."
                    if spec.tool == "auto_heal"
                    else "Artifact-first prelaunch dictionary flow seeded by infer_configs and surfaced through cockpit/dashboard artifacts."
                ),
                "outputs": (
                    ["session_id", "dashboard_url?", "dashboard_path?", "export_url?"]
                    if spec.tool == "auto_heal"
                    else ["dashboard_url?", "dashboard_path?", "xlsx_url?", "summary"]
                ),
            }
            for spec in workflow_template_specs
            if spec.tool in {"auto_heal", "data_dictionary"}
        ],
        "golden_templates": sorted(golden_configs.keys()),
        "modules": modules,
    }


def filter_capability_catalog(
    catalog: dict[str, Any],
    *,
    module: str | None = None,
    search: str | None = None,
    path_prefix: str | None = None,
    compact: bool = False,
) -> dict[str, Any]:
    """Apply optional filters to capability catalog output."""
    modules = list(catalog.get("modules", []))
    search_lc = (search or "").strip().lower()
    prefix = (path_prefix or "").strip()

    if module:
        modules = [m for m in modules if m.get("tool") == module]

    if prefix:
        filtered: list[dict[str, Any]] = []
        for m in modules:
            knobs = [
                k
                for k in m.get("key_knobs", [])
                if isinstance(k, dict) and str(k.get("path", "")).startswith(prefix)
            ]
            if knobs:
                m2 = dict(m)
                m2["key_knobs"] = knobs
                filtered.append(m2)
        modules = filtered

    if search_lc:
        filtered = []
        for m in modules:
            tool_name = str(m.get("tool", "")).lower()
            template_path = str(m.get("template_path", "")).lower()
            knobs = m.get("key_knobs", [])
            matched_knobs = [
                k
                for k in knobs
                if search_lc in str(k.get("path", "")).lower()
                or search_lc in str(k.get("description", "")).lower()
            ]
            if search_lc in tool_name or search_lc in template_path or matched_knobs:
                m2 = dict(m)
                m2["key_knobs"] = matched_knobs if matched_knobs else knobs
                filtered.append(m2)
        modules = filtered

    out = dict(catalog)
    out["modules"] = modules
    out["summary"] = {
        **catalog.get("summary", {}),
        "module_count": len(modules),
        "filters_applied": {
            "module": module or "",
            "search": search or "",
            "path_prefix": path_prefix or "",
            "compact": bool(compact),
        },
    }

    if compact:
        out["modules"] = [
            {
                "tool": m.get("tool"),
                "config_root": m.get("config_root"),
                "key_knobs": [
                    {"path": k.get("path"), "default": k.get("default")}
                    for k in m.get("key_knobs", [])
                ],
            }
            for m in modules
        ]
        out.pop("global_controls", None)
        out.pop("highlight_examples", None)
        out.pop("golden_templates", None)

    return out
