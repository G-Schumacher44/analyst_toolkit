"""Static guide/playbook payloads for cockpit tools."""

import os


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _trusted_history_enabled() -> bool:
    return _env_bool(
        "ANALYST_MCP_ENABLE_TRUSTED_HISTORY_TOOL",
        _env_bool("ANALYST_MCP_STDIO", False),
    )


def user_quickstart_payload() -> dict:
    guide = """
# Analyst Toolkit Quickstart (Human)

## What You Can Control
- You can edit module YAML configs directly before runs.
- You can keep automation (`infer_configs`) and still override any field.
- You can use `runtime` for cross-cutting execution settings like `run_id`, `input_path`, `export_html`, plotting, and output destinations.
- You can enable/disable plotting and HTML export per module.
- When HTML export is enabled, module tools return standalone dashboard artifacts that should be opened or linked for review.

## Input Ingest
- Prefer a canonical `input_id` for user-provided datasets whenever possible.
- If the dataset already lives at `gs://...` or a server-visible mounted path, call `register_input` first and carry the returned `input_id` or `session_id` forward.
- If the user only has a local file on their machine, use the `/inputs/upload` ingest path (or a client helper built on top of it) to obtain an `input_id` before running analysis tools.
- Use `get_input_descriptor` to inspect the resolved canonical input reference when needed.

## Recommended Order (Manual Pipeline)
1. `register_input` or upload to `/inputs/upload`
2. `diagnostics`
3. `infer_configs`
4. Review/edit configs (normalization, duplicates, outliers, imputation, validation)
5. `normalization` -> `duplicates` -> `outliers` -> `imputation` -> `validation`
6. `final_audit`
7. `get_run_history` + `get_data_health_report`

## Dashboard Artifacts
- In trusted/local mode, you can start a review session by building the cockpit dashboard for one human-readable landing page.
- Use `ensure_artifact_server` before relying on localhost dashboard links.
- Module tools can return `dashboard_url` when standalone HTML reports are uploaded or exposed for review.
- Agents should surface those dashboard links to users instead of burying them in long summaries.
- Use the dashboard artifact as the primary review surface when it exists.
- `auto_heal` returns its own standalone dashboard artifact and should be surfaced the same way.
- `data_dictionary` now returns a compact prelaunch dictionary surface with standalone artifacts; prefer the dashboard artifact over summarizing the whole dictionary inline.

## Runtime Overlay
- Use `runtime` for run-scoped execution policy.
- Good `runtime` fields:
  - `runtime.run.run_id`
  - `runtime.run.input_path`
  - `runtime.artifacts.export_html`
  - `runtime.artifacts.plotting`
  - `runtime.destinations.gcs.*`
- Keep module `config` for business logic like normalization rules, validation rules, imputation strategies, and outlier detection settings.
- Prefer `runtime` over editing every module config when the change is cross-cutting.

## Auto Heal
- Use `auto_heal` only when the user explicitly wants one-shot remediation.
- Start from `config/auto_heal_request_template.yaml` for agent-authored requests.
- Prefer `runtime` for `run_id`, `input_path`, HTML export, and destination controls.
- After the call, surface `dashboard_url` first and `dashboard_path` only as fallback.

## Data Dictionary
- `data_dictionary` should still start from `infer_configs`, not from blind profiling alone.
- Treat it as a prelaunch report and dictionary surface that explains expected fields, rules, and readiness before a full run.
- Start from `config/data_dictionary_request_template.yaml` when drafting the request shape.
- Surface the dictionary dashboard from cockpit or direct tool output instead of burying it as a raw artifact link.

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
    trusted_history = _trusted_history_enabled()
    ordered_steps = []
    if trusted_history:
        ordered_steps.append(
            {
                "step": 0,
                "tool": "get_cockpit_dashboard",
                "required_inputs": [],
                "outputs": ["dashboard_url?", "dashboard_path?"],
                "notes": [
                    "Build this first when possible so the user gets one human-readable landing page.",
                    "If local dashboard links should resolve directly, call ensure_artifact_server before depending on localhost artifact URLs.",
                ],
            }
        )
    ordered_steps.extend(
        [
            {
                "step": 1 if trusted_history else 0,
                "tool": "register_input",
                "required_inputs": ["uri"],
                "outputs": ["input_id", "session_id?", "summary"],
                "notes": [
                    "Use this when data already lives at gs:// or a server-visible path.",
                    "If the user only has a local file, upload it through /inputs/upload first and reuse the returned input_id.",
                ],
            },
            {
                "step": 2 if trusted_history else 1,
                "tool": "diagnostics",
                "required_inputs": [
                    "input_id|gcs_path|session_id|runtime.run.input_path",
                    "run_id|runtime.run.run_id",
                ],
                "outputs": ["session_id", "summary", "dashboard_url?"],
            },
            {
                "step": 3 if trusted_history else 2,
                "tool": "infer_configs",
                "required_inputs": ["input_id|gcs_path|session_id"],
            },
            {
                "step": 4 if trusted_history else 3,
                "tool_chain": [
                    "normalization",
                    "duplicates",
                    "outliers",
                    "imputation",
                    "validation",
                ],
                "required_inputs": ["session_id", "run_id", "config", "runtime?"],
            },
            {
                "step": 5 if trusted_history else 4,
                "tool": "final_audit",
                "required_inputs": ["session_id", "run_id"],
            },
        ]
    )
    machine_guide = {
        "ordered_steps": ordered_steps,
        "example_calls": [
            {
                "tool": "register_input",
                "arguments": {
                    "uri": "gs://bucket/data.csv",
                    "load_into_session": True,
                    "idempotency_key": "run_001_source",
                },
            },
            {
                "tool": "diagnostics",
                "arguments": {
                    "input_id": "<input_id_from_register_or_upload>",
                    "run_id": "run_001",
                    "runtime": {"artifacts": {"export_html": True, "plotting": False}},
                },
            },
            {
                "tool": "infer_configs",
                "arguments": {"input_id": "<input_id_from_register_or_upload>"},
            },
            {
                "tool": "auto_heal",
                "arguments": {
                    "input_id": "<input_id_from_register_or_upload>",
                    "runtime": {
                        "run": {"run_id": "auto_heal_run_001"},
                        "artifacts": {"export_html": True, "plotting": False},
                    },
                },
            },
            {
                "tool": "data_dictionary",
                "arguments": {
                    "input_id": "<input_id_from_register_or_upload>",
                    "run_id": "dictionary_prelaunch_001",
                    "prelaunch_report": True,
                    "runtime": {"artifacts": {"export_html": True}},
                },
            },
        ],
    }
    return {
        "status": "pass",
        "format": "markdown",
        "title": "Analyst Toolkit Quickstart",
        "markdown": guide.strip(),
        "machine_guide": machine_guide,
        "quick_actions": (
            [
                {
                    "label": "Ensure artifact server",
                    "tool": "ensure_artifact_server",
                    "arguments_schema_hint": {"required": []},
                },
                {
                    "label": "Open cockpit dashboard",
                    "tool": "get_cockpit_dashboard",
                    "arguments_schema_hint": {"required": []},
                },
            ]
            if trusted_history
            else []
        )
        + [
            {
                "label": "Register input",
                "tool": "register_input",
                "arguments_schema_hint": {"required": ["uri"]},
            },
            {
                "label": "Run diagnostics",
                "tool": "diagnostics",
                "arguments_schema_hint": {"required": ["input_id|gcs_path|session_id", "run_id"]},
            },
            {
                "label": "Infer configs",
                "tool": "infer_configs",
                "arguments_schema_hint": {"required": ["input_id|gcs_path|session_id"]},
            },
            {
                "label": "Open capabilities",
                "tool": "get_capability_catalog",
                "arguments_schema_hint": {"required": []},
            },
            {
                "label": "Run auto-heal",
                "tool": "auto_heal",
                "arguments_schema_hint": {
                    "required": ["input_id|gcs_path|session_id|runtime.run.input_path"]
                },
            },
            {
                "label": "Plan data dictionary",
                "tool": "data_dictionary",
                "arguments_schema_hint": {"required": []},
            },
        ],
    }


def agent_playbook_payload() -> dict:
    trusted_history = _trusted_history_enabled()
    offset = 1 if trusted_history else 0
    return {
        "status": "pass",
        "version": "1.0",
        "goal": "Audit, clean, and certify a dataset with controlled user-editable configs.",
        "prerequisites": [
            "Canonical input_id from upload/register flow (preferred) or existing session_id",
            "If no input_id exists yet: a gs:// URI or server-visible path for register_input, or an upload client that can call /inputs/upload",
            "Stable run_id used across calls",
            "Optional output bucket/prefix overrides",
            "Optional runtime overlay for cross-cutting execution control",
        ],
        "ordered_steps": (
            [
                {
                    "step": 0,
                    "tool": "get_cockpit_dashboard",
                    "required_inputs": [],
                    "outputs": ["dashboard_url?", "dashboard_path?"],
                    "notes": [
                        "Build this at the start of a trusted/local session when possible.",
                        "Return the cockpit dashboard link to the user as the human-facing landing page before deeper tool work.",
                        "If local HTML artifacts should open as localhost links, call ensure_artifact_server first.",
                    ],
                    "next": [offset],
                },
            ]
            if trusted_history
            else []
        )
        + [
            {
                "step": offset,
                "tool": "ensure_artifact_server",
                "required_inputs": [],
                "outputs": ["base_url", "running"],
                "notes": [
                    "Optional for any client; recommended in trusted/local mode before promising direct dashboard links.",
                ],
                "next": [offset + 1],
            },
            {
                "step": offset + 1,
                "tool": "register_input",
                "required_inputs": ["uri"],
                "outputs": ["input_id", "session_id?", "summary"],
                "notes": [
                    "Use this when data already lives at gs:// or a server-visible path.",
                    "If the user only has a local file, upload it through /inputs/upload first and then switch to input_id.",
                ],
                "next": [offset + 2],
            },
            {
                "step": offset + 2,
                "tool": "diagnostics",
                "required_inputs": [
                    "input_id|gcs_path|session_id|runtime.run.input_path",
                    "run_id|runtime.run.run_id",
                ],
                "outputs": [
                    "session_id",
                    "summary",
                    "dashboard_url?",
                    "artifact_url?",
                    "plot_urls?",
                ],
                "next": [offset + 3],
            },
            {
                "step": offset + 3,
                "tool": "get_data_health_report",
                "required_inputs": ["run_id", "session_id?"],
                "outputs": ["health_score", "breakdown"],
                "next": [offset + 4],
            },
            {
                "step": offset + 4,
                "tool": "infer_configs",
                "required_inputs": ["input_id|gcs_path|session_id"],
                "outputs": ["configs (YAML strings by module)"],
                "next": [offset + 5],
            },
            {
                "step": offset + 5,
                "tool": "get_capability_catalog",
                "required_inputs": [],
                "outputs": ["editable knobs + defaults + example paths"],
                "next": [offset + 6],
            },
            {
                "step": offset + 6,
                "tool": "manual config + runtime review",
                "required_inputs": [
                    "inferred configs",
                    "capability catalog",
                    "user intent",
                    "runtime?",
                ],
                "outputs": ["confirmed config per module", "confirmed runtime overlay"],
                "notes": [
                    "Do not flatten nested keys.",
                    "User can override inferred config fields before execution.",
                    "Use runtime for paths, run_id, export_html, plotting, and destination overrides.",
                    "Use module config for business logic and per-module rules.",
                ],
                "next": [offset + 7],
            },
            {
                "step": offset + 7,
                "tool": "auto_heal",
                "required_inputs": [
                    "input_id|gcs_path|session_id|runtime.run.input_path",
                    "run_id|runtime.run.run_id",
                ],
                "outputs": ["session_id", "dashboard_url?", "dashboard_path?", "export_url?"],
                "notes": [
                    "Use only when the user explicitly wants one-shot automation.",
                    "Open or link the auto-heal dashboard artifact for review.",
                ],
                "next": [offset + 8],
            },
            {
                "step": offset + 8,
                "tool_chain": [
                    "normalization",
                    "duplicates",
                    "outliers",
                    "imputation",
                    "validation",
                ],
                "required_inputs": ["session_id", "run_id", "config per tool", "runtime?"],
                "outputs": ["updated session_id", "module summaries", "artifacts"],
                "notes": [
                    "When a module returns dashboard_url, surface that link to the user.",
                    "Prefer the standalone dashboard artifact as the main review surface.",
                    "Use runtime instead of editing every module config when the override is cross-cutting.",
                ],
                "next": [offset + 9],
            },
            {
                "step": offset + 9,
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
                "rule": "Default plotting to off for large data; prefer runtime.artifacts.plotting for run-scoped control.",
            },
            {
                "name": "fuzzy_matching_mode",
                "rule": "If fuzzy matching is enabled, require explicit master_list and cutoff review.",
            },
            {
                "name": "runtime_vs_config",
                "rule": "Use runtime for run-scoped execution settings and destinations; use module config for business logic.",
            },
        ],
    }
