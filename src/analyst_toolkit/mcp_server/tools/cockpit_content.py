"""Static guide/playbook payloads for cockpit tools."""


def user_quickstart_payload() -> dict:
    guide = """
# Analyst Toolkit Quickstart (Human)

## What You Can Control
- You can edit module YAML configs directly before runs.
- You can keep automation (`infer_configs`) and still override any field.
- You can use `runtime` for cross-cutting execution settings like `run_id`, `input_path`, `export_html`, plotting, and output destinations.
- You can enable/disable plotting and HTML export per module.
- When HTML export is enabled, module tools return standalone dashboard artifacts that should be opened or linked for review.

## Recommended Order (Manual Pipeline)
1. `diagnostics`
2. `infer_configs`
3. Review/edit configs (normalization, duplicates, outliers, imputation, validation)
4. `normalization` -> `duplicates` -> `outliers` -> `imputation` -> `validation`
5. `final_audit`
6. `get_run_history` + `get_data_health_report`

## Dashboard Artifacts
- Module tools can return `dashboard_url` when standalone HTML reports are uploaded or exposed for review.
- Agents should surface those dashboard links to users instead of burying them in long summaries.
- Use the dashboard artifact as the primary review surface when it exists.
- `auto_heal` returns its own standalone dashboard artifact and should be surfaced the same way.
- `data_dictionary` is currently a reserved MCP surface. Use its template and plan references to prepare the future prelaunch dictionary flow; do not present it as implemented yet.

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
- The future `data_dictionary` flow should start from `infer_configs`, not from blind profiling alone.
- Treat it as a prelaunch report and dictionary surface that explains expected fields, rules, and readiness before a full run.
- Start from `config/data_dictionary_request_template.yaml` when drafting the future request shape.
- When it lands, it should be surfaced from the cockpit dashboard as a resource/report card, not buried as a raw artifact.

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
                "required_inputs": [
                    "gcs_path|session_id|runtime.run.input_path",
                    "run_id|runtime.run.run_id",
                ],
                "outputs": ["session_id", "summary", "dashboard_url?"],
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
                "required_inputs": ["session_id", "run_id", "config", "runtime?"],
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
                "arguments": {
                    "runtime": {
                        "run": {
                            "input_path": "gs://bucket/data.csv",
                            "run_id": "run_001",
                        },
                        "artifacts": {"export_html": True, "plotting": False},
                    }
                },
            },
            {
                "tool": "infer_configs",
                "arguments": {"session_id": "<session_id_from_diagnostics>"},
            },
            {
                "tool": "auto_heal",
                "arguments": {
                    "runtime": {
                        "run": {
                            "input_path": "gs://bucket/data.csv",
                            "run_id": "auto_heal_run_001",
                        },
                        "artifacts": {"export_html": True, "plotting": False},
                    },
                    "mode": "sync",
                },
            },
            {
                "tool": "data_dictionary",
                "arguments": {
                    "gcs_path": "gs://bucket/data.csv",
                    "run_id": "dictionary_prelaunch_001",
                    "prelaunch_report": True,
                    "runtime": {
                        "run": {"input_path": "gs://bucket/data.csv"},
                        "artifacts": {"export_html": True},
                    },
                },
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
            {
                "label": "Run auto-heal",
                "tool": "auto_heal",
                "arguments_schema_hint": {
                    "required": ["gcs_path|session_id|runtime.run.input_path"]
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
    return {
        "status": "pass",
        "version": "1.0",
        "goal": "Audit, clean, and certify a dataset with controlled user-editable configs.",
        "prerequisites": [
            "Input data path (local csv/parquet or gs:// URI) or existing session_id",
            "Stable run_id used across calls",
            "Optional output bucket/prefix overrides",
            "Optional runtime overlay for cross-cutting execution control",
        ],
        "ordered_steps": [
            {
                "step": 1,
                "tool": "diagnostics",
                "required_inputs": [
                    "gcs_path|session_id|runtime.run.input_path",
                    "run_id|runtime.run.run_id",
                ],
                "outputs": [
                    "session_id",
                    "summary",
                    "dashboard_url?",
                    "artifact_url?",
                    "plot_urls?",
                ],
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
                "next": [6],
            },
            {
                "step": 6,
                "tool": "auto_heal",
                "required_inputs": [
                    "gcs_path|session_id|runtime.run.input_path",
                    "run_id|runtime.run.run_id",
                ],
                "outputs": ["session_id", "dashboard_url?", "dashboard_path?", "export_url?"],
                "notes": [
                    "Use only when the user explicitly wants one-shot automation.",
                    "Open or link the auto-heal dashboard artifact for review.",
                ],
                "next": [7],
            },
            {
                "step": 7,
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
                "next": [8],
            },
            {
                "step": 8,
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
