"""Static guide/playbook payloads for cockpit tools."""


def user_quickstart_payload() -> dict:
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


def agent_playbook_payload() -> dict:
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
