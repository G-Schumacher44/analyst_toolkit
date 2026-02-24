"""MCP tool: cockpit — tools for client delivery, history, and health scoring."""

from analyst_toolkit.m00_utils.scoring import calculate_health_score
from analyst_toolkit.mcp_server.io import get_run_history
from analyst_toolkit.mcp_server.registry import register_tool
from analyst_toolkit.mcp_server.templates import get_golden_configs


async def _toolkit_get_cockpit_help() -> dict:
    """
    Returns a comprehensive guide on using the Cockpit and State Management.
    """
    help_text = """
# 🕹️ Analyst Toolkit Cockpit Guide

Welcome to the 'Cockpit'. This system is designed for autonomous data auditing and self-healing.

## ⛓️ State Management (Pipeline Mode)
Every time you run a tool, the system maintains the data in an in-memory session.
1. **The Handshake:** Your first tool call (e.g., diagnostics) creates a `session_id`.
2. **The Chain:** Pass that `session_id` to subsequent tools. They will operate on the *transformed* data from the previous step.
3. **The Result:** You don't need to download/upload data between steps.

## 📂 Directory Structure (GCS/Local)
Reports are now strictly grouped for clarity:
`reports/<session_timestamp>/<session_id>/<run_id>/<module>/...`
- This ensures that a single multi-step audit run is always kept together in one folder.

## 📊 Plotting (Opt-In)
Plotting is turned **OFF** by default to prevent timeouts on large datasets.
- To see graphs, pass `plotting=true` in your tool call.
- You can limit the number of plots using `max_plots=10`.

## 🧪 Key Tools
- `get_data_health_report`: Get a 0-100 score for your current data session.
- `get_run_history`: See the 'Ledger' of every transformation made in this run.
- `auto_heal`: The autonomous mode. Infer and apply fixes in one step.
- `get_golden_templates`: Discover best-practice configurations for Fraud, Migration, and more.
"""
    return {"status": "pass", "help": help_text}


async def _toolkit_get_golden_templates() -> dict:
    """Returns a library of 'Golden Config' templates."""
    return {"status": "pass", "templates": get_golden_configs()}


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


async def _toolkit_get_agent_instructions() -> dict:
    """Returns the agent flight checklist."""
    instructions = """
# ✈️ MCP Agent Flight Checklist

When you start working with a user on a data audit, follow this checklist in order. Do NOT skip steps or jump ahead.

## 1. Discovery (Pre-Flight)
Before running any tools, confirm with the user:
- [ ] **Data Location:** GCS path or local file?
- [ ] **Output Destination:** Reports default to the server's configured bucket. Override with `output_bucket` if needed.

## 2. Diagnostics (Takeoff)
- [ ] Run `toolkit_diagnostics` to establish a baseline profile.
- [ ] Run `toolkit_get_data_health_report` and share the **Data Health Score** with the user before proceeding.

## 3. Config Inference & Customization
- [ ] Run `toolkit_infer_configs` using the `session_id` from step 2.
- [ ] **Read and reason about the returned YAML configs.** Do not pass them through blindly.
  - Check `normalization` config: are the detected text columns and type coercions correct for this dataset?
  - Check `imputation` config: are the fill strategies appropriate (mean/median/mode/constant)?
  - Check `duplicates` config: are the subset columns meaningful for deduplication?
  - Check `outliers` config: are the flagged numeric columns sensible? Is the IQR multiplier appropriate?
  - Check `validation` config: are range checks, null constraints, and categorical rules reasonable?
- [ ] Present a **summary of the proposed configs** to the user and ask for confirmation or amendments before proceeding.
- [ ] Optionally merge with a **Golden Template** (`toolkit_get_golden_templates`) if the use case matches (Fraud, Migration, Compliance).

> ⚠️ **Config structure is critical — do NOT restructure inferred configs.**
> `toolkit_infer_configs` returns YAML strings keyed by module name. Parse each YAML string into a dict (e.g. with `yaml.safe_load`), then pass the parsed dict directly as the `config` argument to the tool.
> Example: `toolkit_normalization(session_id=..., config={"normalization": <parsed_yaml_dict>["normalization"]})`
> Never hoist nested keys to the top level. For normalization, `standardize_text_columns`, `coerce_dtypes`, `rename_columns` etc. must remain nested inside `rules:` exactly as inferred. Flattening them will cause the pipeline to find no rules and skip all transformations.

## 4. Manual Pipeline (Cruise) — run in this order
Execute each step using the `session_id` and the confirmed/adjusted config. Pause after each to share the summary.

1. `toolkit_normalization` — standardize text, rename columns, coerce types.
2. `toolkit_duplicates` — flag or drop duplicate rows (if duplicates were detected).
3. `toolkit_outliers` — flag or cap outliers (if outliers were detected).
4. `toolkit_imputation` — fill missing values.
5. `toolkit_validation` — enforce business rules and constraints.

> ⚠️ **Do NOT use `toolkit_auto_heal` unless the user explicitly requests a fully automated one-shot fix.**

## 5. Certification (Landing)
- [ ] Run `toolkit_final_audit` to produce the Healing Certificate.
- [ ] Run `toolkit_get_run_history` and share the full ledger with the user.
- [ ] Provide the link to the HTML report as the final **Proof of Health**.

---

## 💡 Pro-Tips
- Always chain steps in memory via `session_id` — do not download/re-upload data between steps.
- Pass `plotting=true` only when the user asks for charts (OFF by default to avoid timeouts).
- If a step produces unexpected output (e.g. too many duplicates flagged), re-examine the config and re-run with adjusted parameters rather than proceeding blindly.
"""
    return {"status": "pass", "instructions": instructions}


register_tool(
    name="get_cockpit_help",
    fn=_toolkit_get_cockpit_help,
    description="Returns a guide on how to use the stateful pipeline, health scoring, and directory structure.",
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

register_tool(
    name="get_agent_instructions",
    fn=_toolkit_get_agent_instructions,
    description="Returns the 'Flight Checklist' for agents.",
    input_schema={"type": "object", "properties": {}},
)
