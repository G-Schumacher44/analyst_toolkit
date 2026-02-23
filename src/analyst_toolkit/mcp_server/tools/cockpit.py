"""MCP tool: cockpit — tools for client delivery, history, and health scoring."""

from analyst_toolkit.m00_utils.scoring import calculate_health_score
from analyst_toolkit.mcp_server.io import default_run_id, get_run_history, get_golden_configs
from analyst_toolkit.mcp_server.registry import register_tool


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
`reports/<session_timestamp>/<session_id>/<module>/...`
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
    return {
        "status": "pass",
        "help": help_text
    }


async def _toolkit_get_golden_templates() -> dict:
    """Returns a library of 'Golden Config' templates."""
    return {
        "status": "pass",
        "templates": get_golden_configs()
    }


async def _toolkit_get_run_history(run_id: str) -> dict:
    """Returns the 'Prescription & Healing Ledger'."""
    history = get_run_history(run_id)
    return {
        "status": "pass",
        "run_id": run_id,
        "history_count": len(history),
        "ledger": history
    }


async def _toolkit_get_data_health_report(run_id: str) -> dict:
    """Calculates a Red/Yellow/Green Data Health Score (0-100)."""
    history = get_run_history(run_id)
    metrics = {"null_rate": 0.0, "validation_pass_rate": 1.0, "outlier_ratio": 0.0, "duplicate_ratio": 0.0}
    
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
        "health_score": score_res["overall_score"],
        "health_status": score_res["status"],
        "breakdown": score_res["breakdown"],
        "message": f"Data Health Score is {score_res['overall_score']}/100 ({score_res['status'].upper()})"
    }


async def _toolkit_get_agent_instructions() -> dict:
    """Returns the agent flight checklist."""
    try:
        with open("MESSAGES.md", "r") as f: content = f.read()
        return {"status": "pass", "instructions": content}
    except Exception as e:
        return {"status": "error", "message": str(e)}


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
    input_schema={"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]},
)

register_tool(
    name="get_data_health_report",
    fn=_toolkit_get_data_health_report,
    description="Returns a Visual Data Health Score (0-100) for a run.",
    input_schema={"type": "object", "properties": {"run_id": {"type": "string"}}, "required": ["run_id"]},
)

register_tool(
    name="get_agent_instructions",
    fn=_toolkit_get_agent_instructions,
    description="Returns the 'Flight Checklist' for agents.",
    input_schema={"type": "object", "properties": {}},
)
