"""MCP tool: cockpit — tools for client delivery, history, and health scoring."""

from analyst_toolkit.m00_utils.scoring import calculate_health_score
from analyst_toolkit.mcp_server.io import default_run_id, get_run_history
from analyst_toolkit.mcp_server.templates import get_golden_configs


async def _toolkit_get_golden_templates() -> dict:
    """
    Returns a library of 'Golden Config' templates for common use cases.
    """
    return {"status": "pass", "templates": get_golden_configs()}


async def _toolkit_get_run_history(run_id: str) -> dict:
    """
    Returns the 'Prescription & Healing Ledger' for a specific run_id.
    """
    history = get_run_history(run_id)
    return {"status": "pass", "run_id": run_id, "history_count": len(history), "ledger": history}


async def _toolkit_get_data_health_report(run_id: str) -> dict:
    """
    Calculates and returns a Red/Yellow/Green Data Health Score (0-100).
    Aggregates metrics from the tool history for the given run_id.
    """
    history = get_run_history(run_id)

    # Extract metrics from history
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
            passed = summary.get("passed", True)
            metrics["validation_pass_rate"] = 1.0 if passed else 0.5
        elif module == "duplicates":
            count = summary.get("duplicate_count", 0)
            if row_count and row_count > 0:
                metrics["duplicate_ratio"] = count / row_count
            else:
                metrics["duplicate_ratio"] = min(0.2, count / 1000)
        elif module == "outliers":
            count = summary.get("outlier_count", 0)
            if row_count and row_count > 0:
                metrics["outlier_ratio"] = count / row_count
            else:
                metrics["outlier_ratio"] = min(0.2, count / 1000)

    score_res = calculate_health_score(metrics)

    return {
        "status": "pass",
        "run_id": run_id,
        "health_score": score_res["overall_score"],
        "health_status": score_res["status"],
        "breakdown": score_res["breakdown"],
        "message": f"Data Health Score is {score_res['overall_score']}/100 ({score_res['status'].upper()})",
    }


async def _toolkit_get_agent_instructions() -> dict:
    """
    Returns the 'Flight Checklist' and recommended protocol for agents using this toolkit.
    """
    try:
        with open("MESSAGES.md", "r") as f:
            content = f.read()
        return {"status": "pass", "instructions": content}
    except Exception as e:
        return {"status": "error", "message": f"Could not load instructions: {str(e)}"}


# Registration
from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="get_agent_instructions",
    fn=_toolkit_get_agent_instructions,
    description="Returns the 'Flight Checklist' and protocol for agents to follow when conducting a data audit.",
    input_schema={"type": "object", "properties": {}},
)

register_tool(
    name="get_golden_templates",
    fn=_toolkit_get_golden_templates,
    description="Returns a library of 'Golden Config' templates for common use cases like Fraud or Migration.",
    input_schema={"type": "object", "properties": {}},
)

register_tool(
    name="get_run_history",
    fn=_toolkit_get_run_history,
    description="Returns the 'Prescription & Healing Ledger' showing the exact sequence of changes for a run.",
    input_schema={
        "type": "object",
        "properties": {
            "run_id": {"type": "string", "description": "The run identifier to look up."}
        },
        "required": ["run_id"],
    },
)

register_tool(
    name="get_data_health_report",
    fn=_toolkit_get_data_health_report,
    description="Returns a Visual Data Health Score (0-100) and Red/Yellow/Green status for a run.",
    input_schema={
        "type": "object",
        "properties": {
            "run_id": {"type": "string", "description": "The run identifier to analyze."}
        },
        "required": ["run_id"],
    },
)
