"""MCP tool: data_dictionary — reserved MCP surface for upcoming dictionary artifacts."""

from analyst_toolkit.mcp_server.registry import register_tool
from analyst_toolkit.mcp_server.response_utils import next_action, with_next_actions
from analyst_toolkit.mcp_server.tools.cockpit_schemas import DATA_DICTIONARY_INPUT_SCHEMA


async def _toolkit_data_dictionary(
    gcs_path: str | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    runtime: dict | None = None,
    profile_depth: str = "standard",
    include_examples: bool = True,
    prelaunch_report: bool = True,
) -> dict:
    """Reserve the data dictionary tool contract while implementation lands in a follow-up PR."""
    result = {
        "status": "not_implemented",
        "module": "data_dictionary",
        "run_id": run_id or "",
        "session_id": session_id or "",
        "summary": {
            "stub": True,
            "planned_outputs": [
                "dictionary dashboard html",
                "dictionary artifact file",
                "compact column summary",
                "prelaunch dictionary report",
            ],
            "profile_depth": profile_depth,
            "include_examples": include_examples,
            "prelaunch_report": prelaunch_report,
        },
        "message": (
            "The data_dictionary MCP surface is reserved, but the dictionary builder and "
            "dashboard pipeline are not implemented yet."
        ),
        "template_path": "config/data_dictionary_request_template.yaml",
        "implementation_plan": ("local_plans/DATA_DICTIONARY_IMPLEMENTATION_WAVE_2026-03-14.md"),
        "input_echo": {
            "gcs_path": gcs_path or "",
            "runtime_present": bool(runtime),
            "prelaunch_report": prelaunch_report,
        },
    }
    return with_next_actions(
        result,
        [
            next_action(
                "infer_configs",
                "Use infer_configs as the schema/rules seed for the future data dictionary and prelaunch report flow.",
                {"gcs_path": gcs_path or "", "session_id": session_id or ""},
            ),
            next_action(
                "get_capability_catalog",
                "Use the capability catalog and template resources while the data dictionary implementation is landing.",
                {},
            ),
        ],
    )


register_tool(
    name="data_dictionary",
    fn=_toolkit_data_dictionary,
    description=(
        "Stub MCP entry point for the upcoming data dictionary artifact and dashboard flow. "
        "Returns the reserved contract surface plus template/plan references."
    ),
    input_schema=DATA_DICTIONARY_INPUT_SCHEMA,
)
