"""MCP tools: async job status/list helpers."""

from analyst_toolkit.mcp_server.job_state import JobStore
from analyst_toolkit.mcp_server.registry import register_tool


async def _toolkit_get_job_status(job_id: str) -> dict:
    """Return status for an async job."""
    job = JobStore.get(job_id)
    if not job:
        return {
            "status": "error",
            "module": "job_status",
            "job_id": job_id,
            "message": f"Job not found: {job_id}",
        }
    return {
        "status": "pass",
        "module": "job_status",
        "job_id": job_id,
        "job": job,
    }


async def _toolkit_list_jobs(limit: int = 20, state: str | None = None) -> dict:
    """List recent async jobs."""
    jobs = JobStore.list(limit=limit, state=state)
    return {
        "status": "pass",
        "module": "job_status",
        "summary": {"count": len(jobs), "limit": limit, "state": state or ""},
        "jobs": jobs,
    }


register_tool(
    name="get_job_status",
    fn=_toolkit_get_job_status,
    description="Get current status for an async toolkit job by job_id.",
    input_schema={
        "type": "object",
        "properties": {
            "job_id": {
                "type": "string",
                "description": "Async job identifier returned by tools that support async_mode.",
            }
        },
        "required": ["job_id"],
    },
)

register_tool(
    name="list_jobs",
    fn=_toolkit_list_jobs,
    description="List recent async toolkit jobs, optionally filtered by state.",
    input_schema={
        "type": "object",
        "properties": {
            "limit": {
                "type": "integer",
                "description": "Max jobs to return (default 20).",
                "default": 20,
            },
            "state": {
                "type": "string",
                "description": "Optional filter: queued|running|succeeded|failed.",
            },
        },
    },
)
