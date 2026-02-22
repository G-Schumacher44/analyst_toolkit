"""
registry.py — Tool registry for the MCP server to avoid circular imports.
"""

import logging
from typing import Any

logger = logging.getLogger("analyst_toolkit.mcp_server.registry")

# Tool registry: tool_name → {fn, description, inputSchema}
TOOL_REGISTRY: dict[str, dict[str, Any]] = {}


def register_tool(name: str, fn, description: str, input_schema: dict) -> None:
    """
    Register an async callable as an MCP tool.
    """
    TOOL_REGISTRY[name] = {
        "fn": fn,
        "description": description,
        "inputSchema": input_schema,
    }
    logger.info(f"Registered tool: {name}")
