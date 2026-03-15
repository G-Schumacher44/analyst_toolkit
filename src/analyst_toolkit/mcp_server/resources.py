"""MCP resource inventory and read handlers."""

import json
import logging

from analyst_toolkit.mcp_server.response_utils import new_trace_id
from analyst_toolkit.mcp_server.templates import (
    get_golden_configs,
    list_template_resources,
    read_template_resource,
)
from analyst_toolkit.mcp_server.tools.cockpit_capabilities import build_capability_catalog
from analyst_toolkit.mcp_server.tools.cockpit_content import (
    agent_playbook_payload,
    user_quickstart_payload,
)

logger = logging.getLogger("analyst_toolkit.mcp_server.resources")

QUICKSTART_URI = "analyst://docs/quickstart"
AGENT_PLAYBOOK_URI = "analyst://docs/agent-playbook"
CAPABILITY_CATALOG_URI = "analyst://catalog/capabilities"


class ResourceNotFoundError(Exception):
    """Stable client-safe missing-resource error."""

    def __init__(self, code: str = "RESOURCE_NOT_FOUND", trace_id: str | None = None):
        self.code = code
        self.trace_id = trace_id or new_trace_id()
        super().__init__(code)


class ResourcePayloadError(Exception):
    """Stable client-safe resource payload validation error."""

    def __init__(self, code: str, message: str, trace_id: str | None = None):
        self.code = code
        self.trace_id = trace_id or new_trace_id()
        super().__init__(message)


def list_mcp_resources() -> list[dict[str, str]]:
    resources = list_template_resources()
    resources.extend(
        [
            {
                "name": "docs::quickstart",
                "uri": QUICKSTART_URI,
                "description": "Human-oriented toolkit quickstart guide.",
                "mimeType": "text/markdown",
                "category": "doc",
            },
            {
                "name": "docs::agent_playbook",
                "uri": AGENT_PLAYBOOK_URI,
                "description": "Strict ordered workflow for client agents.",
                "mimeType": "application/json",
                "category": "doc",
            },
            {
                "name": "catalog::capabilities",
                "uri": CAPABILITY_CATALOG_URI,
                "description": "Editable config knobs, runtime overlays, and workflow templates.",
                "mimeType": "application/json",
                "category": "catalog",
            },
        ]
    )
    return resources


def _read_quickstart_resource() -> tuple[str, str]:
    payload = user_quickstart_payload()
    if not isinstance(payload, dict):
        logger.warning("Quickstart payload must be a mapping, got %r", payload)
        raise ResourcePayloadError(
            code="QUICKSTART_PAYLOAD_INVALID",
            message="Invalid quickstart payload.",
        )
    content = payload.get("content")
    if not isinstance(content, dict):
        logger.warning("Quickstart payload missing content mapping: %r", payload)
        raise ResourcePayloadError(
            code="QUICKSTART_PAYLOAD_INVALID",
            message="Invalid quickstart payload.",
        )
    markdown = content.get("markdown")
    if not isinstance(markdown, str) or not markdown.strip():
        logger.warning("Quickstart payload missing markdown body: %r", payload)
        raise ResourcePayloadError(
            code="QUICKSTART_PAYLOAD_INVALID",
            message="Invalid quickstart payload.",
        )
    return markdown, "text/markdown"


def _read_agent_playbook_resource() -> tuple[str, str]:
    payload = agent_playbook_payload()
    return json.dumps(payload, separators=(",", ":")), "application/json"


def _read_capability_catalog_resource() -> tuple[str, str]:
    payload = build_capability_catalog(golden_configs=get_golden_configs())
    return json.dumps(payload, separators=(",", ":")), "application/json"


def read_mcp_resource(uri: str) -> tuple[str, str]:
    if uri.startswith("analyst://templates/"):
        return read_template_resource(uri), "application/x-yaml"
    if uri == QUICKSTART_URI:
        return _read_quickstart_resource()
    if uri == AGENT_PLAYBOOK_URI:
        return _read_agent_playbook_resource()
    if uri == CAPABILITY_CATALOG_URI:
        return _read_capability_catalog_resource()
    raise ResourceNotFoundError()
