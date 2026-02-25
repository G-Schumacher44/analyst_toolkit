"""
response_utils.py — Shared response helpers for MCP tool and RPC UX.
"""

from typing import Any
from uuid import uuid4


def new_trace_id() -> str:
    """Generate a short correlation ID for request/response tracing."""
    return uuid4().hex[:12]


def build_error_envelope(
    *,
    category: str,
    code: str,
    message: str,
    remediation: str,
    retryable: bool,
    trace_id: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Return a machine- and human-friendly error envelope.
    """
    envelope: dict[str, Any] = {
        "category": category,
        "code": code,
        "message": message,
        "remediation": remediation,
        "retryable": retryable,
        "trace_id": trace_id,
    }
    if details:
        envelope["details"] = details
    return envelope


def attach_trace_id(result: Any, trace_id: str | None = None) -> Any:
    """
    Add trace_id to dict responses, preserving existing values.
    """
    if not isinstance(result, dict):
        return result

    if "trace_id" in result and result["trace_id"]:
        return result

    out = dict(result)
    out["trace_id"] = trace_id or new_trace_id()
    return out

