"""Authentication helpers for MCP HTTP endpoints."""

import secrets

from fastapi import Request


def is_authorized(request: Request, auth_token: str) -> bool:
    """Check request authorization when token auth mode is enabled."""
    if not auth_token:
        return True
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        return False
    provided = auth_header.removeprefix("Bearer ").strip()
    if not provided:
        return False
    return secrets.compare_digest(provided, auth_token)
