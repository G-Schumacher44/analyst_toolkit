"""Domain errors for MCP input ingest and resolution."""

from __future__ import annotations

from typing import Optional


class InputError(Exception):
    """Base error for input ingest and resolution surfaces."""

    code: str = "INPUT_ERROR"

    def __init__(self, message: str, *, trace_id: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.trace_id = trace_id


class InputNotSupportedError(InputError):
    code = "INPUT_NOT_SUPPORTED"


class InputPathNotFoundError(InputError):
    code = "INPUT_PATH_NOT_FOUND"


class InputPathDeniedError(InputError):
    code = "INPUT_PATH_DENIED"


class InputPayloadTooLargeError(InputError):
    code = "INPUT_PAYLOAD_TOO_LARGE"


class InputConflictError(InputError):
    code = "INPUT_CONFLICT"


class InputNotFoundError(InputError):
    code = "INPUT_NOT_FOUND"
