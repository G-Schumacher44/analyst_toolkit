"""In-memory registry for ingested and registered inputs."""

from __future__ import annotations

import threading
from typing import Optional

from analyst_toolkit.mcp_server.input.models import InputDescriptor

_LOCK = threading.Lock()
_INPUTS: dict[str, InputDescriptor] = {}
_SESSION_INPUTS: dict[str, str] = {}


def save_descriptor(descriptor: InputDescriptor) -> InputDescriptor:
    with _LOCK:
        _INPUTS[descriptor.input_id] = descriptor
        if descriptor.session_id:
            _SESSION_INPUTS[descriptor.session_id] = descriptor.input_id
    return descriptor


def get_descriptor(input_id: str) -> Optional[InputDescriptor]:
    with _LOCK:
        return _INPUTS.get(input_id)


def bind_session_input(session_id: str, input_id: str) -> None:
    with _LOCK:
        if input_id not in _INPUTS:
            raise ValueError(f"input_id '{input_id}' not found in registry")
        _SESSION_INPUTS[session_id] = input_id


def get_session_input_id(session_id: str) -> Optional[str]:
    with _LOCK:
        return _SESSION_INPUTS.get(session_id)


def clear() -> None:
    with _LOCK:
        _INPUTS.clear()
        _SESSION_INPUTS.clear()
