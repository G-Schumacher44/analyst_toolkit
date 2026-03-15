"""In-memory registry for ingested and registered inputs."""

from __future__ import annotations

import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from math import inf
from typing import Optional

from analyst_toolkit.mcp_server.input.models import InputDescriptor

_LOCK = threading.Lock()


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


_REGISTRY_MAX_ENTRIES = _env_int("ANALYST_MCP_INPUT_REGISTRY_MAX_ENTRIES", 512)
_REGISTRY_TTL_SEC = _env_float("ANALYST_MCP_INPUT_REGISTRY_TTL_SEC", 21600.0)
INPUT_DESCRIPTOR_CONFLICT_CODE = "INPUT_DESCRIPTOR_CONFLICT"
INPUT_NOT_FOUND_CODE = "INPUT_NOT_FOUND"


@dataclass
class _RegistryEntry:
    descriptor: InputDescriptor
    expires_at: float


@dataclass
class _SessionBinding:
    input_id: str
    expires_at: float


_INPUTS: OrderedDict[str, _RegistryEntry] = OrderedDict()
_SESSION_INPUTS: OrderedDict[str, _SessionBinding] = OrderedDict()


def _now() -> float:
    return time.monotonic()


def _expires_at(now: float) -> float:
    if _REGISTRY_TTL_SEC <= 0:
        return inf
    return now + _REGISTRY_TTL_SEC


def _remove_input_locked(input_id: str) -> None:
    _INPUTS.pop(input_id, None)
    _cleanup_sessions_for_input_locked(input_id)


def _cleanup_sessions_for_input_locked(input_id: str) -> None:
    stale_sessions = [
        session_id
        for session_id, binding in list(_SESSION_INPUTS.items())
        if binding.input_id == input_id
    ]
    for session_id in stale_sessions:
        _SESSION_INPUTS.pop(session_id, None)


def _refresh_input_locked(
    input_id: str, descriptor: InputDescriptor, now: float
) -> InputDescriptor:
    _INPUTS[input_id] = _RegistryEntry(descriptor=descriptor, expires_at=_expires_at(now))
    _INPUTS.move_to_end(input_id)
    return descriptor


def _refresh_session_locked(session_id: str, input_id: str, now: float) -> None:
    _SESSION_INPUTS[session_id] = _SessionBinding(
        input_id=input_id,
        expires_at=_expires_at(now),
    )
    _SESSION_INPUTS.move_to_end(session_id)


def _prune_locked(now: float) -> None:
    expired_inputs = [
        input_id for input_id, entry in list(_INPUTS.items()) if entry.expires_at <= now
    ]
    for input_id in expired_inputs:
        _remove_input_locked(input_id)

    expired_sessions = [
        session_id
        for session_id, binding in list(_SESSION_INPUTS.items())
        if binding.expires_at <= now or binding.input_id not in _INPUTS
    ]
    for session_id in expired_sessions:
        _SESSION_INPUTS.pop(session_id, None)

    while len(_INPUTS) > _REGISTRY_MAX_ENTRIES:
        oldest_input_id, _ = _INPUTS.popitem(last=False)
        _cleanup_sessions_for_input_locked(oldest_input_id)


def save_descriptor(descriptor: InputDescriptor) -> InputDescriptor:
    with _LOCK:
        now = _now()
        _prune_locked(now)
        existing_entry = _INPUTS.get(descriptor.input_id)

        effective_descriptor = descriptor
        if existing_entry is not None:
            existing_descriptor = existing_entry.descriptor
            if not existing_descriptor.same_canonical_input(descriptor):
                raise ValueError(
                    f"[{INPUT_DESCRIPTOR_CONFLICT_CODE}] Conflicting descriptor for input_id "
                    f"'{descriptor.input_id}'."
                )
            effective_descriptor = descriptor.with_runtime_binding(
                session_id=descriptor.session_id or existing_descriptor.session_id,
                run_id=descriptor.run_id or existing_descriptor.run_id,
            )

        effective_descriptor = _refresh_input_locked(
            effective_descriptor.input_id,
            effective_descriptor,
            now,
        )
        if effective_descriptor.session_id:
            _refresh_session_locked(
                effective_descriptor.session_id,
                effective_descriptor.input_id,
                now,
            )
    return effective_descriptor


def get_descriptor(input_id: str) -> Optional[InputDescriptor]:
    with _LOCK:
        now = _now()
        _prune_locked(now)
        entry = _INPUTS.get(input_id)
        if entry is None:
            return None
        return _refresh_input_locked(input_id, entry.descriptor, now)


def bind_session_input(session_id: str, input_id: str) -> None:
    with _LOCK:
        now = _now()
        _prune_locked(now)
        entry = _INPUTS.get(input_id)
        if entry is None:
            raise ValueError(
                f"[{INPUT_NOT_FOUND_CODE}] input_id '{input_id}' not found in registry"
            )
        _refresh_input_locked(input_id, entry.descriptor, now)
        _refresh_session_locked(session_id, input_id, now)


def get_session_input_id(session_id: str) -> Optional[str]:
    with _LOCK:
        now = _now()
        _prune_locked(now)
        binding = _SESSION_INPUTS.get(session_id)
        if binding is None:
            return None
        if binding.input_id not in _INPUTS:
            _SESSION_INPUTS.pop(session_id, None)
            return None
        _refresh_session_locked(session_id, binding.input_id, now)
        entry = _INPUTS[binding.input_id]
        _refresh_input_locked(binding.input_id, entry.descriptor, now)
        return binding.input_id


def remove_descriptor(input_id: str) -> None:
    with _LOCK:
        _remove_input_locked(input_id)


def get_registry_stats() -> dict[str, float | int]:
    with _LOCK:
        now = _now()
        _prune_locked(now)
        return {
            "input_count": len(_INPUTS),
            "session_binding_count": len(_SESSION_INPUTS),
            "max_entries": _REGISTRY_MAX_ENTRIES,
            "ttl_sec": _REGISTRY_TTL_SEC,
        }


def clear() -> None:
    with _LOCK:
        _INPUTS.clear()
        _SESSION_INPUTS.clear()
