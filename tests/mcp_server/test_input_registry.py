import re

import pytest

from analyst_toolkit.mcp_server.input import registry as input_registry
from analyst_toolkit.mcp_server.input.errors import InputConflictError
from analyst_toolkit.mcp_server.input.models import (
    INPUT_ID_HEX_LENGTH,
    INPUT_ID_PREFIX,
    InputDescriptor,
)


@pytest.fixture
def clean_registry(monkeypatch):
    monkeypatch.setattr(input_registry, "_REGISTRY_MAX_ENTRIES", 8)
    monkeypatch.setattr(input_registry, "_REGISTRY_TTL_SEC", 3600.0)
    input_registry.clear()
    yield
    input_registry.clear()


def _descriptor(
    input_id: str,
    *,
    session_id: str | None = None,
    run_id: str | None = None,
    sha256: str | None = None,
) -> InputDescriptor:
    return InputDescriptor(
        input_id=input_id,
        source_type="upload",
        original_reference="dirty_penguins.csv",
        resolved_reference="/tmp/dirty_penguins.csv",
        display_name="dirty_penguins.csv",
        media_type="text/csv",
        file_size_bytes=42,
        sha256=sha256,
        session_id=session_id,
        run_id=run_id,
        metadata={"kind": "test"},
    )


def test_registry_save_is_idempotent_for_same_canonical_descriptor(clean_registry):
    saved = input_registry.save_descriptor(
        _descriptor("input_same", session_id="sess_a", run_id="run_a")
    )
    repeated = input_registry.save_descriptor(
        _descriptor("input_same", session_id="sess_b", run_id="run_b")
    )

    assert saved.input_id == repeated.input_id
    assert repeated.session_id == "sess_b"
    assert input_registry.get_session_input_id("sess_a") == "input_same"
    assert input_registry.get_session_input_id("sess_b") == "input_same"


def test_registry_rejects_conflicting_descriptor_reuse(clean_registry):
    input_registry.save_descriptor(_descriptor("input_conflict", sha256="abc"))

    with pytest.raises(InputConflictError, match="Conflicting descriptor"):
        input_registry.save_descriptor(_descriptor("input_conflict", sha256="def"))

    original = input_registry.get_descriptor("input_conflict")
    assert original is not None
    assert original.sha256 == "abc"


def test_registry_evicts_oldest_entries_when_capacity_is_exceeded(monkeypatch):
    monkeypatch.setattr(input_registry, "_REGISTRY_MAX_ENTRIES", 2)
    monkeypatch.setattr(input_registry, "_REGISTRY_TTL_SEC", 3600.0)
    input_registry.clear()

    input_registry.save_descriptor(_descriptor("input_a", session_id="sess_a"))
    input_registry.save_descriptor(_descriptor("input_b", session_id="sess_b"))
    input_registry.save_descriptor(_descriptor("input_c", session_id="sess_c"))

    assert input_registry.get_descriptor("input_a") is None
    assert input_registry.get_session_input_id("sess_a") is None
    assert input_registry.get_descriptor("input_b") is not None
    assert input_registry.get_descriptor("input_c") is not None


def test_registry_expires_descriptors_and_session_bindings(monkeypatch):
    clock = {"now": 1000.0}

    monkeypatch.setattr(input_registry, "_REGISTRY_MAX_ENTRIES", 8)
    monkeypatch.setattr(input_registry, "_REGISTRY_TTL_SEC", 5.0)
    monkeypatch.setattr(input_registry, "_now", lambda: clock["now"])
    input_registry.clear()

    input_registry.save_descriptor(_descriptor("input_ttl", session_id="sess_ttl"))
    assert input_registry.get_descriptor("input_ttl") is not None
    assert input_registry.get_session_input_id("sess_ttl") == "input_ttl"

    clock["now"] = 1006.0
    assert input_registry.get_descriptor("input_ttl") is None
    assert input_registry.get_session_input_id("sess_ttl") is None


def test_remove_descriptor_cleans_up_session_bindings(clean_registry):
    input_registry.save_descriptor(_descriptor("input_remove", session_id="sess_remove"))
    input_registry.remove_descriptor("input_remove")

    assert input_registry.get_descriptor("input_remove") is None
    assert input_registry.get_session_input_id("sess_remove") is None


def test_get_registry_stats_reports_current_counts(clean_registry):
    input_registry.save_descriptor(_descriptor("input_stats", session_id="sess_stats"))
    stats = input_registry.get_registry_stats()

    assert stats["input_count"] == 1
    assert stats["session_binding_count"] == 1
    assert stats["max_entries"] == 8
    assert stats["ttl_sec"] == 3600.0


def test_new_input_id_uses_16_hex_entropy_budget():
    from analyst_toolkit.mcp_server.input.ingest import _new_input_id

    generated = _new_input_id("stable-key")
    assert generated == _new_input_id("stable-key")
    assert generated.startswith(INPUT_ID_PREFIX)
    assert len(generated) == len(INPUT_ID_PREFIX) + INPUT_ID_HEX_LENGTH
    assert re.fullmatch(rf"{INPUT_ID_PREFIX}[0-9a-f]{{{INPUT_ID_HEX_LENGTH}}}", generated)
