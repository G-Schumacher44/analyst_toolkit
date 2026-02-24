"""
test_hardening.py — Unit tests for StateStore thread safety, coerce_config YAML parsing,
and check_upload warning surfacing.
"""

import threading

import pandas as pd
import pytest

from analyst_toolkit.mcp_server.io import check_upload, coerce_config
from analyst_toolkit.mcp_server.state import StateStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_state():
    """Wipe StateStore before and after every test."""
    StateStore.clear()
    yield
    StateStore.clear()


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


# ---------------------------------------------------------------------------
# StateStore — basic round-trip
# ---------------------------------------------------------------------------


def test_save_and_get(sample_df):
    sid = StateStore.save(sample_df)
    assert sid.startswith("sess_")
    result = StateStore.get(sid)
    pd.testing.assert_frame_equal(result, sample_df)


def test_get_missing_returns_none():
    assert StateStore.get("does_not_exist") is None


def test_save_preserves_run_id(sample_df):
    sid = StateStore.save(sample_df, run_id="run_abc")
    assert StateStore.get_run_id(sid) == "run_abc"


def test_clear_single_session(sample_df):
    sid = StateStore.save(sample_df)
    StateStore.clear(sid)
    assert StateStore.get(sid) is None


def test_clear_all(sample_df):
    sid1 = StateStore.save(sample_df)
    sid2 = StateStore.save(sample_df)
    StateStore.clear()
    assert StateStore.get(sid1) is None
    assert StateStore.get(sid2) is None


# ---------------------------------------------------------------------------
# StateStore — thread safety
# ---------------------------------------------------------------------------


def test_concurrent_saves_are_thread_safe(sample_df):
    """Concurrent saves must not corrupt the store or raise exceptions."""
    ids = []
    errors = []
    lock = threading.Lock()

    def worker(i):
        try:
            df = pd.DataFrame({"col": [i]})
            sid = StateStore.save(df, run_id=f"run_{i}")
            with lock:
                ids.append(sid)
        except Exception as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Errors during concurrent saves: {errors}"
    assert len(ids) == 20
    # All saved sessions should be retrievable
    for sid in ids:
        assert StateStore.get(sid) is not None


def test_concurrent_reads_are_safe(sample_df):
    """Concurrent reads on the same session should not raise."""
    sid = StateStore.save(sample_df)
    errors = []
    lock = threading.Lock()

    def reader():
        try:
            StateStore.get(sid)
        except Exception as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=reader) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors


# ---------------------------------------------------------------------------
# coerce_config — YAML auto-parse
# ---------------------------------------------------------------------------


def test_coerce_config_none_returns_empty():
    assert coerce_config(None, "normalization") == {}


def test_coerce_config_plain_dict_passes_through():
    cfg = {"normalization": {"rules": {"coerce_dtypes": True}}}
    result = coerce_config(cfg, "normalization")
    assert result == cfg


def test_coerce_config_parses_raw_yaml_string():
    yaml_str = "rules:\n  coerce_dtypes: true\n  standardize_text_columns: [name]\n"
    result = coerce_config(yaml_str, "normalization")
    assert isinstance(result, dict)
    assert result["rules"]["coerce_dtypes"] is True
    assert result["rules"]["standardize_text_columns"] == ["name"]


def test_coerce_config_parses_yaml_string_inside_module_key():
    """Agent passes {module: yaml_string} instead of {module: dict}."""
    yaml_str = "rules:\n  coerce_dtypes: true\n"
    cfg = {"normalization": yaml_str}
    result = coerce_config(cfg, "normalization")
    assert isinstance(result["normalization"], dict)
    assert result["normalization"]["rules"]["coerce_dtypes"] is True


def test_coerce_config_invalid_yaml_returns_empty():
    result = coerce_config("{{invalid: yaml: [}}", "normalization")
    assert result == {}


def test_coerce_config_non_dict_non_string_returns_empty():
    assert coerce_config(42, "normalization") == {}  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# check_upload — warning surfacing
# ---------------------------------------------------------------------------


def test_check_upload_passes_through_valid_url():
    warnings: list = []
    url = check_upload("https://storage.googleapis.com/bucket/path.html", "path.html", warnings)
    assert url == "https://storage.googleapis.com/bucket/path.html"
    assert warnings == []


def test_check_upload_appends_warning_on_empty_url():
    warnings: list = []
    url = check_upload("", "exports/reports/normalization/run1_report.html", warnings)
    assert url == ""
    assert len(warnings) == 1
    assert "exports/reports/normalization/run1_report.html" in warnings[0]


def test_check_upload_accumulates_multiple_warnings():
    warnings: list = []
    check_upload("", "report.html", warnings)
    check_upload("", "report.xlsx", warnings)
    assert len(warnings) == 2
