"""
test_hardening.py — Unit tests for StateStore thread safety, coerce_config YAML parsing,
check_upload warning surfacing, TTL eviction, and pipeline integration.
"""

import sys
import threading
import time
import types

import pandas as pd
import pytest

from analyst_toolkit.mcp_server.io import (
    _resolve_path_root,
    append_to_run_history,
    check_upload,
    coerce_config,
    get_run_history,
    save_output,
    upload_artifact,
)
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


def test_coerce_config_unwraps_double_wrapped():
    """Agent passes {module: {module: {...}}} — unwraps one level."""
    inner = {"rules": {"coerce_dtypes": True}}
    cfg = {"normalization": {"normalization": inner}}
    result = coerce_config(cfg, "normalization")
    # After unwrapping: {"normalization": {"rules": ...}}
    assert result == {"normalization": inner}
    assert result["normalization"]["rules"]["coerce_dtypes"] is True


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


def _install_fake_google_storage(monkeypatch, calls: list):
    class FakeBlob:
        def __init__(self, name: str):
            self.name = name

        def upload_from_filename(self, filename: str, content_type: str | None = None):
            calls.append(("upload", self.name, content_type, filename))

    class FakeBucket:
        def __init__(self, name: str):
            self.name = name

        def blob(self, blob_name: str):
            calls.append(("blob", self.name, blob_name))
            return FakeBlob(blob_name)

    class FakeClient:
        def bucket(self, bucket_name: str):
            calls.append(("bucket", bucket_name))
            return FakeBucket(bucket_name)

    storage_mod = types.ModuleType("google.cloud.storage")
    setattr(storage_mod, "Client", FakeClient)

    cloud_mod = types.ModuleType("google.cloud")
    setattr(cloud_mod, "storage", storage_mod)

    google_mod = types.ModuleType("google")
    setattr(google_mod, "cloud", cloud_mod)

    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_mod)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_mod)


def test_save_output_gcs_uses_storage_upload(sample_df, monkeypatch):
    calls: list = []
    _install_fake_google_storage(monkeypatch, calls)

    path = "gs://example-bucket/runs/run_1/imputation_output.csv"
    out = save_output(sample_df, path)

    assert out == path
    assert ("bucket", "example-bucket") in calls
    assert ("blob", "example-bucket", "runs/run_1/imputation_output.csv") in calls
    uploads = [c for c in calls if c[0] == "upload"]
    assert len(uploads) == 1
    assert uploads[0][1] == "runs/run_1/imputation_output.csv"
    assert uploads[0][2] == "text/csv"


def test_save_output_gcs_is_idempotent_for_same_path(sample_df, monkeypatch):
    calls: list = []
    _install_fake_google_storage(monkeypatch, calls)

    path = "gs://example-bucket/runs/shared_run/imputation_output.csv"
    save_output(sample_df, path)
    save_output(sample_df, path)

    uploads = [c for c in calls if c[0] == "upload"]
    assert len(uploads) == 2
    assert uploads[0][1] == "runs/shared_run/imputation_output.csv"
    assert uploads[1][1] == "runs/shared_run/imputation_output.csv"


def test_upload_artifact_falls_back_to_versioned_key_on_primary_failure(monkeypatch, tmp_path):
    local = tmp_path / "report.html"
    local.write_text("<html>ok</html>", encoding="utf-8")

    upload_calls: list[str] = []

    class FakeBlob:
        def __init__(self, name: str):
            self.name = name

        def upload_from_filename(self, filename: str, content_type: str | None = None):
            upload_calls.append(self.name)
            # Simulate a primary-path overwrite/permission failure.
            if len(upload_calls) == 1:
                raise PermissionError("storage.objects.delete access denied")

    class FakeBucket:
        def __init__(self, name: str):
            self.name = name

        def blob(self, blob_name: str):
            return FakeBlob(blob_name)

    class FakeClient:
        def bucket(self, bucket_name: str):
            return FakeBucket(bucket_name)

    storage_mod = types.ModuleType("google.cloud.storage")
    setattr(storage_mod, "Client", FakeClient)
    cloud_mod = types.ModuleType("google.cloud")
    setattr(cloud_mod, "storage", storage_mod)
    google_mod = types.ModuleType("google")
    setattr(google_mod, "cloud", cloud_mod)
    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_mod)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_mod)
    monkeypatch.setenv("ANALYST_REPORT_BUCKET", "gs://example-bucket")
    monkeypatch.setenv("ANALYST_REPORT_PREFIX", "analyst_toolkit/reports")

    out = upload_artifact(
        local_path=str(local),
        run_id="run_retry",
        module="imputation",
        session_id="sess_retry",
    )

    assert out.startswith("https://storage.googleapis.com/example-bucket/")
    assert len(upload_calls) == 2
    assert upload_calls[0].endswith("/imputation/report.html")
    assert upload_calls[1].endswith(".html")
    assert upload_calls[1] != upload_calls[0]


# ---------------------------------------------------------------------------
# History + path root keying — session_id + run_id
# ---------------------------------------------------------------------------


def test_resolve_path_root_includes_session_and_run(sample_df):
    sid = StateStore.save(sample_df, run_id="run_alpha")
    path_root = _resolve_path_root("run_alpha", session_id=sid)
    parts = path_root.split("/")
    assert len(parts) == 3
    assert parts[1] == sid
    assert parts[2] == "run_alpha"


def test_get_run_history_isolation_by_session(sample_df, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    shared_run_id = "run_shared"
    sid_a = StateStore.save(sample_df, run_id=shared_run_id)
    sid_b = StateStore.save(sample_df, run_id=shared_run_id)

    append_to_run_history(shared_run_id, {"module": "diagnostics", "summary": {}}, session_id=sid_a)
    append_to_run_history(shared_run_id, {"module": "imputation", "summary": {}}, session_id=sid_b)

    hist_a = get_run_history(shared_run_id, session_id=sid_a)
    hist_b = get_run_history(shared_run_id, session_id=sid_b)

    assert len(hist_a) == 1
    assert len(hist_b) == 1
    assert hist_a[0]["module"] == "diagnostics"
    assert hist_b[0]["module"] == "imputation"


# ---------------------------------------------------------------------------
# StateStore — TTL eviction
# ---------------------------------------------------------------------------


def test_ttl_eviction_removes_expired_sessions(sample_df, monkeypatch):
    """Sessions accessed longer ago than SESSION_TTL_SECONDS are evicted on next save."""
    import analyst_toolkit.mcp_server.state as state_module

    # Patch TTL to 1 second so we don't wait an hour
    monkeypatch.setattr(state_module, "SESSION_TTL_SECONDS", 1)

    sid = StateStore.save(sample_df)
    assert StateStore.get(sid) is not None

    # Backdate last_accessed so the session appears expired
    with StateStore._lock:
        StateStore._last_accessed[sid] = time.time() - 2  # 2s ago, TTL=1s

    # Trigger cleanup by saving a new session
    StateStore.save(sample_df)

    # The old session should be gone
    assert StateStore.get(sid) is None


def test_non_expired_session_survives_cleanup(sample_df, monkeypatch):
    """Sessions within TTL are NOT evicted."""
    import analyst_toolkit.mcp_server.state as state_module

    monkeypatch.setattr(state_module, "SESSION_TTL_SECONDS", 60)

    sid = StateStore.save(sample_df)
    # Trigger cleanup
    StateStore.cleanup()
    assert StateStore.get(sid) is not None


# ---------------------------------------------------------------------------
# Pipeline integration — normalization changes_made
# ---------------------------------------------------------------------------


def test_normalization_changes_made_rename():
    """apply_normalization changelog counts renamed columns correctly."""
    from analyst_toolkit.m03_normalization.normalize_data import apply_normalization

    df = pd.DataFrame({"old_name": [1, 2], "b": [3, 4]})
    config = {"rules": {"rename_columns": {"old_name": "new_name"}}}
    _, df_norm, changelog = apply_normalization(df, config)

    assert "new_name" in df_norm.columns
    assert "renamed_columns" in changelog
    assert len(changelog["renamed_columns"]) == 1


def test_normalization_changes_made_text_standardize():
    """apply_normalization changelog counts standardized text columns correctly."""
    from analyst_toolkit.m03_normalization.normalize_data import apply_normalization

    df = pd.DataFrame({"name": ["  Alice  ", "BOB"]})
    config = {"rules": {"standardize_text_columns": ["name"]}}
    _, df_norm, changelog = apply_normalization(df, config)

    assert df_norm["name"].tolist() == ["alice", "bob"]
    assert "strings_cleaned" in changelog
    assert len(changelog["strings_cleaned"]) == 1


def test_normalization_no_rules_returns_unchanged():
    """Empty rules → changelog is empty, df unchanged."""
    from analyst_toolkit.m03_normalization.normalize_data import apply_normalization

    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    _, df_norm, changelog = apply_normalization(df, {"rules": {}})

    pd.testing.assert_frame_equal(df, df_norm)
    assert changelog == {}


# ---------------------------------------------------------------------------
# Pipeline integration — imputation empty strategies
# ---------------------------------------------------------------------------


def test_imputation_empty_strategies_returns_unchanged():
    """Empty strategy map should be treated as no-op, not an error."""
    from analyst_toolkit.m07_imputation.run_imputation_pipeline import run_imputation_pipeline

    df = pd.DataFrame({"a": [1, None], "b": ["x", "y"]})
    cfg = {"imputation": {"rules": {"strategies": {}}, "settings": {"plotting": {"run": False}}}}

    out = run_imputation_pipeline(config=cfg, df=df, notebook=False, run_id="run_imp_empty")
    pd.testing.assert_frame_equal(out, df)


# ---------------------------------------------------------------------------
# Pipeline integration — validation pass/fail
# ---------------------------------------------------------------------------


def test_validation_suite_passes_with_correct_schema():
    """run_validation_suite returns passed=True when schema matches."""
    from analyst_toolkit.m02_validation.validate_data import run_validation_suite

    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    config = {
        "schema_validation": {
            "rules": {
                "expected_columns": ["a", "b"],
                "expected_types": {},
                "categorical_values": {},
                "numeric_ranges": {},
            }
        }
    }
    results = run_validation_suite(df, config)
    assert results["schema_conformity"]["passed"] is True


def test_validation_suite_fails_missing_columns():
    """run_validation_suite detects missing columns and marks schema_conformity failed."""
    from analyst_toolkit.m02_validation.validate_data import run_validation_suite

    df = pd.DataFrame({"a": [1, 2]})  # Missing column 'b'
    config = {
        "schema_validation": {
            "rules": {
                "expected_columns": ["a", "b"],
                "expected_types": {},
                "categorical_values": {},
                "numeric_ranges": {},
            }
        }
    }
    results = run_validation_suite(df, config)
    assert results["schema_conformity"]["passed"] is False
    assert "b" in results["schema_conformity"]["details"]["missing_columns"]


def test_validation_suite_fails_dtype_mismatch():
    """run_validation_suite detects dtype mismatches."""
    from analyst_toolkit.m02_validation.validate_data import run_validation_suite

    df = pd.DataFrame({"score": ["high", "low"]})  # object, not int64
    config = {
        "schema_validation": {
            "rules": {
                "expected_columns": [],
                "expected_types": {"score": "int64"},
                "categorical_values": {},
                "numeric_ranges": {},
            }
        }
    }
    results = run_validation_suite(df, config)
    assert results["dtype_enforcement"]["passed"] is False
    assert "score" in results["dtype_enforcement"]["details"]


def test_validation_suite_fails_categorical_violation():
    """run_validation_suite detects values outside allowed set."""
    from analyst_toolkit.m02_validation.validate_data import run_validation_suite

    df = pd.DataFrame({"color": ["red", "blue", "purple"]})  # purple not allowed
    config = {
        "schema_validation": {
            "rules": {
                "expected_columns": [],
                "expected_types": {},
                "categorical_values": {"color": ["red", "blue"]},
                "numeric_ranges": {},
            }
        }
    }
    results = run_validation_suite(df, config)
    assert results["categorical_values"]["passed"] is False


# ---------------------------------------------------------------------------
# Auto-heal integration response behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auto_heal_propagates_child_artifacts_and_status(monkeypatch):
    import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_module

    async def fake_infer(*args, **kwargs):
        return {
            "status": "pass",
            "configs": {
                "normalization": "normalization:\n  rules: {}\n",
                "imputation": (
                    "imputation:\n"
                    "  rules:\n"
                    "    strategies:\n"
                    "      some_col:\n"
                    "        strategy: mode\n"
                ),
            },
            "session_id": "sess_unit",
        }

    async def fake_norm(*args, **kwargs):
        return {
            "status": "pass",
            "session_id": "sess_unit",
            "summary": {"changes_made": 2},
            "artifact_path": "norm_report.html",
            "artifact_url": "https://example.com/norm",
            "export_url": "gs://bucket/norm.csv",
            "plot_urls": {"norm.png": "https://example.com/norm.png"},
        }

    async def fake_imp(*args, **kwargs):
        return {
            "status": "warn",
            "session_id": "sess_unit",
            "summary": {"nulls_filled": 4},
            "artifact_path": "imp_report.html",
            "artifact_url": "https://example.com/imp",
            "export_url": "gs://bucket/imp.csv",
            "plot_urls": {"imp.png": "https://example.com/imp.png"},
        }

    monkeypatch.setattr(auto_heal_module, "_toolkit_infer_configs", fake_infer)
    monkeypatch.setattr(auto_heal_module, "_toolkit_normalization", fake_norm)
    monkeypatch.setattr(auto_heal_module, "_toolkit_imputation", fake_imp)
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "warn"
    assert res["artifact_path"] == "imp_report.html"
    assert res["artifact_url"] == "https://example.com/imp"
    assert res["export_url"] == "gs://bucket/imp.csv"
    assert res["plot_urls"] == {"imp.png": "https://example.com/imp.png"}
    assert res["failed_steps"] == []


@pytest.mark.asyncio
async def test_auto_heal_skips_imputation_when_no_strategies(monkeypatch):
    import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_module

    async def fake_infer(*args, **kwargs):
        return {
            "status": "pass",
            "configs": {
                "normalization": "normalization:\n  rules: {}\n",
                "imputation": "imputation:\n  rules:\n    strategies: {}\n",
            },
            "session_id": "sess_unit",
        }

    async def fake_norm(*args, **kwargs):
        return {
            "status": "pass",
            "session_id": "sess_unit",
            "summary": {"changes_made": 0},
            "artifact_path": "norm_report.html",
            "artifact_url": "https://example.com/norm",
            "export_url": "gs://bucket/norm.csv",
            "plot_urls": {"norm.png": "https://example.com/norm.png"},
        }

    async def fake_imp(*args, **kwargs):
        raise AssertionError("imputation should not be called when strategies are empty")

    monkeypatch.setattr(auto_heal_module, "_toolkit_infer_configs", fake_infer)
    monkeypatch.setattr(auto_heal_module, "_toolkit_normalization", fake_norm)
    monkeypatch.setattr(auto_heal_module, "_toolkit_imputation", fake_imp)
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "pass"
    assert res["artifact_path"] == "norm_report.html"
    assert res["artifact_url"] == "https://example.com/norm"
    assert res["export_url"] == "gs://bucket/norm.csv"
    assert res["plot_urls"] == {"norm.png": "https://example.com/norm.png"}
    assert res["failed_steps"] == []
    assert res["summary"]["imputation"]["skipped"] is True


@pytest.mark.asyncio
async def test_auto_heal_returns_error_when_step_raises(monkeypatch):
    import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_module

    async def fake_infer(*args, **kwargs):
        return {
            "status": "pass",
            "configs": {"normalization": "normalization:\n  rules: {}\n"},
            "session_id": "sess_unit",
        }

    async def fake_norm(*args, **kwargs):
        raise RuntimeError("normalization boom")

    monkeypatch.setattr(auto_heal_module, "_toolkit_infer_configs", fake_infer)
    monkeypatch.setattr(auto_heal_module, "_toolkit_normalization", fake_norm)
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "error"
    assert "normalization" in res["failed_steps"]
    assert "normalization" in res["summary"]
    assert "normalization boom" in res["summary"]["normalization"]["error"]
