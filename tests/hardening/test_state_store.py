import threading
import time

import pytest

from analyst_toolkit.mcp_server.state import StateStore


def test_save_and_get(sample_df):
    sid = StateStore.save(sample_df)
    assert sid.startswith("sess_")
    result = StateStore.get(sid)
    import pandas as pd

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


def test_concurrent_saves_are_thread_safe(sample_df):
    """Concurrent saves must not corrupt the store or raise exceptions."""
    ids = []
    errors = []
    lock = threading.Lock()

    def worker(i):
        try:
            import pandas as pd

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


def test_ttl_eviction_removes_expired_sessions(sample_df, monkeypatch):
    """Sessions accessed longer ago than SESSION_TTL_SECONDS are evicted on next save."""
    import analyst_toolkit.mcp_server.state as state_module

    monkeypatch.setattr(state_module, "SESSION_TTL_SECONDS", 1)

    sid = StateStore.save(sample_df)
    assert StateStore.get(sid) is not None

    StateStore.backdate_session_for_test(sid, time.time() - 2)

    StateStore.save(sample_df)

    assert StateStore.get(sid) is None


def test_non_expired_session_survives_cleanup(sample_df, monkeypatch):
    """Sessions within TTL are NOT evicted."""
    import analyst_toolkit.mcp_server.state as state_module

    monkeypatch.setattr(state_module, "SESSION_TTL_SECONDS", 60)

    sid = StateStore.save(sample_df)
    StateStore.cleanup()
    assert StateStore.get(sid) is not None


def test_sqlite_backend_save_and_get(sample_df, tmp_path, monkeypatch):
    monkeypatch.setenv("ANALYST_MCP_SESSION_BACKEND", "sqlite")
    monkeypatch.setenv("ANALYST_MCP_SESSION_DB_PATH", str(tmp_path / "session_store.db"))

    sid = StateStore.save(sample_df, run_id="sqlite_run")

    assert StateStore.policy()["backend"] == "sqlite"
    assert StateStore.policy()["durable"] is True
    result = StateStore.get(sid)

    import pandas as pd

    pd.testing.assert_frame_equal(result, sample_df)
    assert StateStore.get_run_id(sid) == "sqlite_run"


def test_sqlite_backend_persists_configs_and_fork(sample_df, tmp_path, monkeypatch):
    monkeypatch.setenv("ANALYST_MCP_SESSION_BACKEND", "sqlite")
    monkeypatch.setenv("ANALYST_MCP_SESSION_DB_PATH", str(tmp_path / "session_store.db"))

    sid = StateStore.save(sample_df, run_id="sqlite_run")
    StateStore.save_config(sid, "validation", "validation:\n  run: true\n")

    forked = StateStore.fork(sid, run_id="forked_sqlite_run", copy_configs=True)

    assert forked is not None
    assert StateStore.get_run_id(forked) == "forked_sqlite_run"
    assert StateStore.get_config(forked, "validation") == StateStore.get_config(sid, "validation")


def test_sqlite_backend_rebind_and_clear(sample_df, tmp_path, monkeypatch):
    monkeypatch.setenv("ANALYST_MCP_SESSION_BACKEND", "sqlite")
    monkeypatch.setenv("ANALYST_MCP_SESSION_DB_PATH", str(tmp_path / "session_store.db"))

    sid = StateStore.save(sample_df, run_id="sqlite_run")

    assert StateStore.rebind_run_id(sid, "sqlite_rebound") is True
    assert StateStore.get_run_id(sid) == "sqlite_rebound"

    StateStore.clear(sid)
    assert StateStore.get(sid) is None


def test_sqlite_backend_ttl_cleanup(sample_df, tmp_path, monkeypatch):
    import analyst_toolkit.mcp_server.state as state_module

    monkeypatch.setenv("ANALYST_MCP_SESSION_BACKEND", "sqlite")
    monkeypatch.setenv("ANALYST_MCP_SESSION_DB_PATH", str(tmp_path / "session_store.db"))
    monkeypatch.setattr(state_module, "SESSION_TTL_SECONDS", 1)

    sid = StateStore.save(sample_df, run_id="sqlite_run")
    StateStore.backdate_session_for_test(sid, time.time() - 2)

    StateStore.cleanup()
    assert StateStore.get(sid) is None


def test_sqlite_concurrent_saves_are_thread_safe(tmp_path, monkeypatch):
    monkeypatch.setenv("ANALYST_MCP_SESSION_BACKEND", "sqlite")
    monkeypatch.setenv("ANALYST_MCP_SESSION_DB_PATH", str(tmp_path / "session_store.db"))
    ids = []
    errors = []
    lock = threading.Lock()

    def worker(i):
        try:
            import pandas as pd

            df = pd.DataFrame({"col": [i]})
            sid = StateStore.save(df, run_id=f"run_{i}")
            with lock:
                ids.append(sid)
        except Exception as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, f"Errors during concurrent sqlite saves: {errors}"
    assert len(ids) == 20
    for sid in ids:
        assert StateStore.get(sid) is not None


def test_sqlite_concurrent_reads_are_safe(sample_df, tmp_path, monkeypatch):
    monkeypatch.setenv("ANALYST_MCP_SESSION_BACKEND", "sqlite")
    monkeypatch.setenv("ANALYST_MCP_SESSION_DB_PATH", str(tmp_path / "session_store.db"))
    sid = StateStore.save(sample_df, run_id="sqlite_run")
    errors = []
    lock = threading.Lock()

    def reader():
        try:
            StateStore.get(sid)
        except Exception as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=reader) for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors


def test_sqlite_state_path_defaults_to_private_state_dir(monkeypatch, tmp_path):
    import analyst_toolkit.mcp_server.state as state_module

    monkeypatch.delenv("ANALYST_MCP_SESSION_DB_PATH", raising=False)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state_home"))

    path = state_module._sqlite_state_path()

    assert path == (tmp_path / "state_home" / "analyst_toolkit" / "session_store.db").resolve()


def test_sqlite_state_path_treats_blank_env_as_unset(monkeypatch, tmp_path):
    import analyst_toolkit.mcp_server.state as state_module

    monkeypatch.setenv("ANALYST_MCP_SESSION_DB_PATH", "   ")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state_home"))

    path = state_module._sqlite_state_path()

    assert path == (tmp_path / "state_home" / "analyst_toolkit" / "session_store.db").resolve()


def test_sqlite_state_path_rejects_exports_root(monkeypatch):
    import analyst_toolkit.mcp_server.state as state_module

    monkeypatch.setenv("ANALYST_MCP_SESSION_DB_PATH", "exports/reports/state/session_store.db")

    with pytest.raises(ValueError, match="cannot use a path under ./exports"):
        state_module._sqlite_state_path()
