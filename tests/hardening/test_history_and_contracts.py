import threading

import pandas as pd

from analyst_toolkit.mcp_server.io import (
    _resolve_path_root,
    append_to_run_history,
    build_artifact_contract,
    get_last_history_read_meta,
    get_run_history,
)
from analyst_toolkit.mcp_server.state import StateStore


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


def test_append_to_run_history_is_thread_safe(sample_df, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    run_id = "run_threadsafe_history"
    session_id = StateStore.save(sample_df, run_id=run_id)
    errors: list[Exception] = []
    error_lock = threading.Lock()
    total = 30

    def worker(i: int):
        try:
            append_to_run_history(
                run_id,
                {
                    "module": f"m{i}",
                    "status": "pass",
                    "summary": {"index": i},
                },
                session_id=session_id,
            )
        except Exception as exc:
            with error_lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(total)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    history = get_run_history(run_id, session_id=session_id)
    assert len(history) == total
    assert {entry["module"] for entry in history} == {f"m{i}" for i in range(total)}


def test_append_to_run_history_serializes_dataframe_details(sample_df, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    run_id = "run_jsonsafe_history"
    session_id = StateStore.save(sample_df, run_id=run_id)
    entry = {
        "module": "validation",
        "status": "fail",
        "summary": {"passed": False},
        "violations_detail": {
            "categorical_values": {
                "violating_rows": pd.DataFrame({"bad": ["x", "y"]}),
            }
        },
    }
    append_to_run_history(run_id, entry, session_id=session_id)

    history = get_run_history(run_id, session_id=session_id)
    assert len(history) == 1
    details = history[0]["violations_detail"]["categorical_values"]["violating_rows"]
    assert details["_type"] == "dataframe"
    assert details["row_count"] == 2


def test_get_run_history_recovers_from_corrupt_json(sample_df, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    run_id = "run_corrupt_history"
    session_id = StateStore.save(sample_df, run_id=run_id)
    path_root = _resolve_path_root(run_id, session_id)
    history_dir = tmp_path / "exports/reports/history" / path_root
    history_dir.mkdir(parents=True, exist_ok=True)
    history_file = history_dir / f"{run_id}_history.json"
    history_file.write_text(
        '[{"module":"diagnostics","status":"pass"},{"module":"validation","status":"fail",]',
        encoding="utf-8",
    )

    history = get_run_history(run_id, session_id=session_id)
    meta = get_last_history_read_meta(run_id, session_id=session_id)

    assert history
    assert history[0]["module"] == "diagnostics"
    assert meta["parse_errors"]
    assert meta["skipped_records"] > 0


def test_build_artifact_contract_warns_for_server_local_export(tmp_path):
    local_export = tmp_path / "output.csv"
    local_export.write_text("a\n1\n", encoding="utf-8")

    contract = build_artifact_contract(str(local_export), expect_html=False, expect_xlsx=False)

    assert contract["artifact_matrix"]["data_export"]["status"] == "available"
    assert contract["artifact_matrix"]["data_export"]["reason"] == "server_local_path"
    assert any("server runtime filesystem" in w for w in contract["artifact_warnings"])
