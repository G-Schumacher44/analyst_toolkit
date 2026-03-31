"""test_job_state.py — persistence and concurrency checks for async job store."""

import threading
import time

from analyst_toolkit.mcp_server.job_state import JobStore


def _reset_job_store(tmp_path, monkeypatch):
    state_path = tmp_path / "jobs" / "job_state.json"
    monkeypatch.setenv("ANALYST_MCP_JOB_STATE_PATH", str(state_path))
    monkeypatch.setattr(JobStore, "_jobs", {})
    monkeypatch.setattr(JobStore, "_loaded", False)
    return state_path


def test_job_store_persists_and_recovers(tmp_path, monkeypatch):
    path = _reset_job_store(tmp_path, monkeypatch)

    job_id = JobStore.create(module="auto_heal", run_id="run_1", inputs={"a": 1})
    JobStore.mark_running(job_id)
    JobStore.mark_succeeded(job_id, result={"status": "pass"})

    assert path.exists()

    # Simulate process restart / fresh interpreter load.
    monkeypatch.setattr(JobStore, "_jobs", {})
    monkeypatch.setattr(JobStore, "_loaded", False)

    recovered = JobStore.get(job_id)
    assert recovered is not None
    assert recovered["state"] == "succeeded"
    assert recovered["result"]["status"] == "pass"


def test_job_store_thread_safe_under_concurrent_creates(tmp_path, monkeypatch):
    _reset_job_store(tmp_path, monkeypatch)

    created: list[str] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def worker(i: int):
        try:
            job_id = JobStore.create(module="auto_heal", run_id=f"run_{i}")
            JobStore.mark_running(job_id)
            JobStore.mark_succeeded(job_id, result={"i": i})
            with lock:
                created.append(job_id)
        except Exception as exc:  # pragma: no cover - guard for thread exceptions
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(25)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(created) == 25
    assert len(set(created)) == 25
    jobs = JobStore.list(limit=50)
    assert len(jobs) >= 25


def test_job_store_prunes_old_terminal_jobs_by_ttl(tmp_path, monkeypatch):
    _reset_job_store(tmp_path, monkeypatch)
    monkeypatch.setattr(JobStore, "_job_ttl_sec", 5.0)

    now = 1000.0
    monkeypatch.setattr(time, "time", lambda: now)
    old_job = JobStore.create(module="auto_heal", run_id="run_old")
    JobStore.mark_running(old_job)
    JobStore.mark_succeeded(old_job, result={"status": "pass"})

    now = 1007.0
    fresh_job = JobStore.create(module="auto_heal", run_id="run_fresh")

    assert JobStore.get(old_job) is None
    assert JobStore.get(fresh_job) is not None


def test_job_store_caps_retained_jobs(tmp_path, monkeypatch):
    _reset_job_store(tmp_path, monkeypatch)
    monkeypatch.setattr(JobStore, "_max_jobs", 3)
    monkeypatch.setattr(JobStore, "_job_ttl_sec", 0.0)

    created: list[str] = []
    for idx in range(5):
        job_id = JobStore.create(module="auto_heal", run_id=f"run_{idx}")
        JobStore.mark_running(job_id)
        JobStore.mark_succeeded(job_id, result={"idx": idx})
        created.append(job_id)

    jobs = JobStore.list(limit=10)
    job_ids = {job["job_id"] for job in jobs}

    assert len(jobs) == 3
    assert created[0] not in job_ids
    assert created[1] not in job_ids


def test_job_store_does_not_prune_in_flight_jobs(tmp_path, monkeypatch):
    _reset_job_store(tmp_path, monkeypatch)
    monkeypatch.setattr(JobStore, "_max_jobs", 1)
    monkeypatch.setattr(JobStore, "_job_ttl_sec", 0.0)

    first_succeeded = JobStore.create(module="auto_heal", run_id="done_1")
    JobStore.mark_running(first_succeeded)
    JobStore.mark_succeeded(first_succeeded, result={"status": "pass"})

    queued = JobStore.create(module="auto_heal", run_id="queued")
    running = JobStore.create(module="auto_heal", run_id="running")
    JobStore.mark_running(running)
    second_succeeded = JobStore.create(module="auto_heal", run_id="done_2")
    JobStore.mark_running(second_succeeded)
    JobStore.mark_succeeded(second_succeeded, result={"status": "pass"})

    queued_job = JobStore.get(queued)
    running_job = JobStore.get(running)
    first_succeeded_job = JobStore.get(first_succeeded)
    second_succeeded_job = JobStore.get(second_succeeded)

    assert queued_job is not None
    assert queued_job["state"] == "queued"
    assert running_job is not None
    assert running_job["state"] == "running"
    assert first_succeeded_job is None
    assert second_succeeded_job is not None
    assert second_succeeded_job["state"] == "succeeded"
