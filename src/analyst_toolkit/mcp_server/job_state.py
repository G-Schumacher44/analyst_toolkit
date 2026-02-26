"""
job_state.py — In-memory async job tracking for long-running MCP tools.
"""

from __future__ import annotations

import threading
import time
import uuid
from copy import deepcopy
from typing import Any


class JobStore:
    """Thread-safe in-memory job store."""

    _lock: threading.Lock = threading.Lock()
    _jobs: dict[str, dict[str, Any]] = {}

    @classmethod
    def create(
        cls,
        module: str,
        run_id: str | None = None,
        inputs: dict[str, Any] | None = None,
    ) -> str:
        now = time.time()
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        with cls._lock:
            cls._jobs[job_id] = {
                "job_id": job_id,
                "module": module,
                "run_id": run_id,
                "state": "queued",
                "created_at": now,
                "updated_at": now,
                "started_at": None,
                "finished_at": None,
                "inputs": deepcopy(inputs or {}),
                "result": None,
                "error": None,
            }
        return job_id

    @classmethod
    def mark_running(cls, job_id: str):
        now = time.time()
        with cls._lock:
            job = cls._jobs.get(job_id)
            if not job:
                return
            job["state"] = "running"
            job["started_at"] = now
            job["updated_at"] = now

    @classmethod
    def mark_succeeded(cls, job_id: str, result: dict[str, Any] | None = None):
        now = time.time()
        with cls._lock:
            job = cls._jobs.get(job_id)
            if not job:
                return
            job["state"] = "succeeded"
            job["finished_at"] = now
            job["updated_at"] = now
            job["result"] = deepcopy(result or {})
            job["error"] = None

    @classmethod
    def mark_failed(cls, job_id: str, error: dict[str, Any]):
        now = time.time()
        with cls._lock:
            job = cls._jobs.get(job_id)
            if not job:
                return
            job["state"] = "failed"
            job["finished_at"] = now
            job["updated_at"] = now
            job["error"] = deepcopy(error)

    @classmethod
    def get(cls, job_id: str) -> dict[str, Any] | None:
        with cls._lock:
            job = cls._jobs.get(job_id)
            return deepcopy(job) if job else None

    @classmethod
    def list(cls, limit: int = 20, state: str | None = None) -> list[dict[str, Any]]:
        with cls._lock:
            rows = list(cls._jobs.values())
        if state:
            rows = [r for r in rows if str(r.get("state")) == state]
        rows.sort(key=lambda r: float(r.get("updated_at") or 0), reverse=True)
        return [deepcopy(r) for r in rows[: max(limit, 1)]]

    @classmethod
    def clear(cls):
        with cls._lock:
            cls._jobs.clear()
