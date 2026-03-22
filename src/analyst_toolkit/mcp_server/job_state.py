"""
job_state.py — In-memory async job tracking for long-running MCP tools.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


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
    return parsed if parsed >= 0 else default


class JobStore:
    """
    Thread-safe job store with best-effort local persistence.

    Persistence default path:
      exports/reports/jobs/job_state.json
    Override with ANALYST_MCP_JOB_STATE_PATH.
    """

    _lock: threading.Lock = threading.Lock()
    _jobs: dict[str, dict[str, Any]] = {}
    _loaded: bool = False
    _max_jobs: int = _env_int("ANALYST_MCP_MAX_JOBS", 512)
    _job_ttl_sec: float = _env_float("ANALYST_MCP_JOB_TTL_SEC", 86400.0)

    @classmethod
    def _state_path(cls) -> Path:
        raw = os.environ.get("ANALYST_MCP_JOB_STATE_PATH", "").strip()
        if raw:
            return Path(raw)
        return Path("exports/reports/jobs/job_state.json")

    @classmethod
    def _to_json_safe(cls, value: Any) -> Any:
        # Roundtrip through JSON with default=str to ensure persistence never crashes on
        # unexpected objects in result/error payloads.
        return json.loads(json.dumps(value, default=str))

    @classmethod
    def _ensure_loaded_unsafe(cls):
        if cls._loaded:
            return
        path = cls._state_path()
        if not path.exists():
            cls._loaded = True
            return
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                cls._jobs = loaded
            else:
                cls._jobs = {}
        except Exception as exc:
            logger.warning("Failed to load job state from %s: %s", path, exc)
            cls._jobs = {}
        cls._loaded = True

    @classmethod
    def _persist_unsafe(cls):
        path = cls._state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = cls._to_json_safe(cls._jobs)
        tmp = path.with_suffix(f"{path.suffix}.tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(path)

    @classmethod
    def _prune_unsafe(cls, now: float) -> None:
        ttl = cls._job_ttl_sec
        if ttl > 0:
            expired = []
            for job_id, job in list(cls._jobs.items()):
                if str(job.get("state")) not in {"succeeded", "failed"}:
                    continue
                anchor = float(job.get("finished_at") or job.get("updated_at") or 0)
                if anchor and now - anchor > ttl:
                    expired.append(job_id)
            for job_id in expired:
                cls._jobs.pop(job_id, None)

        overflow = len(cls._jobs) - cls._max_jobs
        if overflow <= 0:
            return

        def sort_key(item: tuple[str, dict[str, Any]]) -> tuple[int, float]:
            _, job = item
            state = str(job.get("state"))
            terminal_rank = 0 if state in {"succeeded", "failed"} else 1
            timestamp = float(job.get("finished_at") or job.get("updated_at") or 0)
            return terminal_rank, timestamp

        for job_id, _job in sorted(cls._jobs.items(), key=sort_key):
            if len(cls._jobs) <= cls._max_jobs:
                break
            cls._jobs.pop(job_id, None)

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
            cls._ensure_loaded_unsafe()
            cls._prune_unsafe(now)
            cls._jobs[job_id] = {
                "job_id": job_id,
                "module": module,
                "run_id": run_id,
                "state": "queued",
                "created_at": now,
                "updated_at": now,
                "started_at": None,
                "finished_at": None,
                "inputs": cls._to_json_safe(deepcopy(inputs or {})),
                "result": None,
                "error": None,
            }
            cls._persist_unsafe()
        return job_id

    @classmethod
    def mark_running(cls, job_id: str):
        now = time.time()
        with cls._lock:
            cls._ensure_loaded_unsafe()
            cls._prune_unsafe(now)
            job = cls._jobs.get(job_id)
            if not job:
                return
            job["state"] = "running"
            job["started_at"] = now
            job["updated_at"] = now
            cls._persist_unsafe()

    @classmethod
    def mark_succeeded(cls, job_id: str, result: dict[str, Any] | None = None):
        now = time.time()
        with cls._lock:
            cls._ensure_loaded_unsafe()
            cls._prune_unsafe(now)
            job = cls._jobs.get(job_id)
            if not job:
                return
            job["state"] = "succeeded"
            job["finished_at"] = now
            job["updated_at"] = now
            job["result"] = cls._to_json_safe(deepcopy(result or {}))
            job["error"] = None
            cls._prune_unsafe(now)
            cls._persist_unsafe()

    @classmethod
    def mark_failed(cls, job_id: str, error: dict[str, Any]):
        now = time.time()
        with cls._lock:
            cls._ensure_loaded_unsafe()
            cls._prune_unsafe(now)
            job = cls._jobs.get(job_id)
            if not job:
                return
            job["state"] = "failed"
            job["finished_at"] = now
            job["updated_at"] = now
            job["error"] = cls._to_json_safe(deepcopy(error))
            cls._prune_unsafe(now)
            cls._persist_unsafe()

    @classmethod
    def get(cls, job_id: str) -> dict[str, Any] | None:
        with cls._lock:
            cls._ensure_loaded_unsafe()
            cls._prune_unsafe(time.time())
            job = cls._jobs.get(job_id)
            return deepcopy(job) if job else None

    @classmethod
    def list(cls, limit: int = 20, state: str | None = None) -> list[dict[str, Any]]:
        with cls._lock:
            cls._ensure_loaded_unsafe()
            cls._prune_unsafe(time.time())
            rows = list(cls._jobs.values())
        if state:
            rows = [r for r in rows if str(r.get("state")) == state]
        rows.sort(key=lambda r: float(r.get("updated_at") or 0), reverse=True)
        return [deepcopy(r) for r in rows[: max(limit, 1)]]

    @classmethod
    def clear(cls):
        with cls._lock:
            cls._ensure_loaded_unsafe()
            cls._jobs.clear()
            cls._persist_unsafe()
