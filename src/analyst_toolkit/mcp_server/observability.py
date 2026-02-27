"""Observability helpers for MCP server runtime metrics and logging."""

import collections
import json
import logging
import threading
import time
from typing import Any


class RuntimeMetrics:
    """Thread-safe request metrics for basic operability endpoints."""

    def __init__(self, *, started_at: float | None = None) -> None:
        self._lock = threading.Lock()
        self._started_at = started_at if started_at is not None else time.time()
        self._rpc_requests_total = 0
        self._rpc_errors_total = 0
        self._rpc_total_latency_ms = 0.0
        self._rpc_by_method: dict[str, int] = collections.defaultdict(int)
        self._rpc_by_tool: dict[str, int] = collections.defaultdict(int)

    def record_rpc(
        self,
        *,
        method: str,
        duration_ms: float,
        ok: bool,
        tool_name: str | None = None,
    ) -> None:
        with self._lock:
            self._rpc_requests_total += 1
            self._rpc_total_latency_ms += max(duration_ms, 0.0)
            self._rpc_by_method[method or "unknown"] += 1
            if tool_name:
                self._rpc_by_tool[tool_name] += 1
            if not ok:
                self._rpc_errors_total += 1

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            requests = self._rpc_requests_total
            avg_latency_ms = round(self._rpc_total_latency_ms / requests, 2) if requests else 0.0
            return {
                "rpc": {
                    "requests_total": requests,
                    "errors_total": self._rpc_errors_total,
                    "avg_latency_ms": avg_latency_ms,
                    "by_method": dict(self._rpc_by_method),
                    "by_tool": dict(self._rpc_by_tool),
                },
                "uptime_sec": int(max(0, time.time() - self._started_at)),
            }


def log_rpc_event(
    *,
    logger: logging.Logger,
    structured_logs: bool,
    level: int,
    event: str,
    **fields: Any,
) -> None:
    payload = {"event": event, **fields}
    if structured_logs:
        logger.log(level, json.dumps(payload, default=str, sort_keys=True))
        return
    compact = " ".join(
        f"{k}={v}" for k, v in payload.items() if v is not None and not isinstance(v, (dict, list))
    )
    logger.log(level, compact)
