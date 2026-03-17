"""
state.py — In-memory state management for MCP tool pipelines.
"""

import logging
import threading
import time
import uuid
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 3600  # 1 hour


class StateStore:
    """
    A simple in-memory store for DataFrames, enabling 'Pipeline Mode'.
    Includes a basic TTL mechanism to prevent memory exhaustion.
    Thread-safe via a class-level lock.
    """

    _lock: threading.Lock = threading.Lock()
    _sessions: Dict[str, pd.DataFrame] = {}
    _metadata: Dict[str, dict] = {}
    _last_accessed: Dict[str, float] = {}
    _session_run_ids: Dict[str, str] = {}
    _session_start_times: Dict[str, str] = {}
    _session_configs: Dict[str, Dict[str, str]] = {}

    @classmethod
    def save(
        cls, df: pd.DataFrame, session_id: Optional[str] = None, run_id: Optional[str] = None
    ) -> str:
        """Save a DataFrame to the store. Generates a new session_id if not provided."""
        with cls._lock:
            cls._cleanup_unsafe()

            if session_id is None:
                session_id = f"sess_{uuid.uuid4().hex[:8]}"
                cls._session_start_times[session_id] = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")

            cls._sessions[session_id] = df
            cls._metadata[session_id] = {
                "row_count": len(df),
                "col_count": len(df.columns),
                "updated_at": pd.Timestamp.now().isoformat(),
            }
            cls._last_accessed[session_id] = time.time()

            if run_id:
                cls._session_run_ids[session_id] = run_id

        logger.info(f"Saved session {session_id} (run_id: {run_id}, shape: {df.shape})")
        return session_id

    @classmethod
    def get(cls, session_id: str) -> Optional[pd.DataFrame]:
        """Retrieve a DataFrame from the store by session_id."""
        with cls._lock:
            if session_id in cls._sessions:
                cls._last_accessed[session_id] = time.time()
                return cls._sessions[session_id]
        return None

    @classmethod
    def get_run_id(cls, session_id: str) -> Optional[str]:
        """Retrieve the run_id associated with a session."""
        with cls._lock:
            return cls._session_run_ids.get(session_id)

    @classmethod
    def get_session_start(cls, session_id: str) -> Optional[str]:
        """Retrieve the start time associated with a session."""
        with cls._lock:
            return cls._session_start_times.get(session_id)

    @classmethod
    def get_metadata(cls, session_id: str) -> Optional[dict]:
        """Retrieve metadata for a session."""
        with cls._lock:
            return cls._metadata.get(session_id)

    @classmethod
    def save_config(cls, session_id: str, module: str, config_yaml: str) -> None:
        """Store an inferred config YAML string for a module in session scope."""
        with cls._lock:
            if session_id not in cls._session_configs:
                cls._session_configs[session_id] = {}
            cls._session_configs[session_id][module] = config_yaml

    @classmethod
    def get_config(cls, session_id: str, module: str) -> Optional[str]:
        """Retrieve a previously stored inferred config for a module."""
        with cls._lock:
            return cls._session_configs.get(session_id, {}).get(module)

    @classmethod
    def get_configs(cls, session_id: str) -> Dict[str, str]:
        """Retrieve all stored inferred configs for a session."""
        with cls._lock:
            return dict(cls._session_configs.get(session_id, {}))

    @classmethod
    def fork(
        cls,
        source_session_id: str,
        *,
        run_id: Optional[str] = None,
        copy_configs: bool = True,
    ) -> Optional[str]:
        """Clone a session's DataFrame (and optionally configs) into a new session.

        Returns the new session_id, or None if the source session does not exist.
        """
        with cls._lock:
            df = cls._sessions.get(source_session_id)
            if df is None:
                return None

            new_session_id = f"sess_{uuid.uuid4().hex[:8]}"
            now_ts = pd.Timestamp.now()
            cls._sessions[new_session_id] = df.copy()
            cls._metadata[new_session_id] = {
                "row_count": len(df),
                "col_count": len(df.columns),
                "updated_at": now_ts.isoformat(),
            }
            cls._last_accessed[new_session_id] = time.time()
            cls._session_start_times[new_session_id] = now_ts.strftime("%Y%m%d_%H%M%S")

            if run_id:
                cls._session_run_ids[new_session_id] = run_id

            if copy_configs and source_session_id in cls._session_configs:
                cls._session_configs[new_session_id] = dict(
                    cls._session_configs[source_session_id]
                )

        logger.info(
            "Forked session %s → %s (run_id: %s, copy_configs: %s)",
            source_session_id,
            new_session_id,
            run_id,
            copy_configs,
        )
        return new_session_id

    @classmethod
    def rebind_run_id(cls, session_id: str, run_id: str) -> bool:
        """Rebind a session to a new run_id. Returns False if session does not exist."""
        with cls._lock:
            if session_id not in cls._sessions:
                return False
            cls._session_run_ids[session_id] = run_id
        logger.info("Rebound session %s to run_id %s", session_id, run_id)
        return True

    @classmethod
    def list_sessions(cls) -> Dict[str, dict]:
        """List available sessions and their metadata."""
        with cls._lock:
            return {k: cls._metadata.get(k, {}) for k in cls._sessions.keys()}

    @classmethod
    def _cleanup_unsafe(cls):
        """Evict expired sessions. Must be called with _lock held."""
        now = time.time()
        expired = [
            sid
            for sid, last_ts in cls._last_accessed.items()
            if now - last_ts > SESSION_TTL_SECONDS
        ]
        for sid in expired:
            cls._sessions.pop(sid, None)
            cls._metadata.pop(sid, None)
            cls._last_accessed.pop(sid, None)
            cls._session_run_ids.pop(sid, None)
            cls._session_start_times.pop(sid, None)
            cls._session_configs.pop(sid, None)
            logger.info(f"Evicted expired session {sid} (TTL reached)")

    @classmethod
    def cleanup(cls):
        """Remove sessions that have exceeded the TTL."""
        with cls._lock:
            cls._cleanup_unsafe()

    @classmethod
    def clear(cls, session_id: Optional[str] = None):
        """Clear one or all sessions."""
        with cls._lock:
            if session_id:
                cls._sessions.pop(session_id, None)
                cls._metadata.pop(session_id, None)
                cls._last_accessed.pop(session_id, None)
                cls._session_run_ids.pop(session_id, None)
                cls._session_start_times.pop(session_id, None)
                cls._session_configs.pop(session_id, None)
            else:
                cls._sessions.clear()
                cls._metadata.clear()
                cls._last_accessed.clear()
                cls._session_run_ids.clear()
                cls._session_start_times.clear()
                cls._session_configs.clear()
