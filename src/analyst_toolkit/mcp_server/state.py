"""state.py — Session state management for MCP tool pipelines."""

import json
import logging
import os
import pickle
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = int(os.environ.get("ANALYST_MCP_SESSION_TTL_SEC", 3600))
SESSION_MAX_ENTRIES = int(os.environ.get("ANALYST_MCP_SESSION_MAX_ENTRIES", 32))
SESSION_SQLITE_PATH_DEFAULT = "analyst_toolkit/session_store.db"


def _session_state_home() -> Path:
    xdg_state_home = os.environ.get("XDG_STATE_HOME", "").strip()
    if xdg_state_home:
        return Path(xdg_state_home).expanduser().resolve(strict=False)
    return (Path.home() / ".local" / "state").resolve(strict=False)


def _session_backend() -> str:
    backend = os.environ.get("ANALYST_MCP_SESSION_BACKEND", "memory").strip().lower()
    return backend if backend in {"memory", "sqlite"} else "memory"


def _sqlite_state_path() -> Path:
    raw_path = os.environ.get("ANALYST_MCP_SESSION_DB_PATH", "").strip()
    if raw_path:
        path = Path(raw_path).expanduser().resolve(strict=False)
    else:
        path = (_session_state_home() / SESSION_SQLITE_PATH_DEFAULT).resolve(strict=False)

    exports_root = (Path.cwd() / "exports").resolve(strict=False)
    path_parents = {path, *path.parents}
    if exports_root in path_parents:
        raise ValueError(
            "SQLite session persistence cannot use a path under ./exports; "
            "choose a private state path outside public artifact roots."
        )
    return path


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
    def _using_sqlite(cls) -> bool:
        return _session_backend() == "sqlite"

    @classmethod
    def _sqlite_connect_unsafe(cls) -> sqlite3.Connection:
        path = _sqlite_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=10.0)
        try:
            os.chmod(path, 0o600)
        except OSError:
            logger.debug("Could not tighten SQLite session store permissions for %s", path)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                run_id TEXT,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_accessed REAL NOT NULL,
                dataframe_blob BLOB NOT NULL,
                metadata_json TEXT NOT NULL,
                configs_json TEXT NOT NULL
            )
            """
        )
        return conn

    @classmethod
    def _sqlite_fetch_row_unsafe(cls, conn: sqlite3.Connection, session_id: str):
        cursor = conn.execute(
            """
            SELECT session_id, run_id, started_at, updated_at, last_accessed, dataframe_blob,
                   metadata_json, configs_json
            FROM sessions
            WHERE session_id = ?
            """,
            (session_id,),
        )
        return cursor.fetchone()

    @classmethod
    def _sqlite_configs_from_row(cls, row) -> Dict[str, str]:
        if row is None or not row[7]:
            return {}
        loaded = json.loads(row[7])
        return loaded if isinstance(loaded, dict) else {}

    @classmethod
    def _sqlite_metadata_from_row(cls, row) -> dict:
        if row is None or not row[6]:
            return {}
        loaded = json.loads(row[6])
        return loaded if isinstance(loaded, dict) else {}

    @classmethod
    def _sqlite_df_from_row(cls, row) -> pd.DataFrame:
        # SQLite session blobs come only from this process's own StateStore writes.
        return pickle.loads(row[5])

    @classmethod
    def _sqlite_evict_session_unsafe(cls, conn: sqlite3.Connection, sid: str, reason: str) -> None:
        conn.execute("DELETE FROM sessions WHERE session_id = ?", (sid,))
        logger.info("Evicted session %s (%s)", sid, reason)

    @classmethod
    def _sqlite_cleanup_unsafe(cls, conn: sqlite3.Connection) -> None:
        now = time.time()
        expiry_cutoff = now - SESSION_TTL_SECONDS
        expired_rows = conn.execute(
            "SELECT session_id FROM sessions WHERE last_accessed < ?",
            (expiry_cutoff,),
        ).fetchall()
        for (sid,) in expired_rows:
            cls._sqlite_evict_session_unsafe(conn, sid, "TTL reached")

        rows = conn.execute("SELECT session_id FROM sessions ORDER BY last_accessed ASC").fetchall()
        overflow = len(rows) - SESSION_MAX_ENTRIES
        if overflow > 0:
            for (sid,) in rows[:overflow]:
                cls._sqlite_evict_session_unsafe(conn, sid, "LRU capacity limit")
        conn.commit()

    @classmethod
    def policy(cls) -> dict[str, object]:
        """Return the current session retention policy."""
        backend = _session_backend()
        return {
            "backend": backend,
            "durable": backend == "sqlite",
            "persistence": "sqlite" if backend == "sqlite" else "in_memory_only",
            "ttl_sec": SESSION_TTL_SECONDS,
            "max_entries": SESSION_MAX_ENTRIES,
        }

    @classmethod
    def save(
        cls, df: pd.DataFrame, session_id: Optional[str] = None, run_id: Optional[str] = None
    ) -> str:
        """Save a DataFrame to the store. Generates a new session_id if not provided."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    existing_row = (
                        cls._sqlite_fetch_row_unsafe(conn, session_id) if session_id else None
                    )
                    now_ts = pd.Timestamp.now()
                    now_iso = now_ts.isoformat()
                    if session_id is None:
                        session_id = f"sess_{uuid.uuid4().hex[:8]}"
                    started_at = (
                        existing_row[2]
                        if existing_row is not None
                        else now_ts.strftime("%Y%m%d_%H%M%S")
                    )
                    effective_run_id = (
                        run_id
                        if run_id is not None
                        else (existing_row[1] if existing_row is not None else None)
                    )
                    configs = cls._sqlite_configs_from_row(existing_row)
                    metadata = {
                        "row_count": len(df),
                        "col_count": len(df.columns),
                        "updated_at": now_iso,
                    }
                    conn.execute(
                        """
                        INSERT INTO sessions (
                            session_id, run_id, started_at, updated_at, last_accessed,
                            dataframe_blob, metadata_json, configs_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(session_id) DO UPDATE SET
                            run_id = excluded.run_id,
                            started_at = excluded.started_at,
                            updated_at = excluded.updated_at,
                            last_accessed = excluded.last_accessed,
                            dataframe_blob = excluded.dataframe_blob,
                            metadata_json = excluded.metadata_json,
                            configs_json = excluded.configs_json
                        """,
                        (
                            session_id,
                            effective_run_id,
                            started_at,
                            now_iso,
                            time.time(),
                            sqlite3.Binary(pickle.dumps(df, protocol=pickle.HIGHEST_PROTOCOL)),
                            json.dumps(metadata),
                            json.dumps(configs),
                        ),
                    )
                    conn.commit()
                    cls._sqlite_cleanup_unsafe(conn)
                finally:
                    conn.close()
                logger.info(
                    "Saved sqlite session %s (run_id: %s, shape: %s)", session_id, run_id, df.shape
                )
                return session_id

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
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, session_id)
                    if row is None:
                        return None
                    now_ts = time.time()
                    conn.execute(
                        "UPDATE sessions SET last_accessed = ? WHERE session_id = ?",
                        (now_ts, session_id),
                    )
                    conn.commit()
                    return cls._sqlite_df_from_row(row)
                finally:
                    conn.close()
            if session_id in cls._sessions:
                cls._last_accessed[session_id] = time.time()
                return cls._sessions[session_id]
        return None

    @classmethod
    def get_run_id(cls, session_id: str) -> Optional[str]:
        """Retrieve the run_id associated with a session."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, session_id)
                    return row[1] if row is not None else None
                finally:
                    conn.close()
            return cls._session_run_ids.get(session_id)

    @classmethod
    def get_session_start(cls, session_id: str) -> Optional[str]:
        """Retrieve the start time associated with a session."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, session_id)
                    return row[2] if row is not None else None
                finally:
                    conn.close()
            return cls._session_start_times.get(session_id)

    @classmethod
    def get_metadata(cls, session_id: str) -> Optional[dict]:
        """Retrieve metadata for a session."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, session_id)
                    return cls._sqlite_metadata_from_row(row) if row is not None else None
                finally:
                    conn.close()
            return cls._metadata.get(session_id)

    @classmethod
    def get_last_accessed(cls, session_id: str) -> Optional[float]:
        """Retrieve the last-access timestamp for a session."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, session_id)
                    return float(row[4]) if row is not None else None
                finally:
                    conn.close()
            return cls._last_accessed.get(session_id)

    @classmethod
    def get_expiry_info(cls, session_id: str) -> dict[str, object]:
        """Return derived expiry information for a session."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, session_id)
                    last_accessed = float(row[4]) if row is not None else None
                finally:
                    conn.close()
            else:
                last_accessed = cls._last_accessed.get(session_id)
        if last_accessed is None:
            return {
                "last_accessed_at": None,
                "expires_at": None,
                "expires_in_sec": None,
            }

        expires_at_ts = last_accessed + SESSION_TTL_SECONDS
        now = time.time()
        return {
            "last_accessed_at": datetime.fromtimestamp(last_accessed, tz=timezone.utc).isoformat(),
            "expires_at": datetime.fromtimestamp(expires_at_ts, tz=timezone.utc).isoformat(),
            "expires_in_sec": max(0, int(expires_at_ts - now)),
        }

    @classmethod
    def save_config(cls, session_id: str, module: str, config_yaml: str) -> None:
        """Store an inferred config YAML string for a module in session scope."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, session_id)
                    if row is None:
                        return
                    configs = cls._sqlite_configs_from_row(row)
                    configs[module] = config_yaml
                    conn.execute(
                        "UPDATE sessions SET configs_json = ?, updated_at = ? WHERE session_id = ?",
                        (
                            json.dumps(configs),
                            pd.Timestamp.now().isoformat(),
                            session_id,
                        ),
                    )
                    conn.commit()
                    return
                finally:
                    conn.close()
            if session_id not in cls._session_configs:
                cls._session_configs[session_id] = {}
            cls._session_configs[session_id][module] = config_yaml

    @classmethod
    def get_config(cls, session_id: str, module: str) -> Optional[str]:
        """Retrieve a previously stored inferred config for a module."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, session_id)
                    return (
                        cls._sqlite_configs_from_row(row).get(module) if row is not None else None
                    )
                finally:
                    conn.close()
            return cls._session_configs.get(session_id, {}).get(module)

    @classmethod
    def get_configs(cls, session_id: str) -> Dict[str, str]:
        """Retrieve all stored inferred configs for a session."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, session_id)
                    return cls._sqlite_configs_from_row(row) if row is not None else {}
                finally:
                    conn.close()
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
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    row = cls._sqlite_fetch_row_unsafe(conn, source_session_id)
                    if row is None:
                        return None
                    df = cls._sqlite_df_from_row(row)
                    new_session_id = f"sess_{uuid.uuid4().hex[:8]}"
                    now_ts = pd.Timestamp.now()
                    now_iso = now_ts.isoformat()
                    configs = cls._sqlite_configs_from_row(row) if copy_configs else {}
                    metadata = {
                        "row_count": len(df),
                        "col_count": len(df.columns),
                        "updated_at": now_iso,
                    }
                    conn.execute(
                        """
                        INSERT INTO sessions (
                            session_id, run_id, started_at, updated_at, last_accessed,
                            dataframe_blob, metadata_json, configs_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            new_session_id,
                            run_id,
                            now_ts.strftime("%Y%m%d_%H%M%S"),
                            now_iso,
                            time.time(),
                            sqlite3.Binary(
                                pickle.dumps(df.copy(), protocol=pickle.HIGHEST_PROTOCOL)
                            ),
                            json.dumps(metadata),
                            json.dumps(configs),
                        ),
                    )
                    conn.commit()
                    cls._sqlite_cleanup_unsafe(conn)
                    logger.info(
                        "Forked sqlite session %s → %s (run_id: %s, copy_configs: %s)",
                        source_session_id,
                        new_session_id,
                        run_id,
                        copy_configs,
                    )
                    return new_session_id
                finally:
                    conn.close()
            cls._cleanup_unsafe()
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
                cls._session_configs[new_session_id] = dict(cls._session_configs[source_session_id])

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
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    cursor = conn.execute(
                        "UPDATE sessions SET run_id = ?, updated_at = ? WHERE session_id = ?",
                        (run_id, pd.Timestamp.now().isoformat(), session_id),
                    )
                    conn.commit()
                    ok = cursor.rowcount > 0
                finally:
                    conn.close()
                if ok:
                    logger.info("Rebound sqlite session %s to run_id %s", session_id, run_id)
                return ok
            cls._cleanup_unsafe()
            if session_id not in cls._sessions:
                return False
            cls._session_run_ids[session_id] = run_id
        logger.info("Rebound session %s to run_id %s", session_id, run_id)
        return True

    @classmethod
    def list_sessions(cls) -> Dict[str, dict]:
        """List available sessions and their metadata."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                    rows = conn.execute(
                        "SELECT session_id, metadata_json FROM sessions ORDER BY last_accessed DESC"
                    ).fetchall()
                    return {
                        sid: (json.loads(metadata_json) if metadata_json else {})
                        for sid, metadata_json in rows
                    }
                finally:
                    conn.close()
            cls._cleanup_unsafe()
            return {k: cls._metadata.get(k, {}) for k in cls._sessions.keys()}

    @classmethod
    def _evict_session_unsafe(cls, sid: str, reason: str) -> None:
        """Remove a single session from all stores. Must be called with _lock held."""
        cls._sessions.pop(sid, None)
        cls._metadata.pop(sid, None)
        cls._last_accessed.pop(sid, None)
        cls._session_run_ids.pop(sid, None)
        cls._session_start_times.pop(sid, None)
        cls._session_configs.pop(sid, None)
        logger.info("Evicted session %s (%s)", sid, reason)

    @classmethod
    def _cleanup_unsafe(cls):
        """Evict expired and over-limit sessions. Must be called with _lock held."""
        now = time.time()
        expired = [
            sid
            for sid, last_ts in cls._last_accessed.items()
            if now - last_ts > SESSION_TTL_SECONDS
        ]
        for sid in expired:
            cls._evict_session_unsafe(sid, "TTL reached")

        # LRU eviction when over capacity
        while len(cls._sessions) > SESSION_MAX_ENTRIES:
            oldest_sid = min(cls._last_accessed, key=cls._last_accessed.get)
            cls._evict_session_unsafe(oldest_sid, "LRU capacity limit")

    @classmethod
    def cleanup(cls):
        """Remove sessions that have exceeded the TTL."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    cls._sqlite_cleanup_unsafe(conn)
                finally:
                    conn.close()
                return
            cls._cleanup_unsafe()

    @classmethod
    def clear(cls, session_id: Optional[str] = None):
        """Clear one or all sessions."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    if session_id:
                        conn.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
                    else:
                        conn.execute("DELETE FROM sessions")
                    conn.commit()
                finally:
                    conn.close()
                # Also clear in-memory state so backend switches in tests stay deterministic.
                cls._sessions.clear()
                cls._metadata.clear()
                cls._last_accessed.clear()
                cls._session_run_ids.clear()
                cls._session_start_times.clear()
                cls._session_configs.clear()
                return
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

    @classmethod
    def backdate_session_for_test(cls, session_id: str, timestamp: float) -> None:
        """Test helper: set last_accessed to a known timestamp for TTL assertions."""
        with cls._lock:
            if cls._using_sqlite():
                conn = cls._sqlite_connect_unsafe()
                try:
                    conn.execute(
                        "UPDATE sessions SET last_accessed = ? WHERE session_id = ?",
                        (timestamp, session_id),
                    )
                    conn.commit()
                finally:
                    conn.close()
                return
            if session_id in cls._last_accessed:
                cls._last_accessed[session_id] = timestamp
