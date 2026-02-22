"""
state.py — In-memory state management for MCP tool pipelines.
"""

import logging
import uuid
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


import logging
import uuid
from typing import Dict, Optional
import time

import pandas as pd

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 3600  # 1 hour


class StateStore:
    """
    A simple in-memory store for DataFrames, enabling 'Pipeline Mode'.
    Includes a basic TTL mechanism to prevent memory exhaustion.
    """

    _sessions: Dict[str, pd.DataFrame] = {}
    _metadata: Dict[str, dict] = {}
    _last_accessed: Dict[str, float] = {}

    @classmethod
    def save(cls, df: pd.DataFrame, session_id: Optional[str] = None) -> str:
        """
        Save a DataFrame to the store.
        If no session_id is provided, a new one is generated.
        """
        cls.cleanup()  # Run cleanup before saving new data
        
        if session_id is None:
            session_id = f"sess_{uuid.uuid4().hex[:8]}"
        cls._sessions[session_id] = df
        cls._metadata[session_id] = {
            "row_count": len(df),
            "col_count": len(df.columns),
            "updated_at": pd.Timestamp.now().isoformat()
        }
        cls._last_accessed[session_id] = time.time()
        logger.info(f"Saved session {session_id} (shape: {df.shape})")
        return session_id

    @classmethod
    def get(cls, session_id: str) -> Optional[pd.DataFrame]:
        """Retrieve a DataFrame from the store by session_id."""
        if session_id in cls._sessions:
            cls._last_accessed[session_id] = time.time()
            return cls._sessions[session_id]
        return None

    @classmethod
    def get_metadata(cls, session_id: str) -> Optional[dict]:
        """Retrieve metadata for a session."""
        return cls._metadata.get(session_id)

    @classmethod
    def list_sessions(cls) -> Dict[str, dict]:
        """List available sessions and their metadata."""
        return {k: cls._metadata.get(k, {}) for k in cls._sessions.keys()}

    @classmethod
    def cleanup(cls):
        """Remove sessions that have exceeded the TTL."""
        now = time.time()
        expired = [
            sid for sid, last_ts in cls._last_accessed.items()
            if now - last_ts > SESSION_TTL_SECONDS
        ]
        for sid in expired:
            cls.clear(sid)
            logger.info(f"Evicted expired session {sid} (TTL reached)")

    @classmethod
    def clear(cls, session_id: Optional[str] = None):
        """Clear one or all sessions."""
        if session_id:
            cls._sessions.pop(session_id, None)
            cls._metadata.pop(session_id, None)
            cls._last_accessed.pop(session_id, None)
        else:
            cls._sessions.clear()
            cls._metadata.clear()
            cls._last_accessed.clear()
