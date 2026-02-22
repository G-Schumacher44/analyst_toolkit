"""
state.py — In-memory state management for MCP tool pipelines.
"""

import logging
import uuid
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class StateStore:
    """
    A simple in-memory store for DataFrames, enabling 'Pipeline Mode'.
    """

    _sessions: Dict[str, pd.DataFrame] = {}

    @classmethod
    def save(cls, df: pd.DataFrame, session_id: Optional[str] = None) -> str:
        """
        Save a DataFrame to the store.
        If no session_id is provided, a new one is generated.
        """
        if session_id is None:
            session_id = f"sess_{uuid.uuid4().hex[:8]}"
        cls._sessions[session_id] = df
        logger.info(f"Saved session {session_id} (shape: {df.shape})")
        return session_id

    @classmethod
    def get(cls, session_id: str) -> Optional[pd.DataFrame]:
        """Retrieve a DataFrame from the store by session_id."""
        return cls._sessions.get(session_id)

    @classmethod
    def list_sessions(cls) -> Dict[str, tuple]:
        """List available sessions and their shapes."""
        return {k: v.shape for k, v in cls._sessions.items()}

    @classmethod
    def clear(cls, session_id: Optional[str] = None):
        """Clear one or all sessions."""
        if session_id:
            cls._sessions.pop(session_id, None)
        else:
            cls._sessions.clear()
