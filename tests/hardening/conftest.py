import pandas as pd
import pytest

from analyst_toolkit.mcp_server.state import StateStore


@pytest.fixture(autouse=True)
def clear_state():
    """Wipe StateStore before and after every test."""
    StateStore.clear()
    yield
    StateStore.clear()


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
