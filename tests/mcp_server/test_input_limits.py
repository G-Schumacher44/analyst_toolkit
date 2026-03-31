import pytest

from analyst_toolkit.mcp_server.input.errors import InputPayloadTooLargeError
from analyst_toolkit.mcp_server.input.limits import enforce_tabular_limits


def test_enforce_tabular_limits_uses_custom_memory_env_name(monkeypatch):
    monkeypatch.setenv("ANALYST_MCP_MAX_INPUT_MEMORY_BYTES", "1000")
    monkeypatch.setenv("ANALYST_CUSTOM_MEMORY_LIMIT", "10")

    with pytest.raises(InputPayloadTooLargeError, match="ANALYST_CUSTOM_MEMORY_LIMIT"):
        enforce_tabular_limits(
            row_count=1,
            memory_usage_bytes=11,
            reference="dataset.csv",
            memory_env_name="ANALYST_CUSTOM_MEMORY_LIMIT",
        )
