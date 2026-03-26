"""Input boundary limits for MCP dataset loading."""

from __future__ import annotations

import os

import pandas as pd

from analyst_toolkit.mcp_server.input.errors import InputPayloadTooLargeError

_DEFAULT_MAX_INPUT_BYTES = 100 * 1024 * 1024
_DEFAULT_MAX_INPUT_ROWS = 1_000_000
_DEFAULT_MAX_INPUT_MEMORY_BYTES = 256 * 1024 * 1024
_DEFAULT_MAX_GCS_PREFIX_OBJECTS = 32


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


def max_input_bytes() -> int:
    return _env_int("ANALYST_MCP_MAX_INPUT_BYTES", _DEFAULT_MAX_INPUT_BYTES)


def max_input_rows() -> int:
    return _env_int("ANALYST_MCP_MAX_INPUT_ROWS", _DEFAULT_MAX_INPUT_ROWS)


def max_input_memory_bytes() -> int:
    return _env_int("ANALYST_MCP_MAX_INPUT_MEMORY_BYTES", _DEFAULT_MAX_INPUT_MEMORY_BYTES)


def max_gcs_prefix_objects() -> int:
    return _env_int("ANALYST_MCP_MAX_GCS_PREFIX_OBJECTS", _DEFAULT_MAX_GCS_PREFIX_OBJECTS)


def enforce_input_bytes_limit(size_bytes: int | None, *, reference: str) -> None:
    if size_bytes is None:
        return
    limit = max_input_bytes()
    if limit and size_bytes > limit:
        raise InputPayloadTooLargeError(
            f"Input '{reference}' exceeds ANALYST_MCP_MAX_INPUT_BYTES "
            f"({size_bytes} bytes > {limit} bytes)."
        )


def enforce_gcs_prefix_object_limit(*, object_count: int, reference: str) -> None:
    limit = max_gcs_prefix_objects()
    if limit and object_count > limit:
        raise InputPayloadTooLargeError(
            f"GCS prefix '{reference}' exceeds ANALYST_MCP_MAX_GCS_PREFIX_OBJECTS "
            f"({object_count} objects > {limit})."
        )


def enforce_dataframe_limits(df: pd.DataFrame, *, reference: str) -> None:
    row_limit = max_input_rows()
    if row_limit and len(df) > row_limit:
        raise InputPayloadTooLargeError(
            f"Input '{reference}' exceeds ANALYST_MCP_MAX_INPUT_ROWS "
            f"({len(df)} rows > {row_limit})."
        )

    memory_limit = max_input_memory_bytes()
    if memory_limit:
        memory_usage = int(df.memory_usage(index=True, deep=True).sum())
        if memory_usage > memory_limit:
            raise InputPayloadTooLargeError(
                f"Input '{reference}' exceeds ANALYST_MCP_MAX_INPUT_MEMORY_BYTES "
                f"({memory_usage} bytes > {memory_limit} bytes)."
            )
