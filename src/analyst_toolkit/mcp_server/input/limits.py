"""Input boundary limits for MCP dataset loading."""

from __future__ import annotations

import os
from collections.abc import Iterable

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
    enforce_tabular_limits(
        row_count=len(df),
        memory_usage_bytes=int(df.memory_usage(index=True, deep=True).sum()),
        reference=reference,
    )


def enforce_tabular_limits(
    *,
    row_count: int,
    memory_usage_bytes: int,
    reference: str,
    memory_env_name: str = "ANALYST_MCP_MAX_INPUT_MEMORY_BYTES",
) -> None:
    row_limit = max_input_rows()
    if row_limit and row_count > row_limit:
        raise InputPayloadTooLargeError(
            f"Input '{reference}' exceeds ANALYST_MCP_MAX_INPUT_ROWS "
            f"({row_count} rows > {row_limit})."
        )

    memory_limit = max_input_memory_bytes()
    if memory_limit and memory_usage_bytes > memory_limit:
        raise InputPayloadTooLargeError(
            f"Input '{reference}' exceeds {memory_env_name} "
            f"({memory_usage_bytes} bytes > {memory_limit} bytes)."
        )


def materialize_chunked_frames(
    frames: Iterable[pd.DataFrame], *, reference: str, copy: bool = False
) -> pd.DataFrame:
    collected: list[pd.DataFrame] = []
    cumulative_rows = 0
    cumulative_memory = 0
    for frame in frames:
        cumulative_rows += len(frame)
        cumulative_memory += int(frame.memory_usage(index=True, deep=True).sum())
        enforce_tabular_limits(
            row_count=cumulative_rows,
            memory_usage_bytes=cumulative_memory,
            reference=reference,
        )
        collected.append(frame.copy() if copy else frame)

    if not collected:
        return pd.DataFrame()
    if len(collected) == 1:
        return collected[0]
    return pd.concat(collected, ignore_index=True)
