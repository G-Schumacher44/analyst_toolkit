"""
io.py — Data loading and artifact upload for the analyst_toolkit MCP server.
"""

import json
import logging
import os
import threading
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import yaml

from analyst_toolkit.mcp_server.destination_routing import (
    compact_destination_metadata as _compact_destination_metadata,
)
from analyst_toolkit.mcp_server.destination_routing import (
    deliver_artifact as _deliver_artifact,
)
from analyst_toolkit.mcp_server.destination_routing import (
    split_artifact_reference as _split_artifact_reference,
)
from analyst_toolkit.mcp_server.input.ingest import load_dataframe as _load_input_dataframe
from analyst_toolkit.mcp_server.io_history_files import (
    read_history_file_safe as _read_history_file_safe,
)
from analyst_toolkit.mcp_server.io_history_files import (
    write_json_atomic as _write_json_atomic,
)
from analyst_toolkit.mcp_server.io_path_normalization import (
    looks_like_bucket_path as _looks_like_bucket_path,
)
from analyst_toolkit.mcp_server.io_path_normalization import (
    normalize_input_path as _normalize_input_path,
)
from analyst_toolkit.mcp_server.io_serialization import (
    build_artifact_contract,
    fold_status_with_artifacts,
    make_json_safe,
)
from analyst_toolkit.mcp_server.io_storage import save_output, should_export_html
from analyst_toolkit.mcp_server.io_storage import upload_artifact as _upload_artifact
from analyst_toolkit.mcp_server.state import StateStore

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


RUN_ID_OVERRIDE_ALLOWED = _env_bool("ANALYST_MCP_ALLOW_RUN_ID_OVERRIDE", False)
DEDUP_RUN_ID_WARNINGS = _env_bool("ANALYST_MCP_DEDUP_RUN_ID_WARNINGS", True)
_HISTORY_LOCKS_GUARD = threading.Lock()
_MAX_HISTORY_LOCKS = 256
_HISTORY_LOCKS: OrderedDict[str, dict[str, Any]] = OrderedDict()
_LIFECYCLE_WARNINGS_GUARD = threading.Lock()
_MAX_LIFECYCLE_WARNING_KEYS = 512
_SEEN_LIFECYCLE_WARNING_KEYS: set[tuple[str, str]] = set()
ALLOW_EMPTY_CERT_RULES = _env_bool("ANALYST_MCP_ALLOW_EMPTY_CERT_RULES", False)
_HISTORY_READ_META_GUARD = threading.Lock()
_MAX_HISTORY_READ_META = 256
_LAST_HISTORY_READ_META: OrderedDict[tuple[str, Optional[str]], dict[str, Any]] = OrderedDict()


def coerce_config(config: Optional[dict], module: str) -> dict:
    """
    Ensure the config passed to a tool is a properly structured dict.

    Handles three agent failure modes:
    1. Agent passes a YAML string instead of a parsed dict — auto-parses it.
    2. Agent passes the full inferred config with the module key containing a YAML
       string — auto-parses: {"normalization": "<yaml>"} → {"normalization": {...}}
    3. Agent double-wraps the module key — auto-unwraps one level:
       {"normalization": {"normalization": {...}}} → {"normalization": {...}}

    Logs a warning whenever a correction is made so it's visible in server logs.
    """
    if config is None:
        return {}

    # If the entire config is a YAML string, parse it first
    if isinstance(config, str):
        logger.warning(
            f"[{module}] config was a raw YAML string — auto-parsing. "
            "Pass a parsed dict to avoid this."
        )
        try:
            config = yaml.safe_load(config)
        except yaml.YAMLError as e:
            logger.error(f"[{module}] Failed to parse YAML string config: {e}")
            return {}

    if not isinstance(config, dict):
        return {}

    # If the module key's value is a YAML string, parse it
    if module in config and isinstance(config[module], str):
        logger.warning(
            f"[{module}] config['{module}'] was a YAML string — auto-parsing. "
            "Pass a parsed dict to avoid this."
        )
        try:
            config = {module: yaml.safe_load(config[module])}
        except yaml.YAMLError as e:
            logger.error(f"[{module}] Failed to parse YAML string in config: {e}")
            return {}

    # If double-wrapped ({"normalization": {"normalization": {...}}}), unwrap one level
    if module in config and isinstance(config[module], dict) and module in config[module]:
        logger.warning(
            f"[{module}] config was double-wrapped — auto-unwrapping. "
            "Pass a single-level dict to avoid this."
        )
        config = config[module]

    return config


def default_run_id() -> str:
    """Return a UTC timestamp-based run ID."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def resolve_run_context(
    run_id: Optional[str], session_id: Optional[str]
) -> tuple[str, dict[str, Any]]:
    """
    Resolve run/session identity with guardrails.

    Default behavior protects session consistency:
    - If session has a bound run_id and caller provides a different run_id,
      the run_id is coerced back to the session run_id unless override is enabled.
    - Enable override with ANALYST_MCP_ALLOW_RUN_ID_OVERRIDE=1.
    """
    requested_run_id = run_id
    session_run_id = get_session_run_id(session_id) if session_id else None
    effective_run_id = run_id
    source = "requested" if run_id else "generated"
    warnings: list[str] = []
    coerced = False

    if session_run_id:
        if not run_id:
            effective_run_id = session_run_id
            source = "session"
        elif run_id != session_run_id:
            if RUN_ID_OVERRIDE_ALLOWED:
                warning_text = (
                    f"run_id '{run_id}' does not match session run_id '{session_run_id}'. "
                    "Proceeding because ANALYST_MCP_ALLOW_RUN_ID_OVERRIDE=1."
                )
                if _should_emit_lifecycle_warning(session_id or "", run_id):
                    warnings.append(warning_text)
            else:
                effective_run_id = session_run_id
                source = "session"
                coerced = True
                warning_text = (
                    f"run_id '{run_id}' does not match session run_id '{session_run_id}'. "
                    "Coerced to session run_id to keep run lifecycle consistent."
                )
                if _should_emit_lifecycle_warning(session_id or "", run_id):
                    warnings.append(warning_text)

    if not effective_run_id:
        effective_run_id = default_run_id()
        source = "generated"

    lifecycle = {
        "requested_run_id": requested_run_id,
        "session_run_id": session_run_id,
        "effective_run_id": effective_run_id,
        "source": source,
        "coerced": coerced,
        "override_allowed": RUN_ID_OVERRIDE_ALLOWED,
        "warnings": warnings,
    }
    return effective_run_id, lifecycle


def load_input(
    path: Optional[str] = None,
    session_id: Optional[str] = None,
    input_id: Optional[str] = None,
) -> pd.DataFrame:
    """Load data from a canonical input reference, GCS, local file, or in-memory session."""
    normalized_path = path
    if normalized_path:
        normalized_path, path_warning = _normalize_input_path(normalized_path)
        if path_warning:
            logger.warning(path_warning)
        if _looks_like_bucket_path(normalized_path):
            raise ValueError(
                f"[INVALID_PATH_FORMAT] Path '{normalized_path}' looks like a bucket path "
                f"but is missing the scheme. Did you mean 'gs://{normalized_path}'?"
            )

    session_data_available = (
        session_id is not None
        and not normalized_path
        and not input_id
        and StateStore.get(session_id) is not None
    )
    df = _load_input_dataframe(path=normalized_path, session_id=session_id, input_id=input_id)
    if session_data_available:
        logger.info(f"Loaded from session: {session_id}")
    return df


def save_to_session(
    df: pd.DataFrame, session_id: Optional[str] = None, run_id: Optional[str] = None
) -> str:
    """Save to in-memory store."""
    return StateStore.save(df, session_id, run_id=run_id)


def save_session_config(session_id: str, module: str, config_yaml: str) -> None:
    """Persist an inferred config YAML string for a module in session scope."""
    StateStore.save_config(session_id, module, config_yaml)


def get_session_config(session_id: str, module: str) -> Optional[str]:
    """Retrieve a previously stored inferred config for a module."""
    return StateStore.get_config(session_id, module)


def get_inferred_config(session_id: str | None, module: str) -> dict:
    """Retrieve and parse the inferred config for *module* from the session store.

    Returns an empty dict when no session or no stored config exists.
    The returned dict is unwrapped from the module-level key if present
    (e.g. ``{"normalization": {...}}`` → ``{...}``).
    """
    if not session_id:
        return {}
    raw_yaml = get_session_config(session_id, module)
    if not raw_yaml:
        return {}
    try:
        parsed = yaml.safe_load(raw_yaml)
    except yaml.YAMLError:
        logger.warning("Failed to parse stored %s config for session %s", module, session_id)
        return {}
    if not isinstance(parsed, dict):
        return {}
    # Unwrap module-level key so the structure matches what resolve_layered_config expects
    if module in parsed and isinstance(parsed[module], dict):
        parsed = parsed[module]
    return parsed


def missing_config_warning(runtime_meta: dict[str, Any]) -> str | None:
    """Return a user-facing warning when no executable config layers were supplied."""
    resolved_layers = runtime_meta.get("resolved_layers", {})
    if any(resolved_layers.get(layer) for layer in ("inferred", "provided", "explicit")):
        return None
    return "No inferred or explicit config found. Run infer_configs first for meaningful results."


def get_session_run_id(session_id: str) -> Optional[str]:
    return StateStore.get_run_id(session_id)


def get_session_start(session_id: str) -> Optional[str]:
    return StateStore.get_session_start(session_id)


def get_session_metadata(session_id: str) -> Optional[dict]:
    """Retrieve metadata for a session."""
    return StateStore.get_metadata(session_id)


def _resolve_path_root(run_id: str, session_id: Optional[str] = None) -> str:
    """
    Resolve storage root using session + run identity.

    Session-aware layout:
      <session_timestamp>/<session_id>/<run_id>

    Non-session layout:
      <current_timestamp>/<run_id>
    """
    if session_id:
        session_ts = get_session_start(session_id) or datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )
        return f"{session_ts}/{session_id}/{run_id}"

    current_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{current_ts}/{run_id}"


def generate_default_export_path(
    run_id: str, module: str, extension: str = "csv", session_id: Optional[str] = None
) -> str:
    """Generate default path: prefix/path_root/module_output.csv"""
    bucket_uri = os.environ.get("ANALYST_REPORT_BUCKET", "").strip().rstrip("/")
    prefix = os.environ.get("ANALYST_REPORT_PREFIX", "analyst_toolkit/reports").strip().strip("/")

    path_root = _resolve_path_root(run_id, session_id)

    if bucket_uri:
        return f"{bucket_uri}/{prefix}/{path_root}/{module}_output.{extension}"

    base_dir = Path("exports/data") / path_root
    base_dir.mkdir(parents=True, exist_ok=True)
    return str((base_dir / f"{module}_output.{extension}").absolute())


def upload_artifact(
    local_path: str,
    run_id: str,
    module: str,
    config: Optional[dict] = None,
    session_id: Optional[str] = None,
) -> str:
    return _upload_artifact(
        local_path=local_path,
        run_id=run_id,
        module=module,
        config=config or {},
        session_id=session_id,
        resolve_path_root=_resolve_path_root,
        logger=logger,
    )


def deliver_artifact(
    local_path: str,
    run_id: str,
    module: str,
    config: Optional[dict] = None,
    session_id: Optional[str] = None,
) -> dict[str, Any]:
    return _deliver_artifact(
        local_path=local_path,
        run_id=run_id,
        module=module,
        config=config or {},
        session_id=session_id,
        resolve_path_root=_resolve_path_root,
        logger=logger,
    )


def split_artifact_reference(reference: str) -> tuple[str, str]:
    return _split_artifact_reference(reference)


def compact_destination_metadata(destinations: dict[str, Any]) -> dict[str, Any]:
    return _compact_destination_metadata(destinations)


def empty_delivery_state() -> dict[str, Any]:
    return {
        "local_path": "",
        "url": "",
        "warnings": [],
        "destinations": {},
    }


def check_upload(url: str, label: str, warnings: list) -> str:
    """Append a warning if the upload failed (url is empty). Returns url unchanged."""
    if not url:
        warnings.append(f"Upload failed or file not found: {label}")
    return url


def append_to_run_history(run_id: str, entry: dict, session_id: Optional[str] = None):
    """Save history to: exports/reports/history/path_root/run_history.json"""
    path_root = _resolve_path_root(run_id, session_id)
    history_dir = Path("exports/reports/history") / path_root
    history_dir.mkdir(parents=True, exist_ok=True)

    history_file = history_dir / f"{run_id}_history.json"
    with _history_lock(history_file):
        history, parse_meta = _read_history_file_safe(history_file)
        if parse_meta["parse_errors"]:
            logger.warning(
                "Recovered run history with parse errors for %s: %s",
                history_file,
                parse_meta["parse_errors"],
            )

        safe_entry = make_json_safe(entry)
        if not isinstance(safe_entry, dict):
            safe_entry = {"entry": safe_entry}
        safe_entry["timestamp"] = datetime.now(timezone.utc).isoformat()
        history.append(safe_entry)
        _write_json_atomic(history_file, history)

    upload_artifact(str(history_file), run_id, "history", session_id=session_id)


def get_run_history(run_id: str, session_id: Optional[str] = None) -> list:
    history, meta = _get_run_history_with_meta(run_id, session_id=session_id)
    _set_last_history_meta(run_id, session_id, meta)
    return history


def get_last_history_read_meta(run_id: str, session_id: Optional[str] = None) -> dict[str, Any]:
    key = (run_id, session_id)
    with _HISTORY_READ_META_GUARD:
        return dict(_LAST_HISTORY_READ_META.get(key, {"parse_errors": [], "skipped_records": 0}))


def _get_run_history_with_meta(
    run_id: str, session_id: Optional[str] = None
) -> tuple[list, dict[str, Any]]:
    meta = {"parse_errors": [], "skipped_records": 0}
    history_root = Path("exports/reports/history")
    if not history_root.exists():
        return [], meta

    if session_id:
        path_root = _resolve_path_root(run_id, session_id)
        history_file = history_root / path_root / f"{run_id}_history.json"
        if history_file.exists():
            with _history_lock(history_file):
                return _read_history_file_safe(history_file)
        return [], meta

    candidates = sorted(
        history_root.glob(f"**/{run_id}_history.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if candidates:
        with _history_lock(candidates[0]):
            return _read_history_file_safe(candidates[0])
    return [], meta


def _history_lock_for(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _HISTORY_LOCKS_GUARD:
        entry = _HISTORY_LOCKS.get(key)
        if entry is not None:
            _HISTORY_LOCKS.move_to_end(key)
            return entry["lock"]
        if len(_HISTORY_LOCKS) >= _MAX_HISTORY_LOCKS:
            for stale_key, stale_entry in list(_HISTORY_LOCKS.items()):
                if stale_entry["use_count"] == 0:
                    _HISTORY_LOCKS.pop(stale_key)
                    break
        lock = threading.Lock()
        _HISTORY_LOCKS[key] = {"lock": lock, "use_count": 0}
        return lock


def _release_history_lock(path: Path) -> None:
    key = str(path.resolve())
    with _HISTORY_LOCKS_GUARD:
        entry = _HISTORY_LOCKS.get(key)
        if entry is None:
            return
        entry["use_count"] = max(0, int(entry.get("use_count", 0)) - 1)
        _HISTORY_LOCKS.move_to_end(key)


@contextmanager
def _history_lock(path: Path):
    key = str(path.resolve())
    lock = _history_lock_for(path)
    with _HISTORY_LOCKS_GUARD:
        entry = _HISTORY_LOCKS.get(key)
        if entry is not None:
            entry["use_count"] += 1
    try:
        with lock:
            yield
    finally:
        _release_history_lock(path)


def _should_emit_lifecycle_warning(session_id: str, requested_run_id: str) -> bool:
    if not DEDUP_RUN_ID_WARNINGS:
        return True
    key = (session_id, requested_run_id)
    with _LIFECYCLE_WARNINGS_GUARD:
        if key in _SEEN_LIFECYCLE_WARNING_KEYS:
            return False
        if len(_SEEN_LIFECYCLE_WARNING_KEYS) >= _MAX_LIFECYCLE_WARNING_KEYS:
            _SEEN_LIFECYCLE_WARNING_KEYS.clear()
        _SEEN_LIFECYCLE_WARNING_KEYS.add(key)
        return True


def _set_last_history_meta(run_id: str, session_id: Optional[str], meta: dict[str, Any]) -> None:
    key = (run_id, session_id)
    with _HISTORY_READ_META_GUARD:
        if key in _LAST_HISTORY_READ_META:
            _LAST_HISTORY_READ_META.move_to_end(key)
        _LAST_HISTORY_READ_META[key] = {
            "parse_errors": list(meta.get("parse_errors", [])),
            "skipped_records": int(meta.get("skipped_records", 0)),
        }
        while len(_LAST_HISTORY_READ_META) > _MAX_HISTORY_READ_META:
            _LAST_HISTORY_READ_META.popitem(last=False)
