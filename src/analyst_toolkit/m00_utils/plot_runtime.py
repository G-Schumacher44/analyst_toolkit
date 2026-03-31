"""Runtime helpers for local plotting cache configuration."""

from __future__ import annotations

import getpass
import os
import tempfile
from pathlib import Path


def _is_writable_path(path: Path) -> bool:
    candidate = path.expanduser()
    if candidate.exists():
        return candidate.is_dir() and os.access(candidate, os.W_OK | os.X_OK)

    parent = candidate.parent
    while not parent.exists() and parent != parent.parent:
        parent = parent.parent
    if not parent.exists():
        return False
    return os.access(parent, os.W_OK | os.X_OK)


def configure_plot_runtime_env() -> None:
    """Set writable cache directories for matplotlib/fontconfig when needed.

    This function may update ``os.environ["XDG_CACHE_HOME"]`` and
    ``os.environ["MPLCONFIGDIR"]`` when the current values or their default
    locations are unset or not writable. It only adjusts cache/config paths;
    it does not alter plot styles, random seeds, timestamps, or other rendering
    settings that would change deterministic plot output.

    The temp-directory fallback is only used when the standard cache locations
    (for example ``~/.cache`` or ``~/.matplotlib``) are not writable. Because
    that fallback is machine-local, callers that need deterministic cache paths
    across environments should set explicit writable values for
    ``XDG_CACHE_HOME`` and ``MPLCONFIGDIR`` before importing plotting modules.
    """

    fallback_root = Path(tempfile.gettempdir()) / f"analyst_toolkit_cache_{getpass.getuser()}"
    fallback_root.mkdir(parents=True, exist_ok=True)

    xdg_cache_home = os.environ.get("XDG_CACHE_HOME", "").strip()
    if xdg_cache_home:
        xdg_cache_path = Path(xdg_cache_home).expanduser()
        if not _is_writable_path(xdg_cache_path):
            xdg_cache_path = fallback_root
            os.environ["XDG_CACHE_HOME"] = str(xdg_cache_path)
    else:
        xdg_cache_path = Path.home() / ".cache"
        if not _is_writable_path(xdg_cache_path):
            xdg_cache_path = fallback_root
        os.environ["XDG_CACHE_HOME"] = str(xdg_cache_path)
    xdg_cache_path.mkdir(parents=True, exist_ok=True)

    mpl_config_dir = os.environ.get("MPLCONFIGDIR", "").strip()
    if mpl_config_dir:
        mpl_config_path = Path(mpl_config_dir).expanduser()
        if not _is_writable_path(mpl_config_path):
            default_mpl_path = Path.home() / ".matplotlib"
            mpl_config_path = (
                default_mpl_path
                if _is_writable_path(default_mpl_path)
                else (xdg_cache_path / "matplotlib")
            )
            os.environ["MPLCONFIGDIR"] = str(mpl_config_path)
    else:
        default_mpl_path = Path.home() / ".matplotlib"
        mpl_config_path = (
            default_mpl_path
            if _is_writable_path(default_mpl_path)
            else (xdg_cache_path / "matplotlib")
        )
        os.environ["MPLCONFIGDIR"] = str(mpl_config_path)
    mpl_config_path.mkdir(parents=True, exist_ok=True)
