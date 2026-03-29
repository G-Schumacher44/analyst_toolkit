import os
import tempfile
from pathlib import Path

from analyst_toolkit.m00_utils.plot_runtime import configure_plot_runtime_env


def test_configure_plot_runtime_env_uses_writable_tmp_cache(monkeypatch, tmp_path):
    fake_home = tmp_path / "fake_home"
    fake_home.mkdir()
    fake_home.chmod(0o500)
    try:
        monkeypatch.setenv("HOME", str(fake_home))
        monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
        monkeypatch.delenv("MPLCONFIGDIR", raising=False)

        configure_plot_runtime_env()

        assert os.environ["XDG_CACHE_HOME"].startswith(tempfile.gettempdir())
        assert os.environ["MPLCONFIGDIR"].startswith(os.environ["XDG_CACHE_HOME"])
        assert Path(os.environ["MPLCONFIGDIR"]).exists()
    finally:
        fake_home.chmod(0o700)


def test_configure_plot_runtime_env_respects_existing_settings(monkeypatch, tmp_path):
    xdg_dir = tmp_path / "cache"
    mpl_dir = tmp_path / "mpl"
    xdg_dir.mkdir()
    mpl_dir.mkdir()
    monkeypatch.setenv("XDG_CACHE_HOME", str(xdg_dir))
    monkeypatch.setenv("MPLCONFIGDIR", str(mpl_dir))

    configure_plot_runtime_env()

    assert os.environ["XDG_CACHE_HOME"] == str(xdg_dir)
    assert os.environ["MPLCONFIGDIR"] == str(mpl_dir)


def test_configure_plot_runtime_env_uses_existing_xdg_cache_when_mpl_is_unset(
    monkeypatch, tmp_path
):
    fake_home = tmp_path / "fake_home"
    fake_home.mkdir()
    xdg_dir = tmp_path / "cache"
    xdg_dir.mkdir()
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("XDG_CACHE_HOME", str(xdg_dir))
    monkeypatch.delenv("MPLCONFIGDIR", raising=False)

    configure_plot_runtime_env()

    expected_mpl_dir = fake_home / ".matplotlib"

    assert os.environ["XDG_CACHE_HOME"] == str(xdg_dir)
    assert os.environ["MPLCONFIGDIR"] == str(expected_mpl_dir)
    assert Path(os.environ["MPLCONFIGDIR"]).exists()


def test_configure_plot_runtime_env_falls_back_when_user_paths_are_unwritable(
    monkeypatch, tmp_path
):
    fake_home = tmp_path / "fake_home"
    fake_home.mkdir()
    fake_home.chmod(0o500)
    bad_cache = fake_home / "missing_cache"
    bad_mpl = fake_home / "missing_mpl"

    try:
        monkeypatch.setenv("HOME", str(fake_home))
        monkeypatch.setenv("XDG_CACHE_HOME", str(bad_cache))
        monkeypatch.setenv("MPLCONFIGDIR", str(bad_mpl))

        configure_plot_runtime_env()

        assert os.environ["XDG_CACHE_HOME"].startswith(tempfile.gettempdir())
        assert os.environ["MPLCONFIGDIR"].startswith(os.environ["XDG_CACHE_HOME"])
        assert Path(os.environ["MPLCONFIGDIR"]).exists()
    finally:
        fake_home.chmod(0o700)
