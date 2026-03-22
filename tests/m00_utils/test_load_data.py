import joblib
import pytest

from analyst_toolkit.m00_utils.load_data import load_joblib


def test_load_joblib_requires_explicit_opt_in(monkeypatch, tmp_path):
    payload_path = tmp_path / "payload.joblib"
    joblib.dump({"status": "ok"}, payload_path)
    monkeypatch.delenv("ANALYST_TOOLKIT_ALLOW_UNSAFE_JOBLIB", raising=False)

    with pytest.raises(ValueError, match="ANALYST_TOOLKIT_ALLOW_UNSAFE_JOBLIB=1"):
        load_joblib(str(payload_path))


def test_load_joblib_allows_trusted_opt_in(monkeypatch, tmp_path):
    payload_path = tmp_path / "payload.joblib"
    joblib.dump({"status": "ok"}, payload_path)
    monkeypatch.setenv("ANALYST_TOOLKIT_ALLOW_UNSAFE_JOBLIB", "1")

    assert load_joblib(str(payload_path)) == {"status": "ok"}
