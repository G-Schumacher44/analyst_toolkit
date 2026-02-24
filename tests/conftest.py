import pytest


@pytest.fixture(autouse=True)
def isolate_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Keep tests deterministic by clearing optional report-upload env vars.
    This prevents accidental network/storage behavior from shell-local settings.
    """
    monkeypatch.delenv("ANALYST_REPORT_BUCKET", raising=False)
    monkeypatch.delenv("ANALYST_REPORT_PREFIX", raising=False)
