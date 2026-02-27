import pytest


@pytest.mark.asyncio
async def test_auto_heal_propagates_child_artifacts_and_status(monkeypatch):
    import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_module

    async def fake_infer(*args, **kwargs):
        return {
            "status": "pass",
            "configs": {
                "normalization": "normalization:\n  rules: {}\n",
                "imputation": (
                    "imputation:\n"
                    "  rules:\n"
                    "    strategies:\n"
                    "      some_col:\n"
                    "        strategy: mode\n"
                ),
            },
            "session_id": "sess_unit",
        }

    async def fake_norm(*args, **kwargs):
        return {
            "status": "pass",
            "session_id": "sess_unit",
            "summary": {"changes_made": 2},
            "artifact_path": "norm_report.html",
            "artifact_url": "https://example.com/norm",
            "export_url": "gs://bucket/norm.csv",
            "plot_urls": {"norm.png": "https://example.com/norm.png"},
        }

    async def fake_imp(*args, **kwargs):
        return {
            "status": "warn",
            "session_id": "sess_unit",
            "summary": {"nulls_filled": 4},
            "artifact_path": "imp_report.html",
            "artifact_url": "https://example.com/imp",
            "export_url": "gs://bucket/imp.csv",
            "plot_urls": {"imp.png": "https://example.com/imp.png"},
        }

    monkeypatch.setattr(auto_heal_module, "_toolkit_infer_configs", fake_infer)
    monkeypatch.setattr(auto_heal_module, "_toolkit_normalization", fake_norm)
    monkeypatch.setattr(auto_heal_module, "_toolkit_imputation", fake_imp)
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "warn"
    assert res["artifact_path"] == "imp_report.html"
    assert res["artifact_url"] == "https://example.com/imp"
    assert res["export_url"] == "gs://bucket/imp.csv"
    assert res["plot_urls"] == {"imp.png": "https://example.com/imp.png"}
    assert res["failed_steps"] == []


@pytest.mark.asyncio
async def test_auto_heal_skips_imputation_when_no_strategies(monkeypatch):
    import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_module

    async def fake_infer(*args, **kwargs):
        return {
            "status": "pass",
            "configs": {
                "normalization": "normalization:\n  rules: {}\n",
                "imputation": "imputation:\n  rules:\n    strategies: {}\n",
            },
            "session_id": "sess_unit",
        }

    async def fake_norm(*args, **kwargs):
        return {
            "status": "pass",
            "session_id": "sess_unit",
            "summary": {"changes_made": 0},
            "artifact_path": "norm_report.html",
            "artifact_url": "https://example.com/norm",
            "export_url": "gs://bucket/norm.csv",
            "plot_urls": {"norm.png": "https://example.com/norm.png"},
        }

    async def fake_imp(*args, **kwargs):
        raise AssertionError("imputation should not be called when strategies are empty")

    monkeypatch.setattr(auto_heal_module, "_toolkit_infer_configs", fake_infer)
    monkeypatch.setattr(auto_heal_module, "_toolkit_normalization", fake_norm)
    monkeypatch.setattr(auto_heal_module, "_toolkit_imputation", fake_imp)
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "pass"
    assert res["artifact_path"] == "norm_report.html"
    assert res["artifact_url"] == "https://example.com/norm"
    assert res["export_url"] == "gs://bucket/norm.csv"
    assert res["plot_urls"] == {"norm.png": "https://example.com/norm.png"}
    assert res["failed_steps"] == []
    assert res["summary"]["imputation"]["skipped"] is True


@pytest.mark.asyncio
async def test_auto_heal_returns_error_when_step_raises(monkeypatch):
    import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_module

    async def fake_infer(*args, **kwargs):
        return {
            "status": "pass",
            "configs": {"normalization": "normalization:\n  rules: {}\n"},
            "session_id": "sess_unit",
        }

    async def fake_norm(*args, **kwargs):
        raise RuntimeError("normalization boom")

    monkeypatch.setattr(auto_heal_module, "_toolkit_infer_configs", fake_infer)
    monkeypatch.setattr(auto_heal_module, "_toolkit_normalization", fake_norm)
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "error"
    assert "normalization" in res["failed_steps"]
    assert "normalization" in res["summary"]
    assert "normalization boom" in res["summary"]["normalization"]["error"]
