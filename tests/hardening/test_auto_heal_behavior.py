from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_auto_heal_propagates_child_artifacts_and_status(monkeypatch, tmp_path):
    import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_module
    from analyst_toolkit.m00_utils.export_utils import export_html_report as real_export_html_report

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

    def fake_export_html_report(report, export_path, module_name, run_id):
        rewritten_path = tmp_path / Path(export_path).name
        return real_export_html_report(report, str(rewritten_path), module_name, run_id)

    monkeypatch.setattr(auto_heal_module, "export_html_report", fake_export_html_report)
    monkeypatch.setattr(
        auto_heal_module,
        "deliver_artifact",
        lambda local_path, *args, **kwargs: {
            "reference": "https://example.com/auto",
            "local_path": local_path,
            "url": "https://example.com/auto",
            "warnings": [],
            "destinations": {"gcs": {"status": "available", "url": "https://example.com/auto"}},
        },
    )
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "warn"
    assert res["artifact_path"].endswith("run_auto_auto_heal_report.html")
    assert res["artifact_url"] == "https://example.com/auto"
    assert res["export_url"] == "gs://bucket/imp.csv"
    assert res["plot_urls"] == {"imp.png": "https://example.com/imp.png"}
    assert res["artifact_matrix"]["plots"]["status"] == "available"
    assert res["artifact_matrix"]["plots"]["count"] == 1
    assert res["failed_steps"] == []
    assert Path(res["artifact_path"]).exists()
    assert "MCP Auto Heal" in Path(res["artifact_path"]).read_text(encoding="utf-8")


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
    monkeypatch.setattr(auto_heal_module, "export_html_report", lambda *args, **kwargs: "auto.html")
    monkeypatch.setattr(
        auto_heal_module,
        "deliver_artifact",
        lambda local_path, *args, **kwargs: {
            "reference": local_path,
            "local_path": local_path,
            "url": "",
            "warnings": ["html report available only on server-local path"],
            "destinations": {"local": {"status": "available", "path": local_path}},
        },
    )
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "pass"
    assert res["artifact_path"] == "auto.html"
    assert res["artifact_url"] == ""
    assert res["export_url"] == "gs://bucket/norm.csv"
    assert res["plot_urls"] == {"norm.png": "https://example.com/norm.png"}
    assert res["failed_steps"] == []
    assert res["summary"]["imputation"]["skipped"] is True
    assert any("server-local path" in warning for warning in res["warnings"])


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
    monkeypatch.setattr(auto_heal_module, "export_html_report", lambda *args, **kwargs: "auto.html")
    monkeypatch.setattr(
        auto_heal_module,
        "deliver_artifact",
        lambda local_path, *args, **kwargs: {
            "reference": "https://example.com/auto",
            "local_path": local_path,
            "url": "https://example.com/auto",
            "warnings": [],
            "destinations": {"gcs": {"status": "available", "url": "https://example.com/auto"}},
        },
    )
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "error"
    assert "normalization" in res["failed_steps"]
    assert "normalization" in res["summary"]
    assert "normalization boom" in res["summary"]["normalization"]["error"]
    assert all(action["tool"] != "final_audit" for action in res["next_actions"])


@pytest.mark.asyncio
async def test_auto_heal_can_disable_dashboard_export_via_runtime(monkeypatch):
    import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_module

    async def fake_infer(*args, **kwargs):
        return {
            "status": "pass",
            "configs": {"normalization": "normalization:\n  rules: {}\n"},
            "session_id": "sess_unit",
        }

    async def fake_norm(*args, **kwargs):
        return {
            "status": "pass",
            "session_id": "sess_unit",
            "summary": {"changes_made": 1},
            "artifact_path": "norm_report.html",
            "artifact_url": "https://example.com/norm",
            "export_url": "gs://bucket/norm.csv",
            "plot_urls": {},
        }

    def fail_export(*args, **kwargs):
        raise AssertionError("export_html_report should not run when export_html is disabled")

    def fail_deliver(*args, **kwargs):
        raise AssertionError("deliver_artifact should not run when export_html is disabled")

    monkeypatch.setattr(auto_heal_module, "_toolkit_infer_configs", fake_infer)
    monkeypatch.setattr(auto_heal_module, "_toolkit_normalization", fake_norm)
    monkeypatch.setattr(auto_heal_module, "export_html_report", fail_export)
    monkeypatch.setattr(auto_heal_module, "deliver_artifact", fail_deliver)
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(
        session_id="sess_input",
        run_id="run_auto",
        runtime={"artifacts": {"export_html": False}},
    )

    assert res["status"] == "pass"
    assert res["artifact_path"] == ""
    assert res["artifact_url"] == ""
    assert res["artifact_matrix"]["html_report"]["status"] == "disabled"


@pytest.mark.asyncio
async def test_auto_heal_preserves_local_dashboard_when_delivery_raises(monkeypatch):
    import analyst_toolkit.mcp_server.tools.auto_heal as auto_heal_module
    from analyst_toolkit.m00_utils.export_utils import export_html_report as real_export_html_report

    async def fake_infer(*args, **kwargs):
        return {
            "status": "pass",
            "configs": {"normalization": "normalization:\n  rules: {}\n"},
            "session_id": "sess_unit",
        }

    async def fake_norm(*args, **kwargs):
        return {
            "status": "pass",
            "session_id": "sess_unit",
            "summary": {"changes_made": 1},
            "artifact_path": "norm_report.html",
            "artifact_url": "https://example.com/norm",
            "export_url": "gs://bucket/norm.csv",
            "plot_urls": {},
        }

    def fake_export_html_report(report, export_path, module_name, run_id):
        return real_export_html_report(report, export_path, module_name, run_id)

    def fail_deliver(*args, **kwargs):
        raise RuntimeError("upload transport failed")

    monkeypatch.setattr(auto_heal_module, "_toolkit_infer_configs", fake_infer)
    monkeypatch.setattr(auto_heal_module, "_toolkit_normalization", fake_norm)
    monkeypatch.setattr(auto_heal_module, "export_html_report", fake_export_html_report)
    monkeypatch.setattr(auto_heal_module, "deliver_artifact", fail_deliver)
    monkeypatch.setattr(auto_heal_module, "append_to_run_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_heal_module, "get_session_metadata", lambda sid: {"row_count": 3})

    res = await auto_heal_module._toolkit_auto_heal(session_id="sess_input", run_id="run_auto")

    assert res["status"] == "pass"
    assert res["artifact_path"].endswith("run_auto_auto_heal_report.html")
    assert Path(res["artifact_path"]).exists()
    assert res["artifact_url"] == ""
    assert "AUTO_HEAL_EXPORT_FAILED" in res["warnings"]
    assert res["artifact_matrix"]["html_report"]["status"] == "available"
