import pytest

import analyst_toolkit.mcp_server.tools.cockpit as cockpit_module
import analyst_toolkit.mcp_server.tools.cockpit_history as cockpit_history_module


@pytest.mark.asyncio
async def test_toolkit_get_cockpit_dashboard_builds_operator_hub(mocker):
    mocker.patch.object(cockpit_module, "_trusted_history_enabled", return_value=True)
    mocker.patch.object(
        cockpit_module,
        "_build_cockpit_dashboard_report",
        return_value={
            "overview": {
                "recent_run_count": 2,
                "warning_runs": 1,
                "failed_runs": 1,
                "pipeline_dashboards_available": 1,
                "auto_heal_dashboards_available": 1,
            },
            "recent_runs": [],
            "resources": [],
            "launchpad": [],
        },
    )
    export_html = mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/cockpit_dashboard.html",
    )
    deliver = mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "https://example.com/cockpit.html",
            "local_path": "/tmp/cockpit_dashboard.html",
            "url": "https://example.com/cockpit.html",
            "warnings": [],
            "destinations": {
                "gcs": {"status": "available", "url": "https://example.com/cockpit.html"}
            },
        },
    )

    result = await cockpit_module._toolkit_get_cockpit_dashboard(limit=5)

    assert result["status"] == "pass"
    assert result["module"] == "cockpit_dashboard"
    assert result["dashboard_label"] == "Cockpit dashboard"
    assert result["artifact_url"] == "https://example.com/cockpit.html"
    assert result["summary"]["recent_run_count"] == 2
    export_html.assert_called_once_with(
        mocker.ANY,
        "exports/reports/cockpit/cockpit_dashboard_limit_5.html",
        "Cockpit Dashboard",
        "cockpit_dashboard_limit_5",
    )
    deliver.assert_called_once_with(
        "/tmp/cockpit_dashboard.html",
        run_id="cockpit_dashboard_limit_5",
        module="cockpit_dashboard",
        config={"upload_artifacts": False},
        session_id=None,
    )


@pytest.mark.asyncio
async def test_toolkit_get_cockpit_dashboard_denies_when_untrusted(mocker):
    mocker.patch.object(cockpit_module, "_trusted_history_enabled", return_value=False)
    export_html = mocker.patch.object(cockpit_module, "export_html_report")
    deliver = mocker.patch.object(cockpit_module, "deliver_artifact")

    result = await cockpit_module._toolkit_get_cockpit_dashboard(limit=5)

    assert result["status"] == "error"
    assert result["code"] == "COCKPIT_HISTORY_DISABLED"
    assert isinstance(result["trace_id"], str)
    assert result["trace_id"]
    export_html.assert_not_called()
    deliver.assert_not_called()


def test_build_recent_run_cards_discovers_local_dashboards(mocker, tmp_path, monkeypatch):
    history_file = tmp_path / "exports" / "reports" / "history" / "run_local_history.json"
    history_file.parent.mkdir(parents=True, exist_ok=True)
    history_file.write_text("[]", encoding="utf-8")
    run_id = "run_local"
    auto_heal_path = (
        tmp_path / "exports" / "reports" / "auto_heal" / f"{run_id}_auto_heal_report.html"
    )
    final_audit_path = (
        tmp_path / "exports" / "reports" / "final_audit" / f"{run_id}_final_audit_report.html"
    )
    pipeline_path = (
        tmp_path / "exports" / "reports" / "pipeline" / f"{run_id}_pipeline_dashboard.html"
    )
    for artifact in (auto_heal_path, final_audit_path, pipeline_path):
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text("<html></html>", encoding="utf-8")

    mocker.patch.object(
        cockpit_history_module,
        "_iter_recent_history_files",
        return_value=[history_file],
    )
    mocker.patch.object(
        cockpit_history_module,
        "_read_history_entries",
        return_value=[
            {
                "run_id": run_id,
                "session_id": "",
                "module": "diagnostics",
                "status": "pass",
                "timestamp": "2026-03-22T12:00:00Z",
                "warnings": [],
            }
        ],
    )
    mocker.patch.object(
        cockpit_history_module,
        "build_data_health_report",
        return_value={"health_score": 94.0, "health_status": "green"},
    )
    mocker.patch.object(cockpit_history_module, "_WORKSPACE_ROOT", tmp_path)
    monkeypatch.chdir(tmp_path)

    cards = cockpit_module._build_recent_run_cards(limit=5)

    assert len(cards) == 1
    card = cards[0]
    assert card["pipeline_dashboard"].endswith(f"{run_id}_pipeline_dashboard.html")
    assert card["auto_heal_dashboard"].endswith(f"{run_id}_auto_heal_report.html")
    assert card["final_audit_dashboard"].endswith(f"{run_id}_final_audit_report.html")
    assert card["best_dashboard"] == card["final_audit_dashboard"]
