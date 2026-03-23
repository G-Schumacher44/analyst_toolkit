import pytest

import analyst_toolkit.mcp_server.tools.cockpit as cockpit_module


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_builds_tabbed_artifact(mocker):
    history = [
        {
            "module": "diagnostics",
            "status": "pass",
            "session_id": "sess_pipeline",
            "summary": {"rows": 100},
            "dashboard_url": "https://example.com/diag.html",
            "export_url": "gs://bucket/diag.csv",
        },
        {
            "module": "validation",
            "status": "warn",
            "session_id": "sess_pipeline",
            "summary": {"passed": False, "failed_rules": 2},
            "dashboard_path": "exports/reports/validation/run_val.html",
            "warnings": ["rule mismatch"],
        },
        {
            "module": "final_audit",
            "status": "fail",
            "session_id": "sess_pipeline",
            "summary": {"passed": False},
            "dashboard_path": "exports/reports/final_audit/run_final.html",
            "export_url": "gs://bucket/final.csv",
        },
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    export_html = mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/run-pipeline-001_pipeline_dashboard.html",
    )
    append_history = mocker.patch.object(cockpit_module, "append_to_run_history")
    deliver = mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "https://example.com/pipeline.html",
            "local_path": "/tmp/run-pipeline-001_pipeline_dashboard.html",
            "url": "https://example.com/pipeline.html",
            "warnings": [],
            "destinations": {
                "gcs": {"status": "available", "url": "https://example.com/pipeline.html"}
            },
        },
    )

    result = await cockpit_module._toolkit_get_pipeline_dashboard(run_id="run-pipeline-001")

    assert result["status"] == "pass"
    assert result["module"] == "pipeline_dashboard"
    assert result["session_id"] == ""
    assert result["dashboard_label"] == "Pipeline dashboard"
    assert result["artifact_url"] == "https://example.com/pipeline.html"
    assert result["summary"]["failed_modules"] == 1
    assert result["summary"]["warned_modules"] == 1
    assert result["summary"]["ready_modules"] == 1
    assert result["summary"]["not_run_modules"] == 5
    export_html.assert_called_once_with(
        mocker.ANY,
        "exports/reports/pipeline/run-pipeline-001_pipeline_dashboard.html",
        "Pipeline Dashboard",
        "run-pipeline-001",
    )
    deliver.assert_called_once_with(
        "/tmp/run-pipeline-001_pipeline_dashboard.html",
        run_id="run-pipeline-001",
        module="pipeline_dashboard",
        config={},
        session_id=None,
    )
    append_history.assert_called_once_with(
        "run-pipeline-001",
        mocker.ANY,
        session_id=None,
    )


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_hides_internal_outlier_handling_stage(mocker):
    history = [
        {
            "module": "outlier_handling",
            "status": "pass",
            "session_id": "sess_pipeline",
            "summary": {"handled_rows": 5},
        }
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    captured_report: dict[str, object] = {}

    def fake_export_html(report, artifact_path, title, safe_run_id):
        captured_report["report"] = report
        return "/tmp/internal-filtered_pipeline_dashboard.html"

    mocker.patch.object(cockpit_module, "export_html_report", side_effect=fake_export_html)
    mocker.patch.object(cockpit_module, "append_to_run_history")
    mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "",
            "local_path": "/tmp/internal-filtered_pipeline_dashboard.html",
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await cockpit_module._toolkit_get_pipeline_dashboard(run_id="run-pipeline-002")

    assert result["status"] == "pass"
    report = captured_report["report"]
    assert isinstance(report, dict)
    assert "Outlier Handling" not in report["module_order"]
    assert "Outlier Handling" not in report["modules"]


def test_build_data_health_report_marks_failed_final_audit_as_advisory():
    health = cockpit_module.build_data_health_report(
        run_id="run-health-001",
        session_id="sess-health-001",
        history=[
            {
                "module": "diagnostics",
                "status": "pass",
                "summary": {"null_rate": 0.0, "row_count": 100},
            },
            {
                "module": "validation",
                "status": "pass",
                "summary": {"passed": True, "row_count": 100},
            },
            {
                "module": "final_audit",
                "status": "fail",
                "summary": {"passed": False, "row_count": 100},
            },
        ],
        history_meta={"parse_errors": [], "skipped_records": 0},
    )

    assert health["status"] == "warn"
    assert health["health_score"] == 100.0
    assert health["health_advisory"] is True
    assert health["certification_status"] == "fail"
    assert health["certification_passed"] is False
    assert "Advisory Data Health Score" in health["message"]
    assert any("final_audit reported certification failures" in msg for msg in health["warnings"])


def test_build_data_health_report_tolerates_malformed_final_audit_summary():
    health = cockpit_module.build_data_health_report(
        run_id="run-health-002",
        session_id="sess-health-002",
        history=[
            {
                "module": "final_audit",
                "status": "fail",
                "summary": ["unexpected", "shape"],
            }
        ],
        history_meta={"parse_errors": [], "skipped_records": 0},
    )

    assert health["status"] == "warn"
    assert health["health_advisory"] is True
    assert health["certification_status"] == "fail"
    assert health["certification_passed"] is None


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_surfaces_advisory_health_when_final_audit_failed(
    mocker,
):
    history = [
        {
            "module": "final_audit",
            "status": "fail",
            "summary": {"passed": False},
            "dashboard_path": "exports/reports/final_audit/run_final.html",
        }
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/run-health-001_pipeline_dashboard.html",
    )
    append_history = mocker.patch.object(cockpit_module, "append_to_run_history")
    mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "https://example.com/pipeline.html",
            "local_path": "/tmp/run-health-001_pipeline_dashboard.html",
            "url": "https://example.com/pipeline.html",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await cockpit_module._toolkit_get_pipeline_dashboard(run_id="run-health-001")

    assert result["summary"]["health_advisory"] is True
    assert result["summary"]["certification_status"] == "fail"
    assert any("Health score is advisory only" in warning for warning in result["warnings"])
    append_history.assert_called_once()


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_sanitizes_run_id_for_artifact_path(mocker):
    mocker.patch.object(cockpit_module, "get_run_history", return_value=[])
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    export_html = mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/pipeline_dashboard.html",
    )
    mocker.patch.object(cockpit_module, "append_to_run_history")
    mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "",
            "local_path": "/tmp/pipeline_dashboard.html",
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    await cockpit_module._toolkit_get_pipeline_dashboard(run_id="../unsafe run")

    export_html.assert_called_once_with(
        mocker.ANY,
        "exports/reports/pipeline/unsafe_run_pipeline_dashboard.html",
        "Pipeline Dashboard",
        "unsafe_run",
    )


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_uses_session_specific_artifact_path(mocker):
    mocker.patch.object(cockpit_module, "get_run_history", return_value=[])
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    export_html = mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/pipeline_dashboard.html",
    )
    mocker.patch.object(cockpit_module, "append_to_run_history")
    mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "",
            "local_path": "/tmp/pipeline_dashboard.html",
            "url": "",
            "warnings": [],
            "destinations": {},
        },
    )

    await cockpit_module._toolkit_get_pipeline_dashboard(
        run_id="run-pipeline-001",
        session_id="session-42",
    )

    export_html.assert_called_once_with(
        mocker.ANY,
        "exports/reports/pipeline/run-pipeline-001_session-42_pipeline_dashboard.html",
        "Pipeline Dashboard",
        "run-pipeline-001",
    )


@pytest.mark.asyncio
async def test_toolkit_get_pipeline_dashboard_does_not_append_duplicate_history_on_retry(mocker):
    history = [
        {
            "module": "pipeline_dashboard",
            "status": "pass",
            "session_id": "",
            "artifact_path": "exports/reports/pipeline/run-pipeline-001_pipeline_dashboard.html",
            "artifact_url": "https://example.com/pipeline.html",
            "summary": {"health_score": 95},
        }
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)
    mocker.patch.object(
        cockpit_module,
        "get_last_history_read_meta",
        return_value={"parse_errors": [], "skipped_records": 0},
    )
    mocker.patch.object(
        cockpit_module,
        "export_html_report",
        return_value="/tmp/run-pipeline-001_pipeline_dashboard.html",
    )
    append_history = mocker.patch.object(cockpit_module, "append_to_run_history")
    deliver = mocker.patch.object(
        cockpit_module,
        "deliver_artifact",
        return_value={
            "reference": "https://example.com/pipeline.html",
            "local_path": "/tmp/run-pipeline-001_pipeline_dashboard.html",
            "url": "https://example.com/pipeline.html",
            "warnings": [],
            "destinations": {},
        },
    )

    result = await cockpit_module._toolkit_get_pipeline_dashboard(run_id="run-pipeline-001")

    assert result["status"] == "pass"
    assert result["artifact_path"] == "/tmp/run-pipeline-001_pipeline_dashboard.html"
    assert result["artifact_url"] == history[0]["artifact_url"]
    assert result["artifact_url"] == "https://example.com/pipeline.html"
    deliver.assert_called_once()
    deliver_call = deliver.call_args.kwargs
    assert deliver_call["run_id"] == "run-pipeline-001"
    assert deliver_call["module"] == "pipeline_dashboard"
    append_history.assert_not_called()
