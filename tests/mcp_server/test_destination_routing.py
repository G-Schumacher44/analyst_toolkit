import logging
from pathlib import Path

from analyst_toolkit.mcp_server import destination_routing
from analyst_toolkit.mcp_server.destination_routing import deliver_artifact


def test_deliver_artifact_mirrors_to_local_root(tmp_path, monkeypatch):
    source = tmp_path / "exports" / "reports" / "diag.html"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("<html>diag</html>", encoding="utf-8")
    monkeypatch.setenv("ANALYST_MCP_LOCAL_OUTPUT_BASE", str(tmp_path))

    delivery = deliver_artifact(
        str(source),
        run_id="run-123",
        module="diagnostics",
        config={"local_output_root": "routed"},
        session_id=None,
        resolve_path_root=lambda run_id, session_id: f"paths/{run_id}",
        logger=logging.getLogger("test"),
    )

    routed = Path(delivery["local_path"])
    assert routed.exists()
    assert routed.read_text(encoding="utf-8") == "<html>diag</html>"
    # _local_relative_path strips the "exports" prefix to avoid doubled paths
    assert routed == tmp_path / "routed" / "reports" / "diag.html"
    assert delivery["reference"] == str(routed)
    assert delivery["destinations"]["local"]["status"] == "available"


def test_deliver_artifact_rejects_missing_source(tmp_path):
    delivery = deliver_artifact(
        str(tmp_path / "missing.html"),
        run_id="run-123",
        module="diagnostics",
        config={},
        session_id=None,
        resolve_path_root=lambda run_id, session_id: f"paths/{run_id}",
        logger=logging.getLogger("test"),
    )

    assert delivery["reference"] == ""
    assert delivery["destinations"]["local"]["status"] == "missing"
    assert any("Artifact not found for routing" in warning for warning in delivery["warnings"])


def test_deliver_artifact_rejects_local_root_traversal(tmp_path, monkeypatch):
    source = tmp_path / "exports" / "reports" / "diag.html"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("<html>diag</html>", encoding="utf-8")
    monkeypatch.setenv("ANALYST_MCP_LOCAL_OUTPUT_BASE", str(tmp_path))

    delivery = deliver_artifact(
        str(source),
        run_id="run-123",
        module="diagnostics",
        config={"local_output_root": "../escape"},
        session_id=None,
        resolve_path_root=lambda run_id, session_id: f"paths/{run_id}",
        logger=logging.getLogger("test"),
    )

    assert delivery["destinations"]["local"]["status"] == "available"
    assert delivery["destinations"]["local"]["path"] == str(source)
    assert delivery["destinations"]["local"]["requested_root_status"] == "rejected"
    assert any(
        "must not contain parent-directory traversal" in warning for warning in delivery["warnings"]
    )


def test_deliver_artifact_rejects_local_root_absolute_path(tmp_path, monkeypatch):
    source = tmp_path / "exports" / "reports" / "diag.html"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("<html>diag</html>", encoding="utf-8")
    monkeypatch.setenv("ANALYST_MCP_LOCAL_OUTPUT_BASE", str(tmp_path))

    delivery = deliver_artifact(
        str(source),
        run_id="run-123",
        module="diagnostics",
        config={"local_output_root": "/tmp/escape"},
        session_id=None,
        resolve_path_root=lambda run_id, session_id: f"paths/{run_id}",
        logger=logging.getLogger("test"),
    )

    assert delivery["destinations"]["local"]["status"] == "available"
    assert delivery["destinations"]["local"]["path"] == str(source)
    assert delivery["destinations"]["local"]["requested_root_status"] == "rejected"
    assert any(
        "must be relative to the configured local output base" in warning
        for warning in delivery["warnings"]
    )


def test_deliver_artifact_surfaces_drive_as_unsupported(tmp_path):
    source = tmp_path / "exports" / "reports" / "diag.html"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("<html>diag</html>", encoding="utf-8")

    delivery = deliver_artifact(
        str(source),
        run_id="run-123",
        module="diagnostics",
        config={"drive_folder_id": "folder-123"},
        session_id=None,
        resolve_path_root=lambda run_id, session_id: f"paths/{run_id}",
        logger=logging.getLogger("test"),
    )

    assert delivery["destinations"]["drive"]["status"] == "unsupported"
    assert any(
        "Drive uploads are not implemented yet" in warning for warning in delivery["warnings"]
    )


def test_deliver_artifact_respects_upload_artifacts_false(tmp_path):
    source = tmp_path / "exports" / "reports" / "diag.html"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("<html>diag</html>", encoding="utf-8")

    delivery = deliver_artifact(
        str(source),
        run_id="run-123",
        module="diagnostics",
        config={"output_bucket": "gs://artifact-bucket", "upload_artifacts": False},
        session_id=None,
        resolve_path_root=lambda run_id, session_id: f"paths/{run_id}",
        logger=logging.getLogger("test"),
    )

    assert delivery["url"] == ""
    assert delivery["destinations"]["gcs"]["status"] == "disabled"


def test_deliver_artifact_uploads_to_gcs_when_configured(tmp_path, monkeypatch):
    source = tmp_path / "exports" / "reports" / "diag.html"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("<html>diag</html>", encoding="utf-8")

    monkeypatch.setattr(
        destination_routing,
        "_upload_artifact",
        lambda **kwargs: "https://storage.googleapis.com/bucket/diag.html",
    )

    delivery = deliver_artifact(
        str(source),
        run_id="run-123",
        module="diagnostics",
        config={"output_bucket": "gs://artifact-bucket"},
        session_id=None,
        resolve_path_root=lambda run_id, session_id: f"paths/{run_id}",
        logger=logging.getLogger("test"),
    )

    assert delivery["url"] == "https://storage.googleapis.com/bucket/diag.html"
    assert delivery["reference"] == "https://storage.googleapis.com/bucket/diag.html"
    assert delivery["destinations"]["gcs"]["status"] == "available"


def test_local_relative_path_no_doubled_exports():
    """Regression: _local_relative_path must not produce exports/exports/... paths."""
    from analyst_toolkit.mcp_server.destination_routing import _local_relative_path

    # Simulate an absolute path containing "exports" that is outside CWD
    fake_abs = Path("/app/exports/reports/auto_heal/run1_report.html")
    result = _local_relative_path(str(fake_abs))
    # Should NOT start with "exports" — that prefix is stripped
    assert result.parts[0] != "exports", f"Doubled prefix: {result}"
    assert result == Path("reports/auto_heal/run1_report.html")


def test_local_relative_path_preserves_relative_input():
    """Relative paths without exports prefix should pass through unchanged."""
    from analyst_toolkit.mcp_server.destination_routing import _local_relative_path

    result = _local_relative_path("reports/diag.html")
    assert result == Path("reports/diag.html")


def test_local_relative_path_strips_exports_from_relative():
    """Relative paths starting with exports/ must strip the prefix to avoid doubling."""
    from analyst_toolkit.mcp_server.destination_routing import _local_relative_path

    result = _local_relative_path("exports/reports/diagnostics/run1_report.html")
    assert result.parts[0] != "exports", f"Doubled prefix: {result}"
    assert result == Path("reports/diagnostics/run1_report.html")


def test_deliver_artifact_no_doubled_exports_path(tmp_path, monkeypatch):
    """End-to-end: routing to an 'exports' local root must not double the prefix."""
    source = tmp_path / "exports" / "reports" / "run1_report.html"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("<html>report</html>", encoding="utf-8")
    monkeypatch.setenv("ANALYST_MCP_LOCAL_OUTPUT_BASE", str(tmp_path))

    delivery = deliver_artifact(
        str(source),
        run_id="run1",
        module="auto_heal",
        config={"local_output_root": "exports"},
        session_id=None,
        resolve_path_root=lambda run_id, session_id: f"paths/{run_id}",
        logger=logging.getLogger("test"),
    )

    routed = Path(delivery["local_path"])
    assert routed.exists()
    # The routed path should be under exports/reports, not exports/exports/reports
    assert "exports/exports" not in str(routed)
    assert routed == tmp_path / "exports" / "reports" / "run1_report.html"


def test_local_relative_path_strips_exports_for_absolute_path_inside_cwd(tmp_path, monkeypatch):
    """Absolute paths under the workspace exports root should also avoid doubled exports."""
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "exports" / "reports" / "data_dictionary" / "run1_report.html"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("<html>dictionary</html>", encoding="utf-8")
    monkeypatch.setenv("ANALYST_MCP_LOCAL_OUTPUT_BASE", str(tmp_path))

    delivery = deliver_artifact(
        str(source),
        run_id="run1",
        module="data_dictionary",
        config={"local_output_root": "exports"},
        session_id=None,
        resolve_path_root=lambda run_id, session_id: f"paths/{run_id}",
        logger=logging.getLogger("test"),
    )

    routed = Path(delivery["local_path"])
    assert routed.exists()
    assert "exports/exports" not in str(routed)
    assert routed == tmp_path / "exports" / "reports" / "data_dictionary" / "run1_report.html"
