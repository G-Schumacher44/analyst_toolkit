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
    assert routed == tmp_path / "routed" / "exports" / "reports" / "diag.html"
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

    assert delivery["destinations"]["local"]["status"] == "rejected"
    assert any(
        "must not contain parent-directory traversal" in warning for warning in delivery["warnings"]
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
