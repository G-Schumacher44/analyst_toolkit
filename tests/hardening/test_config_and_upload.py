import sys
import types

from analyst_toolkit.mcp_server.io import (
    check_upload,
    coerce_config,
    load_input,
    resolve_run_context,
    save_output,
    upload_artifact,
)
from analyst_toolkit.mcp_server.state import StateStore


def test_coerce_config_none_returns_empty():
    assert coerce_config(None, "normalization") == {}


def test_coerce_config_plain_dict_passes_through():
    cfg = {"normalization": {"rules": {"coerce_dtypes": True}}}
    result = coerce_config(cfg, "normalization")
    assert result == cfg


def test_coerce_config_parses_raw_yaml_string():
    yaml_str = "rules:\n  coerce_dtypes: true\n  standardize_text_columns: [name]\n"
    result = coerce_config(yaml_str, "normalization")
    assert isinstance(result, dict)
    assert result["rules"]["coerce_dtypes"] is True
    assert result["rules"]["standardize_text_columns"] == ["name"]


def test_coerce_config_parses_yaml_string_inside_module_key():
    """Agent passes {module: yaml_string} instead of {module: dict}."""
    yaml_str = "rules:\n  coerce_dtypes: true\n"
    cfg = {"normalization": yaml_str}
    result = coerce_config(cfg, "normalization")
    assert isinstance(result["normalization"], dict)
    assert result["normalization"]["rules"]["coerce_dtypes"] is True


def test_coerce_config_invalid_yaml_returns_empty():
    result = coerce_config("{{invalid: yaml: [}}", "normalization")
    assert result == {}


def test_coerce_config_non_dict_non_string_returns_empty():
    assert coerce_config(42, "normalization") == {}  # type: ignore[arg-type]


def test_coerce_config_unwraps_double_wrapped():
    """Agent passes {module: {module: {...}}} - unwraps one level."""
    inner = {"rules": {"coerce_dtypes": True}}
    cfg = {"normalization": {"normalization": inner}}
    result = coerce_config(cfg, "normalization")
    assert result == {"normalization": inner}
    assert result["normalization"]["rules"]["coerce_dtypes"] is True


def test_check_upload_passes_through_valid_url():
    warnings: list = []
    url = check_upload("https://storage.googleapis.com/bucket/path.html", "path.html", warnings)
    assert url == "https://storage.googleapis.com/bucket/path.html"
    assert warnings == []


def test_check_upload_appends_warning_on_empty_url():
    warnings: list = []
    url = check_upload("", "exports/reports/normalization/run1_report.html", warnings)
    assert url == ""
    assert len(warnings) == 1
    assert "exports/reports/normalization/run1_report.html" in warnings[0]


def test_check_upload_accumulates_multiple_warnings():
    warnings: list = []
    check_upload("", "report.html", warnings)
    check_upload("", "report.xlsx", warnings)
    assert len(warnings) == 2


def _install_fake_google_storage(monkeypatch, calls: list):
    class FakeBlob:
        def __init__(self, name: str):
            self.name = name

        def upload_from_filename(self, filename: str, content_type: str | None = None):
            calls.append(("upload", self.name, content_type, filename))

    class FakeBucket:
        def __init__(self, name: str):
            self.name = name

        def blob(self, blob_name: str):
            calls.append(("blob", self.name, blob_name))
            return FakeBlob(blob_name)

    class FakeClient:
        def bucket(self, bucket_name: str):
            calls.append(("bucket", bucket_name))
            return FakeBucket(bucket_name)

    storage_mod = types.ModuleType("google.cloud.storage")
    setattr(storage_mod, "Client", FakeClient)

    cloud_mod = types.ModuleType("google.cloud")
    setattr(cloud_mod, "storage", storage_mod)

    google_mod = types.ModuleType("google")
    setattr(google_mod, "cloud", cloud_mod)

    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_mod)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_mod)


def test_save_output_gcs_uses_storage_upload(sample_df, monkeypatch):
    calls: list = []
    _install_fake_google_storage(monkeypatch, calls)

    path = "gs://example-bucket/runs/run_1/imputation_output.csv"
    out = save_output(sample_df, path)

    assert out == path
    assert ("bucket", "example-bucket") in calls
    assert ("blob", "example-bucket", "runs/run_1/imputation_output.csv") in calls
    uploads = [c for c in calls if c[0] == "upload"]
    assert len(uploads) == 1
    assert uploads[0][1] == "runs/run_1/imputation_output.csv"
    assert uploads[0][2] == "text/csv"


def test_save_output_gcs_is_idempotent_for_same_path(sample_df, monkeypatch):
    calls: list = []
    _install_fake_google_storage(monkeypatch, calls)

    path = "gs://example-bucket/runs/shared_run/imputation_output.csv"
    save_output(sample_df, path)
    save_output(sample_df, path)

    uploads = [c for c in calls if c[0] == "upload"]
    assert len(uploads) == 2
    assert uploads[0][1] == "runs/shared_run/imputation_output.csv"
    assert uploads[1][1] == "runs/shared_run/imputation_output.csv"


def test_save_output_gcs_falls_back_to_versioned_key_on_primary_failure(sample_df, monkeypatch):
    calls: list[tuple[str, str]] = []

    class FakeBlob:
        def __init__(self, name: str):
            self.name = name

        def upload_from_filename(self, filename: str, content_type: str | None = None):
            calls.append(("upload", self.name))
            if len(calls) == 1:
                raise PermissionError("storage.objects.delete access denied")

    class FakeBucket:
        def blob(self, blob_name: str):
            return FakeBlob(blob_name)

    class FakeClient:
        def bucket(self, _bucket_name: str):
            return FakeBucket()

    storage_mod = types.ModuleType("google.cloud.storage")
    setattr(storage_mod, "Client", FakeClient)
    cloud_mod = types.ModuleType("google.cloud")
    setattr(cloud_mod, "storage", storage_mod)
    google_mod = types.ModuleType("google")
    setattr(google_mod, "cloud", cloud_mod)
    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_mod)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_mod)

    path = "gs://example-bucket/runs/run_1/imputation_output.csv"
    out = save_output(sample_df, path)

    assert out.startswith("gs://example-bucket/runs/run_1/")
    assert out.endswith(".csv")
    assert out != path
    assert len([c for c in calls if c[0] == "upload"]) == 2


def test_load_input_auto_normalizes_bucket_like_path(monkeypatch):
    import pandas as pd

    expected = pd.DataFrame({"a": [1]})
    monkeypatch.setattr(
        "analyst_toolkit.mcp_server.io.load_from_gcs",
        lambda gcs_path: expected if gcs_path == "gs://my-bucket/path/file.csv" else None,
    )
    out = load_input("my-bucket/path/file.csv")
    pd.testing.assert_frame_equal(out, expected)


def test_resolve_run_context_dedupes_mismatch_warning(sample_df, monkeypatch):
    import analyst_toolkit.mcp_server.io as io_module

    sid = StateStore.save(sample_df, run_id="run_bound")
    monkeypatch.setattr(io_module, "_SEEN_LIFECYCLE_WARNING_KEYS", set())
    monkeypatch.setattr(io_module, "DEDUP_RUN_ID_WARNINGS", True)

    run_a, lifecycle_a = resolve_run_context("run_other", sid)
    run_b, lifecycle_b = resolve_run_context("run_other", sid)

    assert run_a == "run_bound"
    assert run_b == "run_bound"
    assert lifecycle_a["warnings"]
    assert lifecycle_b["warnings"] == []


def test_upload_artifact_falls_back_to_versioned_key_on_primary_failure(monkeypatch, tmp_path):
    local = tmp_path / "report.html"
    local.write_text("<html>ok</html>", encoding="utf-8")

    upload_calls: list[str] = []

    class FakeBlob:
        def __init__(self, name: str):
            self.name = name

        def upload_from_filename(self, filename: str, content_type: str | None = None):
            upload_calls.append(self.name)
            if len(upload_calls) == 1:
                raise PermissionError("storage.objects.delete access denied")

    class FakeBucket:
        def __init__(self, name: str):
            self.name = name

        def blob(self, blob_name: str):
            return FakeBlob(blob_name)

    class FakeClient:
        def bucket(self, bucket_name: str):
            return FakeBucket(bucket_name)

    storage_mod = types.ModuleType("google.cloud.storage")
    setattr(storage_mod, "Client", FakeClient)
    cloud_mod = types.ModuleType("google.cloud")
    setattr(cloud_mod, "storage", storage_mod)
    google_mod = types.ModuleType("google")
    setattr(google_mod, "cloud", cloud_mod)
    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_mod)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_mod)
    monkeypatch.setenv("ANALYST_REPORT_BUCKET", "gs://example-bucket")
    monkeypatch.setenv("ANALYST_REPORT_PREFIX", "analyst_toolkit/reports")

    out = upload_artifact(
        local_path=str(local),
        run_id="run_retry",
        module="imputation",
        session_id="sess_retry",
    )

    assert out.startswith("https://storage.googleapis.com/example-bucket/")
    assert len(upload_calls) == 2
    assert upload_calls[0].endswith("/imputation/report.html")
    assert upload_calls[1].endswith(".html")
    assert upload_calls[1] != upload_calls[0]
