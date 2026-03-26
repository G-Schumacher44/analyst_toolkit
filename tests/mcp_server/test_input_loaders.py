import sys
import types
from pathlib import Path

import pandas as pd
import pytest

from analyst_toolkit.mcp_server.input.errors import InputPayloadTooLargeError
from analyst_toolkit.mcp_server.input.loaders import load_dataframe_from_descriptor
from analyst_toolkit.mcp_server.input.models import InputDescriptor


def _descriptor_for(path: Path, *, source_type: str = "server_path") -> InputDescriptor:
    return InputDescriptor(
        input_id="input_deadbeefcafebabe",
        source_type=source_type,
        original_reference=str(path),
        resolved_reference=str(path),
        display_name=path.name,
        media_type="text/csv",
    )


def test_load_dataframe_from_descriptor_rejects_local_file_over_byte_limit(monkeypatch, tmp_path):
    source = tmp_path / "wide.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")
    monkeypatch.setenv("ANALYST_MCP_MAX_INPUT_BYTES", "4")

    with pytest.raises(InputPayloadTooLargeError, match="ANALYST_MCP_MAX_INPUT_BYTES"):
        load_dataframe_from_descriptor(_descriptor_for(source))


def test_load_dataframe_from_descriptor_rejects_dataframe_over_row_limit(monkeypatch, tmp_path):
    source = tmp_path / "rows.csv"
    pd.DataFrame({"a": [1, 2]}).to_csv(source, index=False)
    monkeypatch.setenv("ANALYST_MCP_MAX_INPUT_BYTES", "1000000")
    monkeypatch.setenv("ANALYST_MCP_MAX_INPUT_ROWS", "1")

    with pytest.raises(InputPayloadTooLargeError, match="ANALYST_MCP_MAX_INPUT_ROWS"):
        load_dataframe_from_descriptor(_descriptor_for(source))


def test_load_dataframe_from_descriptor_rejects_gcs_prefix_over_object_limit(monkeypatch):
    class FakeBlob:
        def __init__(self, name: str, size: int = 8):
            self.name = name
            self.size = size

        def download_to_filename(self, filename: str) -> None:
            Path(filename).write_text("a\n1\n", encoding="utf-8")

    class FakeBucket:
        def get_blob(self, blob_name: str):
            return None

    class FakeClient:
        def bucket(self, _bucket_name: str):
            return FakeBucket()

        def list_blobs(self, _bucket_name: str, prefix: str):
            assert prefix == "dataset/"
            return [
                FakeBlob("dataset/part-000.csv"),
                FakeBlob("dataset/part-001.csv"),
            ]

    storage_mod = types.ModuleType("google.cloud.storage")
    storage_mod.Client = FakeClient
    cloud_mod = types.ModuleType("google.cloud")
    cloud_mod.storage = storage_mod
    google_mod = types.ModuleType("google")
    google_mod.cloud = cloud_mod

    monkeypatch.setitem(sys.modules, "google", google_mod)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_mod)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_mod)
    monkeypatch.setenv("ANALYST_MCP_MAX_GCS_PREFIX_OBJECTS", "1")
    monkeypatch.setenv("ANALYST_MCP_MAX_INPUT_BYTES", "1000000")

    descriptor = InputDescriptor(
        input_id="input_deadbeefcafebabe",
        source_type="gcs",
        original_reference="gs://bucket/dataset/",
        resolved_reference="gs://bucket/dataset/",
        display_name="dataset/",
        media_type="text/csv",
    )

    with pytest.raises(InputPayloadTooLargeError, match="ANALYST_MCP_MAX_GCS_PREFIX_OBJECTS"):
        load_dataframe_from_descriptor(descriptor)
