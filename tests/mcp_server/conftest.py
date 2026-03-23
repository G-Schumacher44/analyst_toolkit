import pytest
from fastapi.testclient import TestClient

import analyst_toolkit.mcp_server.local_artifact_server as artifact_server_module
from analyst_toolkit.mcp_server.server import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def reset_artifact_server():
    artifact_server_module._reset_local_artifact_server_for_tests()
    yield
    artifact_server_module._reset_local_artifact_server_for_tests()
