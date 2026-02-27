import pytest
from fastapi.testclient import TestClient

from analyst_toolkit.mcp_server.server import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)
