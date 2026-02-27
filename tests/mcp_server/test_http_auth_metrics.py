import analyst_toolkit.mcp_server.server as server_module


def test_health_check(client):
    """Verify the /health endpoint returns the registered tools."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "uptime_sec" in data
    assert "version" in data
    assert "diagnostics" in data["tools"]
    assert "outliers" in data["tools"]


def test_ready_check(client):
    """Verify the readiness endpoint contract."""
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_metrics_endpoint_shape(client):
    """Verify /metrics exposes the expected runtime metrics schema."""
    response = client.get("/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "rpc" in data
    assert "uptime_sec" in data
    rpc = data["rpc"]
    assert "requests_total" in rpc
    assert "errors_total" in rpc
    assert "avg_latency_ms" in rpc
    assert "by_method" in rpc
    assert "by_tool" in rpc


def test_metrics_rpc_counters_delta(client):
    """Verify request/error counters increment based on RPC outcomes."""
    before = server_module.METRICS.snapshot()["rpc"]

    ok_payload = {"jsonrpc": "2.0", "id": 801, "method": "initialize", "params": {}}
    ok_response = client.post("/rpc", json=ok_payload)
    assert ok_response.status_code == 200
    assert "result" in ok_response.json()

    err_payload = {
        "jsonrpc": "2.0",
        "id": 802,
        "method": "tools/call",
        "params": {"name": "missing_tool_for_metrics", "arguments": {}},
    }
    err_response = client.post("/rpc", json=err_payload)
    assert err_response.status_code == 200
    assert "error" in err_response.json()

    after = server_module.METRICS.snapshot()["rpc"]
    assert after["requests_total"] == before["requests_total"] + 2
    assert after["errors_total"] == before["errors_total"] + 1
    assert after["by_method"].get("initialize", 0) >= before["by_method"].get("initialize", 0) + 1
    assert after["by_method"].get("tools/call", 0) >= before["by_method"].get("tools/call", 0) + 1
    assert (
        after["by_tool"].get("missing_tool_for_metrics", 0)
        >= before["by_tool"].get("missing_tool_for_metrics", 0) + 1
    )


def test_auth_mode_rejects_unauthorized_requests(client, monkeypatch):
    """Verify token auth mode blocks unauthenticated calls."""
    monkeypatch.setattr(server_module, "AUTH_TOKEN", "test-token")

    health_response = client.get("/health")
    assert health_response.status_code == 401
    assert health_response.json()["status"] == "unauthorized"

    rpc_payload = {"jsonrpc": "2.0", "id": 901, "method": "initialize", "params": {}}
    rpc_response = client.post("/rpc", json=rpc_payload)
    assert rpc_response.status_code == 401
    assert rpc_response.json()["error"] == "Unauthorized"
    assert isinstance(rpc_response.json().get("trace_id"), str)


def test_auth_mode_allows_bearer_token(client, monkeypatch):
    """Verify token auth mode accepts valid bearer auth."""
    monkeypatch.setattr(server_module, "AUTH_TOKEN", "test-token")
    headers = {"Authorization": "Bearer test-token"}

    ready_response = client.get("/ready", headers=headers)
    assert ready_response.status_code == 200
    assert ready_response.json()["status"] == "ready"

    rpc_payload = {"jsonrpc": "2.0", "id": 902, "method": "initialize", "params": {}}
    rpc_response = client.post("/rpc", json=rpc_payload, headers=headers)
    assert rpc_response.status_code == 200
    assert rpc_response.json()["result"]["serverInfo"]["name"] == "analyst-toolkit"
