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


def test_auth_mode_rejects_unauthorized_input_register(client, monkeypatch, tmp_path):
    """Verify token auth mode blocks unauthenticated input registration."""
    monkeypatch.setattr(server_module, "AUTH_TOKEN", "test-token")
    source = tmp_path / "dirty_penguins.csv"
    source.write_text("species,bill_length_mm\nAdelie,39.1\n")

    response = client.post(
        "/inputs/register", json={"uri": str(source), "load_into_session": False}
    )
    assert response.status_code == 401
    assert response.json()["detail"]["error"] == "Unauthorized"
    assert isinstance(response.json()["detail"]["trace_id"], str)


def test_auth_mode_allows_authorized_input_register(client, monkeypatch, tmp_path):
    """Verify token auth mode allows authorized input registration."""
    monkeypatch.setattr(server_module, "AUTH_TOKEN", "test-token")
    monkeypatch.setenv("ANALYST_MCP_ALLOWED_INPUT_ROOTS", str(tmp_path))
    headers = {"Authorization": "Bearer test-token"}
    source = tmp_path / "dirty_penguins.csv"
    source.write_text("species,bill_length_mm\nAdelie,39.1\n")

    response = client.post(
        "/inputs/register",
        json={"uri": str(source), "load_into_session": False},
        headers=headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "pass"
    assert isinstance(payload["trace_id"], str)
    assert payload["input"]["resolved_reference"] == str(source.resolve())


def test_http_auth_posture_warns_when_token_missing(caplog):
    with caplog.at_level("WARNING", logger="analyst_toolkit.mcp_server"):
        server_module._log_http_auth_posture("127.0.0.1", "")

    assert "HTTP auth is disabled" in caplog.text
    assert "ANALYST_MCP_AUTH_TOKEN" in caplog.text


def test_http_auth_posture_warns_on_non_loopback_host(caplog):
    with caplog.at_level("WARNING", logger="analyst_toolkit.mcp_server"):
        server_module._log_http_auth_posture("0.0.0.0", "")

    assert "non-loopback (0.0.0.0)" in caplog.text


def test_http_auth_posture_is_quiet_when_token_present(caplog):
    with caplog.at_level("WARNING", logger="analyst_toolkit.mcp_server"):
        server_module._log_http_auth_posture("127.0.0.1", "token")

    assert caplog.text == ""
