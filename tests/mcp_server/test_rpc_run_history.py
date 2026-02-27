import analyst_toolkit.mcp_server.tools.cockpit as cockpit_module


def test_rpc_get_run_history_supports_summary_modes(client, mocker):
    history = [
        {
            "module": "diagnostics",
            "status": "pass",
            "summary": {"passed": True, "row_count": 5},
            "timestamp": "2026-02-25T00:00:00Z",
        },
        {
            "module": "validation",
            "status": "fail",
            "summary": {"passed": False, "violations_found": ["schema_conformity"]},
            "timestamp": "2026-02-25T00:01:00Z",
        },
        {
            "module": "imputation",
            "status": "warn",
            "summary": {"nulls_filled": 4},
            "timestamp": "2026-02-25T00:02:00Z",
        },
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)

    payload = {
        "jsonrpc": "2.0",
        "id": 33,
        "method": "tools/call",
        "params": {
            "name": "get_run_history",
            "arguments": {
                "run_id": "run_b3",
                "failures_only": True,
                "latest_errors": True,
                "latest_status_by_module": True,
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["filters"]["failures_only"] is True
    assert result["filters"]["summary_only"] is True
    assert result["filters"]["limit"] == 50
    assert result["history_count"] == 1
    assert result["ledger"][0]["module"] == "validation"
    assert len(result["latest_errors"]) == 1
    assert result["latest_errors"][0]["module"] == "validation"
    assert "validation" in result["latest_status_by_module"]
    assert result["latest_status_by_module"]["validation"]["status"] == "fail"


def test_rpc_get_run_history_limit_and_summary_only(client, mocker):
    history = [
        {
            "module": "diagnostics",
            "status": "pass",
            "summary": {"row_count": 10},
            "timestamp": "2026-02-25T00:00:00Z",
            "artifact_url": "https://example.com/a",
        },
        {
            "module": "normalization",
            "status": "warn",
            "summary": {"changes_made": 3},
            "timestamp": "2026-02-25T00:01:00Z",
            "artifact_url": "https://example.com/b",
        },
        {
            "module": "validation",
            "status": "fail",
            "summary": {"passed": False},
            "timestamp": "2026-02-25T00:02:00Z",
            "artifact_url": "https://example.com/c",
        },
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)

    payload = {
        "jsonrpc": "2.0",
        "id": 40,
        "method": "tools/call",
        "params": {
            "name": "get_run_history",
            "arguments": {
                "run_id": "run_b4",
                "limit": 2,
                "summary_only": True,
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["filters"]["limit"] == 2
    assert result["filters"]["summary_only"] is True
    assert result["total_history_count"] == 3
    assert result["history_count"] == 2
    assert [entry["module"] for entry in result["ledger"]] == ["normalization", "validation"]
    assert all(
        set(entry.keys()) == {"module", "status", "timestamp", "summary"}
        for entry in result["ledger"]
    )


def test_rpc_get_run_history_defaults_to_compact_mode(client, mocker):
    history = [
        {"module": "diagnostics", "status": "pass", "summary": {"row_count": 1}, "timestamp": "t1"},
        {"module": "validation", "status": "pass", "summary": {"passed": True}, "timestamp": "t2"},
    ]
    mocker.patch.object(cockpit_module, "get_run_history", return_value=history)
    payload = {
        "jsonrpc": "2.0",
        "id": 42,
        "method": "tools/call",
        "params": {"name": "get_run_history", "arguments": {"run_id": "run_defaults"}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["filters"]["summary_only"] is True
    assert result["filters"]["limit"] == 50
    assert result["history_count"] == 2
    assert all(
        set(entry.keys()) == {"module", "status", "timestamp", "summary"}
        for entry in result["ledger"]
    )
