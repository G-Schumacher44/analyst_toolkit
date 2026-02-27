def test_rpc_preflight_config_normalizes_validation_shape(client):
    payload = {
        "jsonrpc": "2.0",
        "id": 36,
        "method": "tools/call",
        "params": {
            "name": "preflight_config",
            "arguments": {
                "module_name": "validation",
                "config": {
                    "rules": {
                        "schema_validation": {
                            "rules": {
                                "expected_columns": ["tag_id", "species"],
                            }
                        },
                        "expected_types": {"tag_id": "str"},
                    }
                },
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["module"] == "validation"
    assert result["summary"]["effective_rules_path"] == "validation.schema_validation.rules.*"
    assert isinstance(result["warnings"], list)
    assert result["warnings"]
    assert "schema_validation" in result["warnings"][0]
    assert "schema_validation" in result["effective_config"]
    assert "expected_types" in result["effective_config"]["schema_validation"]["rules"]


def test_rpc_preflight_config_normalizes_outliers_shorthand(client):
    payload = {
        "jsonrpc": "2.0",
        "id": 37,
        "method": "tools/call",
        "params": {
            "name": "preflight_config",
            "arguments": {
                "module_name": "outliers",
                "config": {
                    "method": "iqr",
                    "iqr_multiplier": 1.1,
                    "columns": ["transaction_amount", "frequency_24h"],
                },
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["module"] == "outliers"
    assert (
        result["summary"]["effective_rules_path"] == "outlier_detection.detection_specs.<column>.*"
    )
    specs = result["effective_config"]["detection_specs"]
    assert specs["transaction_amount"]["method"] == "iqr"
    assert specs["frequency_24h"]["method"] == "iqr"


def test_rpc_preflight_config_strict_fails_on_warnings(client):
    payload = {
        "jsonrpc": "2.0",
        "id": 38,
        "method": "tools/call",
        "params": {
            "name": "preflight_config",
            "arguments": {
                "module_name": "validation",
                "strict": True,
                "config": {
                    "rules": {
                        "schema_validation": {
                            "rules": {
                                "expected_columns": ["tag_id", "species"],
                            }
                        }
                    }
                },
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "error"
    assert result["summary"]["strict"] is True
    assert result["warnings"]
    assert "schema_validation" in result["warnings"][0]


def test_rpc_preflight_config_strict_fails_on_unknown_keys(client):
    payload = {
        "jsonrpc": "2.0",
        "id": 39,
        "method": "tools/call",
        "params": {
            "name": "preflight_config",
            "arguments": {
                "module_name": "outliers",
                "strict": True,
                "config": {
                    "method": "iqr",
                    "columns": ["transaction_amount"],
                    "unexpected_flag": True,
                },
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "error"
    assert result["unknown_keys"] == ["unexpected_flag"]
    assert result["summary"]["unknown_key_count"] == 1
    assert any("Unknown top-level keys" in w for w in result["warnings"])


def test_rpc_preflight_config_strict_fails_on_nested_unknown_keys(client):
    payload = {
        "jsonrpc": "2.0",
        "id": 44,
        "method": "tools/call",
        "params": {
            "name": "preflight_config",
            "arguments": {
                "module_name": "normalization",
                "strict": True,
                "config": {"normalization": {"run": True, "bogus_key_for_test": 123}},
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "error"
    assert result["summary"]["unknown_key_count"] >= 1
    assert any("bogus_key_for_test" in k for k in result["unknown_keys"])


def test_rpc_preflight_config_non_strict_warns_on_unknown_keys(client):
    payload = {
        "jsonrpc": "2.0",
        "id": 41,
        "method": "tools/call",
        "params": {
            "name": "preflight_config",
            "arguments": {"module_name": "normalization", "config": {"foo": 1}},
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "pass"
    assert result["unknown_keys"] == ["foo"]
    assert result["summary"]["unknown_key_count"] == 1
    assert any("Unknown top-level keys" in w for w in result["warnings"])


def test_rpc_tools_call_returns_structured_error_envelope_for_tool_failure(client):
    """Verify tool runtime failures are normalized to structured status=error payloads."""
    payload = {
        "jsonrpc": "2.0",
        "id": 28,
        "method": "tools/call",
        "params": {"name": "diagnostics", "arguments": {}},
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] == "error"
    assert result["module"] == "diagnostics"
    assert isinstance(result.get("trace_id"), str)
    assert result["error"]["category"] == "internal"
    assert result["error"]["code"] == "tool_execution_failed"
    assert result["error"]["retryable"] is False
    assert result["error"]["trace_id"] == result["trace_id"]
