import pandas as pd

import analyst_toolkit.mcp_server.tools.data_dictionary as data_dictionary_tool


def test_rpc_data_dictionary_tool(client, mocker, tmp_path):
    """Verify tools/call data_dictionary returns artifact-first prelaunch output seeded by inference."""
    dataframe = pd.DataFrame(
        {
            "customer_id": [1, 2],
            "status": ["new", "done"],
            "amount": [10.5, 12.0],
        }
    )
    load_input = mocker.patch.object(data_dictionary_tool, "load_input", return_value=dataframe)
    save_to_session = mocker.patch.object(
        data_dictionary_tool, "save_to_session", return_value="sess_dictionary"
    )
    append_to_run_history = mocker.patch.object(
        data_dictionary_tool, "append_to_run_history", return_value=None
    )
    export_dataframes = mocker.patch.object(
        data_dictionary_tool, "export_dataframes", return_value=None
    )
    export_html_report = mocker.patch.object(
        data_dictionary_tool,
        "export_html_report",
        return_value=str(tmp_path / "dictionary.html"),
    )
    deliver_artifact = mocker.patch.object(
        data_dictionary_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": local_path,
            "local_path": local_path,
            "url": "" if local_path.endswith(".xlsx") else "https://example.com/dictionary.html",
            "warnings": [],
            "destinations": {},
        },
    )
    infer_configs = mocker.patch.object(
        data_dictionary_tool,
        "_toolkit_infer_configs",
        mocker.AsyncMock(
            return_value={
                "status": "pass",
                "configs": {
                    "validation": (
                        "validation:\n"
                        "  schema_validation:\n"
                        "    rules:\n"
                        "      expected_columns: [customer_id, status, amount]\n"
                        "      categorical_values:\n"
                        "        status: [new, done]\n"
                    ),
                    "normalization": (
                        "normalization:\n  rules:\n    coerce_dtypes:\n      amount: float64\n"
                    ),
                },
                "warnings": [],
            }
        ),
    )

    payload = {
        "jsonrpc": "2.0",
        "id": 41,
        "method": "tools/call",
        "params": {
            "name": "data_dictionary",
            "arguments": {
                "gcs_path": "gs://bucket/data.csv",
                "run_id": "dictionary_prelaunch_001",
                "prelaunch_report": True,
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] in {"pass", "warn"}
    assert result["module"] == "data_dictionary"
    assert result["template_path"] == "config/data_dictionary_request_template.yaml"
    assert result["summary"]["prelaunch_report"] is True
    assert result["summary"]["inference_status"] == "pass"
    assert result["dashboard_label"] == "Data dictionary dashboard"
    assert result["artifact_url"] == "https://example.com/dictionary.html"
    assert result["xlsx_path"].endswith("dictionary_prelaunch_001_data_dictionary_report.xlsx")
    assert result["cockpit_preview"]["overview"]["rows"] == 2
    assert result["cockpit_preview"]["overview"]["expected_columns"] == 3
    assert result["cockpit_preview"]["expected_schema_preview"][0]["Column"] == "customer_id"
    assert result["next_actions"][0]["tool"] == "get_cockpit_dashboard"
    load_input.assert_called_once_with("gs://bucket/data.csv", session_id=None, input_id=None)
    save_to_session.assert_called_once_with(dataframe, run_id="dictionary_prelaunch_001")
    infer_configs.assert_awaited_once_with(
        gcs_path="gs://bucket/data.csv",
        session_id="sess_dictionary",
        runtime=None,
        run_id="dictionary_prelaunch_001",
    )
    export_dataframes.assert_called_once()
    export_html_report.assert_called_once_with(
        mocker.ANY,
        "exports/reports/data_dictionary/dictionary_prelaunch_001_data_dictionary_report.html",
        "Data Dictionary",
        "dictionary_prelaunch_001",
    )
    assert deliver_artifact.call_count == 2
    append_to_run_history.assert_called_once_with(
        "dictionary_prelaunch_001", mocker.ANY, session_id="sess_dictionary"
    )


def test_rpc_data_dictionary_tool_passes_explicit_input_id(client, mocker, tmp_path):
    dataframe = pd.DataFrame(
        {
            "customer_id": [1, 2],
            "status": ["new", "done"],
            "amount": [10.5, 12.0],
        }
    )
    load_input = mocker.patch.object(data_dictionary_tool, "load_input", return_value=dataframe)
    mocker.patch.object(data_dictionary_tool, "save_to_session", return_value="sess_dictionary")
    mocker.patch.object(data_dictionary_tool, "append_to_run_history", return_value=None)
    mocker.patch.object(data_dictionary_tool, "export_dataframes", return_value=None)
    mocker.patch.object(
        data_dictionary_tool,
        "export_html_report",
        return_value=str(tmp_path / "dictionary.html"),
    )
    mocker.patch.object(
        data_dictionary_tool,
        "deliver_artifact",
        side_effect=lambda local_path, *args, **kwargs: {
            "reference": local_path,
            "local_path": local_path,
            "url": "" if local_path.endswith(".xlsx") else "https://example.com/dictionary.html",
            "warnings": [],
            "destinations": {},
        },
    )
    mocker.patch.object(
        data_dictionary_tool,
        "_toolkit_infer_configs",
        mocker.AsyncMock(return_value={"status": "pass", "configs": {}, "warnings": []}),
    )

    payload = {
        "jsonrpc": "2.0",
        "id": 42,
        "method": "tools/call",
        "params": {
            "name": "data_dictionary",
            "arguments": {
                "gcs_path": "gs://bucket/data.csv",
                "input_id": "input_deadbeefcafebabe",
                "run_id": "dictionary_prelaunch_002",
            },
        },
    }
    response = client.post("/rpc", json=payload)
    assert response.status_code == 200
    result = response.json()["result"]
    assert result["status"] in {"pass", "warn"}
    load_input.assert_called_once_with(
        "gs://bucket/data.csv",
        session_id=None,
        input_id="input_deadbeefcafebabe",
    )
