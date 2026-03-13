import base64

import pandas as pd

from analyst_toolkit.m00_utils.export_utils import export_html_report
from analyst_toolkit.m00_utils.report_html import generate_html_report

_ONE_PIXEL_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jXioAAAAASUVORK5CYII="
)


def test_generate_diagnostics_dashboard_embeds_plots(tmp_path):
    plot_path = tmp_path / "plot.png"
    plot_path.write_bytes(_ONE_PIXEL_PNG)

    report = {
        "schema": pd.DataFrame(
            [
                {
                    "Column": "city",
                    "Dtype": "object",
                    "Unique Values": 3,
                    "Audit Remarks": "OK",
                    "Missing Count": 1,
                }
            ]
        ),
        "high_cardinality": pd.DataFrame([{"Column": "city", "Unique Values": 3}]),
        "shape": pd.DataFrame([{"Rows": 10, "Columns": 4}]),
        "memory_usage": pd.DataFrame([{"Memory Usage": "0.01 MB"}]),
        "duplicates_summary": pd.DataFrame([{"Duplicate Rows": 2, "Duplicate %": 20.0}]),
        "duplicated_rows": pd.DataFrame([{"city": "Austin"}]),
        "describe": pd.DataFrame([{"Metric": "amount", "mean": 10.0}]),
        "sample_head": pd.DataFrame([{"city": "Austin"}]),
    }

    html = export_html_report(
        report,
        str(tmp_path / "diagnostics.html"),
        "Diagnostics",
        "run-123",
        plot_paths={"Summary Plots": [str(plot_path)]},
    )

    contents = (tmp_path / "diagnostics.html").read_text(encoding="utf-8")
    assert html.endswith("diagnostics.html")
    assert "M01 Data Diagnostics" in contents
    assert "Columns with Nulls" in contents
    assert "data:image/png;base64," in contents
    assert "Full Profile &amp; Cardinality" in contents


def test_generate_validation_dashboard_renders_failure_details():
    results = {
        "schema_conformity": {
            "rule_description": "Verify schema",
            "passed": False,
            "details": {"missing_columns": ["zip_code"], "unexpected_columns": ["zipcode"]},
        },
        "dtype_enforcement": {
            "rule_description": "Verify dtypes",
            "passed": True,
            "details": {},
        },
        "categorical_values": {
            "rule_description": "Verify categories",
            "passed": False,
            "details": {
                "status": {
                    "allowed_values": ["new", "done"],
                    "invalid_value_summary": pd.DataFrame([{"Invalid Value": "oops", "Count": 2}]),
                    "violating_rows": pd.DataFrame([{"status": "oops"}]),
                }
            },
        },
        "numeric_ranges": {
            "rule_description": "Verify ranges",
            "passed": True,
            "details": {},
        },
        "summary": {"row_coverage_percent": 87.5},
    }

    html = generate_html_report(results, "Validation", "run-456")

    assert "M02 Data Validation" in html
    assert "Checks Passed:</strong> 2/4" in html
    assert "Row Coverage:</strong> 87.5%" in html
    assert "zip_code" in html
    assert "Allowed values:" in html
    assert "oops" in html


def test_generate_final_audit_dashboard_renders_failure_sections():
    report = {
        "Pipeline_Summary": pd.DataFrame(
            [
                {"Metric": "Final Pipeline Status", "Value": "❌ CERTIFICATION FAILED"},
                {"Metric": "Certification Rules Passed", "Value": False},
            ]
        ),
        "Data_Lifecycle": pd.DataFrame([{"Metric": "Initial Rows", "Value": 10}]),
        "Final_Edits_Log": pd.DataFrame([{"Edit": "rename col"}]),
        "FAILURES_schema_conformity": {
            "missing_columns": ["customer_id"],
            "unexpected_columns": [],
        },
        "Null_Check_Failures": {"city": 3},
        "Final_Data_Profile": pd.DataFrame([{"Column": "city", "Audit Remarks": "OK"}]),
        "Final_Descriptive_Stats": pd.DataFrame([{"Metric": "amount", "mean": 10.0}]),
        "Final_Data_Preview": pd.DataFrame([{"city": "Austin"}]),
    }

    html = generate_html_report(report, "Final Audit", "run-789")

    assert "M10 Final Audit" in html
    assert "CERTIFICATION FAILED" in html
    assert "Failure Details" in html
    assert "customer_id" in html
    assert "Final Data Profile" in html


def test_generate_generic_dashboard_falls_back_for_other_modules():
    report = {"summary_table": pd.DataFrame([{"Column": "value"}])}

    html = generate_html_report(report, "Normalization", "run-000")

    assert "Normalization Dashboard" in html
    assert "Summary Table" in html
    assert "value" in html
