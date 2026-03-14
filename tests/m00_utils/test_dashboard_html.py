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

    output_path = export_html_report(
        report,
        str(tmp_path / "diagnostics.html"),
        "Diagnostics",
        "run-123",
        plot_paths={"Summary Plots": [str(plot_path)]},
    )

    contents = (tmp_path / "diagnostics.html").read_text(encoding="utf-8")
    assert output_path.endswith("diagnostics.html")
    assert "M01 Data Diagnostics" in contents
    assert "Columns with Nulls" in contents
    assert "data:image/png;base64," in contents
    assert "Full Profile &amp; Cardinality" in contents
    assert "plot-trigger" in contents
    assert "Click to expand" in contents
    assert "id='plot-modal'" in contents
    assert "window.atkDashboard.openPlot" in contents


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

    assert "M02 Validation Gate" in html
    assert "Validation Requires Attention" in html
    assert "Checks passed: 2/4." in html
    assert "Failure Overview" in html
    assert "Rules Requiring Review" in html
    assert "Issue Units" in html
    assert "Why This Failed" in html
    assert "status-pill fail" in html
    assert "status-pill pass" in html
    assert "zip_code" in html
    assert "Allowed values:" in html
    assert "oops" in html


def test_generate_normalization_dashboard_renders_transform_story():
    report = {
        "row_change_summary": pd.DataFrame(
            [
                {
                    "rows_total": 10,
                    "rows_changed": 4,
                    "rows_unchanged": 6,
                    "rows_changed_percent": 40.0,
                }
            ]
        ),
        "column_changes_summary": pd.DataFrame(
            [
                {"column": "sex", "change_count": 4},
                {"column": "bill_length_mm", "change_count": 1},
            ]
        ),
        "changed_rows_preview": pd.DataFrame(
            [{"index": 1, "sex__self": "male", "sex__other": "MALE"}]
        ),
        "diff_table": pd.DataFrame(
            [{"index": 1, "column": "sex", "original": "male", "transformed": "MALE"}]
        ),
        "changelog": {
            "renamed_columns": pd.DataFrame(
                [{"Original Name": "bill length (mm)", "New Name": "bill_length_mm"}]
            ),
            "strings_cleaned": pd.DataFrame([{"Column": "sex", "Operation": "standardize_text"}]),
            "values_mapped": pd.DataFrame([{"Column": "sex", "Mappings Applied": 6}]),
        },
        "meta_info": pd.DataFrame(
            [
                {
                    "module": "normalization",
                    "run_id": "run-norm-001",
                    "timestamp": "2026-03-12T00:00:00+00:00",
                    "original_shape": "(10, 4)",
                    "transformed_shape": "(10, 4)",
                }
            ]
        ),
    }

    html = generate_html_report(report, "Normalization", "run-norm-001")

    assert "M03 Data Normalization" in html
    assert "Normalization Overview" in html
    assert "Rows Changed" in html
    assert "Transformation Log" in html
    assert "Columns Renamed" in html
    assert "Values Mapped" in html
    assert "Column Change Impact" in html
    assert "Value-Level Differences" in html
    assert "male" in html
    assert "MALE" in html


def test_generate_duplicates_dashboard_renders_mode_criteria_and_plots(tmp_path):
    plot_path = tmp_path / "duplicates.png"
    plot_path.write_bytes(_ONE_PIXEL_PNG)

    report = {
        "summary": pd.DataFrame(
            [
                {"Metric": "Original Row Count", "Value": 10},
                {"Metric": "Deduplicated Row Count", "Value": 8},
                {"Metric": "Rows Removed", "Value": 2},
            ]
        ),
        "dropped_rows": pd.DataFrame(
            [{"customer_id": "A-1", "email": "a@example.com", "city": "Austin"}]
        ),
        "all_duplicate_instances": pd.DataFrame(
            [
                {"customer_id": "A-1", "email": "a@example.com", "city": "Austin"},
                {"customer_id": "A-1", "email": "a@example.com", "city": "Austin"},
            ]
        ),
        "__dashboard_meta__": {"subset_columns": ["customer_id", "email"], "mode": "remove"},
    }

    html = generate_html_report(
        report,
        "Duplicates",
        "run-dup-001",
        plot_paths={"Duplication Summary": [str(plot_path)]},
    )

    assert "M04 Deduplication" in html
    assert "Deduplication Overview" in html
    assert "Rows Removed:</strong> 2" in html
    assert "Criteria:</strong> customer_id, email" in html
    assert "Largest Cluster" in html
    assert "Duplicate Keys &amp; Evidence" in html
    assert "Dropped Duplicate Rows" in html
    assert "data:image/png;base64," in html


def test_generate_outlier_dashboard_renders_summary_and_plots(tmp_path):
    plot_path = tmp_path / "outliers.png"
    plot_path.write_bytes(_ONE_PIXEL_PNG)

    report = {
        "outlier_detection_log": pd.DataFrame(
            [
                {
                    "column": "body_mass_g",
                    "method": "iqr",
                    "outlier_count": 6,
                    "lower_bound": 1200.0,
                    "upper_bound": 6000.0,
                    "outlier_examples": "[6100, 6200]",
                },
                {
                    "column": "flipper_length_mm",
                    "method": "zscore",
                    "outlier_count": 2,
                    "lower_bound": 150.0,
                    "upper_bound": 250.0,
                    "outlier_examples": "[251, 252]",
                },
            ]
        ),
        "outlier_rows_details": pd.DataFrame([{"body_mass_g": 6100.0, "flipper_length_mm": 251.0}]),
    }

    html = generate_html_report(
        report,
        "Outlier Detection",
        "run-outlier-001",
        plot_paths={"Body Mass G": [str(plot_path)]},
    )

    assert "M05 Outlier Detection" in html
    assert "Detection Overview" in html


def test_generate_auto_heal_dashboard_renders_drilldowns_and_sanitized_errors():
    report = {
        "status": "warn",
        "message": "Auto-healing completed with warnings.",
        "row_count": 42,
        "final_session_id": "sess_auto",
        "final_export_url": "gs://bucket/healed.csv",
        "final_dashboard_url": "https://example.com/imputation.html",
        "final_dashboard_path": "exports/reports/imputation/run_imp.html",
        "inferred_modules": ["normalization", "imputation"],
        "failed_steps": ["imputation"],
        "steps": {
            "normalization": {
                "status": "pass",
                "summary": {"changes_made": 4, "columns_changed": ["city", "state"]},
                "artifact_path": "exports/reports/normalization/run_norm.html",
                "artifact_url": "https://example.com/norm.html",
                "export_url": "gs://bucket/norm.csv",
            },
            "imputation": {
                "status": "error",
                "summary": {"error_code": "AUTO_HEAL_STEP_FAILED", "trace_id": "trace123"},
                "artifact_path": "",
                "artifact_url": "",
                "export_url": "",
            },
        },
    }

    html = generate_html_report(report, "Auto Heal", "run-auto-001")

    assert "Auto Heal Overview" not in html
    assert "Outcome Summary" in html
    assert "Ready For Final Audit" not in html
    assert "Needs Operator Review" in html
    assert "Step Drilldowns" in html
    assert "Terminal References" in html
    assert "AUTO_HEAL_STEP_FAILED" in html
    assert "trace123" in html


def test_generate_imputation_dashboard_renders_summary_shift_and_plots(tmp_path):
    plot_path = tmp_path / "imputation.png"
    plot_path.write_bytes(_ONE_PIXEL_PNG)

    report = {
        "imputation_actions_log": pd.DataFrame(
            [
                {"Column": "sex", "Strategy": "mode", "Fill Value": "Male", "Nulls Filled": 12},
                {
                    "Column": "body_mass_g",
                    "Strategy": "median",
                    "Fill Value": "3742.00",
                    "Nulls Filled": 4,
                },
            ]
        ),
        "null_value_audit": pd.DataFrame(
            [
                {"Column": "sex", "Nulls Before": 12, "Nulls After": 0, "Nulls Filled": 12},
                {
                    "Column": "body_mass_g",
                    "Nulls Before": 4,
                    "Nulls After": 0,
                    "Nulls Filled": 4,
                },
            ]
        ),
        "categorical_shift": {
            "sex": pd.DataFrame(
                [
                    {"Value": "Male", "Original Count": 10, "Imputed Count": 22, "Change": 12},
                    {"Value": "Female", "Original Count": 8, "Imputed Count": 8, "Change": 0},
                ]
            )
        },
        "remaining_nulls": pd.DataFrame([{"Column": "tag_id", "Remaining Nulls": 3}]),
    }

    html = generate_html_report(
        report,
        "Imputation",
        "run-imp-001",
        plot_paths={"Imputation Comparison": [str(plot_path)]},
    )

    assert "M07 Data Imputation" in html
    assert "Imputation Overview" in html
    assert "Top Fill Target" in html
    assert "sex (12)" in html
    assert "Categorical Shift Analysis" in html
    assert "Remaining Null Risk" in html
    assert "data:image/png;base64," in html


def test_generate_outlier_handling_dashboard_renders_overview_and_evidence():
    report = {
        "handling_summary_log": pd.DataFrame(
            [
                {
                    "strategy": "clip",
                    "column": "body_mass_g",
                    "outliers_handled": 6,
                    "details": "Clipped 6 values to bounds.",
                },
                {
                    "strategy": "global_drop",
                    "column": "ALL",
                    "outliers_handled": 2,
                    "details": "Removed 2 rows with any outlier.",
                },
            ]
        ),
        "capped_values_log": pd.DataFrame(
            [
                {
                    "Column": "body_mass_g",
                    "Row_Index": 11,
                    "Original_Value": 6100.0,
                    "Capped_Value": 6000.0,
                }
            ]
        ),
        "removed_outlier_rows": pd.DataFrame([{"species": "Adelie", "body_mass_g": 6200.0}]),
    }

    html = generate_html_report(report, "Outlier Handling", "run-handle-001")

    assert "M06 Outlier Handling" in html
    assert "Handling Overview" in html
    assert "Values Handled" in html
    assert "Primary Action" in html
    assert "body_mass_g" in html
    assert "Handling Ledger" in html
    assert "Capped Value Evidence" in html
    assert "Removed Outlier Rows" in html


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
    assert "Healing Certificate Issued" not in html
    assert "Certification Failed" in html
    assert "Certificate Summary" in html
    assert "Failure Ledger" in html
    assert "customer_id" in html
    assert "Final Data Profile" in html


def test_generate_generic_dashboard_falls_back_for_other_modules():
    report = {"summary_table": pd.DataFrame([{"Column": "value"}])}

    html = generate_html_report(report, "Certification", "run-000")

    assert "Certification Dashboard" in html
    assert "Summary Table" in html
    assert "value" in html
