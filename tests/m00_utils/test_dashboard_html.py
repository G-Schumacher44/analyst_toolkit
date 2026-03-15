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


def test_generate_data_dictionary_dashboard_renders_prelaunch_contract():
    report = {
        "overview": pd.DataFrame(
            [
                {
                    "Rows": 10,
                    "Columns": 3,
                    "Expected Columns": 4,
                    "Missing Expected Columns": 1,
                    "Metadata Gaps": 2,
                    "Profile Depth": "standard",
                    "Examples Included": True,
                    "Prelaunch Report": True,
                    "Inference Seeded": True,
                }
            ]
        ),
        "expected_schema": pd.DataFrame(
            [
                {
                    "Column": "customer_id",
                    "Observed": "Yes",
                    "Expected Dtype": "",
                    "Allowed Values Preview": "",
                    "Numeric Rule": "",
                },
                {
                    "Column": "status",
                    "Observed": "Yes",
                    "Expected Dtype": "",
                    "Allowed Values Preview": "new, done",
                    "Numeric Rule": "",
                },
                {
                    "Column": "country",
                    "Observed": "No",
                    "Expected Dtype": "",
                    "Allowed Values Preview": "",
                    "Numeric Rule": "",
                },
            ]
        ),
        "column_dictionary": pd.DataFrame(
            [
                {
                    "Column": "customer_id",
                    "Observed Dtype": "int64",
                    "Expected Dtype": "",
                    "Semantic Type": "identifier",
                    "Expected In Schema": "Yes",
                    "Nullable": "No",
                    "Unique": "Yes",
                    "Distinct Count": 10,
                    "Null Count": 0,
                    "Null %": 0.0,
                    "Example Values": "1, 2",
                    "Allowed Values Preview": "",
                    "Numeric Rule": "",
                    "Transformation Notes": "",
                    "Quality Notes": "OK",
                }
            ]
        ),
        "prelaunch_readiness": pd.DataFrame(
            [
                {
                    "Severity": "fail",
                    "Type": "missing_expected_column",
                    "Column": "country",
                    "Detail": "Present in inferred validation schema but missing from the current dataset.",
                }
            ]
        ),
        "profile_snapshot": pd.DataFrame(
            [
                {
                    "Column": "customer_id",
                    "Dtype": "int64",
                    "Unique Values": 10,
                    "Audit Remarks": "OK",
                    "Missing Count": 0,
                }
            ]
        ),
        "__dashboard_meta__": {"status": "warn"},
    }

    html = generate_html_report(report, "Data Dictionary", "dict-001")

    assert "Data Dictionary" in html
    assert "Dictionary Overview" in html
    assert "Expected Schema And Contract" in html
    assert "Column Dictionary" in html
    assert "Prelaunch Readiness" in html
    assert "metadata gaps" in html.lower()
    assert "country" in html


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
    assert "Primary Hotspot" in html
    assert "Outlier Detection Log" in html
    assert "Affected Row Samples" in html
    assert "data:image/png;base64," in html
    assert "body_mass_g" in html


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


def test_generate_pipeline_dashboard_renders_tabs_and_exec_summary():
    report = {
        "final_status": "warn",
        "session_id": "sess_pipeline",
        "health_score": 84,
        "health_status": "green",
        "ready_modules": 4,
        "warned_modules": 2,
        "failed_modules": 1,
        "not_run_modules": 4,
        "module_order": [
            "Diagnostics",
            "Auto Heal",
            "Normalization",
            "Duplicates",
            "Outliers",
            "Outlier Handling",
            "Imputation",
            "Validation",
            "Final Audit",
        ],
        "modules": {
            "Diagnostics": {
                "status": "pass",
                "summary": {"rows": 100, "null_rate": 0.01},
                "dashboard_url": "https://example.com/diag.html",
                "dashboard_path": "",
                "artifact_url": "",
                "export_url": "gs://bucket/diag.csv",
                "warnings": [],
            },
            "Validation": {
                "status": "warn",
                "summary": {"passed": False, "failed_rules": 2},
                "dashboard_url": "",
                "dashboard_path": "exports/reports/validation/run_val.html",
                "artifact_url": "",
                "export_url": "",
                "warnings": ["rule mismatch"],
            },
            "Outlier Handling": {
                "status": "not_run",
                "summary": {},
                "dashboard_url": "",
                "dashboard_path": "",
                "artifact_url": "",
                "export_url": "",
                "warnings": [],
            },
            "Final Audit": {
                "status": "fail",
                "summary": {"passed": False},
                "dashboard_url": "",
                "dashboard_path": "exports/reports/final_audit/run_final.html",
                "artifact_url": "",
                "export_url": "gs://bucket/final.csv",
                "warnings": [],
            },
        },
        "final_dashboard_url": "https://example.com/final.html",
        "final_dashboard_path": "",
        "final_export_url": "gs://bucket/final.csv",
    }

    html = generate_html_report(report, "Pipeline Dashboard", "run-pipeline-001")

    assert "Pipeline Review Shell" in html
    assert "Executive Summary" in html
    assert "Diagnostics" in html
    assert "Auto Heal" in html
    assert "Outlier Handling" in html
    assert "Validation" in html
    assert "Final Audit" in html
    assert "<span class='tab-status'>PASS</span>" in html
    assert "<span class='tab-status'>NOT_RUN</span>" in html
    assert "<span class='tab-status'>WARN</span>" in html
    assert "<span class='tab-status'>FAIL</span>" in html
    assert "Not Run" in html
    assert "window.atkDashboard.openTab" in html
    assert "Final References" in html
    assert "Terminal artifacts are available for direct review." in html
    assert "Pipeline End State" in html
    assert "Open Report Directly" in html
    assert "Not Run" in html
    assert "No embeddable dashboard reference was recorded" in html
    assert "tab-embed" in html
    assert "<iframe" in html
    assert "https://example.com/diag.html" in html
    assert "src='/exports/reports/validation/run_val.html'" in html
    assert "src='/exports/reports/final_audit/run_final.html'" in html


def test_generate_pipeline_dashboard_surfaces_terminal_fallback_when_final_artifacts_missing():
    report = {
        "final_status": "warn",
        "session_id": "sess_pipeline",
        "health_score": 84,
        "health_status": "green",
        "ready_modules": 2,
        "warned_modules": 1,
        "failed_modules": 1,
        "not_run_modules": 5,
        "module_order": ["Diagnostics", "Validation", "Final Audit"],
        "modules": {
            "Diagnostics": {
                "status": "pass",
                "summary": {"rows": 100},
                "dashboard_url": "",
                "dashboard_path": "exports/reports/diagnostics/sample_diag.html",
                "artifact_url": "",
                "export_url": "gs://bucket/diag.csv",
                "warnings": [],
            },
            "Validation": {
                "status": "warn",
                "summary": {"passed": False},
                "dashboard_url": "",
                "dashboard_path": "exports/reports/validation/sample_validation.html",
                "artifact_url": "",
                "export_url": "",
                "warnings": [],
            },
            "Final Audit": {
                "status": "not_run",
                "summary": {},
                "dashboard_url": "",
                "dashboard_path": "",
                "artifact_url": "",
                "export_url": "",
                "warnings": [],
            },
        },
        "final_dashboard_url": "",
        "final_dashboard_path": "",
        "final_export_url": "",
    }

    html = generate_html_report(report, "Pipeline Dashboard", "run-pipeline-missing-final")

    assert "Awaiting Terminal Artifacts" in html
    assert "Expected Terminal Module:</strong> Final Audit (NOT_RUN)" in html
    assert "Best Available Fallback:</strong> Validation" in html
    assert "Fallback Dashboard" in html
    assert "exports/reports/validation/sample_validation.html" in html


def test_generate_cockpit_dashboard_renders_operator_hub():
    report = {
        "overview": {
            "recent_run_count": 2,
            "warning_runs": 1,
            "failed_runs": 1,
            "healthy_runs": 1,
            "pipeline_dashboards_available": 1,
            "auto_heal_dashboards_available": 1,
        },
        "operating_posture": {
            "label": "Needs Review",
            "detail": "Warn-level outcomes are still present in the current cockpit slice.",
        },
        "operator_brief": {
            "title": "Cockpit Briefing",
            "summary": "This cockpit is the control tower for the toolkit. Use it to assess recent run health, open the strongest available artifact surface, and move into the right guide or tool without guessing where to start.",
            "lanes": [
                {
                    "title": "Review",
                    "detail": "Start with recent runs and best-available surfaces to see what already exists for the current operating slice.",
                },
                {
                    "title": "Orient",
                    "detail": "Use the resource hub when you need human-readable guidance, templates, or capability references before editing config.",
                },
                {
                    "title": "Act",
                    "detail": "Use the launchpad when you are ready to move from review into execution for a specific tool or workflow.",
                },
            ],
        },
        "best_surfaces": {
            "pipeline_dashboard": {
                "run_id": "run_001",
                "reference": "exports/reports/pipeline/run_001_pipeline_dashboard.html",
            },
            "auto_heal_dashboard": {
                "run_id": "run_002",
                "reference": "exports/reports/auto_heal/run_002_auto_heal_report.html",
            },
            "final_audit_dashboard": {"run_id": "", "reference": ""},
        },
        "blockers": [
            {
                "run_id": "run_001",
                "status": "WARN",
                "latest_module": "validation",
                "warning_count": 1,
            }
        ],
        "recent_run_gaps": [
            "No recent final audit dashboard was recorded.",
        ],
        "recent_runs": [
            {
                "run_id": "run_001",
                "session_id": "sess_001",
                "status": "warn",
                "latest_module": "validation",
                "health_score": 82,
                "health_status": "green",
                "warning_count": 1,
                "module_count": 4,
                "pipeline_dashboard": "exports/reports/pipeline/run_001_pipeline_dashboard.html",
                "best_dashboard": "exports/reports/validation/run_001_validation.html",
                "best_export": "gs://bucket/run_001.csv",
            }
        ],
        "resources": [
            {
                "Title": "Quickstart",
                "Kind": "guide",
                "Reference": "tool:get_user_quickstart",
                "Detail": "Human-oriented operating guide.",
            }
        ],
        "launchpad": [
            {
                "Action": "Infer Configs",
                "Tool": "infer_configs",
                "Why": "Seed config review and prelaunch dictionary work.",
            }
        ],
        "data_dictionary": {
            "status": "warn",
            "template_path": "config/data_dictionary_request_template.yaml",
            "implementation_plan": "local_plans/DATA_DICTIONARY_IMPLEMENTATION_WAVE_2026-03-14.md",
            "latest_run_id": "dictionary_001",
            "latest_dashboard": "exports/reports/data_dictionary/dictionary_001.html",
            "latest_export": "exports/reports/data_dictionary/dictionary_001.xlsx",
            "cockpit_preview": {
                "overview": {
                    "rows": 344,
                    "columns": 12,
                    "expected_columns": 15,
                    "metadata_gaps": 4,
                },
                "expected_schema_preview": [
                    {"Column": "customer_id", "Expected Type": "int64", "Required": "True"},
                    {"Column": "status", "Expected Type": "category", "Required": "True"},
                ],
                "readiness_preview": [
                    {
                        "Type": "missing_expected_column",
                        "Column": "region",
                        "Detail": "Column is expected by inferred validation contract but not present.",
                    }
                ],
            },
        },
    }

    html = generate_html_report(report, "Cockpit Dashboard", "cockpit")

    assert "Cockpit Dashboard" in html
    assert "Cockpit Operator Hub" in html
    assert "Needs Review" in html
    assert "Overview" in html
    assert "Recent Runs" in html
    assert "Resources" in html
    assert "Launchpad" in html
    assert "Data Dictionary" in html
    assert "What This Cockpit Helps You Review" in html
    assert "Review" in html
    assert "Orient" in html
    assert "Act" in html
    assert "Recent Run Dashboards" in html
    assert "Current Alerts And Blockers" in html
    assert "Missing Dashboards Or Artifacts" in html
    assert "Missing Dashboard Or Artifact" in html
    assert "Resources For Reading, Planning, And Setup" in html
    assert "Launchpad For Moving From Review To Action" in html
    assert "Recent Dictionary Artifact" in html
    assert "Expected Schema Preview" in html
    assert "Top Readiness Items" in html
    assert "customer_id" in html
    assert "region" in html
    assert "run_001" in html
    assert "dictionary_001" in html
    assert "tool:get_user_quickstart" in html
    assert "infer_configs" in html
    assert "config/data_dictionary_request_template.yaml" in html


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
