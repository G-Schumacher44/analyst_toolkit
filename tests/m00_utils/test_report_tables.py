import pandas as pd

from analyst_toolkit.m00_utils.report_tables import generate_transformation_report


def test_generate_transformation_report_resolves_renamed_preview_columns():
    df_original = pd.DataFrame({"sex": ["male", "female"], "bill_length_mm": [39.1, 40.3]})
    df_transformed = pd.DataFrame(
        {"gender": ["MALE", "FEMALE"], "bill_length_mm": [39.1, 40.3]}
    )
    changelog = {
        "renamed_columns": pd.DataFrame([{"Original Name": "sex", "New Name": "gender"}]),
        "values_mapped": pd.DataFrame([{"Column": "gender", "Mappings Applied": 2}]),
    }

    report = generate_transformation_report(
        df_original=df_original,
        df_transformed=df_transformed,
        changelog=changelog,
        module_name="normalization",
        run_id="run-norm-report",
        export_config={},
        preview_columns=["sex"],
    )

    column_value_analysis = report.get("column_value_analysis", {})
    assert isinstance(column_value_analysis, dict)
    assert "gender" in column_value_analysis

    value_audit = column_value_analysis["gender"]["value_audit"]
    assert isinstance(value_audit, pd.DataFrame)
    assert set(value_audit.columns) == {"Value", "Original Count", "Normalized Count"}
