import pandas as pd

from analyst_toolkit.m00_utils.export_utils import export_dataframes


def test_export_dataframes_keeps_explicit_run_id_excel_path(tmp_path):
    export_path = tmp_path / "run_001_validation_report.xlsx"

    export_dataframes(
        {"summary": pd.DataFrame([{"value": 1}])},
        str(export_path),
        run_id="run_001",
    )

    assert export_path.exists()
    assert not (tmp_path / "run_001_run_001_validation_report.xlsx").exists()


def test_export_dataframes_prefixes_run_id_excel_path_when_missing(tmp_path):
    export_path = tmp_path / "validation_report.xlsx"

    export_dataframes(
        {"summary": pd.DataFrame([{"value": 1}])},
        str(export_path),
        run_id="run_001",
    )

    assert (tmp_path / "run_001_validation_report.xlsx").exists()


def test_export_dataframes_keeps_explicit_run_id_csv_stem(tmp_path):
    export_path = tmp_path / "run_001_duplicates_report.csv"

    export_dataframes(
        {"summary": pd.DataFrame([{"value": 1}])},
        str(export_path),
        file_format="csv",
        run_id="run_001",
    )

    assert (tmp_path / "run_001_duplicates_report_summary.csv").exists()
    assert not (tmp_path / "run_001_run_001_duplicates_report_summary.csv").exists()
