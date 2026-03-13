import importlib

import pandas as pd

imputation_pipeline_module = importlib.import_module(
    "analyst_toolkit.m07_imputation.run_imputation_pipeline"
)

from analyst_toolkit.m05_detect_outliers.run_detection_pipeline import (
    run_outlier_detection_pipeline,
)
from analyst_toolkit.m07_imputation.run_imputation_pipeline import run_imputation_pipeline


def test_outlier_detection_exports_html_with_plot_paths(mocker):
    df = pd.DataFrame({"value": [1.0, 2.0, 100.0]})
    outlier_results = {
        "outlier_log": pd.DataFrame([{"column": "value", "outlier_count": 1}]),
        "outlier_rows": pd.DataFrame([{"value": 100.0}]),
    }
    mocker.patch(
        "analyst_toolkit.m05_detect_outliers.run_detection_pipeline.detect_outliers",
        return_value=outlier_results,
    )
    mocker.patch(
        "analyst_toolkit.m05_detect_outliers.run_detection_pipeline.generate_outlier_report",
        return_value={"outlier_detection_log": outlier_results["outlier_log"]},
    )
    mocker.patch(
        "analyst_toolkit.m05_detect_outliers.run_detection_pipeline.export_dataframes",
    )
    generate_plots = mocker.patch(
        "analyst_toolkit.m05_detect_outliers.run_detection_pipeline.generate_outlier_plots",
        return_value={"Value Plots": ["exports/plots/outliers/test_plot.png"]},
    )
    export_html = mocker.patch(
        "analyst_toolkit.m05_detect_outliers.run_detection_pipeline.export_html_report"
    )

    run_outlier_detection_pipeline(
        config={
            "outlier_detection": {
                "logging": "off",
                "plotting": {"run": True, "plot_save_dir": "exports/plots/outliers/{run_id}"},
                "export": {
                    "run": True,
                    "export_html": True,
                    "export_html_path": "exports/reports/outliers/detection/{run_id}_outlier_report.html",
                },
            }
        },
        df=df,
        notebook=False,
        run_id="run-outlier-001",
    )

    generate_plots.assert_called_once()
    assert export_html.call_args.kwargs["plot_paths"] == {
        "Value Plots": ["exports/plots/outliers/test_plot.png"]
    }


def test_imputation_exports_html_with_plot_paths(mocker):
    df = pd.DataFrame({"value": [1.0, None, 3.0]})
    df_imputed = pd.DataFrame({"value": [1.0, 2.0, 3.0]})
    changelog = pd.DataFrame([{"Column": "value", "Strategy": "mean"}])

    mocker.patch.object(
        imputation_pipeline_module,
        "apply_imputation",
        return_value=(df_imputed, changelog),
    )
    mocker.patch.object(
        imputation_pipeline_module,
        "generate_imputation_report",
        return_value={"imputation_actions_log": changelog},
    )
    mocker.patch.object(imputation_pipeline_module, "export_dataframes")
    mocker.patch.object(
        imputation_pipeline_module,
        "plot_imputation_comparison",
        return_value="exports/plots/imputation/run_imp_plot.png",
    )
    export_html = mocker.patch.object(imputation_pipeline_module, "export_html_report")

    run_imputation_pipeline(
        config={
            "imputation": {
                "logging": "off",
                "rules": {"strategies": {"value": {"method": "mean"}}},
                "settings": {
                    "plotting": {"run": True, "save_dir": "exports/plots/imputation/"},
                    "export": {
                        "run": True,
                        "export_html": True,
                        "export_html_path": "exports/reports/imputation/{run_id}_imputation_report.html",
                    },
                },
            }
        },
        df=df,
        notebook=False,
        run_id="run-imp-001",
    )

    assert export_html.call_args.kwargs["plot_paths"] == {
        "Imputation Comparison": ["exports/plots/imputation/run_imp_plot.png"]
    }
