import pandas as pd
import pytest

import analyst_toolkit.run_toolkit_pipeline as pipeline_module
from analyst_toolkit.m00_utils.pipeline_config_validation import PipelineConfigValidationError


@pytest.fixture
def mock_load_config(mocker):
    def _configure(config_map):
        mocker.patch.object(
            pipeline_module,
            "load_config",
            side_effect=lambda path: config_map[path],
        )

    return _configure


def test_run_full_pipeline_rejects_invalid_master_config(mocker):
    mocker.patch.object(
        pipeline_module,
        "load_config",
        return_value={
            "run_id": "broken",
            "notebook": False,
            "modules": {},
        },
    )
    load_csv = mocker.patch.object(pipeline_module, "load_csv")

    with pytest.raises(PipelineConfigValidationError, match="pipeline_entry_path"):
        pipeline_module.run_full_pipeline("config/run_toolkit_config.yaml")

    load_csv.assert_not_called()


def test_run_full_pipeline_rejects_invalid_module_config_before_runner(mocker, mock_load_config):
    config_map = {
        "config/run_toolkit_config.yaml": {
            "run_id": "cli_run",
            "notebook": False,
            "pipeline_entry_path": "data/raw/example.csv",
            "modules": {
                "validation": {
                    "run": True,
                    "config_path": "config/validation_config_template.yaml",
                }
            },
        },
        "config/validation_config_template.yaml": [],
    }

    mock_load_config(config_map)
    mocker.patch.object(
        pipeline_module,
        "load_csv",
        return_value=pd.DataFrame({"col": [1, 2]}),
    )
    run_validation = mocker.patch.object(pipeline_module, "run_validation_pipeline")

    with pytest.raises(
        PipelineConfigValidationError,
        match="Invalid config for runner module 'validation': expected a mapping",
    ):
        pipeline_module.run_full_pipeline("config/run_toolkit_config.yaml")

    run_validation.assert_not_called()


def test_run_full_pipeline_passes_validated_canonical_config_to_runner(mocker, mock_load_config):
    config_map = {
        "config/run_toolkit_config.yaml": {
            "run_id": "cli_run",
            "notebook": False,
            "pipeline_entry_path": "data/raw/example.csv",
            "modules": {
                "validation": {
                    "run": True,
                    "config_path": "config/validation_config_template.yaml",
                }
            },
        },
        "config/validation_config_template.yaml": {
            "validation": {
                "schema_validation": {
                    "run": True,
                    "fail_on_error": True,
                    "rules": {"expected_columns": ["col"]},
                }
            }
        },
    }

    mock_load_config(config_map)
    mocker.patch.object(
        pipeline_module,
        "load_csv",
        return_value=pd.DataFrame({"col": [1, 2]}),
    )
    run_validation = mocker.patch.object(
        pipeline_module,
        "run_validation_pipeline",
        side_effect=lambda config, df, notebook, run_id: df,
    )

    result = pipeline_module.run_full_pipeline("config/run_toolkit_config.yaml")

    assert list(result["col"]) == [1, 2]
    run_validation.assert_called_once()
    kwargs = run_validation.call_args.kwargs
    assert kwargs["config"] == {
        "validation": {
            "schema_validation": {
                "run": True,
                "fail_on_error": True,
                "rules": {"expected_columns": ["col"]},
            }
        }
    }
