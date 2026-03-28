import pytest
import yaml

from analyst_toolkit.m00_utils.pipeline_config_validation import (
    PipelineConfigValidationError,
    validate_pipeline_runner_config,
    validate_runner_module_config,
)


def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    assert isinstance(loaded, dict)
    return loaded


def test_validate_pipeline_runner_config_accepts_master_template():
    config = _load_yaml("config/run_toolkit_config.yaml")

    validated = validate_pipeline_runner_config(config)

    assert validated["run_id"] == "CLI_2_QA"
    assert validated["pipeline_entry_path"] == "data/raw/synthetic_penguins_v3.5.csv"
    assert validated["modules"]["diagnostics"]["config_path"] == "config/diag_config_template.yaml"


def test_validate_pipeline_runner_config_requires_pipeline_entry_path():
    with pytest.raises(PipelineConfigValidationError, match="pipeline_entry_path"):
        validate_pipeline_runner_config({"run_id": "x", "modules": {}})


def test_validate_runner_module_config_normalizes_validation_gatekeeper_template():
    config = _load_yaml("config/certification_config_template.yaml")

    validated = validate_runner_module_config("validation_gatekeeper", config)

    assert validated["module_name"] == "validation"
    assert validated["root_key"] == "validation"
    assert validated["effective_config"]["schema_validation"]["fail_on_error"] is True
    assert "expected_columns" in validated["effective_config"]["schema_validation"]["rules"]


def test_validate_runner_module_config_normalizes_outlier_detection_template():
    config = _load_yaml("config/outlier_config_template.yaml")

    validated = validate_runner_module_config("outlier_detection", config)

    assert validated["module_name"] == "outliers"
    assert validated["root_key"] == "outlier_detection"
    assert validated["effective_config"]["run"] is True
    assert validated["effective_config"]["detection_specs"]["__default__"]["method"] == "iqr"
    assert "outlier_detection" in validated["canonical_config"]


def test_validate_runner_module_config_rejects_unsupported_runner_module():
    with pytest.raises(PipelineConfigValidationError, match="Unsupported runner module"):
        validate_runner_module_config("outlier_handling", {})
