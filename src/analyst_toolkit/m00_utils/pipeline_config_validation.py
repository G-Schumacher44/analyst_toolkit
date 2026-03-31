"""Shared pipeline/notebook/CLI config validation helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from analyst_toolkit.mcp_server.config_models import CONFIG_MODELS
from analyst_toolkit.mcp_server.config_normalizers import normalize_module_config
from analyst_toolkit.mcp_server.io import coerce_config


class PipelineConfigValidationError(ValueError):
    """Raised when runner-facing pipeline config validation fails."""


class PipelineModuleSelection(BaseModel):
    """Master runner module toggle + config path."""

    model_config = ConfigDict(extra="allow")

    run: bool = False
    config_path: str


class PipelineRunnerConfig(BaseModel):
    """Validated master runner config for the CLI/notebook pipeline entrypoint."""

    model_config = ConfigDict(extra="allow")

    run_id: str
    notebook: bool = False
    pipeline_entry_path: str
    modules: dict[str, PipelineModuleSelection] = Field(default_factory=dict)


class OutlierHandlingExportConfig(BaseModel):
    run: bool = False
    export_path: str | None = None
    as_csv: bool = False
    export_html: bool = False
    export_html_path: str | None = None


class OutlierHandlingCheckpointConfig(BaseModel):
    run: bool = False
    checkpoint_path: str | None = None


class OutlierHandlingSettingsConfig(BaseModel):
    show_inline: bool = True
    export: OutlierHandlingExportConfig = Field(default_factory=OutlierHandlingExportConfig)
    checkpoint: OutlierHandlingCheckpointConfig = Field(
        default_factory=OutlierHandlingCheckpointConfig
    )


class OutlierHandlingConfig(BaseModel):
    run: bool = False
    input_df_path: str | None = None
    detection_results_path: str | None = None
    handling_specs: dict[str, Any] = Field(default_factory=dict)
    settings: OutlierHandlingSettingsConfig = Field(default_factory=OutlierHandlingSettingsConfig)


class RunnerValidationSchemaConfig(BaseModel):
    run: bool = True
    fail_on_error: bool = False
    rules: dict[str, Any] = Field(default_factory=dict)


class RunnerValidationConfig(BaseModel):
    schema_validation: RunnerValidationSchemaConfig = Field(
        default_factory=RunnerValidationSchemaConfig
    )


_RUNNER_MODULE_SPECS: dict[str, dict[str, str]] = {
    "diagnostics": {"module_name": "diagnostics", "root_key": "diagnostics"},
    "validation": {"module_name": "validation", "root_key": "validation"},
    "validation_gatekeeper": {"module_name": "validation", "root_key": "validation"},
    "normalization": {"module_name": "normalization", "root_key": "normalization"},
    "duplicates": {"module_name": "duplicates", "root_key": "duplicates"},
    # Historical naming divergence: the public runner key is "outlier_detection"
    # while the shared MCP/config model name is "outliers".
    "outlier_detection": {
        "module_name": "outliers",
        "root_key": "outlier_detection",
        "coerce_key": "outlier_detection",
    },
    "outlier_handling": {
        "module_name": "outlier_handling",
        "root_key": "outlier_handling",
    },
    "imputation": {"module_name": "imputation", "root_key": "imputation"},
    "final_audit": {"module_name": "final_audit", "root_key": "final_audit"},
}

_RUNNER_ONLY_MODELS: dict[str, type[BaseModel]] = {
    "validation": RunnerValidationConfig,
    "outlier_handling": OutlierHandlingConfig,
}


def validate_pipeline_runner_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize the master pipeline runner config shape."""

    try:
        return PipelineRunnerConfig.model_validate(config).model_dump()
    except ValidationError as exc:
        raise PipelineConfigValidationError(f"Invalid master pipeline config: {exc}") from exc


def validate_runner_module_config(
    runner_module_name: str, config: dict[str, Any]
) -> dict[str, Any]:
    """Validate a runner-facing module config against the shared MCP models."""

    spec = _RUNNER_MODULE_SPECS.get(runner_module_name)
    if spec is None:
        raise PipelineConfigValidationError(
            f"Unsupported runner module for shared validation: {runner_module_name}"
        )

    module_name = spec["module_name"]
    root_key = spec["root_key"]
    if not isinstance(config, dict):
        raise PipelineConfigValidationError(
            f"Invalid config for runner module '{runner_module_name}': expected a mapping."
        )
    # "outlier_detection" keeps its historical runner key for coercion even though
    # the shared model/normalizer is registered under "outliers".
    coerce_key = spec.get("coerce_key", module_name)
    coerced = coerce_config(config, coerce_key)
    normalized = normalize_module_config(module_name, coerced)
    model = _RUNNER_ONLY_MODELS.get(module_name) or CONFIG_MODELS.get(module_name)
    if model is None:
        raise PipelineConfigValidationError(
            f"No shared config model is registered for runner module '{runner_module_name}' "
            f"(resolved module '{module_name}')."
        )

    try:
        validated = model.model_validate(normalized)
    except ValidationError as exc:
        raise PipelineConfigValidationError(
            f"Invalid config for runner module '{runner_module_name}': {exc}"
        ) from exc

    validated_dump = validated.model_dump()
    if root_key in validated_dump and isinstance(validated_dump[root_key], dict):
        effective = validated_dump[root_key]
    else:
        effective = validated_dump
    canonical = deepcopy(effective)

    return {
        "runner_module": runner_module_name,
        "module_name": module_name,
        "root_key": root_key,
        "effective_config": effective,
        "canonical_config": {root_key: canonical},
    }
