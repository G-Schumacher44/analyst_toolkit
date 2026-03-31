"""
config_models.py — Pydantic models for module configurations.
Used to generate JSON Schemas for the MCP server.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class DiagnosticsConfig(BaseModel):
    null_threshold: float = Field(0.1, description="Threshold for null rate to trigger a warning.")
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class ValidationRule(BaseModel):
    passed: Optional[bool] = None
    rule_description: Optional[str] = None


class ValidationConfig(BaseModel):
    rules: Dict[str, Any] = Field(
        default_factory=dict, description="Validation rules (schema, range, etc.)"
    )
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class NormalizationRules(BaseModel):
    rename_columns: Dict[str, str] = Field(
        default_factory=dict, description="Mapping of old names to new names."
    )
    standardize_text_columns: List[str] = Field(
        default_factory=list, description="List of columns to trim and lowercase."
    )
    value_mappings: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Explicit value replacements per column."
    )
    fuzzy_matching: Dict[str, Any] = Field(
        default_factory=dict, description="Fuzzy matching settings."
    )
    parse_datetimes: Dict[str, Any] = Field(
        default_factory=dict, description="Datetime parsing rules."
    )
    preview_columns: List[str] = Field(
        default_factory=list,
        description="Optional columns to highlight in normalization report previews.",
    )
    coerce_dtypes: Dict[str, str] = Field(
        default_factory=dict, description="Final type coercion mapping."
    )


class NormalizationConfig(BaseModel):
    rules: NormalizationRules = Field(default_factory=NormalizationRules)
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class ImputationConfig(BaseModel):
    rules: Dict[str, Any] = Field(
        default_factory=dict, description="Imputation rules per column or strategy."
    )
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class OutlierDetectionConfig(BaseModel):
    run: bool = Field(True, description="Master outlier_detection toggle.")
    detection_specs: Dict[str, Dict[str, Any]] = Field(
        default_factory=lambda: {"__default__": {"method": "iqr", "iqr_multiplier": 1.5}},
        description=(
            "Per-column outlier detection specs. Use '__default__' for fallback and "
            "column keys for overrides."
        ),
    )
    exclude_columns: List[str] = Field(
        default_factory=list, description="Columns excluded from outlier analysis."
    )
    append_flags: bool = Field(
        True, description="Whether to append outlier boolean flag columns to output."
    )
    plotting: Dict[str, Any] = Field(
        default_factory=lambda: {"run": True},
        description="Plotting controls (for example: run, plot_save_dir, plot_types).",
    )
    export: Dict[str, Any] = Field(
        default_factory=lambda: {"run": True, "export_html": True},
        description="Export controls for outlier reports.",
    )
    checkpoint: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional checkpoint controls.",
    )


class OutliersConfig(BaseModel):
    """
    Canonical MCP/runtime shape for outlier detection config.
    Matches M05: outlier_detection.detection_specs with per-column and __default__ overrides.
    """

    outlier_detection: OutlierDetectionConfig = Field(
        default_factory=lambda: OutlierDetectionConfig(
            run=True,
            detection_specs={"__default__": {"method": "iqr", "iqr_multiplier": 1.5}},
            exclude_columns=[],
            append_flags=True,
            plotting={"run": True},
            export={"run": True, "export_html": True},
            checkpoint={},
        ),
        description=(
            "Outlier module config block. Primary knobs live under "
            "outlier_detection.detection_specs.<column>.*"
        ),
    )
    # Backward-compatible shorthand accepted by runtime wrappers.
    detection_specs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Shorthand accepted by MCP wrapper. Prefer "
            "outlier_detection.detection_specs for canonical shape."
        ),
    )
    exclude_columns: List[str] = Field(default_factory=list)
    append_flags: Optional[bool] = None


class FinalAuditSchemaValidationConfig(BaseModel):
    run: bool = Field(True, description="Enable schema validation checks.")
    fail_on_error: bool = Field(
        True, description="Fail certification when validation violations exist."
    )
    rules: Dict[str, Any] = Field(
        default_factory=dict,
        description="Validation rules applied during certification.",
    )


class FinalAuditCertificationConfig(BaseModel):
    run: bool = Field(True, description="Enable certification block.")
    schema_validation: FinalAuditSchemaValidationConfig = Field(
        default_factory=lambda: FinalAuditSchemaValidationConfig(
            run=True,
            fail_on_error=True,
            rules={},
        )
    )


class FinalAuditConfig(BaseModel):
    """
    MCP/runtime shape for final certification config.
    """

    run: bool = Field(True, description="Master final_audit toggle.")
    input_df_path: Optional[str] = Field(
        None,
        description="Optional cleaned input path used by final_audit pipeline wrappers.",
    )
    raw_data_path: Optional[str] = Field(
        None,
        description="Optional path to raw source data for before/after reporting.",
    )
    final_edits: Dict[str, Any] = Field(
        default_factory=dict,
        description="Final edit controls applied before certification.",
    )
    certification: FinalAuditCertificationConfig = Field(
        default_factory=lambda: FinalAuditCertificationConfig(
            run=True,
            schema_validation=FinalAuditSchemaValidationConfig(
                run=True,
                fail_on_error=True,
                rules={},
            ),
        ),
        description=(
            "Certification block. Canonical rules path: certification.schema_validation.rules.*"
        ),
    )
    settings: Dict[str, Any] = Field(
        default_factory=dict,
        description="Export/report settings for final audit artifacts.",
    )
    # Backward-compatible shorthand accepted by _normalize_final_audit_config.
    rules: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Shorthand accepted by MCP wrapper. Prefer "
            "certification.schema_validation.rules for canonical shape."
        ),
    )
    disallowed_null_columns: List[str] = Field(
        default_factory=list,
        description=(
            "Shorthand for certification.schema_validation.rules.disallowed_null_columns."
        ),
    )
    fail_on_error: Optional[bool] = Field(
        None,
        description="Shorthand for certification.schema_validation.fail_on_error.",
    )


class DuplicatesConfig(BaseModel):
    subset_columns: Optional[List[str]] = Field(
        None, description="Columns to consider for duplicate detection."
    )
    mode: str = Field("flag", description="Action: 'flag' or 'remove'. Alias: 'drop'.")
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class RuntimeRunConfig(BaseModel):
    run_id: Optional[str] = Field(None, description="Optional run identifier override.")
    session_id: Optional[str] = Field(
        None, description="Optional existing session_id for runtime-scoped execution."
    )
    input_id: Optional[str] = Field(
        None, description="Optional canonical input reference returned by the ingest subsystem."
    )
    input_path: Optional[str] = Field(None, description="Optional runtime input path override.")


class RuntimeArtifactsConfig(BaseModel):
    export_html: Optional[bool] = Field(None, description="Override HTML artifact export.")
    export_xlsx: Optional[bool] = Field(None, description="Override XLSX artifact export.")
    export_data: Optional[bool] = Field(None, description="Override cleaned data export.")
    plotting: Optional[bool] = Field(None, description="Override plotting for the run.")
    artifact_mode: Optional[Literal["single_html", "html_bundle", "zip_bundle"]] = Field(
        None,
        description="Artifact packaging mode.",
    )
    collision_policy: Optional[Literal["overwrite", "version"]] = Field(
        None,
        description="Artifact collision policy.",
    )


class RuntimeLocalDestinationConfig(BaseModel):
    enabled: Optional[bool] = Field(None, description="Enable local artifact output.")
    root: Optional[str] = Field(
        None,
        description=(
            "Local root for exported artifacts. This path is validated again at routing time "
            "and must stay within the configured local output base."
        ),
    )


class RuntimeGCSDestinationConfig(BaseModel):
    enabled: Optional[bool] = Field(None, description="Enable GCS artifact output.")
    bucket_uri: Optional[str] = Field(None, description="Destination bucket URI.")
    prefix: Optional[str] = Field(None, description="Destination prefix inside the bucket.")


class RuntimeDriveDestinationConfig(BaseModel):
    enabled: Optional[bool] = Field(None, description="Enable Google Drive artifact output.")
    folder_id: Optional[str] = Field(None, description="Drive folder ID for uploaded artifacts.")


class RuntimeDestinationsConfig(BaseModel):
    local: RuntimeLocalDestinationConfig = Field(
        default_factory=lambda: RuntimeLocalDestinationConfig.model_construct()
    )
    gcs: RuntimeGCSDestinationConfig = Field(
        default_factory=lambda: RuntimeGCSDestinationConfig.model_construct()
    )
    drive: RuntimeDriveDestinationConfig = Field(
        default_factory=lambda: RuntimeDriveDestinationConfig.model_construct()
    )


class RuntimePathsConfig(BaseModel):
    report_root: Optional[str] = Field(None, description="Root path for HTML/XLSX reports.")
    plot_root: Optional[str] = Field(None, description="Root path for plots.")
    checkpoint_root: Optional[str] = Field(None, description="Root path for checkpoints.")
    data_root: Optional[str] = Field(None, description="Root path for exported data.")


class RuntimeExecutionConfig(BaseModel):
    allow_plot_generation: Optional[bool] = Field(
        None, description="Allow plot generation during the run."
    )
    upload_artifacts: Optional[bool] = Field(
        None, description="Whether artifacts should be uploaded to remote destinations."
    )
    persist_history: Optional[bool] = Field(
        None, description="Whether run history should be persisted."
    )
    strict_config: Optional[bool] = Field(
        None, description="Fail on unknown runtime config keys when true."
    )


def _default_runtime_run() -> RuntimeRunConfig:
    return RuntimeRunConfig.model_construct()


def _default_runtime_artifacts() -> RuntimeArtifactsConfig:
    return RuntimeArtifactsConfig.model_construct()


def _default_runtime_paths() -> RuntimePathsConfig:
    return RuntimePathsConfig.model_construct()


def _default_runtime_execution() -> RuntimeExecutionConfig:
    return RuntimeExecutionConfig.model_construct()


def _default_runtime_destinations() -> RuntimeDestinationsConfig:
    return RuntimeDestinationsConfig.model_construct(
        local=RuntimeLocalDestinationConfig.model_construct(),
        gcs=RuntimeGCSDestinationConfig.model_construct(),
        drive=RuntimeDriveDestinationConfig.model_construct(),
    )


class RuntimeOverlayConfig(BaseModel):
    run: RuntimeRunConfig = Field(default_factory=_default_runtime_run)
    artifacts: RuntimeArtifactsConfig = Field(default_factory=_default_runtime_artifacts)
    destinations: RuntimeDestinationsConfig = Field(default_factory=_default_runtime_destinations)
    paths: RuntimePathsConfig = Field(default_factory=_default_runtime_paths)
    execution: RuntimeExecutionConfig = Field(default_factory=_default_runtime_execution)


CONFIG_MODELS = {
    "diagnostics": DiagnosticsConfig,
    "validation": ValidationConfig,
    "normalization": NormalizationConfig,
    "imputation": ImputationConfig,
    "outliers": OutliersConfig,
    "duplicates": DuplicatesConfig,
    "final_audit": FinalAuditConfig,
}
