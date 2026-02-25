"""
config_models.py — Pydantic models for module configurations.
Used to generate JSON Schemas for the MCP server.
"""

from typing import Any, Dict, List, Optional

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
            "Certification block. Canonical rules path: "
            "certification.schema_validation.rules.*"
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


CONFIG_MODELS = {
    "diagnostics": DiagnosticsConfig,
    "validation": ValidationConfig,
    "normalization": NormalizationConfig,
    "imputation": ImputationConfig,
    "outliers": OutliersConfig,
    "duplicates": DuplicatesConfig,
    "final_audit": FinalAuditConfig,
}
