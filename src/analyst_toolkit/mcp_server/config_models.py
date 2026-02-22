"""
config_models.py — Pydantic models for module configurations.
Used to generate JSON Schemas for the MCP server.
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class DiagnosticsConfig(BaseModel):
    null_threshold: float = Field(0.1, description="Threshold for null rate to trigger a warning.")
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class ValidationRule(BaseModel):
    passed: Optional[bool] = None
    rule_description: Optional[str] = None


class ValidationConfig(BaseModel):
    rules: Dict[str, Any] = Field(default_factory=dict, description="Validation rules (schema, range, etc.)")
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class NormalizationRules(BaseModel):
    rename_columns: Dict[str, str] = Field(default_factory=dict, description="Mapping of old names to new names.")
    standardize_text_columns: List[str] = Field(default_factory=list, description="List of columns to trim and lowercase.")
    value_mappings: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Explicit value replacements per column.")
    fuzzy_matching: Dict[str, Any] = Field(default_factory=dict, description="Fuzzy matching settings.")
    parse_datetimes: Dict[str, Any] = Field(default_factory=dict, description="Datetime parsing rules.")
    coerce_dtypes: Dict[str, str] = Field(default_factory=dict, description="Final type coercion mapping.")


class NormalizationConfig(BaseModel):
    rules: NormalizationRules = Field(default_factory=NormalizationRules)
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class ImputationConfig(BaseModel):
    rules: Dict[str, Any] = Field(default_factory=dict, description="Imputation rules per column or strategy.")
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class OutliersConfig(BaseModel):
    method: str = Field("iqr", description="Method for detection: 'iqr' or 'zscore'.")
    iqr_multiplier: float = Field(1.5, description="Multiplier for IQR-based detection.")
    zscore_threshold: float = Field(3.0, description="Threshold for Z-score-based detection.")
    export_html: bool = Field(True, description="Whether to export an HTML report.")


class DuplicatesConfig(BaseModel):
    subset_columns: Optional[List[str]] = Field(None, description="Columns to consider for duplicate detection.")
    mode: str = Field("flag", description="Action: 'flag' or 'drop'.")
    export_html: bool = Field(True, description="Whether to export an HTML report.")


CONFIG_MODELS = {
    "diagnostics": DiagnosticsConfig,
    "validation": ValidationConfig,
    "normalization": NormalizationConfig,
    "imputation": ImputationConfig,
    "outliers": OutliersConfig,
    "duplicates": DuplicatesConfig,
}
