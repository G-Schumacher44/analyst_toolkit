"""Parity harness: validation and final_audit should evaluate equivalent rules consistently."""

import pandas as pd

from analyst_toolkit.m02_validation.validate_data import run_validation_suite
from analyst_toolkit.mcp_server.config_normalizers import (
    normalize_final_audit_config,
    normalize_validation_config,
)


def _shorthand_contract() -> dict:
    return {
        "rules": {
            "expected_columns": ["id", "score", "label"],
            "expected_types": {"id": "int64", "score": "float64", "label": "str"},
            "categorical_values": {"label": ["ok", "bad"]},
            "numeric_ranges": {"score": {"min": 0.0, "max": 1.0}},
            "disallowed_null_columns": ["id", "score", "label"],
        },
        "fail_on_error": True,
    }


def test_normalizers_align_on_shared_rule_contract():
    shorthand = _shorthand_contract()
    validation_cfg = normalize_validation_config(shorthand)
    final_audit_cfg = normalize_final_audit_config(shorthand)

    assert validation_cfg["schema_validation"]["rules"] == (
        final_audit_cfg["certification"]["schema_validation"]["rules"]
    )


def test_validation_and_final_audit_have_check_level_parity():
    shorthand = _shorthand_contract()
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "score": [0.2, 2.5],
            "label": ["ok", "unknown"],
        }
    )

    validation_cfg = normalize_validation_config(shorthand)
    final_cert_cfg = normalize_final_audit_config(shorthand)["certification"]

    validation_results = run_validation_suite(df, validation_cfg)
    final_results = run_validation_suite(df, final_cert_cfg)

    checks = ["schema_conformity", "dtype_enforcement", "categorical_values", "numeric_ranges"]
    for check in checks:
        assert validation_results[check]["passed"] == final_results[check]["passed"]
