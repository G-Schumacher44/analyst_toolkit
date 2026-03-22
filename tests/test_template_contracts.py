"""
test_template_contracts.py — Smoke/contract tests for MCP template resources.
"""

from pathlib import Path

import yaml

from analyst_toolkit.mcp_server.config_models import CONFIG_MODELS
from analyst_toolkit.mcp_server.config_normalizers import normalize_module_config
from analyst_toolkit.mcp_server.io import coerce_config
from analyst_toolkit.mcp_server.templates import (
    get_golden_configs,
    list_config_template_specs,
    list_template_resources,
    read_template_resource,
)
from analyst_toolkit.mcp_server.tools.preflight_config import (
    _shape_warnings,
    _unknown_effective_keys,
)


def test_template_resource_uris_cover_template_files():
    resources = list_template_resources()
    uris = {item["uri"] for item in resources}

    expected_config = {
        f"analyst://templates/config/{spec.filename}" for spec in list_config_template_specs()
    }
    expected_golden = {
        f"analyst://templates/golden/{p.name}"
        for p in sorted(
            (Path(__file__).resolve().parent.parent / "config" / "golden_templates").glob("*.yaml")
        )
        if p.is_file()
    }

    assert expected_config <= uris
    assert expected_golden <= uris
    # These are concrete local/internal run configs, not user-facing template resources.
    assert "analyst://templates/config/nightly_silver_qa_config.yaml" not in uris
    assert "analyst://templates/config/run_toolkit_config.yaml" not in uris


def test_all_template_resources_parse_as_yaml_mapping():
    for item in list_template_resources():
        raw_yaml = read_template_resource(item["uri"])
        parsed = yaml.safe_load(raw_yaml)
        assert isinstance(parsed, dict), item["uri"]


def test_golden_templates_have_known_top_level_sections():
    allowed_keys = {
        "description",
        "diagnostics",
        "validation",
        "normalization",
        "duplicates",
        "outliers",
        "imputation",
        "final_audit",
    }

    for name, cfg in get_golden_configs().items():
        assert isinstance(cfg, dict), name
        assert "description" in cfg, name
        unknown = set(cfg.keys()) - allowed_keys
        assert not unknown, f"{name} has unknown top-level sections: {sorted(unknown)}"
        assert any(k != "description" for k in cfg), name


def test_public_module_templates_match_current_config_contracts():
    for spec in list_config_template_specs():
        if not spec.tool or not spec.config_root or spec.tool not in CONFIG_MODELS:
            continue

        raw = yaml.safe_load(spec.path.read_text(encoding="utf-8"))
        assert isinstance(raw, dict), spec.filename
        assert isinstance(raw.get(spec.config_root), dict), spec.filename

        module_name = spec.tool
        coerce_key = "outlier_detection" if module_name == "outliers" else module_name
        coerced = coerce_config(raw, coerce_key)
        normalized = normalize_module_config(module_name, coerced)

        assert not _shape_warnings(module_name, normalized), spec.filename
        assert not _unknown_effective_keys(module_name, normalized), spec.filename
        CONFIG_MODELS[module_name].model_validate(normalized)
