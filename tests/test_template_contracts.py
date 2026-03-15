"""
test_template_contracts.py — Smoke/contract tests for MCP template resources.
"""

from pathlib import Path

import yaml

from analyst_toolkit.mcp_server.templates import (
    get_golden_configs,
    list_config_template_specs,
    list_template_resources,
    read_template_resource,
)


def test_template_resource_uris_cover_template_files():
    resources = list_template_resources()
    uris = {item["uri"] for item in resources}

    expected_config = {
        f"analyst://templates/config/{spec.filename}" for spec in list_config_template_specs()
    }
    expected_golden = {
        f"analyst://templates/golden/{p.name}"
        for p in sorted(Path("config/golden_templates").glob("*.yaml"))
        if p.is_file()
    }

    assert expected_config <= uris
    assert expected_golden <= uris
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
