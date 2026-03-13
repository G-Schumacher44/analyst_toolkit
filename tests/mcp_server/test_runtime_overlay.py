import pytest

from analyst_toolkit.mcp_server.runtime_overlay import (
    RuntimeOverlayError,
    deep_merge_dicts,
    normalize_runtime_overlay,
    resolve_layered_config,
)


def test_deep_merge_dicts_preserves_siblings_and_overrides_scalars():
    base = {
        "settings": {
            "export": {"run": True, "export_html": False},
            "plotting": {"run": False},
        }
    }
    override = {"settings": {"export": {"export_html": True}}}

    merged = deep_merge_dicts(base, override)

    assert merged == {
        "settings": {
            "export": {"run": True, "export_html": True},
            "plotting": {"run": False},
        }
    }
    assert base["settings"]["export"]["export_html"] is False


def test_deep_merge_dicts_replaces_lists():
    base = {"rules": {"expected_columns": ["id", "name"]}}
    override = {"rules": {"expected_columns": ["id", "full_name"]}}

    merged = deep_merge_dicts(base, override)

    assert merged["rules"]["expected_columns"] == ["id", "full_name"]


def test_normalize_runtime_overlay_accepts_yaml_string():
    runtime_yaml = """
runtime:
  artifacts:
    export_html: true
  execution:
    strict_config: false
"""

    normalized, warnings = normalize_runtime_overlay(runtime_yaml)

    assert normalized["artifacts"]["export_html"] is True
    assert normalized["execution"]["strict_config"] is False
    assert warnings == []


def test_normalize_runtime_overlay_warns_and_ignores_unknown_keys():
    runtime = {
        "artifacts": {"export_html": True, "mystery_flag": True},
        "unexpected_top_level": {"enabled": True},
    }

    normalized, warnings = normalize_runtime_overlay(runtime)

    assert normalized["artifacts"]["export_html"] is True
    assert "mystery_flag" not in normalized["artifacts"]
    assert any("artifacts.mystery_flag" in warning for warning in warnings)
    assert any("unexpected_top_level" in warning for warning in warnings)


def test_normalize_runtime_overlay_strict_mode_rejects_unknown_keys():
    runtime = {"artifacts": {"export_html": True, "mystery_flag": True}}

    with pytest.raises(RuntimeOverlayError):
        normalize_runtime_overlay(runtime, strict=True)


def test_resolve_layered_config_applies_precedence():
    resolved, metadata = resolve_layered_config(
        base={"settings": {"export_html": False, "plotting": {"run": False}}},
        inferred={"settings": {"plotting": {"max_plots": 20}}},
        provided={"settings": {"export_html": True}},
        runtime={"artifacts": {"plotting": True}},
        explicit={"settings": {"plotting": {"run": False}}},
    )

    assert resolved["settings"]["export_html"] is True
    assert resolved["settings"]["plotting"]["run"] is False
    assert resolved["settings"]["plotting"]["max_plots"] == 20
    assert resolved["artifacts"]["plotting"] is True
    assert metadata["runtime_applied"] is True


def test_resolve_layered_config_preserves_null_override():
    resolved, _ = resolve_layered_config(
        base={"paths": {"report_root": "exports/reports"}},
        runtime={"paths": {"report_root": None}},
    )

    assert "report_root" in resolved["paths"]
    assert resolved["paths"]["report_root"] is None
