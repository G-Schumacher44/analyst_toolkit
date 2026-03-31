"""
templates.py — Template discovery for tool calls and MCP resources.
"""

from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from pathlib import Path
from urllib.parse import unquote, urlparse

import yaml

RESOURCE_SCHEME = "analyst"
RESOURCE_HOST = "templates"

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_SOURCE_CONFIG_DIR = _PROJECT_ROOT / "config"
_BUNDLED_CONFIG_ROOT = resources.files("analyst_toolkit.config_templates")
GOLDEN_TEMPLATE_FILENAMES: tuple[str, ...] = (
    "compliance_audit.yaml",
    "fraud_detection.yaml",
    "quick_migration.yaml",
)


@dataclass(frozen=True)
class TemplateSpec:
    filename: str
    description: str
    category: str
    tool: str | None = None
    config_root: str | None = None

    @property
    def path(self) -> Path:
        return _config_template_path(self.filename)

    @property
    def relative_path(self) -> str:
        """Portable display path relative to project root (e.g. 'config/foo.yaml')."""
        return f"config/{self.filename}"

    @property
    def uri(self) -> str:
        return f"{RESOURCE_SCHEME}://{RESOURCE_HOST}/config/{self.filename}"

    @property
    def name(self) -> str:
        return f"config::{Path(self.filename).stem}"


CONFIG_TEMPLATE_SPECS: tuple[TemplateSpec, ...] = (
    TemplateSpec(
        filename="diag_config_template.yaml",
        description="Module config template: diagnostics",
        category="module_config",
        tool="diagnostics",
        config_root="diagnostics",
    ),
    TemplateSpec(
        filename="validation_config_template.yaml",
        description="Module config template: validation",
        category="module_config",
        tool="validation",
        config_root="validation",
    ),
    TemplateSpec(
        filename="normalization_config_template.yaml",
        description="Module config template: normalization",
        category="module_config",
        tool="normalization",
        config_root="normalization",
    ),
    TemplateSpec(
        filename="dups_config_template.yaml",
        description="Module config template: duplicates",
        category="module_config",
        tool="duplicates",
        config_root="duplicates",
    ),
    TemplateSpec(
        filename="outlier_config_template.yaml",
        description="Module config template: outlier detection",
        category="module_config",
        tool="outliers",
        config_root="outlier_detection",
    ),
    TemplateSpec(
        filename="imputation_config_template.yaml",
        description="Module config template: imputation",
        category="module_config",
        tool="imputation",
        config_root="imputation",
    ),
    TemplateSpec(
        filename="final_audit_config_template.yaml",
        description="Module config template: final audit",
        category="module_config",
        tool="final_audit",
        config_root="final_audit",
    ),
    TemplateSpec(
        filename="runtime_overlay_template.yaml",
        description="Runtime/shared template: run-scoped overlays and destinations",
        category="runtime_template",
    ),
    TemplateSpec(
        filename="auto_heal_request_template.yaml",
        description="Workflow template: auto-heal request",
        category="workflow_template",
        tool="auto_heal",
    ),
    TemplateSpec(
        filename="data_dictionary_request_template.yaml",
        description="Workflow template: data dictionary request",
        category="workflow_template",
        tool="data_dictionary",
    ),
)


def _iter_golden_template_files() -> list[Path]:
    return [
        _golden_template_dir() / filename
        for filename in GOLDEN_TEMPLATE_FILENAMES
        if (_golden_template_dir() / filename).is_file()
    ]


def _bundled_config_dir() -> Path:
    candidate = Path(str(_BUNDLED_CONFIG_ROOT))
    if candidate.exists():
        return candidate
    return _SOURCE_CONFIG_DIR


def _golden_template_dir() -> Path:
    return _bundled_config_dir() / "golden_templates"


def _config_template_path(filename: str) -> Path:
    bundled = _bundled_config_dir() / filename
    if bundled.is_file():
        return bundled
    return _SOURCE_CONFIG_DIR / filename


@lru_cache(maxsize=1)
def list_config_template_specs() -> list[TemplateSpec]:
    return [spec for spec in CONFIG_TEMPLATE_SPECS if spec.path.is_file()]


def refresh_template_spec_cache() -> None:
    list_config_template_specs.cache_clear()


def list_module_template_specs() -> list[TemplateSpec]:
    return [spec for spec in list_config_template_specs() if spec.category == "module_config"]


def list_workflow_template_specs() -> list[TemplateSpec]:
    return [spec for spec in list_config_template_specs() if spec.category == "workflow_template"]


def list_runtime_template_specs() -> list[TemplateSpec]:
    return [spec for spec in list_config_template_specs() if spec.category == "runtime_template"]


def list_template_resources() -> list[dict[str, str]]:
    """
    Build MCP resource metadata for all YAML templates.
    """
    resources: list[dict[str, str]] = []

    for path in _iter_golden_template_files():
        resources.append(
            {
                "name": f"golden::{path.stem}",
                "uri": f"{RESOURCE_SCHEME}://{RESOURCE_HOST}/golden/{path.name}",
                "description": f"Golden template: {path.stem}",
                "mimeType": "application/x-yaml",
            }
        )

    for spec in list_config_template_specs():
        resources.append(
            {
                "name": spec.name,
                "uri": spec.uri,
                "description": spec.description,
                "mimeType": "application/x-yaml",
                "category": spec.category,
            }
        )

    return resources


def resolve_template_uri(uri: str) -> Path:
    """
    Resolve a template URI to a local path, allowing only whitelisted templates.
    """
    parsed = urlparse(uri)
    if parsed.scheme != RESOURCE_SCHEME or parsed.netloc != RESOURCE_HOST:
        raise FileNotFoundError(f"Unsupported resource URI: {uri}")

    path = unquote(parsed.path.lstrip("/"))
    parts = [p for p in path.split("/") if p]
    if len(parts) != 2:
        raise FileNotFoundError(f"Invalid template resource URI path: {uri}")

    group, filename = parts
    if filename in {"", ".", ".."} or "/" in filename or not filename.endswith(".yaml"):
        raise FileNotFoundError(f"Invalid template filename in URI: {uri}")

    if group == "golden":
        if filename not in GOLDEN_TEMPLATE_FILENAMES:
            raise FileNotFoundError(f"Golden resource is not in the exposed allowlist: {uri}")
        base = _golden_template_dir()
    elif group == "config":
        allowed = {spec.filename for spec in list_config_template_specs()}
        if filename not in allowed:
            raise FileNotFoundError(f"Config resource is not in the exposed allowlist: {uri}")
        base = _bundled_config_dir()
    else:
        raise FileNotFoundError(f"Unknown template group in URI: {uri}")

    candidate = (base / filename).resolve()
    base_resolved = base.resolve()
    if base_resolved not in candidate.parents:
        raise FileNotFoundError(f"Template path escapes template root: {uri}")
    if not candidate.exists() or not candidate.is_file():
        raise FileNotFoundError(f"Template resource not found: {uri}")

    return candidate


def read_template_resource(uri: str) -> str:
    """Read a template resource by URI and return raw YAML text."""
    path = resolve_template_uri(uri)
    return path.read_text(encoding="utf-8")


def get_golden_configs() -> dict:
    """
    Scans config/golden_templates/ for YAML files and returns them as a dictionary.
    """
    templates = {}
    for file in _iter_golden_template_files():
        try:
            with file.open("r", encoding="utf-8") as f:
                templates[file.stem] = yaml.safe_load(f)
        except Exception:
            continue
    return templates


# For backwards compatibility if imported as a constant
GOLDEN_CONFIGS = get_golden_configs()
