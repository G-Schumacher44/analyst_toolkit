"""
templates.py — Template discovery for tool calls and MCP resources.
"""

from pathlib import Path
from urllib.parse import unquote, urlparse

import yaml

RESOURCE_SCHEME = "analyst"
RESOURCE_HOST = "templates"

CONFIG_DIR = Path("config")
GOLDEN_TEMPLATE_DIR = CONFIG_DIR / "golden_templates"


def _iter_golden_template_files() -> list[Path]:
    if not GOLDEN_TEMPLATE_DIR.exists():
        return []
    return sorted(p for p in GOLDEN_TEMPLATE_DIR.glob("*.yaml") if p.is_file())


def _iter_standard_template_files() -> list[Path]:
    if not CONFIG_DIR.exists():
        return []
    return sorted(p for p in CONFIG_DIR.glob("*_template.yaml") if p.is_file())


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

    for path in _iter_standard_template_files():
        resources.append(
            {
                "name": f"config::{path.stem}",
                "uri": f"{RESOURCE_SCHEME}://{RESOURCE_HOST}/config/{path.name}",
                "description": f"Config template: {path.stem}",
                "mimeType": "application/x-yaml",
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
        base = GOLDEN_TEMPLATE_DIR
    elif group == "config":
        if not filename.endswith("_template.yaml"):
            raise FileNotFoundError(
                f"Only *_template.yaml files are allowed for config resources: {uri}"
            )
        base = CONFIG_DIR
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
