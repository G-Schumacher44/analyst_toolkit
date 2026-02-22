"""
templates.py — Dynamically loads Golden Configuration Templates from the config directory.
"""

import os
from pathlib import Path

import yaml


def get_golden_configs() -> dict:
    """
    Scans config/golden_templates/ for YAML files and returns them as a dictionary.
    """
    templates = {}
    # Path relative to the project root (where server usually runs)
    template_dir = Path("config/golden_templates")

    if not template_dir.exists():
        return {}

    for file in template_dir.glob("*.yaml"):
        try:
            with open(file, "r") as f:
                name = file.stem
                templates[name] = yaml.safe_load(f)
        except Exception:
            # Skip malformed templates
            continue

    return templates


# For backwards compatibility if imported as a constant
GOLDEN_CONFIGS = get_golden_configs()
