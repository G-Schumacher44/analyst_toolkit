"""
config_loader.py

Utility for loading structured YAML configuration files used across modules
like ETL, diagnostics, and modeling.
"""
import yaml

def load_config(config_path: str):
    """
    Load a YAML configuration file and return the full config.

    Args:
        config_path (str): Path to the YAML config file.

    Returns:
        dict: The full configuration dictionary.
    """
    # Load YAML file
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config
