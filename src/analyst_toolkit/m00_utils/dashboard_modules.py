"""Compatibility shim for module dashboard renderers."""

from analyst_toolkit.m00_utils.dashboard_auto_heal import render_auto_heal_dashboard
from analyst_toolkit.m00_utils.dashboard_data_prep import (
    render_duplicates_dashboard,
    render_imputation_dashboard,
    render_normalization_dashboard,
    render_outlier_detection_dashboard,
    render_outlier_handling_dashboard,
)
from analyst_toolkit.m00_utils.dashboard_diagnostics import render_diagnostics_dashboard

__all__ = [
    "render_auto_heal_dashboard",
    "render_diagnostics_dashboard",
    "render_duplicates_dashboard",
    "render_imputation_dashboard",
    "render_normalization_dashboard",
    "render_outlier_detection_dashboard",
    "render_outlier_handling_dashboard",
]
