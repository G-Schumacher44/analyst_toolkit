"""Facade module for report generation utilities.

This module preserves import compatibility while delegating implementation to
smaller focused modules.
"""

from analyst_toolkit.m00_utils.report_html import generate_html_report
from analyst_toolkit.m00_utils.report_tables import (
    generate_duplicates_report,
    generate_final_audit_report,
    generate_imputation_report,
    generate_outlier_handling_report,
    generate_outlier_report,
    generate_transformation_report,
)

__all__ = [
    "generate_html_report",
    "generate_transformation_report",
    "generate_duplicates_report",
    "generate_outlier_report",
    "generate_outlier_handling_report",
    "generate_imputation_report",
    "generate_final_audit_report",
]
