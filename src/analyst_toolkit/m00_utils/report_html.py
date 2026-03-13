"""HTML report rendering helpers."""

from analyst_toolkit.m00_utils.dashboard_html import generate_dashboard_html


def generate_html_report(
    report_tables: dict,
    module_name: str,
    run_id: str,
    plot_paths: dict | None = None,
) -> str:
    """Build a single-page self-contained HTML report."""
    return generate_dashboard_html(
        report_tables=report_tables,
        module_name=module_name,
        run_id=run_id,
        plot_paths=plot_paths,
    )
