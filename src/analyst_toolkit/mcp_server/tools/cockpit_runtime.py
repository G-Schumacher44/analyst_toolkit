"""History and health report runtime helpers for cockpit tools."""

from typing import Any

from analyst_toolkit.m00_utils.scoring import calculate_health_score

_CERT_FAILURE_STATUSES = {"fail", "error"}


def _latest_module_entry(history: list[dict[str, Any]], module_name: str) -> dict[str, Any]:
    for entry in reversed(history):
        if str(entry.get("module", "")).strip() == module_name:
            return entry
    return {}


def build_run_history_result(
    *,
    run_id: str,
    session_id: str | None,
    failures_only: bool,
    latest_errors: bool,
    latest_status_by_module: bool,
    limit: int | None,
    summary_only: bool | None,
    run_history_default_summary_only: bool,
    run_history_default_limit: int,
    history: list[dict[str, Any]],
    history_meta: dict[str, Any],
) -> dict[str, Any]:
    total_history_count = len(history)

    if failures_only:
        history = [
            entry
            for entry in history
            if entry.get("status") in {"fail", "error"}
            or bool(entry.get("summary", {}).get("passed") is False)
        ]

    summary_only_effective = (
        bool(summary_only) if isinstance(summary_only, bool) else run_history_default_summary_only
    )
    limit_effective = limit if isinstance(limit, int) and limit > 0 else None
    if limit_effective is None and summary_only_effective:
        limit_effective = run_history_default_limit
    if isinstance(limit_effective, int) and limit_effective > 0:
        history = history[-limit_effective:]

    latest_errors_payload: list[dict[str, Any]] = []
    if latest_errors:
        latest_errors_payload = [
            entry
            for entry in reversed(history)
            if entry.get("status") in {"fail", "error"}
            or bool(entry.get("summary", {}).get("passed") is False)
        ][:5]

    latest_status_payload: dict[str, Any] = {}
    if latest_status_by_module:
        by_module: dict[str, dict[str, Any]] = {}
        for entry in history:
            module = str(entry.get("module", "unknown"))
            by_module[module] = {
                "status": entry.get("status", "unknown"),
                "timestamp": entry.get("timestamp"),
                "summary": entry.get("summary", {}),
            }
        latest_status_payload = by_module

    ledger = [_history_summary(entry) for entry in history] if summary_only_effective else history

    status = "warn" if history_meta.get("parse_errors") else "pass"
    return {
        "status": status,
        "run_id": run_id,
        "session_id": session_id,
        "filters": {
            "failures_only": failures_only,
            "latest_errors": latest_errors,
            "latest_status_by_module": latest_status_by_module,
            "limit": limit_effective,
            "summary_only": summary_only_effective,
            "defaults": {
                "summary_only_default": run_history_default_summary_only,
                "limit_default": run_history_default_limit,
            },
        },
        "history_count": len(ledger),
        "total_history_count": total_history_count,
        "ledger": ledger,
        "latest_errors": latest_errors_payload,
        "latest_status_by_module": latest_status_payload,
        "skipped_records": int(history_meta.get("skipped_records", 0)),
        "parse_errors": list(history_meta.get("parse_errors", [])),
    }


def build_data_health_report(
    *,
    run_id: str,
    session_id: str | None,
    history: list[dict[str, Any]],
    history_meta: dict[str, Any],
) -> dict[str, Any]:
    metrics = {
        "null_rate": 0.0,
        "validation_pass_rate": 1.0,
        "outlier_ratio": 0.0,
        "duplicate_ratio": 0.0,
    }

    for entry in history:
        module = entry.get("module")
        summary_raw = entry.get("summary", {})
        summary = summary_raw if isinstance(summary_raw, dict) else {}
        row_count = summary.get("row_count")

        if module == "diagnostics":
            metrics["null_rate"] = summary.get("null_rate", 0.0)
        elif module == "validation":
            metrics["validation_pass_rate"] = 1.0 if summary.get("passed", True) else 0.5
        elif module == "duplicates":
            count = summary.get("duplicate_count", 0)
            metrics["duplicate_ratio"] = count / row_count if row_count else min(0.2, count / 1000)
        elif module == "outliers":
            count = summary.get("outlier_count", 0)
            metrics["outlier_ratio"] = count / row_count if row_count else min(0.2, count / 1000)

    score_res = calculate_health_score(metrics)
    final_audit_entry = _latest_module_entry(history, "final_audit")
    final_audit_status = str(final_audit_entry.get("status", "not_run") or "not_run")
    final_audit_summary_raw = final_audit_entry.get("summary", {})
    final_audit_summary = (
        final_audit_summary_raw if isinstance(final_audit_summary_raw, dict) else {}
    )
    final_audit_passed = (
        final_audit_summary.get("passed")
        if isinstance(final_audit_summary.get("passed"), bool)
        else None
    )
    health_advisory = bool(
        final_audit_entry
        and (final_audit_status.lower() in _CERT_FAILURE_STATUSES or final_audit_passed is False)
    )
    warnings: list[str] = []
    message = (
        f"Data Health Score is {score_res['overall_score']}/100 ({score_res['status'].upper()})"
    )
    if health_advisory:
        warnings.append(
            "Health score is advisory only because final_audit reported certification failures."
        )
        message = (
            f"Advisory Data Health Score is {score_res['overall_score']}/100 "
            f"({score_res['status'].upper()}). Final audit failed certification for this run."
        )
    status = "warn" if history_meta.get("parse_errors") or health_advisory else "pass"
    return {
        "status": status,
        "run_id": run_id,
        "session_id": session_id,
        "health_score": score_res["overall_score"],
        "health_status": score_res["status"],
        "health_advisory": health_advisory,
        "certification_status": final_audit_status,
        "certification_passed": final_audit_passed,
        "breakdown": score_res["breakdown"],
        "message": message,
        "warnings": warnings,
        "skipped_records": int(history_meta.get("skipped_records", 0)),
        "parse_errors": list(history_meta.get("parse_errors", [])),
    }


def _history_summary(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "module": entry.get("module"),
        "status": entry.get("status"),
        "timestamp": entry.get("timestamp"),
        "summary": entry.get("summary", {}),
    }
