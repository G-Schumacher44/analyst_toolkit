"""Static cockpit template and launch-surface builders."""

from __future__ import annotations

from typing import Any


def _require_resource_by_reference(
    resources_by_reference: dict[str, dict[str, str]],
    reference: str,
) -> dict[str, str]:
    try:
        return resources_by_reference[reference]
    except KeyError as exc:
        raise ValueError(
            f"build_cockpit_resource_groups missing resource reference: {reference}"
        ) from exc


def build_cockpit_resources() -> list[dict[str, str]]:
    return [
        {
            "Title": "Quickstart",
            "Kind": "guide",
            "Reference": "analyst://docs/quickstart",
            "Detail": "Human-oriented operating guide for the toolkit.",
        },
        {
            "Title": "Agent Playbook",
            "Kind": "guide",
            "Reference": "analyst://docs/agent-playbook",
            "Detail": "Strict ordered workflow for client agents.",
        },
        {
            "Title": "Capability Catalog",
            "Kind": "catalog",
            "Reference": "analyst://catalog/capabilities",
            "Detail": "Editable config knobs, runtime overlays, and workflow templates.",
        },
        {
            "Title": "Runtime Template",
            "Kind": "template",
            "Reference": "analyst://templates/config/runtime_overlay_template.yaml",
            "Detail": "Cross-cutting run-time controls for input path, run_id, destinations, and artifacts.",
        },
        {
            "Title": "Auto Heal Template",
            "Kind": "template",
            "Reference": "analyst://templates/config/auto_heal_request_template.yaml",
            "Detail": "One-shot remediation request shape with dashboard output.",
        },
        {
            "Title": "Data Dictionary Template",
            "Kind": "template",
            "Reference": "analyst://templates/config/data_dictionary_request_template.yaml",
            "Detail": "Reserved prelaunch dictionary request shape seeded from infer_configs.",
        },
    ]


def build_cockpit_resource_groups(resources: list[dict[str, str]]) -> list[dict[str, Any]]:
    resources_by_reference = {
        resource["Reference"]: resource for resource in resources if "Reference" in resource
    }
    return [
        {
            "title": "Start Here",
            "intro": (
                "Open these first when you need orientation, a safe execution recipe, or a "
                "human-readable guide before touching module-specific configs."
            ),
            "items": [
                _require_resource_by_reference(
                    resources_by_reference,
                    "analyst://docs/quickstart",
                ),
                _require_resource_by_reference(
                    resources_by_reference,
                    "analyst://docs/agent-playbook",
                ),
                _require_resource_by_reference(
                    resources_by_reference,
                    "analyst://templates/config/runtime_overlay_template.yaml",
                ),
            ],
        },
        {
            "title": "Templates And Contracts",
            "intro": (
                "These are the copyable request shapes for runtime overlays, auto-heal, and the "
                "data dictionary workflow."
            ),
            "items": [
                _require_resource_by_reference(
                    resources_by_reference,
                    "analyst://templates/config/runtime_overlay_template.yaml",
                ),
                _require_resource_by_reference(
                    resources_by_reference,
                    "analyst://templates/config/auto_heal_request_template.yaml",
                ),
                _require_resource_by_reference(
                    resources_by_reference,
                    "analyst://templates/config/data_dictionary_request_template.yaml",
                ),
            ],
        },
        {
            "title": "Capability Surfaces",
            "intro": (
                "Use these to inspect what the toolkit can do right now and which knobs are safe "
                "to edit without rewriting YAML by hand."
            ),
            "items": [
                _require_resource_by_reference(
                    resources_by_reference,
                    "analyst://catalog/capabilities",
                )
            ],
        },
    ]


def build_cockpit_launchpad() -> list[dict[str, str]]:
    return [
        {
            "Action": "Ensure Artifact Server",
            "Tool": "ensure_artifact_server",
            "Why": (
                "Start localhost artifact serving so dashboard links open as stable local URLs "
                "instead of raw paths."
            ),
        },
        {
            "Action": "Infer Configs",
            "Tool": "infer_configs",
            "Why": "Seed config review and the data-dictionary prelaunch contract from inferred rules.",
        },
        {
            "Action": "Open Pipeline Dashboard",
            "Tool": "get_pipeline_dashboard",
            "Why": "Jump into the tabbed run-level review surface for a specific run.",
        },
        {
            "Action": "Run Auto Heal",
            "Tool": "auto_heal",
            "Why": "Start one-shot remediation when the user explicitly wants automation.",
        },
        {
            "Action": "Inspect Run History",
            "Tool": "get_run_history",
            "Why": "Read the prescription and healing ledger behind dashboard surfaces.",
        },
        {
            "Action": "Fork Session",
            "Tool": "manage_session",
            "Why": (
                "Clone the current session into a new run context without re-downloading data "
                "or re-running infer_configs."
            ),
        },
    ]


def build_cockpit_launch_sequences() -> list[dict[str, Any]]:
    return [
        {
            "title": "Raw Dataset To First Pass",
            "steps": [
                "Run infer_configs to derive a safe initial config shape and identify likely module needs.",
                "Use the runtime overlay template to set run-scoped inputs, paths, and artifact policy in one place.",
                "Open the pipeline dashboard once module outputs exist so review stays in one run-level surface.",
            ],
        },
        {
            "title": "Repair And Certify",
            "steps": [
                "Use auto_heal only when the user explicitly wants one-shot remediation.",
                "Review the auto-heal dashboard before trusting downstream artifacts.",
                "Finish in final_audit or the pipeline dashboard to confirm the healed output is certification-ready.",
            ],
        },
        {
            "title": "Prelaunch Dictionary Path",
            "steps": [
                "Start from infer_configs so the data dictionary inherits inferred types, rules, and high-signal column hints.",
                "Use the data_dictionary request template to keep the prelaunch contract consistent.",
                "Treat the prelaunch report as a cockpit-linked surface, not a disconnected export.",
            ],
        },
        {
            "title": "Second Pass With New Run Context",
            "steps": [
                "Use manage_session(action='fork') to clone the current session and its inferred configs into a fresh run_id.",
                "Adjust configs on the forked session as needed — the original session stays untouched.",
                "Run modules on the forked session and compare results via the pipeline dashboard.",
            ],
        },
    ]


def build_cockpit_operator_brief() -> dict[str, Any]:
    return {
        "title": "Cockpit Briefing",
        "summary": (
            "This cockpit is the control tower for the toolkit. Use it to assess recent run health, "
            "open the strongest available artifact surface, and move into the right guide or tool "
            "without guessing where to start."
        ),
        "lanes": [
            {
                "title": "Review",
                "detail": (
                    "Start with recent runs and best-available surfaces to see what already exists "
                    "for the current operating slice."
                ),
            },
            {
                "title": "Orient",
                "detail": (
                    "Use the resource hub when you need human-readable guidance, templates, or "
                    "capability references before editing config."
                ),
            },
            {
                "title": "Act",
                "detail": (
                    "Use the launchpad when you are ready to move from review into execution for "
                    "a specific tool or workflow."
                ),
            },
        ],
    }


def build_data_dictionary_lane(latest_dictionary: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": str(latest_dictionary.get("status", "not_implemented") or "not_implemented"),
        "template_path": "config/data_dictionary_request_template.yaml",
        "implementation_plan": "local_plans/DATA_DICTIONARY_IMPLEMENTATION_WAVE_2026-03-14.md",
        "latest_run_id": str(latest_dictionary.get("run_id", "")),
        "latest_dashboard": str(
            latest_dictionary.get("dashboard_url")
            or latest_dictionary.get("dashboard_path")
            or latest_dictionary.get("artifact_url")
            or latest_dictionary.get("artifact_path")
            or ""
        ),
        "latest_export": str(
            latest_dictionary.get("xlsx_url")
            or latest_dictionary.get("xlsx_path")
            or latest_dictionary.get("export_url")
            or ""
        ),
        "direction": (
            "The data dictionary should be generated from infer_configs output and surfaced as a "
            "prelaunch report inside the cockpit so users can review structure expectations before "
            "running the rest of the pipeline."
        ),
        "cockpit_preview": latest_dictionary.get("cockpit_preview", {}),
    }
