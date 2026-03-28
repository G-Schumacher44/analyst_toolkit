"""MCP tool: toolkit_infer_configs — config generation via analyst_toolkit_deploy."""

import inspect
import os
import tempfile
from pathlib import Path
from typing import Any

import yaml

from analyst_toolkit.mcp_server.config_normalizers import (
    sanitize_inferred_final_audit_config,
    sanitize_inferred_validation_config,
)
from analyst_toolkit.mcp_server.input.ingest import get_input_descriptor
from analyst_toolkit.mcp_server.input.registry import get_session_input_id
from analyst_toolkit.mcp_server.io import (
    get_session_metadata,
    load_input,
    resolve_run_context,
    save_session_config,
    save_to_session,
)
from analyst_toolkit.mcp_server.response_utils import next_action, with_next_actions
from analyst_toolkit.mcp_server.runtime_overlay import (
    normalize_runtime_overlay,
    runtime_to_tool_overrides,
)
from analyst_toolkit.mcp_server.schemas import INPUT_ID_PROP

_SUPPORTED_INFER_MODULES = {
    "diagnostics",
    "validation",
    "certification",
    "normalization",
    "duplicates",
    "outliers",
    "imputation",
    "final_audit",
}

_REQUEST_MODULE_ALIASES = {
    "dups": "duplicates",
    "duplicate": "duplicates",
    "outlier": "outliers",
    "handling": "outliers",
    "outlier_handling": "outliers",
}
_TRANSIENT_PATH_KEYS = ("raw_data_path", "input_path", "input_df_path")


def _call_external_infer_configs(
    infer_configs_fn,
    *,
    input_path: str,
    options: dict,
    modules: list[str] | None,
    sample_rows: int | None,
) -> tuple[dict, list[str]]:
    """Call the external infer_configs helper with signature compatibility."""
    kwargs = {
        "root": options.get("root", "."),
        "input_path": input_path,
        "modules": modules or options.get("modules"),
        "outdir": options.get("outdir"),
        "sample_rows": sample_rows or options.get("sample_rows"),
        "max_unique": options.get("max_unique", 30),
        "exclude_patterns": options.get("exclude_patterns", "id|uuid|tag"),
        "detect_datetimes": options.get("detect_datetimes", True),
        "datetime_hints": options.get("datetime_hints"),
    }

    signature = inspect.signature(infer_configs_fn)
    accepts_var_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    if accepts_var_kwargs:
        return infer_configs_fn(**kwargs), []

    supported = set(signature.parameters)
    filtered_kwargs = {key: value for key, value in kwargs.items() if key in supported}
    dropped = sorted(key for key in kwargs if key not in supported and kwargs[key] is not None)
    warnings = []
    if dropped:
        warnings.append(
            "External infer_configs helper does not support the following arguments and they "
            f"were ignored: {', '.join(dropped)}."
        )
    return infer_configs_fn(**filtered_kwargs), warnings


def _module_name_from_generated_file(path: Path) -> str | None:
    stem = path.stem.lower()
    for suffix in ("_config_autofill", "_config_template", "_template", "_config", "_yaml"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break

    return stem if stem in _SUPPORTED_INFER_MODULES else None


def _module_name_from_generated_yaml(raw_yaml: str) -> str | None:
    try:
        loaded = yaml.safe_load(raw_yaml) or {}
    except yaml.YAMLError:
        return None
    if not isinstance(loaded, dict) or not loaded:
        return None
    for key in loaded:
        if not isinstance(key, str):
            continue
        module_name = key if key in _SUPPORTED_INFER_MODULES else None
        if module_name is None and key == "outlier_detection":
            module_name = "outliers"
        if module_name is not None:
            return module_name
    return None


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def _trusted_generated_config_root() -> Path:
    return (Path.cwd() / "config").resolve()


def _normalize_requested_modules(modules: list[str] | None) -> list[str] | None:
    if modules is None:
        return sorted(_SUPPORTED_INFER_MODULES)

    normalized: list[str] = []
    for module in modules:
        candidate = _REQUEST_MODULE_ALIASES.get(module, module)
        if candidate in _SUPPORTED_INFER_MODULES and candidate not in normalized:
            normalized.append(candidate)
    return normalized


def _replace_transient_paths(
    value: Any,
    *,
    stable_input_path: str | None,
    temp_input_path: str,
) -> Any:
    temp_dir = tempfile.gettempdir()
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, child in value.items():
            cleaned_child = _replace_transient_paths(
                child,
                stable_input_path=stable_input_path,
                temp_input_path=temp_input_path,
            )
            if key in _TRANSIENT_PATH_KEYS and isinstance(child, str):
                child_path = child.strip()
                is_temp_snapshot = child_path == temp_input_path
                is_ephemeral_tmp = child_path.startswith("/tmp/") or child_path.startswith(
                    temp_dir + os.sep
                )
                if is_temp_snapshot or is_ephemeral_tmp:
                    if key in {"raw_data_path", "input_path"} and stable_input_path:
                        cleaned[key] = stable_input_path
                    continue
            if cleaned_child is not None:
                cleaned[key] = cleaned_child
        if "outlier_handling" in cleaned and "outlier_detection" not in cleaned:
            cleaned["outlier_detection"] = cleaned.pop("outlier_handling")
        return cleaned
    if isinstance(value, list):
        return [
            item
            for item in (
                _replace_transient_paths(
                    child,
                    stable_input_path=stable_input_path,
                    temp_input_path=temp_input_path,
                )
                for child in value
            )
            if item is not None
        ]
    return value


def _sanitize_generated_yaml(
    raw_yaml: str,
    *,
    module_name: str | None,
    stable_input_path: str | None,
    temp_input_path: str,
) -> str:
    try:
        loaded = yaml.safe_load(raw_yaml)
    except yaml.YAMLError:
        return raw_yaml
    if not isinstance(loaded, dict):
        return raw_yaml
    sanitized = _replace_transient_paths(
        loaded,
        stable_input_path=stable_input_path,
        temp_input_path=temp_input_path,
    )
    if module_name == "validation":
        sanitized = sanitize_inferred_validation_config(sanitized)
    elif module_name == "final_audit":
        sanitized = sanitize_inferred_final_audit_config(sanitized)
    return yaml.safe_dump(sanitized, sort_keys=False, allow_unicode=True)


def _stable_input_path(
    *,
    gcs_path: str | None,
    input_id: str | None,
    descriptor,
    session_id: str | None,
) -> str | None:
    if gcs_path and Path(gcs_path).exists():
        return gcs_path
    if descriptor and descriptor.source_type != "gcs":
        resolved = descriptor.resolved_reference
        if resolved and Path(resolved).exists():
            return resolved
    if session_id:
        bound_input_id = get_session_input_id(session_id)
        if bound_input_id:
            bound_descriptor = get_input_descriptor(bound_input_id)
            if bound_descriptor and bound_descriptor.source_type != "gcs":
                resolved = bound_descriptor.resolved_reference
                if resolved and Path(resolved).exists():
                    return resolved
    return None


def _normalize_external_configs_result(
    configs_result,
    *,
    trusted_config_root: Path,
    stable_input_path: str | None,
    temp_input_path: str,
) -> tuple[dict[str, str], list[str], str | None]:
    """Normalize external infer_configs output into module->yaml mapping."""
    if isinstance(configs_result, dict):
        normalized: dict[str, str] = {}
        warnings: list[str] = []
        for module, config_yaml in configs_result.items():
            if config_yaml is None:
                continue
            module_name = _module_name_from_generated_file(Path(f"{module}.yaml"))
            if module_name is None and isinstance(config_yaml, str):
                module_name = _module_name_from_generated_yaml(config_yaml)
            if module_name is None:
                warnings.append(f"Ignored unsupported inferred module key: {module}.")
                continue
            normalized[module_name] = _sanitize_generated_yaml(
                str(config_yaml),
                module_name=module_name,
                stable_input_path=stable_input_path,
                temp_input_path=temp_input_path,
            )
        return normalized, warnings, None

    if isinstance(configs_result, str):
        config_dir = Path(configs_result).resolve()
        if config_dir.is_symlink() or not config_dir.is_dir():
            return (
                {},
                [f"External infer_configs returned non-directory path: {configs_result}."],
                None,
            )
        if not _is_relative_to(config_dir, trusted_config_root):
            return (
                {},
                [f"Rejected untrusted generated config directory: {configs_result}."],
                None,
            )

        configs: dict[str, str] = {}
        for path in sorted(config_dir.rglob("*")):
            if path.is_symlink():
                continue
            resolved_path = path.resolve()
            if not _is_relative_to(resolved_path, trusted_config_root):
                continue
            if not resolved_path.is_file() or resolved_path.suffix.lower() not in {".yaml", ".yml"}:
                continue
            raw_yaml = resolved_path.read_text(encoding="utf-8")
            module_name = _module_name_from_generated_yaml(
                raw_yaml
            ) or _module_name_from_generated_file(resolved_path)
            if module_name is None:
                continue
            configs[module_name] = _sanitize_generated_yaml(
                raw_yaml,
                module_name=module_name,
                stable_input_path=stable_input_path,
                temp_input_path=temp_input_path,
            )

        warnings = []
        if not configs:
            warnings.append(
                "External infer_configs generated config files, but none could be mapped to "
                "supported toolkit modules."
            )
        return configs, warnings, str(config_dir)

    return (
        {},
        [f"External infer_configs returned unsupported type: {type(configs_result).__name__}."],
        None,
    )


async def _toolkit_infer_configs(
    gcs_path: str | None = None,
    session_id: str | None = None,
    input_id: str | None = None,
    runtime: dict | str | None = None,
    options: dict | None = None,
    modules: list[str] | None = None,
    sample_rows: int | None = None,
    run_id: str | None = None,
) -> dict:
    """
    Generate YAML config strings for toolkit modules by inspecting the dataset at gcs_path or session_id.

    Returns a dict with the generated YAML config string.
    """
    runtime_cfg, runtime_warnings = normalize_runtime_overlay(runtime)
    runtime_overrides = runtime_to_tool_overrides(runtime_cfg)
    runtime_applied = bool(runtime_cfg)
    gcs_path = gcs_path or runtime_overrides.get("gcs_path")
    session_id = session_id or runtime_overrides.get("session_id")
    input_id = input_id or runtime_overrides.get("input_id")
    run_id = run_id or runtime_overrides.get("run_id")
    run_id, _lifecycle = resolve_run_context(run_id, session_id)
    options = options or {}

    # When input_id is provided with session_id, use input_id as the data source
    # and session_id as the config storage target (not a competing data source).
    if input_id and session_id:
        data_sources = [gcs_path is not None, True]  # input_id counts as data source
    else:
        data_sources = [gcs_path is not None, session_id is not None, input_id is not None]
    if sum(data_sources) > 1:
        return {
            "status": "error",
            "module": "infer_configs",
            "error": "Provide exactly one of gcs_path, session_id, or input_id.",
            "error_code": "AMBIGUOUS_INPUT_SOURCE",
            "config_yaml": "",
        }

    # Load data from the single data source
    load_session = session_id if not input_id else None
    try:
        df = load_input(gcs_path, session_id=load_session, input_id=input_id)
    except Exception as exc:
        return {
            "status": "error",
            "module": "infer_configs",
            "error": f"Failed to load input: {type(exc).__name__}: {exc}",
            "error_code": "INPUT_LOAD_FAILED",
            "config_yaml": "",
        }

    # Resolve session_id from input descriptor if not provided
    descriptor = None
    if not session_id and input_id:
        descriptor = get_input_descriptor(input_id)
        if descriptor and descriptor.session_id:
            session_id = descriptor.session_id
    elif input_id:
        descriptor = get_input_descriptor(input_id)

    # If we still don't have a session, start one
    if not session_id:
        session_id = save_to_session(df, run_id=run_id)
    elif get_session_metadata(session_id) is None:
        # input_id descriptors can outlive a cleared session; recreate the named
        # session so inferred configs have a live persistence target.
        session_id = save_to_session(df, session_id=session_id, run_id=run_id)

    # Always materialize an input snapshot locally for inference.
    # This avoids path-construction drift between modules and ensures deterministic reads.
    temp_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    df.to_csv(temp_file.name, index=False)
    input_path = temp_file.name
    trusted_config_root = _trusted_generated_config_root()
    stable_input_path = _stable_input_path(
        gcs_path=gcs_path,
        input_id=input_id,
        descriptor=descriptor,
        session_id=session_id,
    )
    normalized_modules = _normalize_requested_modules(modules)

    try:
        try:
            from analyst_toolkit_deploy.infer_configs import infer_configs
        except ImportError:
            from analyst_toolkit_deployment_utility.infer_configs import infer_configs
    except ImportError as exc:
        if temp_file:
            os.unlink(temp_file.name)
        return {
            "status": "error",
            "error": (
                f"Deployment utility not found ({str(exc)}). "
                "Ensure analyst_toolkit_deploy is installed from requirements-mcp.txt and rebuild."
            ),
            "config_yaml": "",
        }

    external_warnings: list[str] = []
    generated_config_dir: str | None = None
    try:
        raw_configs, external_warnings = _call_external_infer_configs(
            infer_configs,
            input_path=input_path,
            options=options,
            modules=normalized_modules,
            sample_rows=sample_rows,
        )
        configs, normalization_warnings, generated_config_dir = _normalize_external_configs_result(
            raw_configs,
            trusted_config_root=trusted_config_root,
            stable_input_path=stable_input_path,
            temp_input_path=input_path,
        )
        external_warnings.extend(normalization_warnings)
    finally:
        if temp_file:
            os.unlink(temp_file.name)

    # Persist inferred configs to session so downstream tools can auto-discover them
    for module_name, config_yaml in configs.items():
        save_session_config(session_id, module_name, config_yaml)

    module_order = [
        "diagnostics",
        "normalization",
        "duplicates",
        "outliers",
        "imputation",
        "validation",
        "final_audit",
    ]
    apply_actions = []
    for module in module_order:
        if module not in configs:
            continue
        params = {"session_id": session_id, "run_id": "<set_run_id>"}
        if module == "final_audit":
            params["config"] = f"<configs.{module}>"
        else:
            params["config"] = f"<configs.{module}>"
        apply_actions.append(
            next_action(
                module,
                f"Apply inferred config for {module}.",
                params,
            )
        )
    if "certification" in configs and "final_audit" not in configs:
        apply_actions.append(
            next_action(
                "final_audit",
                "Apply inferred certification rules through final_audit.",
                {
                    "session_id": session_id,
                    "run_id": "<set_run_id>",
                },
            )
        )

    capability_action = next_action(
        "get_capability_catalog",
        "Cross-check inferred YAML paths against supported capability knobs.",
        {},
    )

    if not apply_actions:
        apply_actions = [
            next_action(
                "get_capability_catalog",
                "No executable module configs were inferred; review available knobs manually.",
                {},
            )
        ]
        next_steps = apply_actions
    else:
        next_steps = apply_actions + [capability_action]

    covered_modules = sorted(configs.keys())
    if normalized_modules:
        normalized_requested = set(normalized_modules)
    else:
        normalized_requested = set(_SUPPORTED_INFER_MODULES)
    unsupported_modules = sorted(normalized_requested - set(covered_modules))
    if unsupported_modules:
        external_warnings.append(
            f"Configs were not generated for: {', '.join(unsupported_modules)}."
        )
        external_warnings.append(
            "infer_configs returned partial MCP workflow coverage. Review covered_modules and "
            "unsupported_modules before running downstream tools."
        )

    return with_next_actions(
        {
            "status": "pass",
            "module": "infer_configs",
            "run_id": run_id,
            "session_id": session_id,
            "configs": configs,
            "covered_modules": covered_modules,
            "unsupported_modules": unsupported_modules,
            "config_dir": generated_config_dir or "",
            "runtime_applied": runtime_applied,
            "warnings": runtime_warnings + external_warnings,
        },
        next_steps,
    )


_INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "gcs_path": {
            "type": "string",
            "description": "Path to the dataset (local CSV/parquet or gs:// URI). Optional if session_id is used.",
        },
        "session_id": {
            "type": "string",
            "description": "Optional: In-memory session identifier from a previous tool run.",
        },
        **INPUT_ID_PROP,
        "runtime": {
            "type": ["object", "string"],
            "description": (
                "Optional runtime overlay dict or YAML string for run-scoped settings "
                "such as run_id and input_path."
            ),
            "default": {},
        },
        "run_id": {
            "type": "string",
            "description": "Optional run identifier propagated through runtime-aware workflows.",
        },
        "modules": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Module names to generate configs for. Defaults to all. "
                "Valid public modules: validation, certification, outliers, diagnostics, "
                "normalization, duplicates, imputation, final_audit. Legacy aliases such as "
                "duplicate, dups, outlier, and handling normalize to the public module names. "
                "Certification is inferred-only and should be applied through final_audit."
            ),
        },
        "sample_rows": {
            "type": "integer",
            "description": "Read only the first N rows for speed. Defaults to all rows.",
        },
        "options": {
            "type": "object",
            "description": (
                "Advanced overrides: max_unique, exclude_patterns, detect_datetimes, "
                "datetime_hints, outdir."
            ),
            "default": {},
        },
    },
    "anyOf": [
        {"required": ["gcs_path"]},
        {"required": ["session_id"]},
        {"required": ["input_id"]},
        {"required": ["runtime"]},
    ],
}

from analyst_toolkit.mcp_server.registry import register_tool  # noqa: E402

register_tool(
    name="infer_configs",
    fn=_toolkit_infer_configs,
    description=(
        "Inspect a dataset and generate YAML config strings for toolkit modules. "
        "Returns dict of module_name → YAML string."
    ),
    input_schema=_INPUT_SCHEMA,
)
