# Monolith Decomposition Execution Plan

Date: 2026-02-27  
Status: Active  
Scope: `analyst_toolkit` repository

## Objective

Reduce high-risk monolithic files without changing runtime behavior, then use smaller modules for safer future feature work.

## Baseline (line-count scan)

Current largest source files:
- `src/analyst_toolkit/mcp_server/tools/cockpit.py` (~845)
- `src/analyst_toolkit/mcp_server/io.py` (~805)
- `src/analyst_toolkit/mcp_server/server.py` (~680)
- `src/analyst_toolkit/m00_utils/report_generator.py` (~527)

Current largest test files:
- `tests/test_mcp_tool_regressions.py` (~411)
- `tests/hardening/test_config_and_upload.py` (~263)
- `tests/mcp_server/test_rpc_tools.py` (~227)

## Constraints

1. No behavior changes during extraction phases.
2. Preserve public MCP contracts and response shapes.
3. Keep tests green after each extraction step.
4. Use small, reversible commits.

## Targets

- Source file target: <= 400 lines per module (soft target for first pass).
- Test file target: <= 500 lines per module.
- Extract shared logic into focused modules with explicit naming.

## Execution Order

1. `server.py` (start here)
2. `io.py`
3. `tools/cockpit.py`
4. `m00_utils/report_generator.py`
5. `tests/test_mcp_server.py`
6. `tests/test_hardening.py`

## Phase Plan

## Phase 1: server.py decomposition

### Planned extractions
- `mcp_server/observability.py`
  - runtime metrics container
  - structured/unstructured request event logger
- `mcp_server/auth.py`
  - bearer-token authorization helper
- `mcp_server/rpc_dispatch.py`
  - JSON-RPC method routing logic

### Definition of Done
- Server behavior unchanged.
- `/rpc`, `/health`, `/ready`, `/metrics` contracts unchanged.
- Existing server tests pass.
- `server.py` line count reduced from baseline.

## Phase 2: io.py decomposition

### Planned extractions
- `mcp_server/io_local.py` (local CSV/parquet loading)
- `mcp_server/io_gcs.py` (GCS direct + prefix loading)
- `mcp_server/io_limits.py` (size/row/file guardrails)
- `mcp_server/io_storage.py` (GCS/local export + artifact upload)
- `mcp_server/io_serialization.py` (JSON-safe conversion + artifact contract helpers)
- `mcp_server/io_history_files.py` (history file parsing + atomic write)
- `mcp_server/io_path_normalization.py` (bucket-like path normalization)

### Definition of Done
- Existing data loading behavior unchanged.
- Existing `io` tests pass unchanged.

## Phase 3: cockpit.py decomposition

### Planned extractions
- `tools/cockpit_health.py`
- `tools/cockpit_history.py`
- `tools/cockpit_templates.py`
- `tools/cockpit_capabilities.py`
- `tools/cockpit_content.py`
- `tools/cockpit_runtime.py`
- `tools/cockpit_schemas.py`

### Definition of Done
- Tool IDs, schemas, and outputs unchanged.
- Cockpit-related tests pass.

## Phase 4: report generator decomposition

### Planned extractions
- `m00_utils/report_html.py`
- `m00_utils/report_tables.py`
- `m00_utils/report_generator.py` kept as compatibility facade

### Definition of Done
- Report artifacts and names unchanged.
- Existing report tests/golden checks pass.

## Phase 5: test suite decomposition

### Planned extractions
- split by contract area (`tests/mcp_server/`, `tests/hardening/`)
- keep test intent and assertions identical first, then optional cleanup

### Completed extraction layout
- `tests/mcp_server/test_http_auth_metrics.py`
- `tests/mcp_server/test_rpc_tools.py`
- `tests/mcp_server/test_rpc_preflight.py`
- `tests/mcp_server/test_rpc_resources.py`
- `tests/mcp_server/test_rpc_run_history.py`
- `tests/hardening/test_state_store.py`
- `tests/hardening/test_config_and_upload.py`
- `tests/hardening/test_history_and_contracts.py`
- `tests/hardening/test_pipeline_integrations.py`
- `tests/hardening/test_auto_heal_behavior.py`

### Definition of Done
- No coverage regression.
- CI runtime remains acceptable.

## Validation Protocol (every phase)

1. Run targeted tests for touched surface.
2. Run full `pytest -q`.
3. Verify no public contract drift.
4. Confirm docs/changelog only if externally visible behavior changed.

## Git Discipline

Branch naming:
- `refactor/decompose-server-phase1`
- `refactor/decompose-io-phase2`
- `refactor/decompose-cockpit-phase3`

Commit pattern:
- `refactor(<area>): extract <component> (no behavior change)`
- `test(<area>): keep parity coverage after extraction`

PR scope:
- one monolith phase per PR
- avoid mixing new features into decomposition PRs

## Progress Log

- [x] Plan created and tracked in repo.
- [x] Phase 1 complete.
- [x] Phase 2 complete.
- [x] Phase 3 complete.
- [x] Phase 4 complete.
- [x] Phase 5 complete.
