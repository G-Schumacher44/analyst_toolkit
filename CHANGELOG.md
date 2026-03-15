# Changelog

All notable changes to this project are documented in this file.

Format:

- Deterministic section order per release:
  - `Added`
  - `Changed`
  - `Fixed`
  - `Deprecated`
  - `Removed`
  - `Security`
- One bullet per atomic change.
- Imperative, user-facing language.
- Include issue/PR references when available.

## [Unreleased]

### Added

- None.

### Changed

- None.

### Fixed

- None.

### Deprecated

- None.

### Removed

- None.

### Security

- None.

## [0.4.4] - 2026-03-15

### Added

- Added standalone dashboard HTML exports.
- Added an expandable diagnostics plot modal.
- Rolled out polished module dashboards.
- Surfaced dashboard artifacts in MCP responses.
- Added the runtime overlay merge foundation.
- Wired runtime overlays into core MCP tools.
- Extended runtime overlays across remaining MCP tools.
- Added first-wave artifact destination routing.
- Rolled out runtime overlays across remaining MCP tools.
- Finished the artifact delivery rollout.
- Added auto-heal dashboard export.
- Polished the auto-heal dashboard.
- Added a tabbed pipeline dashboard shell.
- Reserved the data dictionary MCP surface.
- Added the cockpit dashboard hub.
- Implemented data dictionary artifacts.
- Wired the data dictionary into cockpit.
- Exposed cockpit docs as MCP resources.
- Added local artifact server support.

### Changed

- Untracked the monolith decomposition plan and ignored it locally.
- Tightened the CI and review workflow.
- Split stable and local agent guidance.
- Exposed runtime overlays to agents.
- Shared the empty delivery state helper.
- Exposed auto-heal agent templates.
- Extracted shared dashboard helpers.
- Extracted the dashboard page shell.
- Split dashboard view renderers.
- Extracted certification dashboards.
- Tightened shared dashboard renderers.
- Extracted module dashboard renderers.
- Split dashboard modules by domain.
- Decomposed the cockpit dashboard view.
- Centralized the template inventory.
- Updated repo workflow guidance.
- Removed the tracked deployment wheel.
- Cleaned up public repo surfaces.
- Removed unused dev requirements files.
- Centralized MCP dependency metadata.
- Polished MCP docker surfaces.
- Refreshed the conda environment definition.
- Refreshed dashboard screenshots.
- Updated release notes to 0.4.4.

### Fixed

- Corrected yamllint command references.
- Polished the diagnostics dashboard export.
- Hardened dashboard table rendering.
- Tightened diagnostics export defaults.
- Stabilized the imputation plot wiring test.
- Refined dashboard export ergonomics.
- Normalized export artifact file names.
- Tightened export run id detection.
- Avoided duplicate normalization work.
- Hardened runtime overlay and destination routing.
- Stabilized runtime config default factories.
- Tightened runtime routing follow-ups.
- Tightened runtime overlay rollout follow-ups.
- Hardened the auto-heal dashboard export.
- Sanitized auto-heal export warnings.
- Hardened auto-heal artifact fallback.
- Tightened auto-heal dashboard rendering.
- Cleaned up pipeline dashboard cockpit wiring.
- Hardened pipeline dashboard artifact handling.
- Avoided duplicate dashboard titles.
- Hardened cockpit history access.
- Hardened cockpit artifact contracts.
- Honored the runtime export html overlay.
- Narrowed the export html overlay path.
- Escaped cockpit blocker fallback html.
- Hardened cockpit dashboard contracts.
- Hardened cockpit error handling.
- Avoided the pre312 f-string parse error.
- Hardened modular dashboard renderers.
- Tightened dashboard renderer helpers.
- Hardened data dictionary config parsing.
- Hardened MCP resource contracts.
- Tightened MCP resource error handling.
- Exposed the MCP docker server on all interfaces.
- Typed artifact server metadata.
- Cleaned up the artifact server lifecycle.
- Rendered cockpit artifact links correctly.
- Tightened artifact server review followups.
- Hardened local artifact server controls.
- Improved normalization dashboard detail.
- Tightened artifact server error handling.
- Hardened normalization dashboard inputs.
- Preserved normalization preview audits.

### Deprecated

- None.

### Removed

- None.

### Security

- None.


## [0.4.3] - 2026-02-27

### Added

- Added contributor workflow documentation in `CONTRIBUTING.md`.
- Added GitHub issue templates for bug reports, feature requests, and documentation updates.
- Added a pull request template with validation and MCP contract checks.
- Added `SECURITY.md` with a responsible disclosure policy.
- Added MCP `/ready` and `/metrics` operability endpoints.
- Added runtime RPC metrics aggregation (`requests_total`, `errors_total`, latency, method/tool breakdown).
- Added optional structured request lifecycle logging via `ANALYST_MCP_STRUCTURED_LOGS=true`.
- Added MCP server tests for readiness, metrics schema, and RPC counter deltas.
- Added optional bearer-token auth mode via `ANALYST_MCP_AUTH_TOKEN` for MCP and operability endpoints.

### Changed

- Updated `README.md` with pointers to contributing, support, and templates.
- Updated MCP `/health` response to include `version` and `uptime_sec`.
- Updated MCP server guide with operability endpoint usage and a quick triage runbook.
- Updated test suite layout to split MCP server and hardening tests into focused modules.
- Updated release docs and guides to `v0.4.3`.

### Fixed

- Removed stale hardcoded server version fallback by using `ANALYST_MCP_VERSION_FALLBACK` (`0.0.0+local` default) when package metadata is unavailable.
- Fixed stale documentation references to monolithic test files after test decomposition.

### Deprecated

- None.

### Removed

- None.

### Security

- Added explicit security reporting guidance and secret-handling expectations.
- Added optional endpoint auth control for networked MCP deployments.

## [0.4.2] - Baseline (pre-changelog history)

### Added

- Introduced self-healing workflow (`infer_configs`, `auto_heal`, final certification path).

### Changed

- Stabilized MCP server and toolkit integration around modular QA/cleaning pipeline execution.

### Fixed

- Applied multiple MCP/runtime hardening and contract-stability improvements prior to changelog adoption.

### Deprecated

- None.

### Removed

- None.

### Security

- CI includes secret scanning and validation gates.
