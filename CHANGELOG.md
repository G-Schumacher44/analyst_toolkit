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

- Add standalone dashboard HTML exports
- Add expandable diagnostics plot modal
- Roll out polished module dashboards
- Surface dashboard artifacts in MCP responses
- Add runtime overlay merge foundation
- Wire runtime overlays into core MCP tools
- Extend runtime overlays across remaining MCP tools
- Add first-wave artifact destination routing
- Roll out runtime overlays across remaining MCP tools
- Finish artifact delivery rollout
- Add auto-heal dashboard export
- Polish auto-heal dashboard
- Add tabbed pipeline dashboard shell
- Reserve data dictionary MCP surface
- Add cockpit dashboard hub
- Implement data dictionary artifacts
- Wire data dictionary into cockpit
- Expose cockpit docs as MCP resources
- Add local artifact server support

### Changed

- Untrack monolith decomposition plan and ignore locally
- Tighten CI and review workflow
- Split stable and local agent guidance
- Expose runtime overlays to agents
- Share empty delivery state helper
- Expose auto-heal agent templates
- Extract shared dashboard helpers
- Extract dashboard page shell
- Split dashboard view renderers
- Extract certification dashboards
- Tighten shared dashboard renderers
- Extract module dashboard renderers
- Split dashboard modules by domain
- Decompose cockpit dashboard view
- Centralize template inventory
- Update repo workflow guidance
- Remove tracked deployment wheel
- Clean up public repo surfaces
- Remove unused dev requirements files
- Centralize MCP dependency metadata
- Polish MCP docker surfaces
- Refresh conda environment definition
- Refresh dashboard screenshots
- Update release notes to 0.4.4

### Fixed

- Correct yamllint command references
- Polish diagnostics dashboard export
- Harden dashboard table rendering
- Tighten diagnostics export defaults
- Stabilize imputation plot wiring test
- Refine dashboard export ergonomics
- Normalize export artifact file names
- Tighten export run id detection
- Avoid duplicate normalization work
- Harden runtime overlay and destination routing
- Stabilize runtime config default factories
- Tighten runtime routing follow-ups
- Tighten runtime overlay rollout follow-ups
- Harden auto-heal dashboard export
- Sanitize auto-heal export warnings
- Harden auto-heal artifact fallback
- Tighten auto-heal dashboard rendering
- Clean up pipeline dashboard cockpit wiring
- Harden pipeline dashboard artifact handling
- Avoid duplicate dashboard titles
- Harden cockpit history access
- Harden cockpit artifact contracts
- Honor runtime export html overlay
- Narrow export html overlay path
- Escape cockpit blocker fallback html
- Harden cockpit dashboard contracts
- Harden cockpit error handling
- Avoid pre312 f-string parse error
- Harden modular dashboard renderers
- Tighten dashboard renderer helpers
- Harden data dictionary config parsing
- Harden MCP resource contracts
- Tighten MCP resource error handling
- Expose MCP docker server on all interfaces
- Type artifact server metadata
- Clean up artifact server lifecycle
- Render cockpit artifact links correctly
- Tighten artifact server review followups
- Harden local artifact server controls
- Improve normalization dashboard detail
- Tighten artifact server error handling
- Harden normalization dashboard inputs
- Preserve normalization preview audits

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
