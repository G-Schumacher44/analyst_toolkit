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

- Updated MCP runtime installation to pull `analyst_toolkit_deploy` from its pinned source repository instead of a tracked in-repo wheel.

### Fixed

- None.

### Deprecated

- None.

### Removed

- Removed the tracked `vendor/analyst_toolkit_deploy-0.2.6-py3-none-any.whl` build artifact from the repository.

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
