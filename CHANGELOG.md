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

- Add standalone dashboard HTML exports (#33)
- Add expandable diagnostics plot modal (#33)
- Roll out polished module dashboards (#33)
- Surface dashboard artifacts in MCP responses (#33)
- Add runtime overlay merge foundation (#35)
- Wire runtime overlays into core MCP tools (#35)
- Extend runtime overlays across remaining MCP tools (#35)
- Add first-wave artifact destination routing (#35)
- Roll out runtime overlays across remaining MCP tools (#36)
- Finish artifact delivery rollout (#37)
- Add auto-heal dashboard export (#38)
- Polish auto-heal dashboard (#38)
- Add tabbed pipeline dashboard shell (#40)
- Reserve data dictionary MCP surface (#41)
- Add cockpit dashboard hub (#42)
- Implement data dictionary artifacts (#45)
- Wire data dictionary into cockpit (#45)
- Expose cockpit docs as MCP resources (#46)
- Add local artifact server support (#47)

### Changed

- Untrack monolith decomposition plan and ignore locally
- Tighten CI and review workflow (#32)
- Split stable and local agent guidance (#33)
- Expose runtime overlays to agents (#35)
- Share empty delivery state helper (#37)
- Expose auto-heal agent templates (#39)
- Extract shared dashboard helpers (#43)
- Extract dashboard page shell (#43)
- Split dashboard view renderers (#43)
- Extract certification dashboards (#43)
- Tighten shared dashboard renderers (#43)
- Extract module dashboard renderers (#44)
- Split dashboard modules by domain (#44)
- Decompose cockpit dashboard view (#44)
- Centralize template inventory (#46)
- Update repo workflow guidance (#48)
- Remove tracked deployment wheel
- Clean up public repo surfaces
- Remove unused dev requirements files
- Centralize MCP dependency metadata
- Polish MCP docker surfaces
- Refresh conda environment definition
- Refresh dashboard screenshots
- Update release notes to 0.4.4
- V0.4.4 docs sweep — usage guide, changelog, contributing, agents
- Polish changelog release notes

### Fixed

- Correct yamllint command references (#32)
- Polish diagnostics dashboard export (#33)
- Harden dashboard table rendering (#33)
- Tighten diagnostics export defaults (#33)
- Stabilize imputation plot wiring test (#33)
- Refine dashboard export ergonomics (#33)
- Normalize export artifact file names (#34)
- Tighten export run id detection (#34)
- Avoid duplicate normalization work (#34)
- Harden runtime overlay and destination routing (#35)
- Stabilize runtime config default factories (#35)
- Tighten runtime routing follow-ups (#35)
- Tighten runtime overlay rollout follow-ups (#36)
- Harden auto-heal dashboard export (#38)
- Sanitize auto-heal export warnings (#38)
- Harden auto-heal artifact fallback (#38)
- Tighten auto-heal dashboard rendering (#38)
- Clean up pipeline dashboard cockpit wiring (#40)
- Harden pipeline dashboard artifact handling (#40)
- Avoid duplicate dashboard titles (#41)
- Harden cockpit history access (#42)
- Harden cockpit artifact contracts (#42)
- Honor runtime export html overlay (#42)
- Narrow export html overlay path (#42)
- Escape cockpit blocker fallback html (#42)
- Harden cockpit dashboard contracts (#42)
- Harden cockpit error handling (#42)
- Avoid pre312 f-string parse error (#42)
- Harden modular dashboard renderers (#44)
- Tighten dashboard renderer helpers (#44)
- Harden data dictionary config parsing (#45)
- Harden MCP resource contracts (#46)
- Tighten MCP resource error handling (#46)
- Expose MCP docker server on all interfaces (#46)
- Type artifact server metadata (#47)
- Clean up artifact server lifecycle (#47)
- Render cockpit artifact links correctly (#48)
- Tighten artifact server review followups (#48)
- Harden local artifact server controls (#48)
- Improve normalization dashboard detail (#49)
- Tighten artifact server error handling (#48)
- Harden normalization dashboard inputs (#49)
- Preserve normalization preview audits (#50)

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
