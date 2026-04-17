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

## [0.5.1] - 2026-04-17

### Fixed

- Remove top-level `anyOf`/`oneOf` from MCP tool schemas (`auto_heal`, `infer_configs`, `cockpit_schemas`, `drift`) to comply with OpenAI API stricter tool schema validation; fixes Codex CLI startup failure (#181).
- Pipeline dashboard iframes now prefer local file server URLs over remote GCS URLs when `destination_delivery` provides them, so embedded reports load in the browser (#181).

### Security

- Acknowledge `CVE-2025-71176` (pytest 8.x) pending pytest 9 compatibility review.

## [0.5.0] - 2026-03-30

### Added

- Add MCP input ingest subsystem
- Add manage_session MCP tool for session lifecycle management
- Add upload_input and read_artifact MCP tools for container isolation
- Add session memory guardrails and manage_session clear action
- Auto-discover inferred configs from session in all module tools
- Add sqlite session backend

### Changed

- Clean up MCP env-var wall of text in README
- Clarify data dictionary input schema semantics
- Standardize input_id schema usage
- Publish shared input_id schema fragment
- Lead clients through input ingest first
- Enumerate all MCP environment variables in .envrc.example
- Pass all MCP env variables through docker-compose
- Add manage_session to agent/user-facing resources
- Add manage_session to cockpit dashboard launchpad and launch sequences
- Update changelog, add stdio-aware input guidance and session clear to resources
- Dedupe input contract literals
- Split MCP IO regressions
- Split infer config regressions
- Add release profiles and governance
- Align release docs with auth posture
- Pin patched pygments for audit
- Add CI coverage release gate
- Always upload coverage artifacts
- Add shared pipeline config validation helpers
- Polish runner validation tests
- Normalize config and CLI surface text
- Normalize validation gatekeeper messaging

### Fixed

- Address ingest review followups
- Tighten ingest review followups
- Harden input registry idempotency semantics
- Harden ingest trust boundaries and errors
- Tighten ingest hardening followups
- Align data dictionary input schema
- Align data dictionary run_id default
- Tolerate older infer_configs helper signatures
- Flatten quickstart tool response contract
- Map generated infer configs into module payloads
- Harden infer config result normalization
- Restore full infer_configs module coverage for MCP workflows
- Align artifact publication and routing for MCP module outputs
- Separate artifact delivery warnings from status-affecting warnings
- Preserve certification as distinct module in infer_configs
- Handle load_input failures in infer_configs and pass run_id to session
- Show disabled instead of missing for report artifacts when no work done
- Auto-discover inferred certification config in final_audit
- Lift certification.rules shorthand in final_audit config normalizer
- Strip stale temp paths from inferred config in final_audit
- Resolve session_id from input_id for config discovery in final_audit
- Allow input_id + session_id in infer_configs and resolve session from descriptor
- Strip stale /tmp paths from provided config and auto-create output dirs in final_audit
- Prevent path traversal in final_audit output directory creation
- Remove traversal paths from pipeline config, not just block mkdir
- Run TTL cleanup before fork/rebind/list and avoid run_id collisions
- Resolve doubled exports path and surface allowed input roots
- Redact allowed input roots from client errors by default
- Restrict read_artifact to artifact root only in HTTP mode
- Update agent resources, docker-compose, and env config
- Advertise 127.0.0.1 in artifact URLs when bound to 0.0.0.0
- Strip exports prefix from relative paths and rename content field
- Doubled exports path for relative inputs, read_artifact field name, and add next_actions hints to ingestion errors
- Guide agents to HTTP upload for large files
- Auto-enable local defaults in stdio mode and resolve config paths from package root
- Evaluate trusted history at call time and require infer_configs in pipeline docs
- Harden pipeline core regressions
- Harden joblib and notebook trust boundaries
- Avoid tabulate dependency in markdown fallback
- Harden cockpit history contracts
- Bound long-running state growth
- Harden infer config contracts
- Align artifact and dashboard contracts
- Align cockpit health and template contracts
- Guard malformed cockpit summaries
- Advertise mcp templates and validate auto_heal input ids
- Harden input ids and client error codes
- Bound MCP input loading
- Harden input guardrail errors
- Surface session retention policy
- Add on-demand session config retrieval
- Trim redundant session inspect actions
- Harden sqlite session backend surface
- Tighten sqlite session path defaults
- Harden shared config validation helpers
- Fail early on invalid pipeline configs
- Reject non-mapping runner configs
- Polish artifact server and plotting runtime

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
