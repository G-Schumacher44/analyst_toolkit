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

- Added contributor workflow documentation in `CONTRIBUTING.md`.
- Added GitHub issue templates for bug reports, feature requests, and documentation updates.
- Added a pull request template with validation and MCP contract checks.
- Added `SECURITY.md` with a responsible disclosure policy.

### Changed

- Updated `README.md` with pointers to contributing, support, and templates.

### Fixed

- None.

### Deprecated

- None.

### Removed

- None.

### Security

- Added explicit security reporting guidance and secret-handling expectations.

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

