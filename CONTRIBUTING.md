# Contributing to Analyst Toolkit

Thanks for contributing.

## Development Setup

1. Fork and clone the repo.
2. Install dev dependencies:

```bash
make install-dev
```

3. (Optional) Install MCP runtime dependencies for local MCP server testing:

```bash
make install-mcp
```

## Branch and PR Workflow

1. Create a branch from `main`.
2. Keep changes scoped to one concern per PR.
3. Open a pull request using the PR template.
4. Link related issues in the PR description (`Closes #123`).
5. If behavior, API, or docs meaningfully change, add an entry to `CHANGELOG.md` under `Unreleased`.

## Quality Gates

Before opening or updating a PR, run:

```bash
ruff check src/
mypy src tests
pytest tests/
```

Or run the repo shortcut:

```bash
make check
```

Also run pre-commit hooks:

```bash
pre-commit run --all-files
```

CI enforces linting, type checks, tests, and Docker smoke tests.

## Testing Guidance

- Add or update tests for behavior changes.
- Prefer focused unit tests near the changed module.
- For MCP tool changes, include regression coverage in `tests/test_mcp_server.py` and/or `tests/test_mcp_tool_regressions.py`.

## MCP and Data Pipeline Changes

- Preserve stable response contracts where possible.
- If a response field changes shape, document it in the PR.
- Avoid introducing hidden behavior changes; make config-driven behavior explicit.
- Keep `run_id` and `session_id` lifecycle behavior deterministic and test-covered.

## Commit Message Guidance

Use clear, imperative commit messages. Example:

- `fix: harden run history JSON parsing`
- `feat: add strict preflight unknown-key validation`
- `docs: add contribution and issue templates`

## Changelog Policy (Deterministic)

- `CHANGELOG.md` is the source of truth for release notes.
- Keep section order fixed for every release:
  - `Added`, `Changed`, `Fixed`, `Deprecated`, `Removed`, `Security`
- Add entries under `## [Unreleased]`.
- Write one bullet per atomic change.
- Prefer user-visible behavior and contract changes over internal refactors.
- Move `Unreleased` entries into a versioned section during release cut.

## Release Announcement Automation

- Publishing a GitHub Release triggers `.github/workflows/discussions-announcement.yml`.
- The workflow posts to the Discussions category named `Announcements` by default.
- Override category name with repository variable `DISCUSSIONS_ANNOUNCEMENTS_CATEGORY`.
- Ensure the target Discussions category exists before cutting a release.

## Security and Secrets

- Do not commit credentials, tokens, or `.env` secrets.
- If you discover a security issue, do not open a public issue with exploit details. Contact the maintainer directly first.

## Documentation

When behavior changes, update docs in:

- `README.md`
- `resource_hub/`
- relevant config templates in `config/`
