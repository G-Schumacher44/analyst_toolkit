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

1. Create a branch from `dev`.
2. Keep changes scoped to one concern per PR.
3. Open a pull request targeting `dev`.
4. Link related issues in the PR description (`Closes #123`).
5. If CodeRabbit reviews the PR, triage every finding before merge (see [CodeRabbit Review Workflow](AGENTS.md#coderabbit-review-workflow)).
6. If behavior changes, add or update tests in the same PR.
7. Promote `dev` into `main` only after the integration slice is green and ready to release publicly.

## Quality Gates

Before opening or updating a PR, run:

```bash
ruff check src/ tests/
ruff format --check src/ tests/
yamllint .github/workflows .coderabbit.yaml
mypy src/analyst_toolkit/mcp_server
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

CodeRabbit is included in the PR review loop. Triage its findings using the [CodeRabbit Review Workflow](AGENTS.md#coderabbit-review-workflow) before merge.

CI enforces linting, type checks, tests, and Docker smoke tests.

## Testing Guidance

- Add or update tests for behavior changes.
- Prefer focused unit tests near the changed module.
- For MCP tool changes, include regression coverage in `tests/mcp_server/` and/or `tests/test_mcp_tool_regressions.py`.
- Do not defer test coverage for code behavior changes to a follow-up PR unless the current PR is strictly docs-only.

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

## Changelog Policy

`CHANGELOG.md` is generated deterministically from git history using `scripts/generate_changelog.py`. Commit prefixes (`feat:`, `fix:`, `refactor:`, etc.) map directly to changelog sections.

- **Preview** the next entry: `make changelog`
- **Write** a versioned entry: `make changelog-write VERSION=0.4.5`

Because the changelog is derived from commits, keep commit messages clear and imperative — they become the release notes. No manual changelog edits are needed for individual PRs.

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
