# AGENTS.md

Repository guidance for coding agents working in `analyst_toolkit`.

## Scope

This file applies to the entire repository unless a deeper `AGENTS.md` overrides it.

Treat this file as the stable repo contract for agents.
Keep temporary rollout notes and active-phase sequencing out of this file.

For local phase-specific notes, use:
- `local_plans/AGENT_NOTES.md`

## Operating Values

Optimize for:
1. Reviewable, incremental changes
2. Secure defaults and explicit trust boundaries
3. Deterministic and idempotent behavior
4. Compact contracts and artifact-first outputs
5. Reuse of existing pipeline/reporting logic over parallel implementations

## Branch And PR SOP

### Git **Flow**

```text
main  (stable, public-facing — never commit directly)
  └── dev  (active working branch — integration point)
        └── feat/<scope>  (scoped PR branch — one concern per branch)
        └── fix/<scope>
        └── refactor/<scope>
        └── docs/<scope>
        └── chore/<scope>
```

Rules:
- `main` is never touched directly — it receives PRs from `dev` only when stable
- `dev` is the integration branch — all scoped PR branches merge here first
- scoped branches cut from `dev`, not from `main`
- one branch = one reviewable unit of change
- keep PRs scoped to a single concern or a single reviewable slice
- prefer a sequence of small PRs over one large umbrella PR
- if behavior changes, tests must land in the same PR

Merge policy:
- default to `rebase merge`
- avoid merge commits
- use `squash merge` only as an exception
- keep commit messages intentional enough that rebased history remains useful

### CodeRabbit Review Workflow

When CodeRabbit reviews a PR, triage each finding into one of these categories — in order:

1. **Fix in this PR** — the finding is valid, in scope, and actionable. Resolve it before merge.
2. **Resolve in conversation** — you disagree with the finding or it does not apply. Reply in the PR thread explaining why, then mark the comment resolved so reviewers can see it was considered.
3. **Log as a GitHub issue** — the finding is valid but out of scope for this PR. Open a GitHub issue so it is tracked and not lost. Link the issue in the PR thread. Do not let valid findings disappear because they are inconvenient to fix right now.
4. **Dismiss** — CodeRabbit misunderstood the code or the suggestion is not applicable. Dismiss with a brief explanation in the PR comment.

Do not merge with unresolved category-1 findings. Every other finding must be explicitly triaged into one of the remaining categories before merge — no silently ignoring feedback.

## Local Quality Gate

Before opening or updating a PR:
- run `pre-commit run --all-files`
- run `ruff check src/ tests/`
- run `ruff format --check src/ tests/`
- run `python -m yamllint .github/workflows .coderabbit.yaml`
- run `mypy src/analyst_toolkit/mcp_server`
- run targeted tests for the changed area at minimum
- run broader tests when shared MCP or reporting layers change
- run local CodeRabbit review when available:
  - `coderabbit review --plain --no-color --type uncommitted`

Before merging:
- CI must be green
- CodeRabbit feedback should be reviewed and resolved or consciously dismissed
- docs must be updated when behavior, contracts, or defaults change

## MCP Guidance

Relevant code lives under:
- `src/analyst_toolkit/mcp_server/`

Default posture:
- prefer secure defaults
- do not silently widen the trust boundary
- treat local filesystem access as privileged in HTTP mode
- prefer localhost-first HTTP posture and explicit opt-in for remote exposure
- client-visible errors should prefer stable codes and `trace_id` over raw internal exceptions
- prefer compact response shapes and artifact references over large in-band payloads

Do not treat retry resilience as idempotency.
If retries can change artifact paths, URLs, job identity, or run namespace, the behavior is not idempotent yet.

Do not introduce new defaults that break deterministic reruns unless the change is explicit and documented.

## Dashboard And Reporting Guidance

Relevant code lives under:
- `src/analyst_toolkit/m00_utils/`
- `src/analyst_toolkit/m01_diagnostics/`
- `src/analyst_toolkit/m02_validation/`
- `src/analyst_toolkit/m03_normalization/`
- `src/analyst_toolkit/m04_duplicates/`
- `src/analyst_toolkit/m05_detect_outliers/`
- `src/analyst_toolkit/m06_outlier_handling/`
- `src/analyst_toolkit/m07_imputation/`
- `src/analyst_toolkit/m10_final_audit/`

Rules:
- notebook-exported HTML in `notebooks/*.html` is design reference only
- do not scrape nbconvert output into production artifacts
- prefer shared standalone renderers over duplicated export logic
- keep module dashboards composable so future combined dashboards can be built from module outputs
- prefer standalone HTML artifacts over fragmented dashboard bundles unless there is a clear need otherwise

If reporting or export UX changes materially:
- add or update regression tests
- regenerate representative artifacts
- inspect the rendered outputs, not just the unit tests

## Planning Docs

Place planning documents in `local_plans/`.

Use descriptive filenames with explicit dates for review or implementation waves, for example:
- `MCP_SERVER_REVIEW_REMEDIATION_PLAN_YYYY-MM-DD.md`
- `MCP_EXPORTABLE_DASHBOARD_PR_IMPLEMENTATION_PLAN_YYYY-MM-DD.md`

## Editing Guidance

Prefer small, composable refactors over broad rewrites.

When extracting shared logic:
- preserve behavior first
- keep notebook wrappers thin
- move reusable logic into pure functions with minimal side effects
