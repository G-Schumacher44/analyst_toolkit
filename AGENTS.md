# AGENTS.md

Repository guidance for coding agents working in `analyst_toolkit`.

## Scope

This file applies to the entire repository unless a deeper `AGENTS.md` overrides it.

## Project Priorities

When working on the MCP server, optimize for these in order:

1. Security hardening
2. Deterministic and idempotent behavior
3. Compact MCP response contracts
4. Reuse of existing pipeline/reporting logic over parallel implementations
5. Backwards-compatible migrations where practical

When working on dashboard/reporting features, optimize for these in order:

1. Preserve the tone and structure of the current notebook dashboards
2. Make the new dashboard the primary HTML export for both CLI and MCP
3. Produce exportable standalone HTML artifacts
4. Prefer a single self-contained HTML file with embedded PNGs by default
5. Lay composition groundwork for a future combined dashboard without making it phase-1 scope
4. Keep MCP tool responses compact and artifact-first
6. Avoid Jupyter-only runtime dependencies in exported artifacts

## Repo Conventions

### Planning Docs

Place planning documents in `local_plans/`.

Use descriptive filenames with an explicit date when the document is tied to a review or implementation wave, for example:
- `MCP_SERVER_REVIEW_REMEDIATION_PLAN_YYYY-MM-DD.md`
- `MCP_EXPORTABLE_DASHBOARD_PR_IMPLEMENTATION_PLAN_YYYY-MM-DD.md`

Do not create ad hoc planning files in random directories.

### Branch and PR SOP

Use short-lived branches from `main`.

Preferred branch prefixes:
- `feat/<scope>`
- `fix/<scope>`
- `refactor/<scope>`
- `docs/<scope>`
- `chore/<scope>`

Examples:
- `feat/mcp-dashboard-renderer`
- `fix/mcp-http-default-hardening`
- `refactor/validation-dashboard-model`

Branch rules:
- one branch should represent one reviewable unit of change
- avoid umbrella branches that mix renderer work, contract changes, hardening, and docs unless the work is truly inseparable
- branch names should describe the outcome, not the implementation detail dump

PR rules:
- open a draft PR early for non-trivial work
- keep each PR scoped to a single concern or a single reviewable slice
- if a reviewer cannot understand the change in one sitting, the PR is probably too large
- prefer a sequence of small PRs over one large PR when workstreams are separable
- if behavior changes, tests must land in the same PR

Merge policy:
- default to `rebase merge`
- avoid merge commits
- use `squash merge` only as an exception for trivial cleanup or a branch whose intermediate commits are not worth preserving
- keep commit messages intentional enough that rebased history remains useful

Recommended PR structure:
1. Summary: what changed and why
2. Scope: what is intentionally not included
3. Validation: tests, checks, artifacts, screenshots if relevant
4. Contract/behavior impact: MCP fields, artifact paths, defaults, migration notes
5. Reviewer focus: where risk is concentrated

Before opening or updating a PR:
- run `pre-commit run --all-files`
- run `ruff format --check src/ tests/`
- run `yamlint .github/workflows .coderabbit.yaml`
- run `mypy src/analyst_toolkit/mcp_server`
- run targeted tests for the changed area at minimum
- run broader checks if the change affects shared MCP or reporting layers
- request CodeRabbit review if CodeRabbit is enabled for the repository
- for local CLI review, use plain output:
  - `coderabbit review --plain --no-color --type uncommitted`

Before merging a PR:
- CI must be green
- `pre-commit` must already have been run locally
- CodeRabbit feedback should be reviewed and resolved or explicitly dismissed with reason if CodeRabbit is enabled
- docs should be updated if behavior, contracts, or defaults changed

Recommended PR slicing for this repo:
- shared renderer or shared contract changes first
- then module adoption PRs
- then MCP response/doc updates

Avoid:
- mixing MCP security hardening with dashboard renderer refactors in one PR
- mixing response compaction with unrelated visual restyling
- pushing large unreviewed branches directly to `main`

### MCP Server Work

Relevant code lives under:
- `src/analyst_toolkit/mcp_server/`

For MCP work, inspect these layers before changing behavior:
- transport and auth: `server.py`, `auth.py`, `rpc_dispatch.py`
- registry/response shape: `registry.py`, `response_utils.py`
- persistence and IO: `io.py`, `io_storage.py`, `io_serialization.py`, `state.py`, `job_state.py`
- tool wrappers: `src/analyst_toolkit/mcp_server/tools/*.py`

### Dashboard and Report Work

Relevant code lives under:
- `src/analyst_toolkit/m00_utils/`
- `src/analyst_toolkit/m01_diagnostics/`
- `src/analyst_toolkit/m02_validation/`
- `src/analyst_toolkit/m03_normalization/`
- `src/analyst_toolkit/m04_duplicates/`
- `src/analyst_toolkit/m05_detect_outliers/`
- `src/analyst_toolkit/m07_imputation/`
- `src/analyst_toolkit/m10_final_audit/`

Notebook-exported HTML in `notebooks/*.html` is design reference only, not a production artifact source.

If you need the current dashboard style, derive it from the module display renderers, not from nbconvert output.

## MCP Hardening Guidance

### HTTP Posture

Treat HTTP mode as higher risk than stdio mode.

Preferred defaults:
- localhost bind by default
- remote HTTP exposure is supported, but only via explicit opt-in
- explicit opt-in for unauthenticated HTTP
- explicit documentation for local-only vs networked deployment

Do not widen the trust boundary silently.

### Filesystem Access

Local file reads and writes should be treated as privileged operations in HTTP mode.

If adding or modifying local file access:
- prefer explicit allowlists/roots
- keep GCS behavior separate from local path behavior
- fail clearly on rejected paths

### Errors

Client-visible responses should not expose raw internal exception strings in generic failure paths.

Prefer:
- stable machine-readable codes
- human-usable remediation
- `trace_id`

Keep raw exception detail in logs.

### Idempotency

Do not equate retry resilience with idempotency.

If a retry can change:
- output path
- artifact URL
- job identity
- run namespace

then it is not idempotent yet.

Prefer explicit collision policies and idempotency keys over silent random fallback behavior.

Current default policy decisions:
- default output collision policy: `overwrite`
- async job dedupe: exact input match plus same `idempotency_key`

### Response Size

MCP tool responses should default to compact shapes.

Avoid returning large nested structures by default, including:
- full effective configs
- full YAML blobs
- large violation detail payloads
- full persisted async job results
- large duplicated fields at both top level and inside `summary`

If verbose detail is useful, make it opt-in.

Also consider the persistence effect: if a tool response is large and is appended to run history, future MCP reads get larger too.

## Dashboard Export Guidance

### Artifact Strategy

Preferred default:
- one standalone HTML file
- PNG plots embedded as base64 if they are part of the dashboard
- embedded PNGs should be automatic in `single_html` mode when plots were generated
- plotting remains off by default unless the module/config enables it
- emit a soft warning when the HTML artifact grows beyond roughly `25 MB`

Sidecar PNGs can still exist for reuse/debugging, but they should not be the primary delivery mode unless explicitly needed.

Zip bundles are optional and should not be the default unless artifact size becomes a real issue.

### Implementation Strategy

Prefer this flow:
1. build a pure dashboard view model
2. render it through a shared standalone HTML renderer
3. reuse the same semantic structure in notebook and export modes
4. make module dashboards composable so a future combined dashboard can be built on top of the same contract

Do not:
- treat Jupyter widget output as exportable HTML
- scrape notebook-exported HTML
- duplicate dashboard logic separately for MCP and notebook unless there is a clear reason
- delay per-module dashboard delivery waiting for a combined dashboard implementation

Combined dashboard policy:
- phase 1 delivers per-module dashboards
- phase 1 should lay groundwork for composition
- any combined dashboard should be introduced later as a separate CLI/MCP tool built from module outputs, not by coupling all module exports now

### Starting Points

For exportable dashboard work, the preferred implementation order is:
1. diagnostics
2. validation
3. final audit
4. normalization
5. duplicates
6. outliers
7. imputation

Rationale:
- diagnostics and validation already have strong notebook-style layouts
- final audit is the best executive-style artifact once the shared renderer exists
- outliers and imputation currently lean more on widget-based plot browsing and should come after the shared static export path is proven

## Editing Guidance

Prefer small, composable refactors over broad rewrites.

When extracting shared logic:
- preserve current public behavior first
- keep notebook wrappers thin
- move reusable logic into pure functions with minimal side effects

If adding new renderer or contract layers, choose names that are explicit rather than generic.

Examples:
- `build_validation_dashboard(...)`
- `render_dashboard_document(...)`
- `compact_history_entry(...)`

## Testing Expectations

For MCP changes, add or update tests in:
- `tests/mcp_server/`
- `tests/hardening/`
- `tests/test_mcp_tool_regressions.py`

For dashboard export changes, add:
- focused renderer/export tests
- at least one module-level artifact regression test for the module being changed
- do not defer export/render regression tests to a follow-up PR

When behavior is contract-sensitive, prefer assertion of:
- key response fields
- artifact path behavior
- verbosity/compaction behavior
- embedded plot presence or absence

Do not rely only on visual/manual inspection when deterministic assertions are possible.

Required testing rule:
- no MCP hardening PR without regression coverage for the changed behavior
- no dashboard export/refactor PR without renderer or artifact regression coverage
- do not use "tests later" as the default plan for code changes in this repo

## Docs Expectations

If a change alters any of these, update docs in the same workstream:
- MCP auth/bind defaults
- local path restrictions
- output collision policy
- idempotency key behavior
- dashboard artifact format
- response verbosity contract

Primary docs to update when relevant:
- `resource_hub/mcp_server_guide.md`
- `README.md`
- `CHANGELOG.md`
- `CONTRIBUTING.md`

## Review Expectations

When asked for a review, focus on:
- security exposure
- behavioral regressions
- idempotency gaps
- response-contract bloat
- missing tests

Summaries are secondary to findings.
