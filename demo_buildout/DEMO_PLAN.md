# FridAI Demo Buildout — Planning Doc

## Overview

A connected portfolio demo showing FridAI as the intelligence layer over a full ecom data stack.
Five personas, real artifacts, manufactured problems, SLM autonomy where it earns it.

**Stack involved:**

- `fridai-core` — hub, system service, MCP, sandbox
- `ecom_datalake_pipelines` — medallion lakehouse (Bronze → Base Silver → Enriched Silver → Gold)
- `analyst_toolkit` — YAML-driven modular QA pipeline + MCP server
- `analyst_toolkit_deployment_utility` — config scaffolding CLI (`analyst-infer-configs`, `analyst-deploy`)
- `model_evaluation_suite` — ML evaluation, champion validation, HTML export

---

## Personas

| Persona | Role | Profile |
|---|---|---|
| Engineer | Daily QA check on silver layers | `data_engineering` |
| Analyst | Investigates flagged data problem | `analysts` |
| Data Scientist | Model evaluation, champion validation | `data_scientist` (TBD) |
| Frontend Dev | Contract validation, schema checks | `frontend` (TBD) |

> `data_scientist` and `frontend` presets need to be created.

---

## The Demo Arc

### Nightly (Autonomous, No Human)

```
Hub cron (APScheduler in system service)
  → triggers daily_silver_qa spec
  → SLM runs in sandbox:
      gcs_read: silver_quality_{run_id}.json
      gcs_read: enriched_silver_{run_id}.json
      gcs_read: SILVER_QUALITY.md
      gcs_read: ENRICHED_QUALITY.md
      [if flagged tables] gcs_parquet_query: enriched/{table}/{partition}
      toolkit_run: diagnostics + outliers on flagged sample (via toolkit MCP server)
      HTML report + prose summary written as artifacts
  → notification sent (Slack/email/alert chat)
  → manufactured problem baked into report
```

### Morning (Engineer / Analyst In Loop)

```
Engineer/Analyst receives alert
  → reads summary + problem description
  → opens FridAI session
  → client LLM has artifact context

Engineer flow:
  → uses toolkit hub tools to dial into specific table/column
  → client LLM proposes normalization mappings (fills infer-configs judgment gap)
  → uses infer_configs hub tool to scaffold targeted YAML config
  → uses deployment utility tool to scaffold a new analysis repo

Analyst flow:
  → investigates the flagged distribution
  → client LLM walks through diagnostics + validation results
  → proposes remediation

[Optional] docker_exec:
  → client runs ad-hoc Python in ephemeral container to verify findings
  → container auto-removed after execution
```

---

## Architecture Decisions

### Where does the SLM live?

- Nightly autonomous spec: **SLM in sandbox** (autonomous, no human in loop)
- Morning interactive session: **client LLM only** (engineer/analyst in loop)
- Two distinct layers, each doing what they're good at
- The nightly run is where the A/B-tested qwen2.5-coder:7b earns its place

### Toolkit YAML config — where does it come from?

- Nightly spec: **pre-authored base YAML checked into repo** (deterministic, predictable)
- Interactive session: **`infer_configs` hub tool** generates targeted config dynamically
- Normalization judgment gap: **client LLM** proposes value mappings — the AI's natural role

### Toolkit execution — where does it run?

- **Persistent containerized MCP server** living in the `analyst_toolkit` repo
- FridAI `remote_manager` connects to it as a remote MCP server (already supported)
- Docker MCP Toolkit manages the container lifecycle
- Container is always-on, idle between calls — not ephemeral per-call
- Hub exposes toolkit tools to the client via remote tool proxying (no hub-side Python dep)

### Docker — two distinct patterns

| Pattern | What | When |
|---|---|---|
| Toolkit MCP server | Persistent containerized MCP endpoint, hub proxies via `remote_manager` (`http` or `stdio`) | Toolkit module invocation |
| `docker_exec` handler | Ephemeral container per call, Python SDK, spins up and destroys | Client ad-hoc code execution |

### Cron — where does it live?

- **Hub-owned** via APScheduler in the system service
- Config: `config/schedules.yaml` → spec_id + cron expression
- No OS cron, no Airflow dependency for the QA trigger
- Airflow handles the *pipeline* run; hub handles the *QA* trigger post-pipeline

---

## GCS Artifact Map (what the spec can read)

**Bucket: `gs://data-reporting/`**

| File | Path | Content |
|---|---|---|
| Silver quality metrics | `pipeline_metrics/{run_id}/silver_quality_{run_id}.json` | Row counts, pass rates, quarantine breakdown, contract issues |
| Enriched quality metrics | `pipeline_metrics/{run_id}/enriched_silver_{run_id}.json` | Null rates, sanity/semantic issues, row deltas per table |
| Silver markdown report | `validation_reports/{run_id}/SILVER_QUALITY.md` | Full prose, FK mismatches, status per table |
| Enriched markdown report | `validation_reports/{run_id}/ENRICHED_QUALITY.md` | Issues by table, partition values |

**Bucket: `gs://gcs-automation-project-silver/`**

| File | Path | Content |
|---|---|---|
| Enriched parquet | `data/silver/enriched/{table}/{partition_col}={date}/*.parquet` | Actual enriched silver data |
| Manifest | `data/silver/enriched/{table}/{partition}/_MANIFEST.json` | Row counts, file list — read first to avoid full glob |

> Note: dbt HTML report is NOT currently written to GCS (only lands in `/tmp/dbt_target/` locally). Skipping for demo — JSON/markdown artifacts are richer anyway.

---

## Toolkit MCP Server Design

**Location:** `analyst_toolkit/mcp_server/` (new package within existing repo)

**Framework:** MCP server implementation with transport matching host integration:
- FridAI HTTP remote integration expects JSON-RPC at `POST /rpc`
- `stdio` mode is recommended when targeting desktop MCP hosts directly

**Deployment:** Persistent container managed by Docker MCP Toolkit. FridAI `remote_manager` points at it as a remote MCP endpoint.

**Data I/O:** GCS path in → server pulls parquet → runs modules → returns JSON results + HTML. Keeps the container stateless (no shared volume needed).

**Proposed tool surface:**

| Tool | Input | Output |
|---|---|---|
| `toolkit_diagnostics` | `gcs_path`, `config` | JSON profile + HTML report |
| `toolkit_validation` | `gcs_path`, `config` | JSON validation results + HTML |
| `toolkit_outliers` | `gcs_path`, `config` | JSON outlier log + HTML |
| `toolkit_normalization` | `gcs_path`, `config` | JSON changelog + HTML |
| `toolkit_duplicates` | `gcs_path`, `config` | JSON summary + HTML |
| `toolkit_imputation` | `gcs_path`, `config` | JSON actions log + HTML |
| `toolkit_infer_configs` | `gcs_path`, `modules`, `options` | Generated YAML config strings |

**Effort:** ~2-3 days. Transport wiring (`/rpc` and/or `stdio`) + data I/O interface + Docker setup are the bulk of the work.

---

## HTML Reports for Toolkit

**Pattern source:** `model_evaluation_suite/src/model_eval_suite/utils/export_utils.py` → `_generate_html_report()` (~70 lines)

**What already exists in `analyst_toolkit`:**

- `rendering_utils.to_html_table()` — converts DataFrame to HTML string ✅
- `report_generator.py` — produces `dict[str, pd.DataFrame]` per module ✅ (right shape already)
- Inline CSS pattern from eval suite — portable ✅

**What needs building:**

- `generate_html_report(report_tables: dict, module_name: str, run_id: str) -> str` in `report_generator.py`
- `export_html_report(report_tables, path, module_name, run_id)` in `export_utils.py`
- Call it alongside existing Excel export in each module
- Embed plots as base64 (same pattern as eval suite)

**Effort:** ~1 day. Mostly execution, not design — pattern is fully established.

**Build order:** HTML reports first — the MCP server tool responses will return HTML as part of their output.

---

## Docker Security — `docker_exec` Handler

The `docker_exec` handler executes LLM-generated or LLM-directed code. This is **untrusted execution** and must be treated as such.

### Patterns to avoid

| Pattern | Why |
|---|---|
| Docker-in-Docker (DinD) | Requires `--privileged` — near-host-level kernel access, unacceptable for untrusted code |
| Docker-outside-of-Docker (DooD) | Mounts `/var/run/docker.sock` — root escalation risk, effectively host remote control |

### Safe approach for local demo (Mac)

Docker Desktop on Mac already runs containers inside a LinuxKit VM — the host kernel is not directly exposed. This gives a reasonable isolation baseline for a portfolio demo without extra infrastructure.

**Recommended pattern:** Hub runs as a native process (not containerized), spawns ephemeral containers via Docker Python SDK directly. No socket mounting into a hub container, no DinD.

```text
Hub process (native)
  → Docker Python SDK → Docker Desktop (LinuxKit VM)
    → ephemeral fridai-sandbox container (existing image, digest-pinned)
      → non-root sandbox user, allowlist-controlled handlers
      → stdout/stderr captured, container auto-removed
```

### Reuse the existing sandbox image

No custom image needed. The `fridai-sandbox` image already has everything required:

- `pandas`, `numpy`, `scipy`, `matplotlib`, `seaborn`, `pyarrow` — full data science stack ✅
- `google-cloud-storage`, `google-cloud-bigquery` — cloud access if needed ✅
- Non-root `sandbox` user (UID 1000), digest-pinned in `config/sandbox_images.yaml` ✅
- Already built, pushed to GHCR, security-hardened ✅

**Variant selection for `docker_exec`:**

| Variant | Use when |
|---|---|
| `data-write` | Pure Python analysis, no cloud access needed |
| `sensitive` | Client code needs to read from GCS mid-execution |

One image maintained, zero extra build work, digest pinning already enforced.

### Constraints on the handler

- **Pinned to `fridai-sandbox` image** — same image spec sandbox uses, no surprise dependencies
- **No network access** by default (`--network none`), override to `sensitive` variant if GCS needed
- **Read-only mounts** except `/tmp` scratch space
- **CPU/memory limits** via Docker SDK resource constraints
- **Auto-remove** on completion or timeout

### Open source + Docker licensing

- Docker Engine / Python SDK: Apache 2.0 — use freely in open source ✅
- Not distributing Docker itself — just using it as infrastructure (same as thousands of OSS projects) ✅
- Docker Desktop: free for personal/open source use; large enterprise users may need a license ✅
- **Podman** (rootless, daemonless, Apache 2.0): drop-in alternative worth noting in docs for users who prefer it — same Dockerfile, same SDK interface ✅

### Production upgrade path

If this moves beyond local demo: Podman rootless, gVisor, or Firecracker microVMs for real isolation. Cloud Run Jobs / Kubernetes Jobs for remote execution. The handler interface stays the same — only the backend changes.

---

## Build List

| Item | Effort | Repo | Status | Notes |
|---|---|---|---|---|
| **HTML reports for toolkit** | Small (~1d) | `analyst_toolkit` | Not started | Port `_generate_html_report()` from eval suite; builds first, MCP server returns HTML |
| **Toolkit MCP server** | Medium (~2-3d) | `analyst_toolkit` | Not started | `mcp_server/` package, containerized, GCS path I/O, transport contract (`/rpc` and/or `stdio`) |
| Hub cron scheduler | Small | `fridai-core` | Not started | APScheduler in system service, `config/schedules.yaml` |
| `daily_silver_qa` spec YAML | Medium | `fridai-core` | Not started | SLM autonomous, pulls GCS artifacts + runs toolkit on flagged tables |
| Notification wiring | Small | `fridai-core` | Partial | `notify: true` exists, check what channels are live |
| `infer_configs` hub tool | Small | `fridai-core` | Not started | Plugin wrapping `analyst-infer-configs` programmatic API |
| Expand `infer-configs` | Medium | `analyst_toolkit_deployment_utility` | Not started | See gaps section |
| `data_scientist` preset | Small | `fridai-core` | Not started | New persona profile YAML |
| `frontend` preset | Small | `fridai-core` | Not started | New persona profile YAML |
| `docker_exec` handler | Medium | `fridai-core` | Not started | Hub native → Docker SDK → ephemeral `fridai-sandbox` container (reuses existing image, digest-pinned) |
| Persona workflow specs | Large | `fridai-core` | Not started | Analyst, data scientist, frontend demo specs |
| Pre-author base toolkit YAML | Small | `analyst_toolkit` | Not started | Base config for nightly spec, checked into repo |
| Manufacture demo problem | Small | `ecom_datalake_pipelines` | Not started | Inject anomaly into enriched silver — candidate: null spike in `int_returns_risk.return_reason` |

---

## `infer-configs` Expansion Gaps

Currently generates **3 of 9 required configs** (validation, certification, outlier detection).

**Missing configs** (need template generation at minimum):

- `diag_config.yaml` — diagnostics/profiling settings
- `normalization_config.yaml` — rename, map, fuzzy match rules (client LLM fills judgment gap interactively)
- `dups_config.yaml` — dedup strategy
- `handling_config.yaml` — outlier treatment (clip, median, constant, drop)
- `imputation_config.yaml` — missing value fill strategies
- `final_audit_config.yaml` — final certification + drop columns

**Input handling gap:**

- Currently only accepts local CSV in `data/raw/`
- Needs to accept GCS path or Parquet for toolkit MCP server use

**Other gaps:**

- No `--modules` flag (generates same 3 every time, no targeting)
- No `run_toolkit_config.yaml` master orchestration file generated

---

## Open Questions

- [ ] `run_id` resolution for nightly spec — pipeline writes a `_latest_run.json` pointer, or run_id passed as schedule param? Check if pipeline writes any "latest" pointer to GCS.
- [ ] Notification channels — what's actually wired in system service? Slack? Email? Check `notify: true` implementation before assuming.
- [ ] `docker_exec` surface area — confirmed: reuse `fridai-sandbox` image (already has full data science stack). `data-write` variant for pure analysis, `sensitive` for GCS access. No custom image needed. ✅
- [ ] Manufactured problem specifics — null spike in `int_returns_risk.return_reason` is a candidate. Must be detectable by toolkit modules but not caught by the pipeline's own validators.
- [x] `remote_manager` config for toolkit MCP server — local HTTP example:
  `transport: http`, `base_url: "http://127.0.0.1:8080"` (server must expose `POST /rpc`).
- [ ] Production upgrade path for `docker_exec` — Podman rootless, gVisor, or Cloud Run Jobs. Not needed for demo but worth noting in design.
