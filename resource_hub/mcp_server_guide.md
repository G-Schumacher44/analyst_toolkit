<p align="center">
  <img src="../repo_files/analyst_toolkit_banner.png" alt="Analyst Toolkit Logo" width="1000"/>
  <br>
  <em>Analyst Toolkit — MCP Server Guide</em>
</p>
<p align="center">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-stable-brightgreen">
  <img alt="Version" src="https://img.shields.io/badge/version-v0.4.4-blueviolet">
  <a href="https://github.com/G-Schumacher44/analyst_toolkit/actions/workflows/analyst-toolkit-mcp-ci.yml">
    <img alt="CI" src="https://github.com/G-Schumacher44/analyst_toolkit/actions/workflows/analyst-toolkit-mcp-ci.yml/badge.svg">
  </a>
  <img alt="GHCR" src="https://img.shields.io/badge/ghcr.io-analyst--toolkit--mcp-blue?logo=docker">
</p>

---

# 📡 MCP Server Guide

The analyst toolkit MCP server exposes every toolkit module as a callable tool over the [Model Context Protocol](https://modelcontextprotocol.io). Any MCP-compatible host — FridAI, Claude Desktop, VS Code, or a plain JSON-RPC 2.0 client — can invoke toolkit operations against local or GCS-hosted data without any Python dependency on the host side.

## 🆕 Version 0.4.4 Highlights

- **Full Dashboard Surface:** Every pipeline module now produces a standalone HTML dashboard artifact — diagnostics, validation, normalization, duplicates, outlier detection, outlier handling, imputation, auto-heal, data dictionary, and final audit. Dashboard artifact paths are returned in tool responses.
- **Cockpit Hub:** `get_cockpit_dashboard` returns a unified operator hub linking recent-run cards, module dashboards, artifact rows, and a data dictionary preview into a single HTML artifact.
- **Local Artifact Server:** `ensure_artifact_server` starts an optional localhost file server so cockpit and module artifact references become browser-openable URLs instead of raw file paths.
- **Data Dictionary:** `data_dictionary` is now fully implemented — generates a column-level schema report as a standalone HTML artifact and surfaces a preview in the cockpit dictionary tab.
- **Pipeline Dashboard:** `get_pipeline_dashboard` produces a combined multi-module dashboard for a specific run, linked from the cockpit hub.
- **Resource Inventory:** Quickstart, agent playbook, and capability catalog are now exposed as first-class MCP resources (in addition to tools) via `resources/list` and `resources/read`.
- **Observability + Auth:** `/ready`, `/metrics`, structured lifecycle logs (`ANALYST_MCP_STRUCTURED_LOGS`), and optional bearer token auth (`ANALYST_MCP_AUTH_TOKEN`).
- **Manual Pipeline:** Recommended workflow — diagnostics → infer → normalize → dedupe → outliers → impute → validate → final audit.
- **GCS Direct File Loading:** Pass a direct `.parquet` or `.csv` GCS URI — no trailing slash required.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Deployment Profiles](#deployment-profiles)
- [Operability Endpoints](#operability-endpoints)
- [Tool Reference](#tool-reference) ▾
- [Pipeline Mode](#pipeline-mode-state-management)
- [Template Resources](#template-resources)
- [Local Artifact Server](#local-artifact-server)
- [Usage Examples](#usage-examples) ▾
- [Host Integration](#host-integration) ▾
- [Environment Variables](#environment-variables)
- [GCS Data Loading](#gcs-data-loading)

---

## Quick Start

**Prerequisites:** Docker or Podman. GCS credentials if reading from GCS.

```bash
# Clone and build
git clone https://github.com/G-Schumacher44/analyst_toolkit.git
cd analyst_toolkit

# Start the server
docker compose -f docker-compose.mcp.yml up --build
```

Or pull from GHCR and run directly:

```bash
docker pull ghcr.io/g-schumacher44/analyst-toolkit-mcp:latest

docker run -p 8001:8001 \
  -v ~/.secrets/gcp_creds.json:/secrets/gcp_creds.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp_creds.json \
  ghcr.io/g-schumacher44/analyst-toolkit-mcp:latest
```

Verify the server and list available tools:

```bash
curl http://localhost:8001/health | python3 -m json.tool
```

```json
{
  "status": "ok",
  "version": "0.4.4",
  "uptime_sec": 42,
  "tools": [
    "diagnostics", "validation", "outliers", "normalization",
    "duplicates", "imputation", "infer_configs", "auto_heal",
    "drift_detection", "get_config_schema", "preflight_config", "get_golden_templates",
    "get_job_status", "list_jobs",
    "get_agent_playbook", "get_user_quickstart", "get_capability_catalog",
    "get_run_history", "get_data_health_report",
    "data_dictionary", "get_pipeline_dashboard", "get_cockpit_dashboard",
    "ensure_artifact_server", "manage_session",
    "upload_input", "read_artifact"
  ]
}
```

## Deployment Profiles

Choose an operating mode explicitly. The server is designed to be frictionless locally and explicit when you widen the trust boundary.

| Profile | Host/Auth Posture | Intended Use | Minimum Expectations |
| --- | --- | --- | --- |
| `local-dev` | loopback bind, auth token optional | local development, desktop MCP hosts, local FridAI integration | localhost-only exposure, local review workflow |
| `internal-trusted` | explicit non-loopback bind, bearer token strongly recommended | private team/internal network deployment | documented environment, normal network controls, token policy applied by operator |
| `public-or-prod` | explicit non-loopback bind, bearer token strongly recommended | managed or internet-reachable deployment | secure deployment review complete, token policy applied by operator, operator docs reviewed |

Recommended environment posture:

| Setting | `local-dev` | `internal-trusted` | `public-or-prod` |
| --- | --- | --- | --- |
| `ANALYST_MCP_HOST` | default loopback | explicit non-loopback | explicit non-loopback |
| `ANALYST_MCP_AUTH_TOKEN` | optional | strongly recommended | strongly recommended |
| `ANALYST_MCP_ENABLE_ARTIFACT_SERVER` | optional | optional | only if intentionally exposed |
| `ANALYST_MCP_ARTIFACT_SERVER_HOST` | loopback | loopback unless justified | loopback unless explicitly reviewed |
| `ANALYST_MCP_SESSION_BACKEND` | `memory` or explicit `sqlite` | prefer `memory` unless operators accept durable local state | prefer `memory` unless durable local state is explicitly reviewed |

Release note:
- The toolkit is production-oriented, but production claims should only be made for the deployment profile that matches the actual tested posture.
- Current runtime behavior is localhost-first and logs when `ANALYST_MCP_AUTH_TOKEN` is unset on non-loopback binds. It does not currently hard-fail startup in that posture.
- Treat HTTP access to local files and local artifact paths as privileged. If you intentionally expose non-loopback HTTP, pair it with token auth and normal network controls.
- Enabling `ANALYST_MCP_SESSION_BACKEND=sqlite` writes durable session state to the local filesystem at `ANALYST_MCP_SESSION_DB_PATH`. That is an explicit trust-boundary expansion: keep filesystem permissions narrow, prefer localhost binding, and do not persist long-lived sensitive session payloads unless the operator intentionally accepts that posture.

## Operability Endpoints

Use these endpoints for runtime checks and automation diagnostics:

```bash
# Liveness + server metadata + registered tools
curl http://localhost:8001/health | python3 -m json.tool

# Readiness probe
curl http://localhost:8001/ready | python3 -m json.tool

# Runtime counters and latency summary
curl http://localhost:8001/metrics | python3 -m json.tool
```

If `ANALYST_MCP_AUTH_TOKEN` is configured, include:

```bash
-H "Authorization: Bearer <token>"
```

`/metrics` response shape (JSON):

```json
{
  "rpc": {
    "requests_total": 120,
    "errors_total": 3,
    "avg_latency_ms": 42.7,
    "by_method": {"tools/call": 98, "tools/list": 12, "initialize": 10},
    "by_tool": {"diagnostics": 24, "validation": 18}
  },
  "uptime_sec": 3600
}
```

## Operational Triage (Quick Runbook)

When diagnosing failures, use `trace_id` from the JSON-RPC error payload and correlate with server logs.

| Signal | Likely Cause | Operator Action |
|---|---|---|
| JSON-RPC `-32700 Parse error` | malformed request body | validate client payload and retry |
| JSON-RPC `-32601 Tool not found` | tool name mismatch/client prefix issue | verify `tools/list`, then correct tool name |
| `resources_*_timeout` envelope code | template/resource I/O timeout | increase `ANALYST_MCP_RESOURCE_TIMEOUT_SEC` or `ANALYST_MCP_TEMPLATE_IO_TIMEOUT_SEC`; verify storage latency |
| JSON-RPC `-32603 Internal error` | tool/runtime exception | inspect logs by `trace_id`, rerun once, then escalate with run evidence |

> **Note:** Most MCP clients (Claude Desktop, FridAI) will prefix these with `toolkit_`, e.g. `toolkit_diagnostics`. The server registers them without the prefix to avoid double-prefixing.

---

<details>
<summary><strong>🛠️ Tool Reference</strong></summary>

### Core Pipeline Tools

| Tool | Description |
|---|---|
| `diagnostics` | Data profiling — types, nulls, cardinality, distribution summary |
| `validation` | Schema validation against expected columns, types, and rules |
| `normalization` | String cleaning, type casting, datetime parsing, rename mapping |
| `duplicates` | Duplicate detection and removal or flagging by subset key |
| `outliers` | Outlier detection via IQR and z-score methods |
| `imputation` | Missing value imputation (mean, median, mode, constant, KNN) |

### Autonomous Tools

| Tool | Description |
|---|---|
| `infer_configs` | Predicts recommended module configs from data characteristics |
| `auto_heal` | One-click: runs `infer_configs` → `normalization` → `imputation` |
| `drift_detection` | Compares two datasets for schema and statistical drift |
| `get_config_schema` | Returns the JSON Schema for any module's config |
| `preflight_config` | Normalizes candidate config and returns effective runtime shape; supports `strict=true` to fail on warnings/unknown top-level keys |
| `get_job_status` | Poll status/result for async jobs (`job_id`) |
| `list_jobs` | List recent async jobs and optionally filter by state (job state persists across restarts) |

### Cockpit + Dashboard Tools

| Tool | Description |
|---|---|
| `get_data_health_report` | 0-100 health score (Completeness, Validity, Uniqueness, Consistency) |
| `get_run_history` | Full "Healing Ledger" with filters (`failures_only`, `latest_errors`, `latest_status_by_module`) and payload controls (`limit`, `summary_only`) |
| `get_golden_templates` | Returns example templates tuned for typical fraud/migration/compliance patterns |
| `get_agent_playbook` | Structured JSON execution plan for client agents (ordered steps + gates) |
| `get_user_quickstart` | Human quickstart payload for UI rendering (`content.format=markdown`, `content.markdown`, `quick_actions`) |
| `get_capability_catalog` | Editable config knobs by module (supports `module`, `search`, `path_prefix`, `compact` filters) |
| `final_audit` | Final certification step — produces the Healing Certificate HTML report |
| `get_cockpit_dashboard` | Operator hub HTML artifact — recent-run cards, module dashboard links, artifact rows, data dictionary preview |
| `get_pipeline_dashboard` | Combined multi-module HTML dashboard for a specific `run_id`; linked from the cockpit hub |
| `data_dictionary` | Column-level schema report as a standalone HTML artifact; preview surfaced in the cockpit dictionary tab |
| `ensure_artifact_server` | Start/status the local artifact server — converts artifact file paths into browser-openable localhost URLs |
| `manage_session` | Session lifecycle: list active sessions, inspect details, fork a session into a new run context, or rebind a session to a different run_id |
| `upload_input` | Upload a local file as base64-encoded content through the MCP protocol — use when the file is not server-visible (e.g., server runs in a container) |
| `read_artifact` | Read a container-local artifact and return its content through MCP — use when localhost artifact URLs are not reachable from the client |

</details>

---

## Pipeline Mode (State Management)

Every tool accepts either a `gcs_path`/file path **or** a `session_id`. When a tool runs, it saves its output to an in-memory `StateStore` and returns a `session_id`. Pass that `session_id` to the next tool to operate on the already-transformed data — no intermediate files needed.

For this release, session persistence defaults to **in-memory only**, but a durable SQLite-backed session store is available via `ANALYST_MCP_SESSION_BACKEND=sqlite`. In both modes, sessions are still bounded by `ANALYST_MCP_SESSION_TTL_SEC` and `ANALYST_MCP_SESSION_MAX_ENTRIES`: cleanup is enforced lazily on session reads, writes, and explicit `manage_session` / `cleanup` activity, so SQLite sessions can survive process restarts while still expiring or being evicted once the server touches the store again. `manage_session` surfaces the live retention policy plus per-session expiry timestamps.
Use `manage_session(action="inspect", include_configs=true)` when you need the stored inferred config payloads; the default inspect/list responses stay compact and only include config names/counts.

```text
1. diagnostics(gcs_path="gs://bucket/path/file.parquet", run_id="my_run")
     → creates session_id: "sess_abc123", establishes baseline profile

2. get_data_health_report(run_id="my_run")
     → 0-100 health score before any changes

3. infer_configs(session_id="sess_abc123")
     → returns YAML configs per module; review and adjust before using

4. normalization(session_id="sess_abc123", config={...})
5. duplicates(session_id="sess_abc123", config={...})
6. outliers(session_id="sess_abc123", config={...})
7. imputation(session_id="sess_abc123", config={...})
8. validation(session_id="sess_abc123", config={...})

9. final_audit(session_id="sess_abc123")
     → produces Healing Certificate HTML report

10. get_run_history(run_id="my_run")
      → full ledger of every transformation
```

A `run_id` ties all steps together in the Healing Ledger. Pass the same `run_id` across calls to build a full audit trail.

If a tool call provides both `session_id` and `run_id`, the server enforces lifecycle consistency by default:
- If the session already has a bound run id and it differs from the requested run id, the tool coerces to the session run id and emits a warning.
- To allow explicit overrides, set `ANALYST_MCP_ALLOW_RUN_ID_OVERRIDE=1`.
- To start a new run context without re-downloading data, use `manage_session(action="fork", session_id="...", run_id="new_run")`. This clones the session's DataFrame and inferred configs into a fresh session with its own run_id.
- To change the run_id on an existing session in-place, use `manage_session(action="rebind", session_id="...", run_id="new_run")`.

> **Config structure note:** `infer_configs` returns YAML strings. Parse each one with `yaml.safe_load` and pass the resulting dict directly to the relevant tool. Never flatten nested keys — for normalization, `standardize_text_columns`, `coerce_dtypes`, etc. must stay nested inside `rules:` or the pipeline will skip all transformations.
>
> Canonical nested paths to preserve:
> - Outliers: `outlier_detection.detection_specs.<column>.*`
> - Validation: `validation.schema_validation.rules.*`
> - Final audit certification: `final_audit.certification.schema_validation.rules.*`

---

## Template Resources

In addition to tools, the server exposes YAML templates and informational surfaces through MCP resources so clients/agents can pull them into context without calling a tool.

- Standard templates: `analyst://templates/config/{name}_template.yaml`
- Golden templates: `analyst://templates/golden/{name}.yaml`
- Agent playbook: `analyst://resources/agent_playbook`
- User quickstart: `analyst://resources/user_quickstart`
- Capability catalog: `analyst://resources/capability_catalog`

Example JSON-RPC calls:

```bash
# List all available template resources
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 11,
    "method": "resources/list",
    "params": {}
  }'
```

```bash
# Read one template resource
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 12,
    "method": "resources/read",
    "params": {"uri": "analyst://templates/golden/fraud_detection.yaml"}
  }'
```

By default, `resources/templates/list` returns an empty list so clients that render resources + templates together do not show duplicates.
If your host needs URI templates explicitly, enable:

```bash
export ANALYST_MCP_ADVERTISE_RESOURCE_TEMPLATES=true
```

---

## Local Artifact Server

By default, tool responses return artifact file paths (e.g. `exports/reports/run_001_diagnostics_report.html`). If you're running the server locally and want cockpit and module artifact references to be browser-openable URLs instead, enable the artifact server:

```bash
export ANALYST_MCP_ENABLE_ARTIFACT_SERVER=true
```

Then call `ensure_artifact_server` once to start it:

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "ensure_artifact_server",
      "arguments": {}
    }
  }'
```

The server binds to `127.0.0.1:8765` by default and serves the `exports/` directory. Artifact paths in subsequent tool responses will be replaced with `http://localhost:8765/...` URLs.

| Variable | Default | Description |
|---|---|---|
| `ANALYST_MCP_ENABLE_ARTIFACT_SERVER` | `false` | Enable the local artifact server |
| `ANALYST_MCP_ARTIFACT_SERVER_HOST` | `127.0.0.1` | Bind address (localhost only by default) |
| `ANALYST_MCP_ARTIFACT_SERVER_PORT` | `8765` | Port for the artifact server |
| `ANALYST_MCP_ARTIFACT_SERVER_ROOT` | `exports` | Root directory to serve artifacts from |
| `ANALYST_MCP_ALLOW_BIND_ALL` | `false` | Allow binding to `0.0.0.0` (explicit opt-in) |

---

<details>
<summary><strong>📋 Usage Examples</strong></summary>

All examples use `curl` against a local server at `http://localhost:8001/rpc`. Swap `gcs_path` for a GCS URI (`gs://bucket/path/`) when reading from GCS.

---

### Run Diagnostics

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "diagnostics",
      "arguments": {
        "gcs_path": "data/raw/synthetic_penguins_v3.5.csv",
        "run_id": "audit_001"
      }
    }
  }'
```

**Response shape:**

```json
{
  "status": "pass",
  "run_id": "audit_001",
  "session_id": "sess_abc123",
  "artifact_url": "gs://my-bucket/analyst_toolkit/reports/audit_001_diagnostics_report.html",
  "plot_urls": { "null_heatmap.png": "gs://..." }
}
```

---

### Run a Full Pipeline (Chained with session_id)

```bash
# Step 1 — Diagnostics
SESSION=$(curl -s -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {"name": "diagnostics", "arguments": {"gcs_path": "data/raw/file.csv", "run_id": "run_001"}}
  }' | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['session_id'])")

# Step 2 — Normalization (reads from state)
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d "{
    \"jsonrpc\": \"2.0\", \"id\": 2, \"method\": \"tools/call\",
    \"params\": {\"name\": \"normalization\", \"arguments\": {\"session_id\": \"$SESSION\", \"run_id\": \"run_001\"}}
  }"

# Step 3 — Imputation (reads from state)
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d "{
    \"jsonrpc\": \"2.0\", \"id\": 3, \"method\": \"tools/call\",
    \"params\": {\"name\": \"imputation\", \"arguments\": {\"session_id\": \"$SESSION\", \"run_id\": \"run_001\"}}
  }"
```

---

### Auto-Heal (One Command)

Runs `infer_configs` → `normalization` → `imputation` automatically:

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "auto_heal",
      "arguments": {
        "gcs_path": "data/raw/file.csv",
        "run_id": "heal_001"
      }
    }
  }'
```

For long-running data, queue async execution and poll status:

```bash
# Queue
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 41,
    "method": "tools/call",
    "params": {
      "name": "auto_heal",
      "arguments": {
        "gcs_path": "gs://bucket/path.csv",
        "run_id": "auto_heal_001",
        "async_mode": true
      }
    }
  }'

# Poll
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 42,
    "method": "tools/call",
    "params": {
      "name": "get_job_status",
      "arguments": {"job_id": "job_xxxxxxxx"}
    }
  }'
```

---

### Get a Data Health Report

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "get_data_health_report",
      "arguments": { "run_id": "audit_001" }
    }
  }'
```

**Response shape:**

```json
{
  "status": "pass",
  "run_id": "audit_001",
  "health_score": 82,
  "health_status": "yellow",
  "breakdown": {
    "completeness": 91,
    "validity": 78,
    "uniqueness": 95,
    "consistency": 64
  }
}
```

---

### Capability Catalog (Filtered for Agent UX)

Use filters to return only knobs relevant to the current agent task:

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 7,
    "method": "tools/call",
    "params": {
      "name": "get_capability_catalog",
      "arguments": {
        "module": "normalization",
        "search": "fuzzy",
        "path_prefix": "rules.fuzzy_matching",
        "compact": true
      }
    }
  }'
```

---

### Drift Detection

Compare a baseline dataset to a new one:

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "drift_detection",
      "arguments": {
        "base_path": "data/raw/baseline.csv",
        "target_path": "data/raw/new_extract.csv",
        "run_id": "drift_check_001"
      }
    }
  }'
```

You can also compare two in-memory sessions:

```bash
"arguments": {
  "base_session_id": "sess_abc123",
  "target_session_id": "sess_xyz789"
}
```

---

### Use a Golden Template

Fetch available templates, then read one directly as a resource:

```bash
# List available templates
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {"name": "get_golden_templates", "arguments": {}}
  }'

# Read fraud_detection YAML template
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0", "id": 2, "method": "resources/read",
    "params": {"uri": "analyst://templates/golden/fraud_detection.yaml"}
  }'
```

Use the returned YAML as your starting point, then pass module-specific sections into tool `config` objects.

---

### Get the Healing Ledger

Review every transformation made across a run:

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "get_run_history",
      "arguments": { "run_id": "audit_001" }
    }
  }'
```

For summary-oriented agent flows (less payload, faster triage), pass filters:

```json
{
  "run_id": "audit_001",
  "failures_only": true,
  "latest_errors": true,
  "latest_status_by_module": true,
  "limit": 25,
  "summary_only": true
}
```

`get_run_history` now defaults to compact mode when omitted:
- `summary_only=true`
- `limit=50`

</details>

---

<details>
<summary><strong>🖥️ Host Integration</strong></summary>

### Claude Desktop

Add to `~/.config/claude/claude_desktop_config.json` (Mac: `~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "analyst_toolkit": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-v", "/path/to/data:/app/data",
        "-e", "GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp_creds.json",
        "-v", "~/.secrets/gcp_creds.json:/secrets/gcp_creds.json",
        "ghcr.io/g-schumacher44/analyst-toolkit-mcp:latest",
        "python", "-m", "analyst_toolkit.mcp_server.server", "--stdio"
      ]
    }
  }
}
```

### FridAI (HTTP Transport)

In your FridAI `remote_manager` config, point to the running server:

```json
{
  "name": "analyst_toolkit",
  "transport": "http",
  "url": "http://localhost:8001/rpc"
}
```

### VS Code (MCP Extension)

```json
{
  "mcp.servers": {
    "analyst_toolkit": {
      "url": "http://localhost:8001/rpc"
    }
  }
}
```

</details>

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `GCP_CREDS_PATH` | For compose + GCS | — | Host path to service account JSON key (compose maps this to `GOOGLE_APPLICATION_CREDENTIALS`) |
| `GOOGLE_APPLICATION_CREDENTIALS` | For direct docker run + GCS | — | In-container path to service account JSON key |
| `ANALYST_REPORT_BUCKET` | No | _(unset — local mode)_ | GCS bucket for HTML/XLSX/plot upload, e.g. `gs://my-bucket` |
| `ANALYST_REPORT_PREFIX` | No | `analyst_toolkit/reports` | Blob path prefix within the bucket |
| `ANALYST_MCP_PORT` | No | `8001` | Override the server port |
| `ANALYST_MCP_VERSION_FALLBACK` | No | `0.0.0+local` | Version string used when package metadata is unavailable in local/source execution |
| `ANALYST_MCP_AUTH_TOKEN` | No | _(unset)_ | If set, require `Authorization: Bearer <token>` for `/rpc`, `/health`, `/ready`, and `/metrics` |
| `ANALYST_MCP_RESOURCE_TIMEOUT_SEC` | No | `8.0` | Timeout for MCP `resources/list` and `resources/read` filesystem work |
| `ANALYST_MCP_MAX_INPUT_BYTES` | No | `104857600` | Maximum single-input byte budget for local files, GCS objects, and cumulative GCS prefix loads |
| `ANALYST_MCP_MAX_GCS_PREFIX_OBJECTS` | No | `32` | Maximum number of `.csv` / `.parquet` blobs loaded from a single GCS prefix |
| `ANALYST_MCP_MAX_INPUT_ROWS` | No | `1000000` | Maximum row count allowed after an input is loaded into a DataFrame |
| `ANALYST_MCP_MAX_INPUT_MEMORY_BYTES` | No | `268435456` | Maximum in-memory DataFrame size allowed after an input is loaded |
| `ANALYST_MCP_ADVERTISE_RESOURCE_TEMPLATES` | No | `false` | If `true`, `resources/templates/list` returns URI templates (otherwise empty to avoid duplicate UI listings) |
| `ANALYST_MCP_TEMPLATE_IO_TIMEOUT_SEC` | No | `8.0` | Timeout for cockpit template reads (`get_capability_catalog`, `get_golden_templates`) |
| `ANALYST_MCP_STRUCTURED_LOGS` | No | `false` | Emit JSON-structured request lifecycle logs (`trace_id`, method, tool, duration) |
| `ANALYST_MCP_JOB_STATE_PATH` | No | `exports/reports/jobs/job_state.json` | Local JSON persistence path for async job state (`get_job_status`, `list_jobs`) |
| `ANALYST_MCP_SESSION_BACKEND` | No | `memory` | Session backend: `memory` or `sqlite` |
| `ANALYST_MCP_SESSION_DB_PATH` | No | `exports/reports/state/session_store.db` | SQLite database path when `ANALYST_MCP_SESSION_BACKEND=sqlite` |
| `ANALYST_MCP_SESSION_TTL_SEC` | No | `3600` | Session time-to-live for both backends; SQLite cleanup is applied lazily on session reads/writes and explicit cleanup/list activity |
| `ANALYST_MCP_SESSION_MAX_ENTRIES` | No | `32` | Maximum number of retained sessions for both backends before LRU eviction |
| `ANALYST_MCP_ALLOW_RUN_ID_OVERRIDE` | No | `false` | Allow a requested `run_id` to differ from the session-bound run id (otherwise run id is coerced) |
| `ANALYST_MCP_RUN_HISTORY_SUMMARY_ONLY_DEFAULT` | No | `true` | Default compact ledger mode for `get_run_history` when caller omits `summary_only` |
| `ANALYST_MCP_RUN_HISTORY_DEFAULT_LIMIT` | No | `50` | Default max ledger entries returned in compact mode when caller omits `limit` |
| `ANALYST_MCP_DEDUP_RUN_ID_WARNINGS` | No | `true` | Deduplicate repeated run-id coercion warnings for the same session/request pair |
| `ANALYST_MCP_ALLOW_EMPTY_CERT_RULES` | No | `false` | If `false`, `final_audit` fails closed when certification rule contract is empty |
| `ANALYST_MCP_ENABLE_ARTIFACT_SERVER` | No | `false` | Enable the optional local artifact server for browser-openable dashboard links |
| `ANALYST_MCP_ARTIFACT_SERVER_HOST` | No | `127.0.0.1` | Bind address for the artifact server (localhost only by default) |
| `ANALYST_MCP_ARTIFACT_SERVER_PORT` | No | `8765` | Port for the local artifact server |
| `ANALYST_MCP_ARTIFACT_SERVER_ROOT` | No | `exports` | Root directory served by the artifact server |
| `ANALYST_MCP_ALLOW_BIND_ALL` | No | `false` | Allow artifact server to bind to `0.0.0.0` (explicit opt-in) |

Copy `.envrc.example` to `.envrc` and fill in your values before starting the server.

## MCP Compatibility Policy

The project aims to preserve stable MCP response contracts where practical.

Policy:
- additive response fields are preferred over breaking field-shape changes
- behavior or contract changes should land with regression tests in the same PR
- externally visible contract changes should be called out in release notes and PR descriptions
- `dev` is the integration branch; public contract changes should not bypass it on the way to `main`

---

## GCS Data Loading

The server dispatches on path format:

| Input | Behavior |
| --- | --- |
| `gs://bucket/path/to/file.parquet` | Downloads the single blob directly |
| `gs://bucket/path/to/file.csv` | Downloads the single blob directly |
| `gs://bucket/path/to/partition/` | Lists all `.parquet` / `.csv` blobs under the prefix and concatenates them |
| `path/to/file.parquet` | `pd.read_parquet()` (local) |
| `path/to/file.csv` | `pd.read_csv()` (local) |
| `session_id` | Reads from in-memory `StateStore` (no I/O) |

Boundary guards:
- local files, single GCS objects, and cumulative GCS prefix loads respect `ANALYST_MCP_MAX_INPUT_BYTES`
- GCS prefix scans stop once `ANALYST_MCP_MAX_GCS_PREFIX_OBJECTS` is exceeded
- loaded DataFrames are rejected if they exceed `ANALYST_MCP_MAX_INPUT_ROWS` or `ANALYST_MCP_MAX_INPUT_MEMORY_BYTES`

Session lifecycle notes:
- `manage_session(action="list")` returns the active retention policy (`backend`, `durable`, `ttl_sec`, `max_entries`) alongside the session summaries
- `manage_session(action="inspect")` returns `last_accessed_at`, `expires_at`, and `expires_in_sec` for the selected session
- `manage_session(action="inspect", include_configs=true)` retrieves the stored inferred config YAML payloads on demand
- `manage_session(action="fork")` clones the in-memory DataFrame and optionally the stored inferred configs into a new session with its own run context

> **Note:** Partition-style directory paths must end with `/`. Direct file paths (ending in `.parquet` or `.csv`) are read without listing.

---

<p align="center">
  🔙 <a href="../README.md"><strong>Return to Project README</strong></a>
</p>
