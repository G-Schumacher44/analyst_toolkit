<p align="center">
  <img src="../repo_files/analyst_toolkit_banner.png" alt="Analyst Toolkit Logo" width="1000"/>
  <br>
  <em>Analyst Toolkit — MCP Server Guide</em>
</p>
<p align="center">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-stable-brightgreen">
  <img alt="Version" src="https://img.shields.io/badge/version-v0.4.2-blueviolet">
</p>

---

# 📡 MCP Server Guide

The analyst toolkit MCP server exposes every toolkit module as a callable tool over the [Model Context Protocol](https://modelcontextprotocol.io). Any MCP-compatible host — FridAI, Claude Desktop, VS Code, or a plain JSON-RPC 2.0 client — can invoke toolkit operations against local or GCS-hosted data without any Python dependency on the host side.

## 🆕 Version 0.4.2 Highlights

- **Pipeline Mode:** In-memory state management via `session_id` allows chaining multiple tools without manual file saving.
- **Client Cockpit:** Tools for executive reporting, including a 0-100 Data Health Score, a "Healing Ledger" history, and an agent flight checklist.
- **Golden Templates:** Example templates tuned for typical fraud/migration/compliance patterns.
- **Manual Pipeline:** Recommended workflow — diagnostics → infer → normalize → dedupe → outliers → impute → validate → final audit.
- **GCS Direct File Loading:** Pass a direct `.parquet` or `.csv` GCS URI — no trailing slash required.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Tool Reference](#tool-reference) ▾
- [Pipeline Mode](#pipeline-mode-state-management)
- [Template Resources](#template-resources)
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
docker-compose -f docker-compose.mcp.yml up --build
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
  "tools": [
    "diagnostics", "validation", "outliers", "normalization",
    "duplicates", "imputation", "infer_configs", "auto_heal",
    "drift_detection", "get_config_schema", "get_golden_templates",
    "get_agent_playbook", "get_user_quickstart", "get_capability_catalog",
    "get_run_history", "get_data_health_report"
  ]
}
```

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

### Cockpit Tools

| Tool | Description |
|---|---|
| `get_data_health_report` | 0-100 health score (Completeness, Validity, Uniqueness, Consistency) |
| `get_run_history` | Full "Healing Ledger" with optional summary filters (`failures_only`, `latest_errors`, `latest_status_by_module`) |
| `get_golden_templates` | Returns example templates tuned for typical fraud/migration/compliance patterns |
| `get_agent_playbook` | Structured JSON execution plan for client agents (ordered steps + gates) |
| `get_user_quickstart` | Human quickstart payload for UI rendering (`content.format=markdown`, `content.markdown`, `quick_actions`) |
| `get_capability_catalog` | Editable config knobs by module (supports `module`, `search`, `path_prefix`, `compact` filters) |
| `final_audit` | Final certification step — produces the Healing Certificate HTML report |

</details>

---

## Pipeline Mode (State Management)

Every tool accepts either a `gcs_path`/file path **or** a `session_id`. When a tool runs, it saves its output to an in-memory `StateStore` and returns a `session_id`. Pass that `session_id` to the next tool to operate on the already-transformed data — no intermediate files needed.

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

> **Config structure note:** `infer_configs` returns YAML strings. Parse each one with `yaml.safe_load` and pass the resulting dict directly to the relevant tool. Never flatten nested keys — for normalization, `standardize_text_columns`, `coerce_dtypes`, etc. must stay nested inside `rules:` or the pipeline will skip all transformations.
>
> Canonical nested paths to preserve:
> - Outliers: `outlier_detection.detection_specs.<column>.*`
> - Validation: `validation.schema_validation.rules.*`
> - Final audit certification: `final_audit.certification.schema_validation.rules.*`

---

## Template Resources

In addition to tools, the server exposes YAML templates through MCP resources so clients/agents can pull them into context without calling a tool.

- Standard templates: `analyst://templates/config/{name}_template.yaml`
- Golden templates: `analyst://templates/golden/{name}.yaml`

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
  "latest_status_by_module": true
}
```

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
| `ANALYST_MCP_RESOURCE_TIMEOUT_SEC` | No | `8.0` | Timeout for MCP `resources/list` and `resources/read` filesystem work |
| `ANALYST_MCP_ADVERTISE_RESOURCE_TEMPLATES` | No | `false` | If `true`, `resources/templates/list` returns URI templates (otherwise empty to avoid duplicate UI listings) |
| `ANALYST_MCP_TEMPLATE_IO_TIMEOUT_SEC` | No | `8.0` | Timeout for cockpit template reads (`get_capability_catalog`, `get_golden_templates`) |

Copy `.envrc.example` to `.envrc` and fill in your values before starting the server.

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

> **Note:** Partition-style directory paths must end with `/`. Direct file paths (ending in `.parquet` or `.csv`) are read without listing.

---

<p align="center">
  🔙 <a href="../README.md"><strong>Return to Project README</strong></a>
</p>
