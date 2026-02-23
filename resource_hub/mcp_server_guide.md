<p align="center">
  <img src="../repo_files/analyst_toolkit_banner.png" alt="Analyst Toolkit Logo" width="1000"/>
  <br>
  <em>Analyst Toolkit — MCP Server Guide</em>
</p>
<p align="center">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-stable-brightgreen">
  <img alt="Version" src="https://img.shields.io/badge/version-v0.4.1-blueviolet">
</p>

---

# 📡 MCP Server Guide

The analyst toolkit MCP server exposes every toolkit module as a callable tool over the [Model Context Protocol](https://modelcontextprotocol.io). Any MCP-compatible host — FridAI, Claude Desktop, VS Code, or a plain JSON-RPC 2.0 client — can invoke toolkit operations against local or GCS-hosted data without any Python dependency on the host side.

## 🆕 Version 0.4.1 Highlights

- **Pipeline Mode:** In-memory state management via `session_id` allows chaining multiple tools without manual file saving.
- **Client Cockpit:** New tools for executive reporting, including a 0-100 Data Health Score and a "Healing Ledger" history.
- **Golden Templates:** Mountable library of industry-standard configurations for Fraud, Migration, and Compliance.
- **Auto-Healing:** One-click inference and repair tool.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Tool Reference](#tool-reference) ▾
- [Pipeline Mode](#pipeline-mode-state-management)
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
  -e GCP_CREDS_PATH=/secrets/gcp_creds.json \
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
| `get_run_history` | Full "Healing Ledger" — every transformation made in a run |
| `get_golden_templates` | Returns industry-standard starter configs (Fraud, Migration, Compliance) |

</details>

---

## Pipeline Mode (State Management)

Every tool accepts either a `gcs_path`/file path **or** a `session_id`. When a tool runs, it saves its output to an in-memory `StateStore` and returns a `session_id`. Pass that `session_id` to the next tool to operate on the already-transformed data — no intermediate files needed.

```text
Call diagnostics(gcs_path="data/raw/file.csv")
  → returns session_id: "sess_abc123"

Call normalization(session_id="sess_abc123")
  → reads cleaned df from state, returns session_id: "sess_abc123"

Call imputation(session_id="sess_abc123")
  → reads imputed df from state, returns session_id: "sess_abc123"

Call get_data_health_report(run_id="my_run")
  → returns 0-100 score aggregated from all steps
```

A `run_id` ties all steps together in the Healing Ledger. Pass the same `run_id` across calls to build a full audit trail.

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
  }' | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['content'][0]['text'])" \
  | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['session_id'])")

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
  "grade": "Yellow",
  "breakdown": {
    "completeness": 91,
    "validity": 78,
    "uniqueness": 95,
    "consistency": 64
  }
}
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

Fetch available templates, then pass one as your config:

```bash
# List available templates
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
    "params": {"name": "get_golden_templates", "arguments": {}}
  }'

# Use fraud_detection template config in a run
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0", "id": 2, "method": "tools/call",
    "params": {
      "name": "auto_heal",
      "arguments": {
        "gcs_path": "data/raw/transactions.csv",
        "config": "<paste fraud_detection config block here>",
        "run_id": "fraud_run_001"
      }
    }
  }'
```

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
        "-e", "GCP_CREDS_PATH=/secrets/gcp_creds.json",
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
| `GCP_CREDS_PATH` | For GCS data | `~/.secrets/gcp_creds.json` | Host path to service account key |
| `ANALYST_REPORT_BUCKET` | No | _(unset)_ | GCS bucket for HTML report upload |
| `ANALYST_REPORT_PREFIX` | No | `analyst_toolkit/reports` | Blob path prefix in GCS |

---

## GCS Data Loading

The server dispatches on path prefix:

| Input | Behavior |
| --- | --- |
| `gs://bucket/path/` | Downloads and concatenates all files (supports `_MANIFEST.json`) |
| `path/to/file.parquet` | `pd.read_parquet()` |
| `path/to/file.csv` | `pd.read_csv()` |
| `session_id` | Reads from in-memory `StateStore` (no I/O) |

---

<p align="center">
  🔙 <a href="../README.md"><strong>Return to Project README</strong></a>
</p>
