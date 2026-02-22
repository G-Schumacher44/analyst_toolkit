<p align="center">
  <img src="../repo_files/analyst_toolkit_banner.png" alt="Analyst Toolkit Logo" width="1000"/>
  <br>
  <em>Analyst Toolkit — MCP Server Guide</em>
</p>
<p align="center">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-stable-brightgreen">
  <img alt="Version" src="https://img.shields.io/badge/version-v0.4.0-blueviolet">
</p>

---

# 📡 MCP Server Guide

The analyst toolkit MCP server exposes every toolkit module as a callable tool over the [Model Context Protocol](https://modelcontextprotocol.io). Any MCP-compatible host — FridAI, Claude Desktop, VS Code, or a plain JSON-RPC 2.0 client — can invoke toolkit operations against local or GCS-hosted data without any Python dependency on the host side.

### 🆕 Version 0.4.0 Highlights
- **Pipeline Mode:** In-memory state management via `session_id` allows chaining multiple tools without manual file saving.
- **Client Cockpit:** New tools for executive reporting, including a 0-100 Data Health Score and a "Healing Ledger" history.
- **Golden Templates:** Mountable library of industry-standard configurations for Fraud, Migration, and Compliance.
- **Auto-Healing:** One-click inference and repair tool.

---

## Quick Start

**Prerequisites:** Docker or Podman. GCS credentials if reading from GCS.

```bash
# Clone and build
git clone https://github.com/G-Schumacher44/analyst_toolkit.git
cd analyst_toolkit

# Start the server (Docker)
docker-compose -f docker-compose.mcp.yml up --build
```

The server starts on port `8001` by default. You can verify the tool list:

```bash
curl http://localhost:8001/health
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

*Note: Most MCP clients (like Gemini or Claude) will show these tools with a `toolkit_` prefix, e.g., `toolkit_diagnostics`.*

---

## ⛓️ Pipeline Mode (State Management)

The server now supports **Stateful Sessions**. When you run a tool on a file, it returns a `session_id`. You can pass this `session_id` to the next tool to operate on the *transformed* data in memory.

1.  **Step 1:** Call `normalization` with `gcs_path`. Get back `session_id: "sess_123"`.
2.  **Step 2:** Call `validation` with `session_id: "sess_123"`. It validates the cleaned data.
3.  **Step 3:** Call `imputation` with `session_id: "sess_123"`. It fills nulls in the same session.

---

## 🕹️ The Cockpit (Client Delivery Tools)

These tools are designed to provide transparency and "executive-level" insights into the data cleaning process.

### `get_data_health_report`
Calculates a **0-100 Data Health Score** based on Completeness, Validity, Uniqueness, and Consistency. Returns a Red/Yellow/Green status.

### `get_run_history`
Returns the **"Prescription & Healing Ledger"** for a specific `run_id`. Shows exactly what changes were made at every step of the pipeline.

### `get_golden_templates`
Returns a library of "Golden Configs" from `config/golden_templates/`. Use these as high-quality starting points for specific use cases:
- `fraud_detection`: Strict outliers and identity-based duplicates.
- `quick_migration`: Heavy on renaming and type-coercion.
- `compliance_audit`: Focuses on PII and strict range checks.

---

## 🤖 Autonomous Tools

### `auto_heal`
The "Easy Button" for data cleaning. It runs `infer_configs` to predict needs, then automatically applies `normalization` and `imputation` rules in a single step.

### `drift_detection`
Compares two datasets (Base vs. Target) and reports on schema changes (added/removed columns) and statistical drift in numeric distributions.

### `get_config_schema`
Returns the JSON Schema for any module's configuration. Use this to ensure your custom JSON configs are perfectly structured.

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `GCP_CREDS_PATH` | For GCS data | `~/.secrets/gcp_creds.json` | Host path to service account key. |
| `ANALYST_REPORT_BUCKET` | No | _(unset)_ | GCS bucket for HTML report upload. |
| `ANALYST_REPORT_PREFIX` | No | `analyst_toolkit/reports` | Blob path prefix in GCS. |

---

## GCS Data Loading

The server dispatches on path prefix:
- `gs://bucket/path/` → Downloads and concatenates (supports `_MANIFEST.json`).
- `*.parquet` → `pd.read_parquet()`
- `*.csv` → `pd.read_csv()`

---

<p align="center">
  🔙 <a href="../README.md"><strong>Return to Project README</strong></a>
</p>
