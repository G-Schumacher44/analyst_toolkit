<p align="center">
  <img src="../repo_files/analyst_toolkit_banner.png" alt="Analyst Toolkit Logo" width="1000"/>
  <br>
  <em>Analyst Toolkit — MCP Server Guide</em>
</p>
<p align="center">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-stable-brightgreen">
  <img alt="Version" src="https://img.shields.io/badge/version-v0.3.0-blueviolet">
</p>

---

# 📡 MCP Server Guide

The analyst toolkit MCP server exposes every toolkit module as a callable tool over the [Model Context Protocol](https://modelcontextprotocol.io). Any MCP-compatible host — FridAI, Claude Desktop, VS Code, or a plain JSON-RPC 2.0 client — can invoke toolkit operations against local or GCS-hosted data without any Python dependency on the host side.

The server is stateless. Data comes in via a path (GCS URI or local file), results go back as JSON. HTML reports are written to disk (and optionally uploaded to GCS) as side-effect artifacts.

---

## Quick Start

**Prerequisites:** Docker or Podman. GCS credentials if reading from GCS.

```bash
# Clone and start
git clone https://github.com/G-Schumacher44/analyst_toolkit.git
cd analyst_toolkit

# Start the server (Docker)
make mcp-up
make mcp-health
```

Optional: pull the prebuilt image (for your own orchestration):

```bash
docker pull ghcr.io/g-schumacher44/analyst-toolkit-mcp:latest
```

The server starts on port `8001` by default. You can also verify directly:

```bash
curl http://localhost:8001/health
```

```json
{
  "status": "ok",
  "tools": [
    "toolkit_diagnostics",
    "toolkit_validation",
    "toolkit_outliers",
    "toolkit_normalization",
    "toolkit_duplicates",
    "toolkit_imputation",
    "toolkit_infer_configs"
  ]
}
```

Useful commands:

- `make mcp-logs` — tail server logs
- `make mcp-down` — stop the server

---

## Environment Variables

Set these in your shell, `.envrc`, or Docker environment before starting the server.

| Variable | Required | Default | Description |
|---|---|---|---|
| `GCP_CREDS_PATH` | For GCS data (Docker) | `~/.secrets/gcp_creds.json` | Host path to service account key. Mounted into the container at `/run/secrets/gcp_creds`. |
| `GOOGLE_APPLICATION_CREDENTIALS` | For GCS data (no Docker) | _(unset)_ | Path to service account key for local Python runs. |
| `ANALYST_MCP_PORT` | No | `8001` | Host port to bind. |
| `ANALYST_REPORT_BUCKET` | No | _(unset — local only)_ | GCS bucket for HTML report upload, e.g. `gs://my-reports`. If unset, reports are written to `./exports` only. |
| `ANALYST_REPORT_PREFIX` | No | `analyst_toolkit/reports` | Blob path prefix within `ANALYST_REPORT_BUCKET`. Reports land at `{prefix}/{run_id}/{module}/{filename}`. |

**Local dev example (`.envrc`):**

```bash
export GCP_CREDS_PATH=~/.secrets/analyst-toolkit-mcp.json
export ANALYST_REPORT_BUCKET=gs://my-fridai-reports
```

If running without Docker, set `GOOGLE_APPLICATION_CREDENTIALS` instead of `GCP_CREDS_PATH`.

---

## Tool Reference

All tools share the same base input shape:

| Parameter | Type | Required | Description |
|---|---|---|---|
| `gcs_path` | string | Yes | Data source — GCS URI (`gs://bucket/path/`), local `.parquet`, or local `.csv`. |
| `config` | object | No | Module config dict. Matches the relevant YAML block structure. Merged with defaults. |
| `run_id` | string | No | Run identifier used in output paths and artifact naming. Default: `"mcp_run"`. |

All tools return a JSON object with at minimum:

```json
{
  "status": "pass | warn | fail | error",
  "module": "<module_name>",
  "run_id": "<run_id>",
  "summary": { ... },
  "artifact_path": "/abs/path/to/report.html",
  "artifact_url": "https://storage.googleapis.com/..."
}
```

`artifact_path` is the local path to the HTML report (empty string if HTML export is disabled). `artifact_url` is the GCS public URL if `ANALYST_REPORT_BUCKET` is configured, otherwise empty.

HTML reports are generated automatically when `ANALYST_REPORT_BUCKET` is set. Override per call with `export_html: true/false` in `config`:

```json
{ "gcs_path": "gs://bucket/table/", "config": { "export_html": true } }
```

---

<details>
<summary><strong>toolkit_diagnostics</strong> — Data profiling</summary>

Runs a non-destructive structural and statistical profile of the dataset.

**Additional response fields:**

| Field | Type | Description |
|---|---|---|
| `profile_shape` | `[rows, cols]` | Dataset dimensions. |
| `null_rate` | float | Mean null rate across all columns (0–1). |
| `column_count` | int | Number of columns. |

**Status logic:** `"warn"` if `null_rate` exceeds `config.null_threshold` (default `0.1`), otherwise `"pass"`.

**Example:**

```json
{
  "gcs_path": "gs://my-bucket/silver/orders/date=2024-01-01/",
  "config": { "null_threshold": 0.05, "export_html": true },
  "run_id": "nightly_20240101"
}
```

</details>

<details>
<summary><strong>toolkit_validation</strong> — Schema and content validation</summary>

Runs schema conformity, dtype enforcement, categorical value, and numeric range checks.

**Additional response fields:**

| Field | Type | Description |
|---|---|---|
| `passed` | bool | `true` if all rules passed. |
| `failed_rules` | list[str] | Names of rules that failed. |
| `issue_count` | int | Number of failing rules. |

**Config keys** (passed inside `config`):

```json
{
  "schema_validation": {
    "rules": {
      "expected_columns": ["id", "amount", "status"],
      "expected_types": { "amount": "float64" },
      "categorical_values": { "status": ["active", "closed"] },
      "numeric_ranges": { "amount": { "min": 0, "max": 100000 } }
    }
  }
}
```

</details>

<details>
<summary><strong>toolkit_outliers</strong> — Statistical outlier detection</summary>

Runs IQR or z-score outlier detection on numeric columns. Non-destructive — no data is modified.

**Additional response fields:**

| Field | Type | Description |
|---|---|---|
| `flagged_columns` | list[str] | Columns with detected outliers. |
| `outlier_count` | int | Total flagged rows across all columns. |

**Status logic:** `"warn"` if any outliers detected, `"pass"` if none.

**Config keys:**

```json
{
  "detection_specs": {
    "amount": { "method": "iqr", "iqr_multiplier": 1.5 },
    "age": { "method": "zscore", "zscore_threshold": 3.0 }
  }
}
```

</details>

<details>
<summary><strong>toolkit_normalization</strong> — Cleaning and standardization</summary>

Applies rename, value mapping, fuzzy matching, datetime parsing, and dtype coercion.

**Additional response fields:**

| Field | Type | Description |
|---|---|---|
| `changes_made` | int | Number of changelog entries (cell-level changes). |

**Config keys** follow the `normalization_config.yaml` structure — see [Config Guide](config_guide.md) for full reference.

</details>

<details>
<summary><strong>toolkit_duplicates</strong> — Duplicate detection</summary>

Detects duplicate rows, optionally over a column subset.

**Additional input parameters:**

| Parameter | Type | Description |
|---|---|---|
| `subset_columns` | list[str] | Columns to use for duplicate matching. Defaults to all columns. |

**Additional response fields:**

| Field | Type | Description |
|---|---|---|
| `duplicate_count` | int | Number of duplicate rows detected. |
| `mode` | string | `"flag"` (default) or `"remove"`. |

**Status logic:** `"warn"` if duplicates found, `"pass"` if none.

</details>

<details>
<summary><strong>toolkit_imputation</strong> — Missing value imputation</summary>

Fills missing values using per-column strategies. Returns immediately with `status: "warn"` if no rules are provided.

**Additional response fields:**

| Field | Type | Description |
|---|---|---|
| `columns_imputed` | list[str] | Columns where nulls were filled. |
| `nulls_filled` | int | Total null values filled. |

**Config keys:**

```json
{
  "rules": {
    "strategies": {
      "age": "mean",
      "status": "mode",
      "tag_id": { "strategy": "constant", "value": "UNKNOWN" }
    }
  }
}
```

</details>

<details>
<summary><strong>toolkit_infer_configs</strong> — Config generation</summary>

Inspects a dataset and generates YAML config strings for the specified toolkit modules. Useful for bootstrapping a new dataset's config without manually authoring YAML.

Requires the deployment utility package to be installed in the container (see [requirements-mcp.txt](../requirements-mcp.txt)).

**Input parameters** (different shape from other tools — no `config` or `run_id`):

| Parameter | Type | Description |
|---|---|---|
| `gcs_path` | string | Data source path. |
| `modules` | list[str] | Module names to generate configs for. Empty = all inferrable. |
| `options` | object | Optional overrides: `sample_rows`, `max_unique`, `exclude_patterns`, `detect_datetimes`, `datetime_hints`, `outdir`. |

**Response:**

```json
{
  "configs": {
    "validation": "validation:\n  schema_validation:\n    ...",
    "outliers": "outlier_detection:\n  ..."
  },
  "modules_generated": ["validation", "outliers"]
}
```

**Example:**

```json
{
  "gcs_path": "gs://my-bucket/silver/orders/date=2024-01-01/",
  "modules": ["validation", "outliers", "normalization"],
  "options": { "max_unique": 20 }
}
```

</details>

---

## Connecting to FridAI Hub

Add the following to your fridai-core `remote_manager` config (typically `config/remote_servers.yaml` or equivalent):

```yaml
toolkit_local:
  enabled: true
  namespace: toolkit
  transport: http
  base_url: "http://127.0.0.1:8001"
  init_timeout: 20.0
  targets: []
  description: "Analyst toolkit MCP server (local Docker)"
```

The hub will call `POST /rpc` with standard JSON-RPC 2.0 payloads. Tools are proxied to connected clients under the `toolkit` namespace.

**Start order:** start the toolkit container before the hub, or set `init_timeout` high enough to tolerate a cold start.

---

## Connecting to Claude Desktop

Claude Desktop uses stdio transport. Run the server directly without Docker:

```bash
# Install MCP extras
pip install -e ".[mcp]"

# Start in stdio mode
python -m analyst_toolkit.mcp_server.server --stdio
```

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "analyst-toolkit": {
      "command": "python",
      "args": ["-m", "analyst_toolkit.mcp_server.server", "--stdio"],
      "env": {
        "PYTHONPATH": "/path/to/analyst_toolkit/src",
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/gcp_creds.json"
      }
    }
  }
}
```

Restart Claude Desktop. The toolkit tools will appear in the tool picker.

---

## Standalone Usage (curl / any HTTP client)

The server speaks JSON-RPC 2.0 at `POST /rpc`. No MCP SDK required on the client side.

**List available tools:**

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

**Call a tool:**

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/call",
    "params": {
      "name": "toolkit_outliers",
      "arguments": {
        "gcs_path": "gs://my-bucket/silver/orders/date=2024-01-01/",
        "config": { "export_html": true },
        "run_id": "manual_check_001"
      }
    }
  }'
```

---

## GCS Data Loading

The server dispatches on path prefix:

| Path format | How it's loaded |
|---|---|
| `gs://bucket/path/to/partition/` | Downloads from GCS. Reads `_MANIFEST.json` if present to get the file list, otherwise globs `*.parquet` and `*.csv`. Concatenates all files. |
| `path/to/file.parquet` | `pd.read_parquet()` |
| `path/to/file.csv` (or anything else) | `pd.read_csv()` |

**Manifest support:** If a `_MANIFEST.json` exists at the partition path with a `"files"` key, only those files are fetched. This avoids full partition globs for large Spark-written tables.

```json
{ "files": ["part-00000.parquet", "part-00001.parquet"] }
```

---

## HTML Report Artifacts

HTML reports are generated automatically when `ANALYST_REPORT_BUCKET` is set, or explicitly with `export_html: true` in the tool config.

1. The tool generates a self-contained HTML report and writes it to `exports/reports/{module}/{run_id}_report.html` inside the container.
2. If `ANALYST_REPORT_BUCKET` is set, the file is uploaded to GCS at `{prefix}/{run_id}/{module}/{filename}` and the public URL is returned in `artifact_url`.
3. If the `./exports` volume mount is active (default in `docker-compose.mcp.yml`), the file is also accessible on the host at `./exports/reports/...`.

The `artifact_path` in the response is always the absolute local path inside the container. The `artifact_url` is the GCS URL (empty string if upload is disabled).

---

## Running Without Docker

For local development or stdio hosts:

```bash
# Install
pip install -e ".[mcp]"

# HTTP mode (default, port 8001)
python -m analyst_toolkit.mcp_server.server

# HTTP on a custom port
python -m analyst_toolkit.mcp_server.server --port 9000

# Stdio mode (for Claude Desktop, VS Code, etc.)
python -m analyst_toolkit.mcp_server.server --stdio

# Stdio mode via environment variable
ANALYST_MCP_STDIO=true python -m analyst_toolkit.mcp_server.server
```

---

## Podman (rootless alternative)

The same `Dockerfile.mcp` works with Podman:

```bash
podman-compose -f docker-compose.mcp.yml up --build
```

Podman runs rootless by default — no daemon, no root. Recommended for production or security-sensitive environments.

---

<p align="center">
  🔙 <a href="../README.md"><strong>Return to Project README</strong></a>
</p>
