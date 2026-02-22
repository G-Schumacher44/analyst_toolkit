# Analyst Toolkit — Facelift Build Plan

## Context

This buildout prepares `analyst_toolkit` to serve as the intelligence layer in a FridAI-connected
demo system. Three distinct workstreams: HTML report output, an MCP server exposing toolkit modules
as tools, and an expansion of the `infer-configs` CLI to cover the full module surface.

These changes are driven by the FridAI demo buildout but are designed to be useful standalone —
the MCP server in particular makes the toolkit consumable by any MCP-compatible client.

---

## Workstream 1 — HTML Reports (~1 day)

### Goal

Each toolkit module can write a self-contained HTML report as a side-effect artifact alongside
the existing Excel/CSV export. The HTML is for human consumption (engineers reading nightly QA
output). It is not the primary output of any function — JSON/DataFrames remain the programmatic
interface.

### What already exists

| File | What it provides |
|---|---|
| `m00_utils/rendering_utils.py` | `to_html_table(df)` — already converts DataFrame to HTML string |
| `m00_utils/report_generator.py` | All report functions already return `dict[str, pd.DataFrame]` — right shape |
| `m00_utils/export_utils.py` | `export_dataframes()` handles Excel/CSV — HTML export slots in alongside it |
| `model_evaluation_suite/utils/export_utils.py` | `_generate_html_report()` — the pattern to port (~70 lines) |

### What needs building

**`m00_utils/report_generator.py`** — add:

```python
def generate_html_report(
    report_tables: dict[str, pd.DataFrame],
    module_name: str,
    run_id: str,
    plot_paths: dict[str, str] | None = None,
) -> str:
    """
    Builds a single-page HTML report from a dict of DataFrames.
    Embeds plots as base64. Returns HTML string.
    """
```

- Inline CSS (port from eval suite — no external dependencies)
- Each DataFrame key becomes a section with a heading + `to_html_table()` output
- Plots embedded as base64 PNG if `plot_paths` provided
- Module name + run_id in the title and header

**`m00_utils/export_utils.py`** — add:

```python
def export_html_report(
    report_tables: dict[str, pd.DataFrame],
    export_path: str,
    module_name: str,
    run_id: str,
    plot_paths: dict[str, str] | None = None,
) -> str:
    """Writes HTML report to disk. Returns the path written."""
```

- Called alongside `export_dataframes()` in each module's export step
- Controlled by `export_html: true` in module config (opt-in, default off for now)
- Returns path so callers (e.g. MCP server) can reference the artifact

### Modules to wire up (in order of priority)

1. Outlier detection + handling (nightly QA primary output)
2. Validation + certification
3. Diagnostics
4. Imputation
5. Duplicates
6. Final audit

### Design constraint

HTML is always a **side-effect artifact written to a path**. It is never returned inline from a
function or stuffed into an MCP tool response. MCP tools return JSON; the HTML path is included
in the JSON response so the caller knows where to find it.

---

## Workstream 2 — MCP Server (~2-3 days)

### Goal

Expose toolkit modules as MCP tools so any MCP-compatible client (FridAI hub, Claude Desktop,
VS Code, etc.) can invoke them. Containerized, persistent, stateless. FridAI's `remote_manager`
connects to it as a remote MCP endpoint.

### Location

New package within this repo: `analyst_toolkit/mcp_server/`

```
analyst_toolkit/
  mcp_server/
    __init__.py
    server.py          # MCP server app + tool registrations
    tools/
      diagnostics.py
      validation.py
      outliers.py
      normalization.py
      duplicates.py
      imputation.py
      infer_configs.py
    io.py              # GCS path resolution, parquet loading
    schemas.py         # Pydantic input/output models
  Dockerfile.mcp       # Separate from any existing Dockerfiles
  docker-compose.mcp.yml
```

### Framework

Use an MCP server implementation that matches the transport contract used by the host:

- **FridAI hub HTTP remote path today:** JSON-RPC over `POST /rpc` (hub HTTP remotes call `/rpc`).
- **External MCP hosts (Claude Desktop, etc.):** typically `stdio` (or host-specific HTTP/SSE).

For demo speed, pick one of:

- **Option A (FridAI-first, recommended):** expose HTTP JSON-RPC at `/rpc`.
- **Option B (multi-host):** implement `stdio` server mode (and optionally keep `/rpc` for FridAI).

```python
# Keep tool logic transport-agnostic. Wire the same functions to either:
# - HTTP /rpc JSON-RPC handlers (FridAI HTTP remote), or
# - stdio MCP handlers (desktop hosts).
```

### Data I/O design

**Input:** GCS path → server pulls parquet → runs toolkit module → returns results.

Keeps the container fully stateless. No shared volumes, no file mounts. The server needs GCS
credentials (mounted as env var or secret at runtime).

```python
# io.py
def load_from_gcs(gcs_path: str) -> pd.DataFrame:
    """Pull parquet from GCS path into DataFrame. Handles _MANIFEST.json lookup."""
```

If a `_MANIFEST.json` exists at the partition path, read it first to get file list rather than
doing a full glob.

### Tool surface

| Tool | Inputs | JSON response keys |
|---|---|---|
| `toolkit_diagnostics` | `gcs_path`, `config` | `status`, `profile`, `quality_flags`, `artifact_path` |
| `toolkit_validation` | `gcs_path`, `config` | `status`, `passed`, `failed_rules`, `issue_count`, `artifact_path` |
| `toolkit_outliers` | `gcs_path`, `config` | `status`, `flagged_columns`, `outlier_count`, `outlier_log`, `artifact_path` |
| `toolkit_normalization` | `gcs_path`, `config` | `status`, `changes_made`, `changelog`, `artifact_path` |
| `toolkit_duplicates` | `gcs_path`, `config` | `status`, `duplicate_count`, `mode`, `artifact_path` |
| `toolkit_imputation` | `gcs_path`, `config` | `status`, `columns_imputed`, `nulls_filled`, `artifact_path` |
| `toolkit_infer_configs` | `gcs_path`, `modules`, `options` | `configs` (dict of YAML strings keyed by module name) |

All tools include `artifact_path` in the response — the path to the HTML report written as a
side-effect (empty string if HTML export not enabled).

### Tool response pattern

```python
# Every tool returns this shape
{
    "status": "pass" | "warn" | "fail" | "error",
    "module": "outliers",
    "run_id": "...",
    "summary": { ... structured metrics ... },
    "artifact_path": "exports/reports/outliers/run123.html",  # or ""
    # module-specific keys below
    "flagged_columns": [...],
    "outlier_count": 47,
    ...
}
```

### Deployment

```dockerfile
# Dockerfile.mcp
FROM python:3.13-slim
WORKDIR /app
COPY requirements-mcp.txt .
RUN pip install -r requirements-mcp.txt
COPY analyst_toolkit/ analyst_toolkit/
EXPOSE 8080
CMD ["python", "-m", "analyst_toolkit.mcp_server.server"]
```

```yaml
# docker-compose.mcp.yml
services:
  analyst-toolkit-mcp:
    build:
      context: .
      dockerfile: Dockerfile.mcp
    ports:
      - "8080:8080"
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/gcp_creds
    restart: unless-stopped
```

**FridAI remote_manager config (HTTP):**

```yaml
toolkit_local:
  enabled: true
  namespace: toolkit
  transport: http
  base_url: "http://127.0.0.1:8080"
  init_timeout: 20.0
  targets: []
  description: "Analyst toolkit MCP server (local docker)"
```

For this HTTP path, the toolkit server must accept JSON-RPC at `POST /rpc`.

**Podman alternative:** same Dockerfile, same compose — `podman-compose` is a drop-in. Worth
noting in README for open source users who prefer rootless containers.

### Open source + Docker licensing

- MCP server libraries (FastMCP / MCP SDK): MIT ✅
- Docker Engine / Python SDK: Apache 2.0 ✅
- Not distributing Docker — using it as infrastructure ✅
- Docker Desktop: free for personal/open source use ✅

---

## Workstream 3 — `infer-configs` Expansion (~1-2 days)

### Current state

Generates **3 of 9 required configs** from a local CSV only:

- `validation_config_autofill.yaml` ✅
- `certification_config_autofill.yaml` ✅
- `outlier_config_autofill.yaml` ✅

### Changes needed

#### 1. Accept GCS path / Parquet input

Currently hardcoded to local CSV discovery. Needs to accept:

```bash
analyst-infer-configs --input gs://bucket/path/to/partition/
analyst-infer-configs --input path/to/file.parquet
```

Add `_load_input(path: str) -> pd.DataFrame` to `infer_configs.py` that dispatches on path
prefix: `gs://` → GCS pull via `google-cloud-storage`, `.parquet` → `pd.read_parquet()`,
`.csv` → existing logic.

#### 2. `--modules` flag

Generate only the configs needed, not always the same 3:

```bash
analyst-infer-configs --input gs://... --modules validation,outliers,diagnostics
```

Maps module names to their respective config builders.

#### 3. Template generation for missing 6 configs

These can't be fully inferred from data — they require human judgment. But they should be
generated as **well-commented templates with sensible defaults** so the engineer only needs to
fill in the judgment gaps (which the client LLM can assist with interactively).

| Config | What's inferrable | What needs human/LLM judgment |
|---|---|---|
| `diag_config.yaml` | input_path, column list | quality thresholds, skew limits |
| `normalization_config.yaml` | column names | rename rules, value mappings, fuzzy lists |
| `dups_config.yaml` | column names | subset columns, keep policy |
| `handling_config.yaml` | flagged columns from outlier config | strategy per column (clip/median/constant/drop) |
| `imputation_config.yaml` | null rates, dtypes | fill strategy per column |
| `final_audit_config.yaml` | column list, dtypes | columns to drop, final rules |

For normalization specifically — the config template is generated with detected value samples
listed as comments, so the engineer or client LLM can propose mappings:

```yaml
# normalization_config.yaml (generated template)
normalization:
  rules:
    value_mappings:
      # Detected values for 'gender': ['m', 'f', 'male', 'female', 'M', 'F', 'unknown']
      # Suggested: map to normalized form — fill in below
      gender: {}
```

#### 4. Generate `run_toolkit_config.yaml`

The master orchestration file listing all modules, their config paths, and run flags. Currently
missing. Should be generated alongside the individual configs with all detected modules included
and `run: true` for the ones configs were generated for, `run: false` for the rest.

#### 5. Programmatic API stays stable

The `infer_configs.infer_configs()` function is what the MCP server's `toolkit_infer_configs`
tool calls. Its signature expands but remains importable without the CLI:

```python
from analyst_toolkit_deploy.infer_configs import infer_configs

result = infer_configs(
    root=None,           # None when using gcs_path directly
    input_path="gs://bucket/path/",
    modules=["validation", "outliers"],
    outdir="/tmp/configs",
    ...
)
# returns: dict[str, str]  — module_name → YAML string
```

---

## Build Order

1. **HTML reports** — foundation, MCP server artifact_path depends on this
2. **`infer-configs` input handling** — GCS/Parquet support, needed by MCP server
3. **MCP server skeleton** — server.py, io.py, Dockerfile, transport wiring (`/rpc` and/or `stdio`)
4. **MCP tools** — wire each module, starting with outliers + validation (nightly QA priority)
5. **`infer-configs` full expansion** — remaining 6 config templates, `--modules` flag, master config
6. **Pre-author base toolkit YAML** — the static config used by the nightly FridAI spec

---

## Open Questions

- [ ] HTML export path convention — local only, or optionally write to GCS? (GCS useful for nightly artifacts the engineer accesses remotely)
- [ ] MCP server port — 8080 conflicts with anything? Check local dev env.
- [ ] GCS credentials in MCP container — env var mount or Workload Identity for production?
- [ ] `toolkit_infer_configs` return format — dict of YAML strings, or write to a temp path and return paths? YAML strings are more portable for interactive use.
- [ ] `run_toolkit_config.yaml` generation — generate into `outdir` alongside module configs, or separately?
