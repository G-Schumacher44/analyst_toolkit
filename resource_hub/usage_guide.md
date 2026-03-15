<p align="center">
  <img src="../repo_files/analyst_toolkit_banner.png" alt="Analyst Toolkit Logo" width="1000"/>
  <br>
  <em>Self-Healing Data Audit &nbsp;·&nbsp; Data QA + Cleaning Engine &nbsp;·&nbsp; MCP Server</em>
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


# 📘 Analyst Toolkit — Usage Guide

This guide walks through how to use the Analyst Toolkit for data cleaning, validation, and QA auditing — via notebooks, CLI, programmatic import, or MCP server.


## ⚙️ Setup

**🔧 Local Development**

Clone the repo and install locally:

```bash
git clone https://github.com/G-Schumacher44/analyst_toolkit.git
cd analyst_toolkit
make install-dev       # editable install + dev tooling + notebook extras
```

**With MCP server deps**

```bash
pip install -e ".[mcp]"
```

**With notebook extras**

```bash
pip install "analyst_toolkit[notebook] @ git+https://github.com/G-Schumacher44/analyst_toolkit.git"
```

**🌐 Install Directly via GitHub (bare)**

```bash
pip install git+https://github.com/G-Schumacher44/analyst_toolkit.git
```

This installs the latest version from main. To target a specific branch or tag, append `@branchname` or `@v0.4.4` to the URL.

---

## ⚙️ Configuration Files

Each module is configured via a YAML file located in the `config/` directory. These files control:

- File paths for inputs/outputs
- Behavior toggles (e.g., `run: true`, `show_inline: true`)
- Thresholds and expected schema
- Plotting and export options

> 📌 See each config template in `config/` for structure and examples. For the full configuration reference, see [🧭 Config Guide](config_guide.md).

<details>
<summary><strong>⚙️ YAML Example (Final Audit Template)</strong></summary>

**Sample Configuration (`final_audit_config_template.yaml`)**
```yaml
final_audit:
  run: true
  final_edits:
    drop_columns:
      - 'body_mass_g_zscore_outlier'
      - 'bill_length_mm_iqr_outlier'
    # You can also add rename_columns and coerce_dtypes here
  certification:
    run: true
    fail_on_error: true
    rules:
      # ... strict validation rules ...
      disallowed_null_columns:
        - 'tag_id'
        - 'species'
```

When running the full pipeline in either `notebook` or `CLI` each module reads its own YAML config file, with optional global overrides in `config/run_toolkit_config.yaml`.

**Example:**

```yaml
# --- Global Run Settings ---
run_id: "CLI_2_QA"
notebook: false

# --- Pipeline Entry Point ---
pipeline_entry_path: "data/raw/synthetic_penguins_v3.5.csv"

modules:
  diagnostics:
    run: true
    config_path: "config/diag_config_template.yaml"

  validation:
    run: true
    config_path: "config/validation_config_template.yaml"
```

</details>

---


## 🧪 Using the Toolkit

<details>
<summary><strong>📚 Modular Notebook Use</strong></summary>
<br>


Use `notebooks/00_analyst_toolkit_modular_demo.ipynb` to:

- Run one module at a time
- Inspect intermediate results
- Display inline dashboards
- Tweak parameters or YAML and re-run

Each stage (M01–M10) can be executed individually with full visibility.

>See [📗 Notebook Usage Guide](notebook_usage_guide.md) for a full breakdown

<details>
<summary><strong>Notebook Example</strong></summary>


**🔬 Modular Stage (M05: Outlier Detection)**

```python
from analyst_toolkit.m00_utils.config_loader import load_config
from analyst_toolkit.m05_detect_outliers.run_detection_pipeline import run_outlier_detection_pipeline

# --- Load Config & Data ---
config = load_config("config/outlier_config_template.yaml")
run_id = config.get("run_id", "notebook_run_01")
notebook_mode = config.get("notebook", True)

# --- Run Outlier Detection ---
df_outliers_flagged, results = run_outlier_detection_pipeline(
    config=config,
    df=df_deduped,
    notebook=notebook_mode,
    run_id=run_id
)
```

</details>

---

</details>

<details>
<summary><strong>⚙️ Pipeline Execution</strong></summary>
<br>

Use `notebooks/01_analyst_toolkit_pipeline_demo.ipynb` or run the CLI directly:

### 🔩 For pipeline use with CLI or Notebook

**In Notebook**
```python
from analyst_toolkit.run_toolkit_pipeline import run_full_pipeline

final_df = run_full_pipeline(config_path="config/run_toolkit_config.yaml")
```

**In CLI**

```bash
make pipeline                              # uses config/run_toolkit_config.yaml
make pipeline CONFIG=config/my_config.yaml # custom config
# or directly:
python -m analyst_toolkit.run_toolkit_pipeline --config config/run_toolkit_config.yaml
```

This runs all pipeline stages in order using the config file. Outputs include:

- Final certified CSV
- Joblib checkpoints
- Standalone HTML dashboards per module
- Exported XLSX/CSV reports
- Saved plots for every module

You can also set `notebook: false` to run in silent (headless) mode for automation.

</details>

<details>
<summary><strong>📦 Programmatic Use</strong></summary>
<br>


You can also use the Analyst Toolkit as a package by installing it directly from GitHub — no cloning required:

```bash
pip install git+https://github.com/G-Schumacher44/analyst_toolkit.git
```

Then, import and use modules like any Python package:

```python
from analyst_toolkit.m02_validation.run_validation_pipeline import run_validation_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config

# Load the full config object
config = load_config("config/validation_config_template.yaml")

validated_df = run_validation_pipeline(
    config=config,
    df=df,
    run_id="demo_run",
    notebook=True
)
```

This allows programmatic access to every pipeline module without running the full system.

</details>

---

## 📡 MCP Server

The toolkit ships as an MCP server, exposing every module as a tool callable by any MCP-compatible host — Claude Desktop, FridAI, VS Code, or any JSON-RPC 2.0 client.

<details>
<summary><strong>🔧 Quick Start</strong></summary>
<br>

**Pull from GHCR:**

```bash
docker pull ghcr.io/g-schumacher44/analyst-toolkit-mcp:latest
```

**Or build and start locally:**

```bash
make mcp-up        # docker compose up --build -d
make mcp-health    # curl /health and pretty-print response
make mcp-logs      # tail logs
make mcp-down      # stop
```

**Call a tool:**

```bash
curl -X POST http://localhost:8001/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"outliers","arguments":{"gcs_path":"gs://my-bucket/data/"}}}'
```

Tools accept a `gcs_path` (GCS URI, local `.parquet`, or local `.csv`) and an optional `config` dict matching the module's YAML structure.

> See [📡 MCP Server Guide](mcp_server_guide.md) for full setup, tool reference, and host integration details.

</details>

<details>
<summary><strong>🤖 Auto-Heal</strong></summary>
<br>

One-click remediation — from raw data to certified output in a single tool call. Auto-heal combines three steps automatically:

1. **Infer configs** — profiles data and generates module configurations
2. **Normalize** — applies inferred cleaning rules (whitespace, casing, type coercion)
3. **Impute** — fills missing values using inferred strategies

Returns a healed `session_id`, step-by-step status, and a standalone HTML dashboard summarizing all transformations.

Available as the `auto_heal` MCP tool. See `config/auto_heal_request_template.yaml` for the request shape.

</details>

<details>
<summary><strong>📊 Data Dictionary</strong></summary>
<br>

Generates a column-level schema and metadata report as a standalone HTML dashboard. Includes:

- Inferred column types (identifier, boolean, datetime, numeric, categorical, text)
- Null rates, cardinality, and sample values
- Prelaunch readiness assessment — flags columns missing validation rules or with metadata gaps
- Configurable profile depth: `light`, `standard` (default), or `deep`

Available as the `data_dictionary` MCP tool. See `config/data_dictionary_request_template.yaml` for the request shape.

</details>

<details>
<summary><strong>🕹️ Cockpit Dashboard</strong></summary>
<br>

A unified operator hub that links every module dashboard for a run into one reviewable session view. Shows:

- Recent run cards with health score, status, and module count
- Quick-access links to every module's HTML dashboard
- Operating posture (Healthy / Needs Review / Blocked) based on run outcomes
- Data dictionary preview and prelaunch readiness
- Launchpad quickstart actions (infer configs, open pipeline dashboard, run auto-heal)

Available as the `get_cockpit_dashboard` MCP tool.

</details>

<details>
<summary><strong>🧭 Local Artifact Server</strong></summary>
<br>

An optional localhost server that converts dashboard file paths into browser-openable URLs — no manual file navigation needed.

Enable with environment variables:

```bash
ANALYST_MCP_ENABLE_ARTIFACT_SERVER=1    # enable the server
ANALYST_MCP_ARTIFACT_SERVER_PORT=8765   # default port
```

Once enabled, cockpit and module dashboard artifact references become live links (e.g., `http://127.0.0.1:8765/exports/reports/...`). Localhost-only by default.

Available as the `ensure_artifact_server` MCP tool.

</details>

<details>
<summary><strong>⚡ Runtime Overlays</strong></summary>
<br>

A shared configuration pattern that centralizes run-scoped settings across all MCP tools, instead of repeating them in every module config:

```yaml
runtime:
  run:
    run_id: "my_run"
    input_path: "data/raw/my_data.csv"
  artifacts:
    export_html: true
    export_xlsx: false
    plotting: true
  destinations:
    local:
      enabled: true
      root: "exports/"
```

Pass `runtime` as a parameter to any MCP tool. Runtime values deep-merge into tool-specific configs with runtime taking precedence.

See `config/runtime_overlay_template.yaml` for the full shape.

</details>

---

## 📸 Dashboard Exports

Every pipeline module now produces a standalone, self-contained HTML dashboard — no external dependencies, single-file export.

| Dashboard | Source |
| --------- | ------ |
| Cockpit Hub | `get_cockpit_dashboard` tool — links all module dashboards for a run |
| Pipeline Dashboard | `get_pipeline_dashboard` tool — multi-module tabbed view for a run |
| Diagnostics | M01 — data profile, nulls, types, cardinality, distributions |
| Validation | M02 — schema rules, column expectations, pass/fail results |
| Normalization | M03 — before/after transformations, string cleaning, type casting |
| Duplicates | M04 — duplicate detection, subset clusters, removal summary |
| Outlier Detection | M05 — IQR and z-score methods, outlier flags, statistics |
| Outlier Handling | M06 — imputation/transformation applied, before/after comparisons |
| Imputation | M07 — missing value strategies, before/after distributions |
| Final Audit | M10 — certification results, final edits, pass/fail gates |
| Auto-Heal | Multi-step healing summary (infer → normalize → impute) |
| Data Dictionary | Column metadata, semantic types, prelaunch readiness |

Enable HTML export with `export_html: true` in a runtime overlay or module config.

> See [sample HTML dashboards](../exports/sample/) for full rendered examples.

---

## 🧭 Module Index

| Stage | Module Name       | Description                                                   |
| ----- | ----------------- | ------------------------------------------------------------- |
| M01   | Diagnostics       | Profile data: shape, types, nulls, skew, sample               |
| M02   | Validation        | Schema check, dtype verification, null rules (soft or strict) |
| M03   | Normalization     | Clean up whitespace, case, type coercion, and fuzzy matching  |
| M04   | Duplicates        | Flag or remove exact row duplicates                           |
| M05   | Outlier Detection | Detect outliers using IQR or Z-score                          |
| M06   | Outlier Handling  | Transform, impute, or clip flagged outliers                   |
| M07   | Imputation        | Fill missing values via mean, median, mode, or constant       |
| M08   | Visuals           | *Utility module for generating plots (not run directly)*      |
| M10   | Final Audit       | Final cleanup, schema certification, and export               |

---

## 🛠️ YAML Tips

- Use `{run_id}` in paths for auto-named outputs
- Set `show_inline: true` for notebook dashboards
- Use `checkpoint: true` to save intermediate DataFrames
- You can safely skip modules by setting `run: false`
- Set `logging:` to control global logging behavior:
  - `on`: always log to console or file
  - `off`: disable logging entirely
  - `auto`: follow `notebook_mode` logic — quiet in notebooks, verbose in CLI
- Golden templates in `config/golden_templates/` provide pre-tuned configs for common patterns (fraud detection, data migration, compliance audit)

---

## 📦 Artifacts Produced

- Standalone HTML dashboards per module (cockpit, diagnostics, validation, normalization, duplicates, outliers, outlier handling, imputation, final audit, auto-heal, data dictionary)
- Multi-module pipeline dashboard (tabbed view across all stages for a run)
- Checkpointed `.joblib` files per module
- XLSX/CSV summary reports
- Boxplots, histograms, and validation plots
- Data health score (0–100) and healing ledger (JSON history of all transformations)

---

## 🔗 Quick Access

[📁 View YAML Configs ›](https://github.com/G-Schumacher44/analyst_toolkit/tree/main/config)

<div style="margin-top: 1em; margin-bottom: 1em;">

<a href="https://github.com/G-Schumacher44/analyst_toolkit/blob/main/notebooks/00_analyst_toolkit_modular_demo.ipynb">
  <img src="https://img.shields.io/badge/Demo_Notebook-00_Modular-blue?logo=jupyter" style="margin: 4px;" />
</a>

<a href="https://github.com/G-Schumacher44/analyst_toolkit/blob/main/notebooks/01_analyst_toolkit_pipeline_demo.ipynb">
  <img src="https://img.shields.io/badge/Demo_Notebook-01_Full_Pipeline-purple?logo=jupyter" style="margin: 4px;" />
</a>

</div>

___

## 🧠 Need Help?

This project is designed to be auditable and transparent. For help:

- View example notebooks in `/notebooks/`
- Read module docstrings for function usage
- See the [📡 MCP Server Guide](mcp_server_guide.md) for MCP tool reference and host integration
- See the [🧭 Config Guide](config_guide.md) for YAML configuration details
- Ask a question via GitHub Issues

---

<p align="center">
  🔙 <a href="../README.md"><strong>Return to Project README</strong></a>
</p>
