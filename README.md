<p align="center">
  <img src="repo_files/analyst_toolkit_banner.png" alt="Analyst Toolkit Logo" width="1000"/>
  <br>
  <em>Data QA + Cleaning Engine</em>
</p>
<p align="center">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-stable-brightgreen">
  <img alt="Version" src="https://img.shields.io/badge/version-v0.2.0-blueviolet">
</p>

# 🧪 Analyst Toolkit

A modular, end-to-end data QA and preprocessing pipeline designed for analysts and data scientists.


## 👀 Ecosystem Improvements(NEW)

To make getting started even easier, two companion projects are available:

-   [**Deployment Utility**](https://github.com/G-Schumacher44/analyst_toolkit_deployment_utility): A utility to automate project setup, manage configurations, and run pipelines from a simple interface. Spend less time on scaffolding and more time analyzing data.

-   [**Starter Kit (Zip)**](https://github.com/G-Schumacher44/analyst_toolkit_starter_kit): A portable, one-stop project builder. Download the zip to get a ready-to-use project structure with pre-configured templates, making it easier than ever to use the toolkit.

---

## TLDR;

- Modular execution by stage (diagnostics, validation, normalization, etc.)  
- Inline dashboards and exportable reports  
- Full pipeline execution (notebook or CLI)  
- YAML-configurable logic per module  
- Checkpointing and joblib persistence  
- 🐧 Built using synthetic data from the [dirty_birds_data_generator](https://github.com/G-Schumacher44/dirty_birds_data_generator)
- 📂 [Sample output](exports/sample/)(plots, reports, cleaned dataset)

---

### 📚 Quick Start Notebooks

<p align="left">
  <a href="notebooks/00_analyst_toolkit_modular_demo.ipynb" style="margin-right: 10px;">
    <img alt="Modular Demo" src="https://img.shields.io/badge/Demo%20Notebook-Modular-blue?style=for-the-badge&logo=jupyter" />
  </a>
  &nbsp;&nbsp;
  <a href="notebooks/01_analyst_toolkit_pipeline_demo.ipynb">
    <img alt="Pipeline Demo" src="https://img.shields.io/badge/Demo%20Notebook-Full%20Pipeline-green?style=for-the-badge&logo=python" />
  </a>
</p>

---

<details>
<summary><strong>📝 Notes from the Dev Team</strong></summary>
<br>

**Why build a toolkit for analysts?**

I built the Analyst Toolkit to eliminate the most frustrating part of the analytics workflow — wasting hours on boilerplate cleaning when we should be exploring, validating, and learning. This system gives you:

- A one-stop first-pass QA and cleaning run, fully executable in a single notebook
- Total modularity — run stage by stage or all at once
- YAML-driven control over everything from null handling to audit thresholds

 Every step leaves behind artifacts: dashboards, exports, warnings, checkpoints. You don’t just *run* the pipeline — you *see* it working. You know what changed, where it changed, and what the implications are downstream. Giving the user **auditable automation**, and the insights needed to solve downsteam problems.

It is overbuilt in the ways that matter: transparency, reproducibility, trust. It’s designed for team collaboration, for portfolio projects, for production QA. It’s for your current self — and your future self — when you need to revisit a workflow six months from now.

The system is human readable and YAML-driven — for your team, stakeholders, and yourself.

</details>

<details>
<summary><strong>🫆 version release notes</strong></summary>

**v0.2.0**
  - **Standardized Configuration Handling**: All modules (`diagnostics`, `validation`, `normalization`, `outliers`, `imputation`, `final_audit`) now intelligently parse their own configuration blocks.
  - **Simplified Module API**: Module runners can now be called with the full toolkit configuration object, removing the need for manual unpacking in notebooks or scripts. This makes the API consistent across the entire toolkit.
  - **Notebook & Documentation Updates**: The demo notebook and usage guides have been updated to reflect the simpler, more robust module-calling convention.
  - **Bug Fixes**: Corrected several minor bugs where modules were not correctly passing or interpreting their configurations, leading to more stable and predictable behavior.
  - **Packaging**: Corrected `pyproject.toml` to ensure proper package discovery and installation from GitHub.

**v0.1.3**
  
  - Refactored Duplicates Module (M04):
    - Correctly implemented distinct flag and remove modes.
    - Decoupled detection logic from handling logic for improved robustness and clarity.
    - Enhanced reporting artifacts for both modes, including flagged datasets and - duplicate clusters.

  - Bug Fixes & Stability:
    - Resolved critical bug where flag mode was incorrectly removing rows.
    - Fixed various ImportError and ModuleNotFoundError issues related to project structure and dependencies.
    - Standardized module calls in notebooks to prevent configuration caching issues.

**v0.1.2**
- Core module scaffolding complete (M01–M10)
- Full pipeline execution works in notebook and CLI mode
- Dashboard rendering with inline or exportable options
- Joblib-based checkpointing and YAML-driven behavior

**Plans for v0.2.0**
- Add dynamic changelog to track transformations end-to-end
- Reporting systems and exporting refractor 
- Expand visual EDA and statistical audit tools
- Add streaming-friendly dashboard format (e.g., Streamlit or Voila prototype)

</details>

<details><summary>📎 Resource Hub Links</summary>

- [🧭 Config Guide](resource_hub/config_guide.md) — Overview of all YAML configuration files
- [📦 Config Template Bundle (ZIP)](resource_hub/config.zip) — Full set of starter YAMLs for each module
- [📘 Usage Guide](resource_hub/usage_guide.md) — Running the toolkit via notebooks or CLI
- [📗 Notebook Usage Guide](resource_hub/notebook_usage_guide.md) — Full breakdown of how each module is used in notebooks
</details>

<details>
<summary>📂 Project Structure</summary>

```
📦 src/                              # Source root
│
├── __init__.py                     # (Optional) top-level init
│
├── analyst_toolkit/                # 🔧 Main toolkit package
│   ├── __init__.py
│   ├── run_toolkit_pipeline.py     # CLI + notebook entrypoint
│
│   ├── m00_utils/                  # Shared utilities (config, loading, exporting, rendering)
│   ├── m01_diagnostics/           # Data profiling and structural diagnostics
│   ├── m02_validation/            # Schema validation and certification gate
│   ├── m03_normalization/         # Data cleaning and standardization
│   ├── m04_duplicates/            # Duplicate detection and removal
│   ├── m05_detect_outliers/       # Outlier detection (IQR, z-score)
│   ├── m06_outlier_handling/      # Outlier imputation or transformation
│   ├── m07_imputation/            # Missing data imputation
│   ├── m08_visuals/               # Plotting utilities and dashboard rendering
│   └── m10_final_audit/           # Final audit, edits, and pipeline certification
└── archive/                        # Legacy or prototype modules (optional, safe to ignore)
│
├── 🧪 notebooks/                   # Interactive tutorial notebooks (modular & full run)
│
├── ⚙️ config/                     # YAML configuration files (one per module + full run)
│
├── 📂 data/
│   ├── raw/                       # Original input datasets (e.g., synthetic_penguins_v3.5.csv)
│   ├── processed/                 # Final certified outputs (.csv)
│   └── features/                  # Optional engineered features (if extended)
│
├── 📤 exports/
│   └── samples/                   # sample media from a QA run
│
├── resource_hub                   # Reference, Guidebooks, Documentation
├── pyproject.toml                 # Build config for TOML-based packaging
├── requirements.txt              # Required packages for pip installs
├── .env / .env.template           # Environment variables (if needed)
├── .gitignore                    # Standard ignore patterns
└── README.md                     # Project overview and usage instructions
```
</details>

<details>
<summary><strong>🐧 Dirty Birds: Palmer Penguins Synthetic Dataset v3.5</strong></summary>
<br>

This toolkit is developed and tested using the <strong>Dirty Birds v3.5</strong> dataset — a fully synthetic recreation of the Palmer Penguins dataset, purposefully enriched with ambiguity, anomalies, and missing data. The dataset is generated using <a href="https://github.com/G-Schumacher44/dirty_birds_data_generator">penguin_synthetic_data_generator.py</a>, a synthentic data generator that simulates viable research data and injects realistic biological variance and field collection noise for robust QA testing.


🐧 Features include:
- Categorical anomalies (typos, whitespace, & swaps)
- Numeric outliers and skew (both in error and in biological boundaries)
- Nullable fields in both wide and narrow formats
- Simulated noise to match real-world field data collection

</details>
 
## 🧰 Installation

**🔧 Local Development**

Clone the repo and install locally using the provided `pyproject.toml`:

```bash
git clone https://github.com/G-Schumacher44/analyst_toolkit.git
cd analyst_toolkit
pip install -e .[dev]
```
**🌐 Install Directly via GitHub**

```bash
pip install git+https://github.com/G-Schumacher44/analyst_toolkit.git
```
This installs the latest version from main. To target a specific branch or tag, append @branchname or @v0.1.0 to the URL.

---

## 🧾 Configuration

Each module is controlled by a YAML file stored in `config/`.

Example:

```yaml
validation:
  input_path: "data/raw/synthetic_penguins_v3.5.csv"
  schema_validation:
    run: true
    rules:
      expected_columns: [...]
```

For full structure and explanation, [📘 Read the Full Configuration Guide](resource_hub/config_guide.md)


---

## 🧪 Usage

<details>
<summary>📓 Notebook Use (Modular)</summary>

Run each module interactively inside a Jupyter notebook. 

**Example**

```python
from analyst_toolkit.m02_validation.run_validation_pipeline import run_validation_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config
from analyst_toolkit.m00_utils.load_data import load_csv

# --- Load config and data ---
config = load_config("config/validation_config_template.yaml")
df = load_csv("path/to/your/data.csv")

# --- Extract global settings ---
notebook_mode = config.get("notebook", True)
run_id = config.get("run_id", "demo_run")

# --- Run Validation Module ---
df_validated = run_validation_pipeline(
    config=config, # Pass the full config object
    df=df,
    notebook=notebook_mode,
    run_id=run_id
)
```

Modules render dashboards inline if `notebook: true` is set in the YAML config.

>See [📗 Notebook Usage Guide](resource_hub/notebook_usage_guide.md) for a full breakdown

</details>

<details>
<summary>📓 Notebook Use (Full Pipeline)</summary>

Run the full pipeline interactively inside a Jupyter notebook.

**Example**

```python
from analyst_toolkit.run_toolkit_pipeline import run_full_pipeline

final_df = run_full_pipeline(config_path="config/run_toolkit_config.yaml")

```

Modules render dashboards inline if `notebook: true` is set in the YAML config.

Each module reads its own YAML config file, with optional global overrides in `config/run_toolkit_config.yaml`. Example:

```YAML
# --- Global Run Settings ---
run_id: "CLI_2_QA"
notebook: false

# --- Pipeline Entry Point ---
# The single, explicit path for the initial raw data load.
pipeline_entry_path: "data/raw/synthetic_penguins_v3.5.csv"

#individual module entry points
modules:
  diagnostics:
    run: true
    config_path: "config/diag_config_template.yaml"

  validation:
    run: true
    config_path: "config/validation_config_template.yaml"

```

>See [📗 Notebook Usage Guide](resource_hub/notebook_usage_guide.md) for a full breakdown

</details>

<details>
<summary>🔁 Full Pipeline (CLI)</summary>

Run the pipeline in `CLI` using the fallowing command.

```bash

python -m analyst_toolkit.run_toolkit_pipeline --config config/run_toolkit_config.yaml

```

>For full structure and explanation, [📘 Read the Full Usage Guide](resource_hub/usage_guide.md) 

</details>

<details>
<summary>📃 Dashboard Snapshots</summary>

<div align="center">
  <table>
    <tr>
      <td><img src="repo_files/db_screen_00.png" width="400"/></td>
      <td><img src="repo_files/db_screen_1.png" width="400"/></td>
    </tr>
    <tr>
      <td><img src="repo_files/db_screen_2.png" width="400"/></td>
      <td><img src="repo_files/db_screen_3.png" width="400"/></td>
    </tr>
  </table>
</div>

</details>

</details>

---

## 🤝 On Generative AI Use

Generative AI tools (Gemini 2.5-PRO, ChatGPT 4o - 4.1) were used throughout this project as part of an integrated workflow — supporting code generation, documentation refinement, and idea testing. These tools accelerated development, but the logic, structure, and documentation reflect intentional, human-led design. This repository reflects a collaborative process: where automation supports clarity, and iteration deepens understanding.

---

## 📦 Licensing

This project is licensed under the [MIT License](LICENSE).