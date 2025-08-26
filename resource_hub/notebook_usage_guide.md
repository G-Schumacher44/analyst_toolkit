<p align="center">
  <img src="../repo_files/dark_logo_banner.png" width="1000"/>
  <br>
  <em>Data QA + Cleaning Engine</em>
</p>
<p align="center">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-stable-brightgreen">
  <img alt="Version" src="https://img.shields.io/badge/version-v0.1.0-blueviolet">
</p>


## 🔰 Analyst Toolkit Notebook Usage Guide

This guide explains how to use each module in an interactive notebook session. It assumes you've installed the toolkit using `pip install -e .`.

>💡 As a best practice across all modules, we recommend renaming each DataFrame after a stage is complete (e.g., `df_diagnosed`, `df_validated`, `df_normalized`) to maintain clarity in modular workflows.


### ✅ Getting Started

```bash
pip install -e .
```

### 🧭 Standard Import Pattern

```python
# Example imports after installation
from analyst_toolkit.m00_utils.load_data import load_csv
from analyst_toolkit.m01_diagnostics import run_diag_pipeline
from analyst_toolkit.m02_validation import run_validation_pipeline
from analyst_toolkit.m03_normalization import run_normalization_pipeline
from analyst_toolkit.m04_duplicates import run_duplicates_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config
```

---

## 🧪 General Usage Pattern

Each module follows a standard structure:

```python
df, results = run_<module>_pipeline(df, config)
```

Where:
- `df`: the working DataFrame (output from previous module or `load_csv()`)
- `config`: dictionary loaded from your YAML config file
- `results`: diagnostic summary, export paths, changelogs, or other metadata

---

## 📦 Module Walkthrough

### 1. Load Data

```python
from analyst_toolkit.m00_utils.load_data import load_csv

df = load_csv("data/raw/my_data.csv")
```

---

### 2. Diagnostics

```python
from analyst_toolkit.m01_diagnostics import run_diag_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config

config = load_config("config/diagnostics_config_template.yaml")
diag_cfg = config.get("diagnostics", {})
run_id = config.get("run_id", "demo_run")
notebook_mode = config.get("notebook", True)

df = run_diag_pipeline(
    df=df,
    config=diag_cfg,
    notebook=notebook_mode,
    run_id=run_id
)
```

Returns unmodified `df` and outputs plots and summary data. Reports are saved to `exports/reports/diagnostics/`.

---

### 3. Validation

```python
from analyst_toolkit.m02_validation import run_validation_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config

config = load_config("config/validation_config_template.yaml")
val_cfg = config.get("validation", {})
run_id = config.get("run_id", "demo_run")
notebook_mode = config.get("notebook", True)

df = run_validation_pipeline(
    df=df,
    config=val_cfg,
    notebook=notebook_mode,
    run_id=run_id
)
```

Checks schema, types, nulls. Will error if `fail_on_error=True`.

---

### 4. Normalization

```python
from analyst_toolkit.m03_normalization import run_normalization_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config

# Load configuration and extract relevant values
config = load_config("config/normalization_config_template.yaml")
norm_cfg = config.get("normalization", {})
run_id = config.get("run_id", "demo_run")
notebook_mode = config.get("notebook", True)

# Run normalization module with notebook-specific arguments
df = run_normalization_pipeline(
    df=df,
    config=norm_cfg,
    notebook=notebook_mode,
    run_id=run_id
)
```

Returns cleaned `df` after applying normalization steps.

---

### 5. Duplicates

```python
from analyst_toolkit.m04_duplicates import run_duplicates_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config

config = load_config("config/duplicates_config_template.yaml")
dupes_cfg = config.get("duplicates", {})
run_id = config.get("run_id", "demo_run")
notebook_mode = config.get("notebook", True)

df = run_duplicates_pipeline(
    df=df,
    config=dupes_cfg,
    notebook=notebook_mode,
    run_id=run_id
)
```

Returns deduplicated data and an export log. Configurable to `flag`, `drop`, or `report`.

---

### 6. Detect Outliers
```python
from analyst_toolkit.m05_detect_outliers.run_detection_pipeline import run_outlier_detection_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config

config = load_config("config/outlier_config_template.yaml")
detect_cfg = config.get("outlier_detection", {})
run_id = config.get("run_id", "demo_run")
notebook_mode = config.get("notebook", True)

df_outliers_flagged, detection_results = run_outlier_detection_pipeline(
    df=df,
    config=detect_cfg,
    notebook=notebook_mode,
    run_id=run_id
)
```

Flags potential outliers and logs summary statistics. Outlier scores or flags may be added to the DataFrame depending on configuration.

---

### 7. Handle Outliers

```python
from analyst_toolkit.m06_outlier_handling import run_outlier_handling_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config

config = load_config("config/outlier_handle_config_template.yaml")
handle_cfg = config.get("outlier_handling", {})
run_id = config.get("run_id", "demo_run")
notebook_mode = config.get("notebook", True)

df = run_outlier_handling_pipeline(
    df=df,
    config=handle_cfg,
    notebook=notebook_mode,
    run_id=run_id
)
```

Applies chosen strategy (e.g., winsorization, removal, replacement) to outliers flagged in the previous step.

---

### 8. Imputation

```python
from analyst_toolkit.m07_imputation import run_imputation_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config

config = load_config("config/imputation_config_template.yaml")
imp_cfg = config.get("imputation", {})
run_id = config.get("run_id", "demo_run")
notebook_mode = config.get("notebook", True)

df = run_imputation_pipeline(
    df=df,
    config=imp_cfg,
    notebook=notebook_mode,
    run_id=run_id
)
```

Fills missing values based on rule-based or statistical methods defined in the config.

---

### 9. Final Audit

```python
from analyst_toolkit.m10_final_audit import run_final_audit_pipeline
from analyst_toolkit.m00_utils.config_loader import load_config

config = load_config("config/final_audit_config_template.yaml")
audit_cfg = config.get("final_audit", {})
run_id = config.get("run_id", "demo_run")
notebook_mode = config.get("notebook", True)

df = run_final_audit_pipeline(
    df=df,
    config=audit_cfg,
    notebook=notebook_mode,
    run_id=run_id
)
```

Runs final shape check, summary statistics, and quality verification. Marks completion of the pipeline process.

---

### 🧠 Full Pipeline (Optional)

You can run the entire pipeline via:

```python
from analyst_toolkit.run_toolkit_pipeline import run_toolkit_pipeline

df_final, all_results = run_toolkit_pipeline(config)
```

---

Let us know if you’d like module-by-module walkthroughs or demo notebooks.

<p align="center">
  🔙 <a href="../README.md"><strong>Return to Project README</strong></a>
</p>