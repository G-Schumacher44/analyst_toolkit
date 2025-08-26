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

---

## 🧩 Configuration Guide Overview

The Analyst Toolkit is governed entirely by modular YAML configuration files. These configs define the behavior of each module — from data validation and profiling to outlier handling and certification testing.

Each YAML configuration file serves a specific purpose and is passed to the pipeline via CLI or notebook workflows.

### Douwloadable YAML Templates

>[Download Templates](config.zip)
___

## 📚 YAML Setup Guidebook


### 🔧 Key Global Sections

- **`notebook` / `logging` / `run_id`**: 
  - Control inline output and logging behavior.
  - `run_id` tags all output artifacts.

---

<details>
<summary><strong>📊 diag_config_template.yaml</strong> — Data Profiling & Quality Audits</summary>

This configuration controls the **Diagnostics** module (`run_diag_pipeline.py`), which generates a non-destructive structural and statistical profile of a dataset.

### 🔧 Key Sections

Runs the core data profiling logic, producing:
- Schema overview with types and uniqueness
- Missing value counts and percentages
- High-cardinality string fields
- Descriptive statistics (mean, std, skew, kurtosis)
- Sample data and duplicate summaries
- Audit flags (e.g., high skew, unexpected dtypes)

### `settings` (under `profile`)
- `export`: Save profile to disk (XLSX or CSV)
- `as_csv`: If true, exports CSV instead of Excel
- `export_path`: Path for saved summary
- `checkpoint`: Enable joblib-based caching
- `include_samples`: Show `df.head()` preview
- `include_metadata`: Include memory and shape stats
- `max_rows`: Row limit for previews
- `high_cardinality_threshold`: Max unique values before flagging a column

#### `quality_checks`
Used to flag data issues:
- `skew_threshold`: Flag numeric columns exceeding this skew
- `expected_dtypes`: Optional map of columns to expected types. Flags mismatches.

---

### `diagnostics.plotting`
Controls if diagnostic visualizations are generated:
- `run`: Toggle diagnostic plot generation
- `save_dir`: Path to save the plots (e.g., histograms, outlier visuals)

---

### ✅ Example

```yaml
diagnostics:
  input_path: "data/raw/my_dataset.csv"

  profile:
    run: true
    settings:
      export: true
      as_csv: false
      export_path: "exports/reports/diagnostics/diagnostics_summary.xlsx"
      max_rows: 5
      high_cardinality_threshold: 10
      quality_checks:
        skew_threshold: 2.0
        expected_dtypes:
          age: "int64"
          income: "float64"
          gender: "object"

  plotting:
    run: true
    save_dir: "exports/plots/diagnostics/"
```

</details>

<details>
<summary><strong>🛡️ validation_config.yaml & certification_config.yaml</strong> — Schema and Content Validation (Soft vs Strict Modes)</summary>

This configuration governs the schema and content validation stage of the pipeline. It can operate in two distinct modes:

- **Validation Mode (soft)** — used during exploratory analysis. The pipeline continues even if errors are detected.
- **Certification Mode (strict)** — used as a final QA gate. If any check fails, the pipeline halts (`fail_on_error: true`).

### 🔧 Key Sections
- `input_path`: path to the dataset under validation
- `schema_validation.run`: toggles the schema validation logic
- `schema_validation.fail_on_error`: enforces strict blocking in certification mode
- `rules.expected_columns`: required column names
- `rules.expected_types`: expected dtypes (e.g., float64, object, datetime64[ns])
- `rules.categorical_values`: allowed values for string columns
- `rules.numeric_ranges`: minimum/maximum thresholds for numeric fields
- `settings`: controls export paths and joblib checkpointing

Example:
```yaml
validation:
  input_path: "data/raw/my_dataset.csv"
  schema_validation:
    run: true
    fail_on_error: true  # Set false for non-blocking validation

    rules:
      expected_columns:
        - "tag_id"
        - "species"
        - "bill_length_mm"
      expected_types:
        tag_id: "object"
        species: "object"
        bill_length_mm: "float64"
      categorical_values:
        species: ["Adelie", "Chinstrap", "Gentoo"]
      numeric_ranges:
        bill_length_mm:
          min: 30.0
          max: 65.0

  settings:
    checkpoint: true
    export: true
    export_path: "exports/reports/certification/my_report.xlsx"
```

> This module supports precise QA policies and lets you define flexible column validation logic. Use `fail_on_error: false` to run audits without blocking your pipeline.
</details>

<details>
<summary><strong>🔀 normalization_config.yaml</strong> — Column cleaning, type coercion, and fuzzy matching</summary>

This configuration governs the **Normalization** module, which applies rule-based data cleaning transformations including column renaming, value mapping, typo correction, and type enforcement.

### 🔧 Key Sections

- `rename_columns`: Rename messy or inconsistent column headers
- `standardize_text_columns`: Auto-title-case or upper-case entries
- `value_mappings`: Explicit mapping dictionary for known text replacements
- `fuzzy_matching`: Runs fuzzy string matching on selected fields
  - `master_list`: List of valid values
  - `score_cutoff`: Minimum similarity score
- `parse_datetimes`: Force conversion of date columns
- `coerce_dtypes`: Enforce column data types (e.g. float64, int64)
- `preview_columns`: Fields shown in preview output or reports
- `settings`: Controls export, joblib checkpointing, and inline rendering

### ✅ Example
```yaml
normalization:
  run: true

  rules:
    rename_columns:
      'bill length (mm)': 'bill_length_mm'

    standardize_text_columns:
      - 'sex'

    value_mappings:
      sex:
        'f': 'FEMALE'
        'm': 'MALE'
        '?': 'UNKNOWN'

    fuzzy_matching:
      run: true
      settings:
        species:
          master_list: ["Adelie", "Chinstrap", "Gentoo"]
          score_cutoff: 80

    parse_datetimes:
      capture_date:
        format: '%Y-%m-%d'
        errors: 'coerce'

    coerce_dtypes:
      bill_length_mm: 'float64'

  settings:
    show_inline: true
    export: true
    export_path: "exports/reports/normalization/normalization_report.xlsx"
    checkpoint:
      run: true
      checkpoint_path: "exports/joblib/{run_id}/{run_id}_m03_df_normalized.joblib"
```
</details>

<details>
<summary><strong>📛 dups_config_template.yaml</strong> — Detecting and Handling Duplicates</summary>

This configuration governs the **Duplicate Detection** module. It supports both **flagging** and **removing** duplicate rows using custom logic.

### 🔧 Key Sections

- `subset_columns`: Columns to consider for duplicate matching (default: all)
- `keep`: Which duplicate to retain — `'first'`, `'last'`, or `False` to drop all duplicates
- `mode`: Whether to `'remove'` or `'flag'` duplicates
- `input_path`: Path to the input dataset
- `settings`: Controls export, checkpointing, and visualization

### ✅ Example
```yaml
duplicates:
  run: true
  subset_columns: null
  keep: "first"
  mode: "remove"
  input_path: "exports/joblib/{run_id}_m02_2_df_certified.joblib"

  settings:
    checkpoint: true
    checkpoint_path: "exports/joblib/{run_id}/{run_id}_m04__dupes_checkpoint.joblib"

    export: true
    export_path: "exports/reports/duplicates/duplicates_report.xlsx"
    export_format: "xlsx"

    show_inline: true

    plotting:
      run: true
      save_dir: "exports/plots/duplicates/"
```
> Optional cleanup is available for schema-variant files using `preview_drop_columns`.
</details>

<details>
<summary><strong>📉 outlier_config_template.yaml</strong> — Statistical Outlier Detection</summary>

This configuration governs the **Outlier Detection** module, which identifies anomalous values in numeric fields based on statistical rules. No data is modified — this is a non-destructive detection pass.

### 🔧 Key Sections

- `run`: Toggle to enable detection
- `method`: Detection strategy — supports `'zscore'`, `'iqr'`, or `'percentile'`
- `threshold`: Cutoff value (e.g., `3.0` for z-score, `1.5` for IQR multiplier)
- `features`: List of numeric columns to scan for outliers

### ✅ Example
```yaml
outlier_detection:
  run: true
  method: "iqr"
  threshold: 1.5
  features:
    - "bill_length_mm"
    - "body_mass_g"
```

> This stage produces a flagged dataset and optionally exports outlier distributions as plots.
</details>

<details>
<summary><strong>🧼 handling_config_template.yaml</strong> — Outlier Correction Strategies</summary>

This configuration governs the **Outlier Handling** module (`run_handling_pipeline.py`). It takes in the outputs of the outlier detection stage and applies **corrective transformations** to flagged values based on global or column-specific rules.

### 🔧 Key Sections

- **`input_df_path`**: Path to the flagged dataset from the previous detection stage  
- **`detection_results_path`**: Path to the joblib file containing outlier masks  
- **`handling_specs`**: Core section specifying how each column's outliers should be treated:
  - `clip`: cap values to upper/lower statistical bounds
  - `median`: replace with column median
  - `constant`: replace with a specified fallback value (`fill_value`)
  - `none`: leave outliers untouched
- **`__default__` / `__global__`**:
  - `__global__`: strategy applied unless overridden  
  - `__default__`: fallback if column not explicitly mentioned
- **`settings`**: Export and checkpointing behavior, display toggles

### ✅ Example
```yaml
outlier_handling:
  run: true

  input_df_path: "exports/joblib/{run_id}_m05_outliers_flagged.joblib"
  detection_results_path: "exports/joblib/{run_id}_m05_detection_results.joblib"

  handling_specs:
    __global__:
      strategy: 'none'

    bill_length_mm:
      strategy: 'clip'

    body_mass_g:
      strategy: 'median'

    flipper_length_mm:
      strategy: 'constant'
      fill_value: -999

    __default__:
      strategy: 'clip'

  settings:
    show_inline: true

    export:
      run: true
      export_path: "exports/reports/outliers/handling/outlier_handling_report.xlsx"
      as_csv: false

    checkpoint:
      run: true
      checkpoint_path: "exports/joblib/{run_id}/{run_id}_m06_df_handled.joblib"
```

> This module is typically used after `detect_outliers.yaml`. It does not perform new detection — only applies remediation to already-flagged values.

</details>

<details>
<summary><strong>🩹 imputation_config_template.yaml</strong> — Missing Value Imputation</summary>

This configuration governs the **Imputation** module (`run_imputation_pipeline.py`). It fills missing values (`NaN`) using specified strategies per column and supports both numeric and categorical imputation.

### 🔧 Key Sections

- **`input_path`**: Path to the input dataset (typically from previous outlier handling step)
- **`rules.strategies`**: Dictionary specifying how to impute each column:
  - `'mean'`: Fill with column mean (numeric only)
  - `'median'`: Fill with column median (numeric only)
  - `'mode'`: Fill with most common value
  - `'constant'`: Replace with fixed value via nested dict `{strategy: 'constant', value: ...}`

- **`settings`**: Controls plotting, inline output, checkpointing, and export location

### ✅ Example
```yaml
imputation:
  run: true
  input_path: "exports/joblib/{run_id}_m06_df_handled.joblib"

  rules:
    strategies:
      bill_length_mm: 'mean'
      body_mass_g: 'mean'
      bill_depth_mm: 'median'
      flipper_length_mm: 'median'
      sex: 'mode'
      tag_id:
        strategy: 'constant'
        value: 'UNKNOWN'
      capture_date:
        strategy: 'constant'
        value: "1900-01-01"

  settings:
    show_inline: true
    export:
      run: true
      export_path: "exports/reports/imputation/imputation_report.xlsx"
    plotting:
      run: true
      save_dir: "exports/plots/imputation/"
    checkpoint:
      run: true
      checkpoint_path: "exports/joblib/{run_id}/{run_id}_m07_df_imputed.joblib"
```

> Use this module to address data sparsity before modeling or final audits. Can be customized per column using a mix of strategy types.
</details>

<details>
<summary><strong>✅ final_audit_config.yaml</strong> — Sanity Check Before Export</summary>

This configuration governs the **Final Audit** module, a lightweight but essential QA step applied just before the final export of cleaned data. It checks for any lingering issues that may have slipped through prior transformations.

### 🔧 Key Sections

- `run`: Toggles the final audit logic
- `input_path`: Path to the dataset being audited
- `checks`: List of final sanity checks to run:
  - `no_nulls`: Flags if any null values remain
  - `expected_columns`: Ensures final schema matches expectations
  - `range_checks`: Optional column-level numeric thresholds
- `settings`: Controls export and checkpoint behavior

### ✅ Example
```yaml
final_audit:
  run: true
  input_path: "exports/joblib/{run_id}_m07_cleaned_dataset.joblib"

  checks:
    no_nulls: true
    expected_columns:
      - "tag_id"
      - "species"
      - "bill_length_mm"
      - "body_mass_g"
    range_checks:
      bill_length_mm:
        min: 25
        max: 65
      body_mass_g:
        min: 2500
        max: 6500

  settings:
    show_inline: true
    export: true
    export_path: "exports/reports/final_audit/final_audit_report.xlsx"
```

> This step ensures your output is clean, consistent, and ready for analysis or delivery. It's often used as a guardrail before dataset certification or ML model ingestion.

</details>

---

<p align="center">
  🔙 <a href="../README.md"><strong>Return to Project README</strong></a>
</p>