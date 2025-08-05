"""
⚙️ Module: normalize_data.py

Core producer for the M03 Normalization module.

This module applies configurable rule-based transformations to clean and standardize
a pandas DataFrame. It supports column renaming, string cleaning, value remapping,
fuzzy matching, datetime parsing, and final dtype coercion — with detailed changelog tracking.

Used in stateful pipeline steps to transform validated data into a modeling-ready form.
"""
import pandas as pd
import logging
import numpy as np
from thefuzz import process as fuzz_process

def standardize_text(series: pd.Series) -> pd.Series:
    """Standardizes strings by trimming and lowercasing, preserving nulls."""
    return series.apply(lambda x: x.strip().lower() if isinstance(x, str) else x)

def apply_normalization(df: pd.DataFrame, rules: dict) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Applies all configured normalization rules in a defined, fail-safe order."""
    df_original = df.copy()
    df_normalized = df.copy()
    changelog = {}

    # --- 1. Rename Columns ---
    try:
        rename_map = rules.get("rename_columns", {})
        if rename_map:
            df_normalized.rename(columns=rename_map, inplace=True)
            changelog['renamed_columns'] = pd.DataFrame(rename_map.items(), columns=["Original Name", "New Name"])
    except Exception as e:
        logging.error(f"Failed during column renaming: {e}")

    # --- 2. Standardize Text ---
    try:
        string_cols = rules.get("standardize_text_columns", [])
        if string_cols:
            cleaned_info = [{"Column": col, "Operation": "standardize_text"} for col in string_cols if col in df_normalized.columns]
            for item in cleaned_info:
                df_normalized[item["Column"]] = standardize_text(df_normalized[item["Column"]])
            if cleaned_info: changelog['strings_cleaned'] = pd.DataFrame(cleaned_info)
    except Exception as e:
        logging.error(f"Failed during text standardization: {e}")

    # --- 3. Map Known Values ---
    try:
        value_maps = rules.get("value_mappings", {})
        if value_maps:
            mapped_info = []
            for col, mapping in value_maps.items():
                if col in df_normalized.columns:
                    if 'null' in mapping: mapping[np.nan] = mapping.pop('null')
                    df_normalized[col] = df_normalized[col].replace(mapping)
                    mapped_info.append({"Column": col, "Mappings Applied": len(mapping)})
            if mapped_info: changelog['values_mapped'] = pd.DataFrame(mapped_info)
    except Exception as e:
        logging.error(f"Failed during value mapping: {e}")
    
    # --- 4. Fuzzy Match Unknown Typos ---
    try:
        fuzzy_config = rules.get("fuzzy_matching", {})
        if fuzzy_config and fuzzy_config.get("run", False):
            fuzzy_settings = fuzzy_config.get("settings", {})
            fuzzy_info = []
            for col, settings in fuzzy_settings.items():
                if col in df_normalized.columns and df_normalized[col].dtype == 'object':
                    master_list = settings.get("master_list", [])
                    cutoff = settings.get("score_cutoff", 90)
                    if not master_list:
                        logging.warning(f"Fuzzy matching for '{col}' skipped: master_list is empty.")
                        continue

                    correction_map = {}
                    vals_to_check = [v for v in df_normalized[col].dropna().unique() if v not in master_list]
                    for val in vals_to_check:
                        match, score = fuzz_process.extractOne(val, master_list)
                        if score >= cutoff:
                            correction_map[val] = match
                            fuzzy_info.append({"Column": col, "Original": val, "Corrected": match, "Score": score})

                    if correction_map:
                        df_normalized[col] = df_normalized[col].replace(correction_map)
            if fuzzy_info: 
                changelog['fuzzy_matches'] = pd.DataFrame(fuzzy_info)
    except Exception as e:
        logging.error(f"Critical failure during fuzzy matching: {e}")

    # --- 5. Parse Datetime Columns ---
    try:
        datetime_rules = rules.get("parse_datetimes", {})
        if datetime_rules:
            parsed_info = []
            for col, settings in datetime_rules.items():
                if col in df_normalized.columns:
                    df_normalized[col] = pd.to_datetime(df_normalized[col], format=settings.get('format', 'auto'), errors='coerce')
                    parsed_info.append({"Column": col, "Target Type": "datetime64[ns]"})
            if parsed_info: changelog['datetimes_parsed'] = pd.DataFrame(parsed_info)
    except Exception as e:
        logging.error(f"Failed during datetime parsing: {e}")
            
    # --- 6. Coerce Final Dtypes ---
    try:
        type_map = rules.get("coerce_dtypes", {})
        if type_map:
            coerced_info = []
            for col, new_type in type_map.items():
                if col in df_normalized.columns:
                    original_type = str(df_normalized[col].dtype)
                    if original_type != new_type:
                        try:
                            numeric_series = pd.to_numeric(df_normalized[col])
                            df_normalized[col] = numeric_series.astype(new_type)
                            coerced_info.append({"Column": col, "Original Type": original_type, "Target Type": new_type, "Status": "✅ Success"})
                        except (ValueError, TypeError):
                            error_msg = f"Failed to coerce '{col}' to '{new_type}'. Contains non-numeric values."
                            logging.error(error_msg)
                            coerced_info.append({"Column": col, "Original Type": original_type, "Target Type": new_type, "Status": "⚠️ FAILED"})
            if coerced_info: changelog['types_coerced'] = pd.DataFrame(coerced_info)
    except Exception as e:
        logging.error(f"Failed during final type coercion: {e}")

    return df_original, df_normalized, changelog