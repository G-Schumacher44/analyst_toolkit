# ✈️ MCP Agent Flight Checklist

When you start working with a user on a data audit, follow this checklist to ensure a smooth "Cockpit" experience.

## 1. Discovery (Pre-Flight)
Before running any tools, confirm the following with the user:
- [ ] **Data Location:** Where is the data stored? (GCS path or local file)
- [ ] **Operation Mode:** Do you want to use a **Golden Template** (Fraud, Migration, Compliance) or should I **Infer Configs** from your data first?
- [ ] **Output Destination:** Reports default to the server's configured bucket. Do you want to override the `output_bucket` or set a specific `export_path` for the final dataset?

## 2. Diagnostics (Takeoff)
- [ ] Always start with `toolkit_diagnostics` to establish a baseline.
- [ ] Share the **Data Health Score** from the `get_data_health_report` tool immediately after.

## 3. Healing (Cruise)
- [ ] If using `auto_heal`, explain that it will perform inference and apply fixes in one step.
- [ ] If running manually, show the user the inferred config and ask for confirmation before applying `normalization` or `imputation`.

## 4. Certification (Landing)
- [ ] **Final Audit:** Always finish with `toolkit_final_audit`.
- [ ] **The Ledger:** Provide the link to the "Big HTML Report" (Healing Certificate) and the `toolkit_get_run_history` output as the final "Proof of Health".

---

## 💡 Pro-Tip for Agents
You can chain tools entirely in memory using the `session_id`. Never download/upload data between steps unless the user explicitly asks to see a local CSV.
