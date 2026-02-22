<p align="center">
  <img src="repo_files/analyst_toolkit_banner.png" alt="Analyst Toolkit Logo" width="1000"/>
  <br>
  <em>Self-Healing Data Audit &nbsp;·&nbsp; Data QA + Cleaning Engine &nbsp;·&nbsp; MCP Server</em>
</p>
<p align="center">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-stable-brightgreen">
  <img alt="Version" src="https://img.shields.io/badge/version-v0.4.0-blueviolet">
  <a href="https://github.com/G-Schumacher44/analyst_toolkit/actions/workflows/analyst-toolkit-mcp-ci.yml">
    <img alt="CI" src="https://github.com/G-Schumacher44/analyst_toolkit/actions/workflows/analyst-toolkit-mcp-ci.yml/badge.svg">
  </a>
  <img alt="GHCR" src="https://img.shields.io/badge/ghcr.io-analyst--toolkit--mcp-blue?logo=docker">
</p>

# 🧪 Analyst Toolkit

Modular data QA and preprocessing toolkit — run as a Jupyter notebook pipeline, CLI, or MCP server with Docker and GCS support.

## 🆕 Version 0.4.0: The "Self-Healing" Audit

This major update transforms the toolkit from a collection of utilities into a cohesive, autonomous auditing engine.

1.  **Listen (Inference):** Predict data needs automatically using `toolkit_infer_configs`.
2.  **Diagnose (Validation):** Detect holes (nulls) and bumps (outliers) with a single score.
3.  **Heal (Auto-Apply):** Automatically repair data based on inferred rules using `toolkit_auto_heal`.
4.  **Certify (Audit):** Generate a tamper-proof health report and sequence ledger.

---

## 👀 MCP Ecosystem (New)

Ship the toolkit as an MCP server and plug it into Claude Desktop, FridAI, or any JSON-RPC 2.0 client.

- **⛓️ Pipeline Mode:** Chain multiple tools in memory using `session_id`.
- **🕹️ Executive Cockpit:** Get a **0-100 Data Health Score** and a detailed **Healing Ledger**.
- **📀 Golden Templates:** Mountable library of industry-standard configurations for Fraud, Migration, and Compliance.
- [📡 MCP Server Guide](resource_hub/mcp_server_guide.md) — full setup, tool reference, and host integrations

---

## TL;DR

- Modular execution by stage (diagnostics, validation, normalization, etc.)
- Inline dashboards and exportable HTML + Excel reports
- Full pipeline execution (notebook or CLI)
- YAML-configurable logic per module
- Checkpointing and joblib persistence
- MCP server — expose all toolkit modules as tools to any MCP-compatible host
- 📂 [Sample output](exports/sample/) (plots, reports, cleaned dataset)

---

## 📎 Resource Hub (Start Here)

- [📡 MCP Server Guide](resource_hub/mcp_server_guide.md) — Setup, tool reference, FridAI + Claude Desktop integration
- [🧭 Config Guide](resource_hub/config_guide.md) — Overview of all YAML configuration files
- [📦 Config Template Bundle (ZIP)](resource_hub/config.zip) — Full set of starter YAMLs for each module
- [📘 Usage Guide](resource_hub/usage_guide.md) — Running the toolkit via notebooks or CLI

---

<details>
<summary><strong>🫆 version release notes</strong></summary>

**v0.4.0 — The Cockpit Upgrade**
- **State Management:** Introduced `StateStore` for in-memory DataFrame persistence between tool calls via `session_id`.
- **Data Health Score:** Every run now generates a weighted 0-100 score (Completeness, Validity, Uniqueness, Consistency).
- **Healing Ledger:** Persistent JSON/GCS history tracking every transformation made during a run.
- **Golden Templates:** A library of "best-practice" configs for Fraud, Migration, and Compliance (mountable via `config/golden_templates/`).
- **Autonomous Tools:** Added `auto_heal` (one-click cleaning) and `drift_detection` (schema/statistical comparison).
- **Configuration Intelligence:** Added `get_config_schema` to return JSON Schemas for every module.

**v0.3.0**
- **MCP Server:** New `analyst_toolkit/mcp_server/` package exposes all toolkit modules as MCP tools.
- **HTML Reports:** All modules can emit self-contained single-page HTML reports.
</details>

---

## 🤝 On Generative AI Use

Generative AI tools (Gemini 2.5-PRO, ChatGPT 4o - 4.1, Claude Sonnet) were used throughout this project as part of an integrated workflow — supporting code generation, documentation refinement, and idea testing. These tools accelerated development, but the logic, structure, and documentation reflect intentional, human-led design. This repository reflects a collaborative process: where automation supports clarity, and iteration deepens understanding.

---

## 📦 Licensing

This project is licensed under the [MIT License](LICENSE).
