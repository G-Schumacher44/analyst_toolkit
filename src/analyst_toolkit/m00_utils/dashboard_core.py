"""Shared page shell for standalone dashboard HTML rendering."""

from __future__ import annotations

import html
from datetime import datetime, timezone

from analyst_toolkit.m00_utils.dashboard_shared import _slugify

_DASHBOARD_CSS = """
<style>
  :root {
    --bg: #f4f1ea;
    --paper: #fffdf8;
    --ink: #1f2933;
    --muted: #52606d;
    --line: #d9d3c7;
    --accent: #1f4b4a;
    --accent-soft: #dceae7;
    --warn: #9a3412;
    --warn-soft: #fef0e8;
    --ok: #14532d;
    --ok-soft: #e8f5ec;
    --shadow: 0 14px 30px rgba(31, 41, 51, 0.08);
    --radius: 18px;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: Georgia, "Times New Roman", serif;
    background:
      radial-gradient(circle at top left, rgba(31, 75, 74, 0.10), transparent 28%),
      linear-gradient(180deg, #f7f2ea 0%, #f1ece2 100%);
    color: var(--ink);
  }
  .page {
    max-width: 1180px;
    margin: 0 auto;
    padding: 40px 20px 64px;
  }
  .hero {
    background: linear-gradient(135deg, rgba(31, 75, 74, 0.98), rgba(50, 82, 117, 0.92));
    color: #f9f6ef;
    border-radius: 28px;
    padding: 28px;
    box-shadow: var(--shadow);
    margin-bottom: 24px;
  }
  .hero-kicker {
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-size: 0.76rem;
    opacity: 0.82;
    margin-bottom: 10px;
  }
  .hero h1 {
    margin: 0 0 10px;
    font-size: 2.2rem;
    line-height: 1.1;
  }
  .hero-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 10px 16px;
    color: rgba(249, 246, 239, 0.88);
    font-size: 0.95rem;
  }
  .banner {
    background: var(--accent-soft);
    border: 1px solid rgba(31, 75, 74, 0.15);
    border-radius: var(--radius);
    padding: 16px 18px;
    margin-bottom: 20px;
    display: flex;
    flex-wrap: wrap;
    gap: 10px 18px;
    box-shadow: var(--shadow);
  }
  .banner.warn {
    background: #fbe4df;
    border-color: rgba(153, 27, 27, 0.22);
  }
  .banner.fail {
    background: #fbe4df;
    border-color: rgba(159, 18, 57, 0.28);
  }
  .banner.ok {
    background: var(--ok-soft);
    border-color: rgba(20, 83, 45, 0.18);
  }
  .banner-item {
    font-size: 0.96rem;
  }
  .toc {
    background: rgba(255, 253, 248, 0.72);
    backdrop-filter: blur(6px);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 16px 18px;
    margin-bottom: 18px;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px 14px;
  }
  .toc strong { color: #334155; }
  .toc a {
    color: var(--accent);
    text-decoration: none;
    font-weight: 600;
    line-height: 1.3;
  }
  details.section {
    background: var(--paper);
    border: 1px solid rgba(31, 41, 51, 0.08);
    border-radius: 22px;
    padding: 0 18px 18px;
    margin-bottom: 18px;
    box-shadow: var(--shadow);
  }
  details.section[open] {
    animation: fade-in 160ms ease-out;
  }
  summary {
    cursor: pointer;
    list-style: none;
    padding: 18px 0 14px;
    font-weight: 700;
    font-size: 1.12rem;
    color: #22303a;
  }
  summary::-webkit-details-marker { display: none; }
  .section-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px;
  }
  .card {
    background: #fcfaf5;
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: 14px;
    min-width: 0;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .card.wide {
    grid-column: span 2;
  }
  .card h3 {
    margin: 0 0 10px;
    font-size: 1rem;
    color: #22303a;
  }
  .key {
    margin-top: 12px;
    border-top: 1px solid var(--line);
    padding-top: 12px;
    color: var(--muted);
    font-size: 0.9rem;
  }
  .key ul {
    margin: 8px 0 0 18px;
    padding: 0;
  }
  .stack > * + * {
    margin-top: 14px;
  }
  .ledger-stack {
    width: 100%;
  }
  .ledger-stack .card {
    width: 100%;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.92rem;
    background: #fff;
    border-radius: 12px;
    table-layout: fixed;
    min-width: 100%;
  }
  th, td {
    padding: 8px 10px;
    border: 1px solid #e7dfd1;
    text-align: left;
    vertical-align: top;
    white-space: normal;
    overflow-wrap: anywhere;
    word-break: break-word;
  }
  th {
    background: #f1ece2;
    color: #23303b;
    font-weight: 700;
  }
  td {
    color: #334155;
  }
  tr:nth-child(even) td {
    background: #fdfaf3;
  }
  .subtle {
    color: var(--muted);
    font-size: 0.88rem;
    overflow-wrap: anywhere;
    word-break: break-word;
  }
  .table-wrap {
    width: 100%;
    max-width: 100%;
    overflow-x: auto;
    overflow-y: auto;
    max-height: min(360px, 68vh);
    border-radius: 12px;
    border: 1px solid #e7dfd1;
    background: #fff;
  }
  .empty {
    color: var(--muted);
    font-style: italic;
    margin: 0;
  }
  .plot-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px;
  }
  .plot-intro {
    margin: 0 0 14px;
    color: var(--muted);
    font-size: 0.95rem;
  }
  .plot-card img {
    width: 100%;
    height: auto;
    display: block;
    border-radius: 14px;
    border: 1px solid var(--line);
    background: #fff;
    cursor: zoom-in;
  }
  .plot-card h3 {
    margin: 0 0 10px;
  }
  .plot-trigger {
    appearance: none;
    border: 0;
    padding: 0;
    margin: 0;
    background: transparent;
    width: 100%;
    text-align: left;
    cursor: zoom-in;
  }
  .plot-trigger:focus-visible {
    outline: 3px solid rgba(31, 75, 74, 0.35);
    outline-offset: 6px;
    border-radius: 16px;
  }
  .plot-caption {
    margin: 10px 0 0;
    color: var(--muted);
    font-size: 0.88rem;
  }
  .plot-modal {
    border: 0;
    padding: 0;
    width: min(92vw, 1280px);
    max-height: 92vh;
    background: transparent;
  }
  .plot-modal::backdrop {
    background: rgba(17, 24, 39, 0.74);
    backdrop-filter: blur(4px);
  }
  .plot-modal-card {
    background: var(--paper);
    border: 1px solid rgba(255, 255, 255, 0.22);
    border-radius: 24px;
    overflow: hidden;
    box-shadow: 0 22px 48px rgba(15, 23, 42, 0.28);
  }
  .plot-modal-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding: 18px 20px 0;
  }
  .plot-modal-header h3 {
    margin: 0;
    font-size: 1.08rem;
    color: #22303a;
  }
  .plot-modal-close {
    appearance: none;
    border: 1px solid var(--line);
    background: #fff;
    color: #22303a;
    border-radius: 999px;
    width: 36px;
    height: 36px;
    font-size: 1.1rem;
    line-height: 1;
    cursor: pointer;
  }
  .plot-modal-body {
    padding: 14px 20px 20px;
    overflow: auto;
    max-height: calc(92vh - 72px);
  }
  .plot-modal-body img {
    width: 100%;
    height: auto;
    display: block;
    border-radius: 18px;
    border: 1px solid var(--line);
    background: #fff;
  }
  .badge-ok {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 2px 8px;
    border-radius: 999px;
    background: var(--ok-soft);
    color: var(--ok);
    font-weight: 700;
    font-size: 0.82rem;
  }
  .badge-warn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 2px 8px;
    border-radius: 999px;
    background: var(--warn-soft);
    color: var(--warn);
    font-weight: 700;
    font-size: 0.82rem;
  }
  .drilldown {
    margin-top: 12px;
    padding-top: 12px;
    border-top: 1px solid var(--line);
  }
  .drilldown h4 {
    margin: 0 0 10px;
    font-size: 0.95rem;
  }
  .failure-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
  }
  .metric-stat {
    font-size: clamp(1.15rem, 2.1vw, 2rem);
    line-height: 1.08;
    font-weight: 700;
    color: #22303a;
    margin: 4px 0 0;
    overflow-wrap: anywhere;
    word-break: break-word;
    text-wrap: balance;
  }
  .metric-stat.compact {
    font-size: clamp(1rem, 1.65vw, 1.42rem);
    line-height: 1.15;
  }
  .pill-list {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }
  .pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 6px 10px;
    border-radius: 999px;
    background: #f3ede2;
    color: #334155;
    font-size: 0.88rem;
    font-weight: 600;
  }
  .pill.warn {
    background: #fbe4df;
    color: #9f1239;
  }
  .status-pill {
    display: inline-flex;
    align-items: center;
    padding: 4px 10px;
    border-radius: 999px;
    font-size: 0.84rem;
    font-weight: 700;
    line-height: 1.2;
  }
  .status-pill.pass {
    background: var(--ok-soft);
    color: var(--ok);
  }
  .status-pill.fail {
    background: #fbe4df;
    color: #9f1239;
  }
  .cert-hero {
    border-radius: 28px;
    padding: 24px 26px;
    margin-bottom: 22px;
    color: #f8fafc;
    box-shadow: var(--shadow);
  }
  .cert-hero.pass {
    background: linear-gradient(135deg, #123c2b, #1f6f54);
  }
  .cert-hero.fail {
    background: linear-gradient(135deg, #5f1726, #9f1239);
  }
  .cert-kicker {
    text-transform: uppercase;
    letter-spacing: 0.14em;
    font-size: 0.74rem;
    opacity: 0.82;
    margin-bottom: 10px;
  }
  .cert-title {
    margin: 0 0 10px;
    font-size: 2rem;
    line-height: 1.08;
  }
  .cert-copy {
    margin: 0;
    max-width: 760px;
    color: rgba(248, 250, 252, 0.92);
    font-size: 0.98rem;
    line-height: 1.55;
  }
  .cert-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 16px;
    margin-bottom: 18px;
  }
  .cert-stat-card {
    background: #fcfaf5;
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: 16px;
    box-shadow: var(--shadow);
    min-width: 0;
  }
  .cert-stat-card h3 {
    margin: 0 0 6px;
    font-size: 0.96rem;
    color: #22303a;
  }
  .cert-stat-card .metric-stat {
    margin-bottom: 6px;
  }
  .cert-stat-card p,
  .cert-stat-card code,
  .cert-stat-card a {
    overflow-wrap: anywhere;
    word-break: break-word;
  }
  .cert-stat-card.pass {
    background: var(--ok-soft);
    border-color: rgba(20, 83, 45, 0.22);
  }
  .cert-stat-card.pass .metric-stat {
    color: var(--ok);
  }
  .cert-stat-card.warn {
    background: var(--warn-soft);
    border-color: rgba(154, 52, 18, 0.22);
  }
  .cert-stat-card.warn .metric-stat {
    color: var(--warn);
  }
  .cert-stat-card.fail {
    background: #fbe4df;
    border-color: rgba(159, 18, 57, 0.24);
  }
  .cert-stat-card.fail .metric-stat {
    color: #9f1239;
  }
  .cert-ledger {
    display: grid;
    grid-template-columns: 1.2fr 0.8fr;
    gap: 16px;
  }
  .tab-shell {
    display: flex;
    flex-direction: column;
    gap: 16px;
  }
  .tab-nav {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    position: sticky;
    top: 10px;
    z-index: 3;
    padding: 10px 12px;
    border: 1px solid rgba(91, 106, 115, 0.18);
    border-radius: 20px;
    background: rgba(252, 250, 245, 0.94);
    backdrop-filter: blur(8px);
    box-shadow: var(--shadow);
  }
  .tab-shell[data-tab-shell='cockpit'] .tab-nav {
    position: static;
    top: auto;
  }
  .tab-button {
    appearance: none;
    border: 1px solid var(--line);
    background: #fcfaf5;
    color: #22303a;
    border-radius: 999px;
    padding: 10px 14px;
    font: inherit;
    font-size: 0.92rem;
    font-weight: 700;
    cursor: pointer;
    box-shadow: var(--shadow);
  }
  .tab-button.active {
    background: var(--accent);
    color: #f8fafc;
    border-color: rgba(31, 75, 74, 0.75);
  }
  .tab-button .tab-status {
    display: inline-flex;
    margin-left: 8px;
    padding: 2px 8px;
    border-radius: 999px;
    background: rgba(34, 48, 58, 0.1);
    font-size: 0.78rem;
    letter-spacing: 0.03em;
  }
  .tab-button.active .tab-status {
    background: rgba(248, 250, 252, 0.18);
  }
  .tab-panel {
    display: none;
  }
  .tab-panel.active {
    display: block;
    animation: fade-in 160ms ease-out;
  }
  .module-shell {
    display: flex;
    flex-direction: column;
    gap: 16px;
  }
  .module-mini-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 14px;
  }
  .module-mini-card {
    background: #fcfaf5;
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: 14px 16px;
    box-shadow: var(--shadow);
  }
  .module-mini-card.pass {
    background: var(--ok-soft);
    border-color: rgba(20, 83, 45, 0.22);
  }
  .module-mini-card.pass .metric-stat {
    color: var(--ok);
  }
  .module-mini-card.warn {
    background: var(--warn-soft);
    border-color: rgba(154, 52, 18, 0.22);
  }
  .module-mini-card.warn .metric-stat {
    color: var(--warn);
  }
  .module-mini-card.fail {
    background: #fbe4df;
    border-color: rgba(159, 18, 57, 0.24);
  }
  .module-mini-card.fail .metric-stat {
    color: #9f1239;
  }
  .module-mini-card h3 {
    margin: 0 0 6px;
    font-size: 0.9rem;
    color: #22303a;
  }
  .module-callout {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 14px;
    padding: 16px 18px;
    border: 1px dashed rgba(91, 106, 115, 0.35);
    border-radius: 18px;
    background: rgba(244, 239, 226, 0.62);
  }
  .module-callout p {
    margin: 0;
  }
  .terminal-card {
    display: flex;
    flex-direction: column;
    gap: 14px;
  }
  .terminal-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 14px;
  }
  .terminal-slot {
    background: #fcfaf5;
    border: 1px solid var(--line);
    border-radius: 16px;
    padding: 14px 16px;
  }
  .terminal-slot h4 {
    margin: 0 0 8px;
    font-size: 0.88rem;
    color: #22303a;
  }
  .terminal-art {
    min-height: 150px;
    border: 1px dashed rgba(91, 106, 115, 0.35);
    border-radius: 18px;
    padding: 18px;
    background:
      radial-gradient(circle at top left, rgba(190, 149, 67, 0.12), transparent 40%),
      linear-gradient(135deg, rgba(244, 239, 226, 0.9), rgba(252, 250, 245, 0.98));
  }
  .terminal-art h3 {
    margin: 0 0 8px;
    font-size: 1rem;
  }
  .terminal-art p {
    margin: 0;
  }
  .hub-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px;
  }
  .hub-card {
    background: linear-gradient(180deg, #fcfaf5 0%, #f4efe2 100%);
    border: 1px solid var(--line);
    border-radius: 20px;
    padding: 18px;
    box-shadow: var(--shadow);
  }
  .hub-card h3 {
    margin: 0 0 8px;
    font-size: 1rem;
    color: #22303a;
  }
  .hub-kicker {
    margin: 0 0 8px;
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #5f6b6f;
  }
  .status-chip {
    display: inline-flex;
    align-items: center;
    padding: 4px 10px;
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 800;
    letter-spacing: 0.04em;
  }
  .status-chip.ok {
    background: rgba(37, 99, 78, 0.14);
    color: #1f5e4a;
  }
  .status-chip.warn {
    background: rgba(190, 149, 67, 0.18);
    color: #8a5b12;
  }
  .status-chip.fail {
    background: rgba(140, 34, 34, 0.14);
    color: #8b1e1e;
  }
  .resource-card {
    display: flex;
    flex-direction: column;
    gap: 10px;
    padding: 18px;
    border-radius: 18px;
    border: 1px solid var(--line);
    background: #fcfaf5;
    box-shadow: var(--shadow);
  }
  .resource-card p {
    margin: 0;
  }
  .resource-meta {
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: #6c5f45;
  }
  .launch-list {
    display: grid;
    gap: 14px;
  }
  .launch-item {
    padding: 16px 18px;
    border-radius: 18px;
    border: 1px solid var(--line);
    background: #fcfaf5;
    box-shadow: var(--shadow);
  }
  .launch-item h3,
  .launch-item p {
    margin: 0;
  }
  .launch-item h3 {
    margin-bottom: 8px;
  }
  .hub-stack {
    display: grid;
    gap: 16px;
  }
  .brief-card {
    padding: 24px;
    border-radius: 22px;
    border: 1px solid rgba(91, 106, 115, 0.22);
    background:
      radial-gradient(circle at top right, rgba(190, 149, 67, 0.14), transparent 30%),
      linear-gradient(135deg, #22303a 0%, #324754 100%);
    color: #f8fafc;
    box-shadow: 0 20px 40px rgba(34, 48, 58, 0.18);
  }
  .brief-card h3,
  .brief-card p,
  .brief-card li {
    color: #f8fafc;
  }
  .brief-card h3 {
    margin: 0 0 10px;
    font-size: 1.25rem;
  }
  .brief-card p {
    margin: 0 0 10px;
  }
  .brief-lanes {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
    margin-top: 14px;
  }
  .brief-lane {
    padding: 14px 16px;
    border-radius: 16px;
    background: rgba(248, 250, 252, 0.08);
    border: 1px solid rgba(248, 250, 252, 0.12);
  }
  .brief-lane h4,
  .brief-lane p {
    margin: 0;
    color: #f8fafc;
  }
  .brief-lane h4 {
    margin-bottom: 6px;
    font-size: 0.95rem;
  }
  .brief-list,
  .sequence-list {
    margin: 0;
    padding-left: 18px;
    display: grid;
    gap: 8px;
  }
  .readme-grid {
    display: grid;
    gap: 18px;
  }
  .readme-section {
    padding: 22px;
    border-radius: 20px;
    border: 1px solid var(--line);
    background: #fcfaf5;
    box-shadow: var(--shadow);
  }
  .readme-section h3 {
    margin: 0 0 8px;
    color: #22303a;
  }
  .readme-section p {
    margin: 0 0 12px;
  }
  .resource-group-grid,
  .sequence-grid {
    display: grid;
    gap: 16px;
  }
  .resource-inline-list {
    display: grid;
    gap: 12px;
  }
  .resource-inline-list.scroll-pane {
    max-height: 360px;
    overflow-y: auto;
    padding-right: 6px;
  }
  .resource-inline-item {
    padding: 14px 16px;
    border-radius: 16px;
    border: 1px solid var(--line);
    background: rgba(244, 239, 226, 0.54);
  }
  .resource-inline-item h4 {
    margin: 0 0 6px;
    color: #22303a;
  }
  .sequence-card {
    padding: 18px;
    border-radius: 18px;
    border: 1px solid var(--line);
    background: #fcfaf5;
    box-shadow: var(--shadow);
  }
  .sequence-card h3 {
    margin: 0 0 10px;
    color: #22303a;
  }
  .overview-split {
    display: grid;
    grid-template-columns: minmax(0, 1.15fr) minmax(0, 0.85fr);
    gap: 18px;
    align-items: start;
  }
  .overview-column {
    display: grid;
    gap: 18px;
  }
  .alert-list {
    display: grid;
    gap: 12px;
  }
  .alert-card {
    padding: 16px;
    border-radius: 16px;
    border: 1px solid var(--line);
    background: #fcfaf5;
  }
  .alert-card h4,
  .alert-card p {
    margin: 0;
  }
  .alert-card h4 {
    margin-bottom: 6px;
    color: #22303a;
  }
  .surface-list,
  .missing-list {
    display: grid;
    gap: 12px;
  }
  .surface-item,
  .missing-item {
    padding: 14px 16px;
    border-radius: 16px;
    border: 1px solid var(--line);
    background: rgba(244, 239, 226, 0.54);
  }
  .surface-item h4,
  .missing-item h4,
  .surface-item p,
  .missing-item p {
    margin: 0;
  }
  .surface-item h4,
  .missing-item h4 {
    margin-bottom: 6px;
    color: #22303a;
  }
  .action-link {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 180px;
    padding: 10px 14px;
    border-radius: 999px;
    background: var(--accent);
    color: #f8fafc;
    text-decoration: none;
    font-weight: 700;
  }
  .action-link.secondary {
    background: #fcfaf5;
    color: #22303a;
    border: 1px solid var(--line);
  }
  .tab-embed {
    width: 100%;
    min-height: 920px;
    border: 1px solid var(--line);
    border-radius: 18px;
    background: #fff;
    box-shadow: var(--shadow);
  }
  @media (max-width: 920px) {
    .cert-ledger {
      grid-template-columns: 1fr;
    }
    .module-callout {
      align-items: flex-start;
      flex-direction: column;
    }
    .overview-split {
      grid-template-columns: 1fr;
    }
    .tab-embed {
      min-height: 760px;
    }
  }
  pre {
    white-space: pre-wrap;
    word-break: break-word;
    font-size: 0.84rem;
    background: #fbf7ef;
    border: 1px solid var(--line);
    border-radius: 14px;
    padding: 12px;
    margin: 0;
  }
  @media (max-width: 720px) {
    .page { padding: 20px 12px 44px; }
    .hero { padding: 22px 18px; }
    .hero h1 { font-size: 1.8rem; }
    .card.wide { grid-column: auto; }
  }
  @keyframes fade-in {
    from { opacity: 0; transform: translateY(2px); }
    to { opacity: 1; transform: translateY(0); }
  }
</style>
"""

_DASHBOARD_SCRIPT = """
<script>
  window.atkDashboard = {
    openPlot(button) {
      const modal = document.getElementById("plot-modal");
      const image = document.getElementById("plot-modal-image");
      const title = document.getElementById("plot-modal-title");
      if (!modal || !image || !title) return;
      const thumbnail = button.querySelector("img");
      const plotSrc = button.dataset.plotSrc || thumbnail?.src || "";
      const plotTitle = button.dataset.plotTitle || "Plot";
      image.src = plotSrc;
      image.alt = plotTitle;
      title.textContent = plotTitle;
      if (typeof modal.showModal === "function") {
        modal.showModal();
      }
    },
    closePlot() {
      const modal = document.getElementById("plot-modal");
      const image = document.getElementById("plot-modal-image");
      if (!modal) return;
      modal.close();
      if (image) image.src = "";
    },
    openTab(button) {
      const shell = button.closest("[data-tab-shell]");
      if (!shell) return;
      const target = button.dataset.tabTarget || "";
      shell.querySelectorAll(".tab-button").forEach((node) => {
        node.classList.toggle("active", node === button);
      });
      shell.querySelectorAll(".tab-panel").forEach((panel) => {
        panel.classList.toggle("active", panel.id === target);
      });
    }
  };

  document.addEventListener("click", (event) => {
    const trigger = event.target.closest(".plot-trigger");
    if (trigger) {
      window.atkDashboard.openPlot(trigger);
      return;
    }
    const closeButton = event.target.closest(".plot-modal-close");
    if (closeButton) {
      window.atkDashboard.closePlot();
      return;
    }
    const modal = document.getElementById("plot-modal");
    if (!modal || event.target !== modal) return;
    window.atkDashboard.closePlot();
  });
</script>
"""


def _assemble_page(
    *,
    module_name: str,
    run_id: str,
    banner_html: str,
    toc_items: list[tuple[str, str]],
    sections: list[str],
) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    page_title = (
        module_name
        if module_name.strip().lower().endswith("dashboard")
        else f"{module_name} Dashboard"
    )
    toc_html = ""
    if toc_items:
        toc_links = "".join(
            f"<a href='#{html.escape(_slugify(anchor), quote=True)}'>{html.escape(label)}</a>"
            for anchor, label in toc_items
        )
        toc_html = f"<div class='toc'><strong>Sections:</strong> {toc_links}</div>"

    body = "".join(sections) or "<p class='empty'>No report data was produced for this run.</p>"
    return (
        "<!DOCTYPE html><html><head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{html.escape(page_title)} - {html.escape(run_id)}</title>"
        f"{_DASHBOARD_CSS}{_DASHBOARD_SCRIPT}</head><body><div class='page'>"
        "<div class='hero'>"
        "<div class='hero-kicker'>Analyst Toolkit Export</div>"
        f"<h1>{html.escape(page_title)}</h1>"
        "<div class='hero-meta'>"
        f"<span><strong>Run ID:</strong> {html.escape(run_id)}</span>"
        f"<span><strong>Generated:</strong> {generated_at}</span>"
        "</div></div>"
        f"{banner_html}{toc_html}{body}"
        "<dialog class='plot-modal' id='plot-modal'>"
        "<div class='plot-modal-card'>"
        "<div class='plot-modal-header'>"
        "<h3 id='plot-modal-title'>Plot</h3>"
        "<button class='plot-modal-close' type='button' aria-label='Close expanded plot'>&times;</button>"
        "</div>"
        "<div class='plot-modal-body'>"
        "<img id='plot-modal-image' src='' alt='Expanded plot'>"
        "</div></div></dialog>"
        "</div></body></html>"
    )
