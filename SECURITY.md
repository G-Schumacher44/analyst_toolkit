# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability, please report it privately.

- Do **not** open a public GitHub issue with exploit details.
- Contact the maintainer directly with:
  - a clear description of the issue,
  - reproduction steps,
  - impact assessment,
  - any proposed remediation.

The maintainer will acknowledge receipt and coordinate remediation and disclosure timing.

## Scope

This project includes:

- Python package code under `src/`
- MCP server functionality
- Docker image build/runtime configuration
- CI workflow definitions

## Supported Versions

Security fixes are prioritized for:

- the latest released version,
- and the current `main` branch.

Older versions may not receive patches.

## Secrets and Credentials

- Never commit credentials, tokens, service-account keys, or `.env` secrets.
- Use environment variables and local secret management.
- Rotate any exposed secret immediately.

## Dependency and Supply Chain Hygiene

Contributors should:

- keep dependencies current,
- run CI checks before merge,
- avoid introducing unvetted third-party code without review.

