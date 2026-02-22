.PHONY: help install install-mcp install-notebook install-dev \
        lint format typecheck test check \
        mcp-up mcp-down mcp-logs mcp-health \
        docker-build docker-pull \
        pipeline clean

# ─── Default ───────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  Analyst Toolkit — available commands"
	@echo ""
	@echo "  Install"
	@echo "    install          Core package (editable)"
	@echo "    install-mcp      Core + MCP server deps"
	@echo "    install-notebook Core + notebook/Jupyter deps"
	@echo "    install-dev      Core + all dev tooling"
	@echo ""
	@echo "  Code quality"
	@echo "    lint             Ruff lint check"
	@echo "    format           Ruff auto-format"
	@echo "    typecheck        Mypy type check (mcp_server)"
	@echo "    test             Run pytest suite"
	@echo "    check            lint + typecheck + test"
	@echo ""
	@echo "  MCP server (Docker)"
	@echo "    mcp-up           Start MCP server via docker-compose"
	@echo "    mcp-down         Stop MCP server"
	@echo "    mcp-logs         Tail MCP server logs"
	@echo "    mcp-health       Hit /health endpoint"
	@echo "    docker-build     Build image locally"
	@echo "    docker-pull      Pull latest image from GHCR"
	@echo ""
	@echo "  Pipeline (CLI)"
	@echo "    pipeline         Run full toolkit pipeline (set CONFIG= to override)"
	@echo ""
	@echo "  Misc"
	@echo "    clean            Remove build artifacts and caches"
	@echo ""

# ─── Install ───────────────────────────────────────────────────────────────────
install:
	pip install -e .

install-mcp:
	pip install -r requirements-mcp.txt
	pip install -e .

install-notebook:
	pip install -e ".[notebook]"

install-dev:
	pip install -e ".[dev]"
	pre-commit install

# ─── Code quality ──────────────────────────────────────────────────────────────
lint:
	ruff check src/

format:
	ruff format src/
	ruff check --fix src/

typecheck:
	mypy src/analyst_toolkit/mcp_server

test:
	pytest tests/ -v

check: lint typecheck test

# ─── MCP server ────────────────────────────────────────────────────────────────
mcp-up:
	docker-compose -f docker-compose.mcp.yml up --build -d

mcp-down:
	docker-compose -f docker-compose.mcp.yml down

mcp-logs:
	docker-compose -f docker-compose.mcp.yml logs -f

mcp-health:
	curl -s http://localhost:8001/health | python3 -m json.tool

docker-build:
	docker build -f Dockerfile.mcp -t analyst-toolkit-mcp:local .

docker-pull:
	docker pull ghcr.io/g-schumacher44/analyst-toolkit-mcp:latest

# ─── Pipeline ──────────────────────────────────────────────────────────────────
CONFIG ?= config/run_toolkit_config.yaml

pipeline:
	python -m analyst_toolkit.run_toolkit_pipeline --config $(CONFIG)

# ─── Misc ──────────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ .mypy_cache/ .pytest_cache/ .ruff_cache/
