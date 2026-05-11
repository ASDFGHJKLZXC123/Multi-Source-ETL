# =============================================================================
# Multi-Source ETL Pipeline — Makefile
# =============================================================================
# Usage: make <target>
# Run `make help` to list all available targets.
# =============================================================================

.DEFAULT_GOAL := help

.PHONY: help install install-dev \
        db-up db-down db-reset db-shell db-status \
        smoke-test \
        init setup extract silver gold warehouse quality \
        full-refresh incremental \
        docker-build docker-run docker-run-incremental docker-exec \
        test lint format typecheck \
        clean logs

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' \
		| sort

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

install: ## Install runtime dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies (testing, linting, formatting)
	pip install -r requirements-dev.txt

# ---------------------------------------------------------------------------
# Database — Docker Compose
# ---------------------------------------------------------------------------

db-up: ## Start the PostgreSQL container in the background
	docker compose up -d

db-down: ## Stop the PostgreSQL container
	docker compose down

db-reset: ## Destroy all volumes and restart the PostgreSQL container (data is lost)
	docker compose down -v && docker compose up -d

db-shell: ## Open a psql shell inside the running container
	docker exec -it etl-postgres psql -U postgres -d etl_pipeline

db-status: ## Show the running status of all Compose services
	docker compose ps

# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------

smoke-test: ## Verify database connectivity before running the pipeline
	python scripts/test_db_connection.py

# ---------------------------------------------------------------------------
# Pipeline stages — single-stage mode
# ---------------------------------------------------------------------------

init: ## Stage 0a — Create PostgreSQL schemas and pipeline_metadata table
	python main.py --stage init

setup: ## Stage 0b — Download Olist dataset, create source_system schema, load CSVs
	python main.py --stage setup

extract: ## Stage 1  — DB snapshot + APIs (Open-Meteo + Frankfurter) + raw Olist Bronze snapshots
	python main.py --stage extract

silver: ## Stage 3  — Transform Bronze → Silver (clean, validate, quarantine)
	python main.py --stage silver

gold: ## Stage 4  — Build Gold star schema Parquet files from Silver
	python main.py --stage gold

warehouse: ## Stage 5  — Load Gold Parquet into PostgreSQL analytics schema
	python main.py --stage warehouse

quality: ## Stage 7  — Run automated data quality checks; results → data_quality_log
	python main.py --stage quality

# ---------------------------------------------------------------------------
# Pipeline orchestration modes
# ---------------------------------------------------------------------------

full-refresh: ## Full run: extract → silver → gold → warehouse → quality
	python main.py --full-refresh

incremental: ## Re-transform + re-load only (Bronze already fresh): silver → gold → warehouse → quality
	python main.py --incremental

bootstrap: ## First-time end-to-end: init schemas → load source DB → full refresh (idempotent)
	$(MAKE) init && $(MAKE) setup && $(MAKE) full-refresh

# ---------------------------------------------------------------------------
# Docker — containerised pipeline
# ---------------------------------------------------------------------------

docker-build: ## Build (or rebuild) the etl-pipeline image
	docker compose build etl-pipeline

docker-run: ## Run the full-refresh pipeline inside a container (starts Postgres if needed)
	docker compose run --rm etl-pipeline --full-refresh

docker-run-incremental: ## Run incremental pipeline inside a container (Silver → Gold → Warehouse → Quality)
	docker compose run --rm etl-pipeline --incremental

docker-exec: ## Open an interactive shell inside the pipeline container
	docker compose run --rm --entrypoint /bin/bash etl-pipeline

# ---------------------------------------------------------------------------
# Testing and code quality
# ---------------------------------------------------------------------------

test: ## Run the full test suite with verbose output
	pytest tests/ -v

lint: ## Run ruff linter over src/ and tests/
	ruff check src/ tests/

format: ## Auto-format src/, tests/, and main.py with black
	black src/ tests/ main.py

typecheck: ## Run mypy static type checks over src/
	mypy src/

# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------

clean: ## Remove Python bytecode, caches, and tool artefacts
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
	find . -type d -name .ruff_cache -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -rf {} +

logs: ## Tail the ETL log file in real time
	tail -f logs/etl.log
