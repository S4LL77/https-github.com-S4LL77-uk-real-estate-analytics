# ============================================================================
# UK Real Estate Analytics — Developer Commands
# ============================================================================
# Usage: make <target>
#
# This Makefile provides shortcuts for common development tasks.
# Run `make help` to see all available targets.
# ============================================================================

.PHONY: help setup ingest-land-registry ingest-all test lint format clean

# Default Python — override with: make PYTHON=python3.11 <target>
PYTHON ?= python

help: ## Show this help message
	@echo.
	@echo   UK Real Estate Analytics — Available Commands
	@echo   =============================================
	@echo.
	@findstr /R /C:"## " Makefile

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

setup: ## Install dependencies and create .env from template
	$(PYTHON) -m pip install -r requirements.txt
	@if not exist .env copy .env.example .env
	@echo [OK] Setup complete. Edit .env with your configuration.

# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

ingest-land-registry: ## Ingest Land Registry Price Paid Data (incremental)
	$(PYTHON) -m ingestion.land_registry

ingest-land-registry-full: ## Ingest Land Registry data (full refresh)
	$(PYTHON) -m ingestion.land_registry --full-refresh

ingest-land-registry-years: ## Ingest specific years (e.g., make ingest-land-registry-years YEARS=2022,2023)
	$(PYTHON) -m ingestion.land_registry --years $(YEARS)

ingest-boe: ## Ingest Bank of England Official Bank Rate
	$(PYTHON) -m ingestion.boe_rates

ingest-ons: ## Ingest ONS Demographics Data
	$(PYTHON) -m ingestion.ons_demographics

ingest-all: ingest-land-registry ingest-boe ingest-ons ## Run all ingestion pipelines sequentially

# ---------------------------------------------------------------------------
# Orchestration (Airflow)
# ---------------------------------------------------------------------------

airflow-up: ## Start Airflow locally via Docker Compose
	docker-compose up -d

airflow-down: ## Stop Airflow and remove containers
	docker-compose down
	
airflow-logs: ## View Airflow logs
	docker-compose logs -f

# ---------------------------------------------------------------------------
# Serving Layer (FastAPI)
# ---------------------------------------------------------------------------

run-api: ## Run the local FastAPI server
	$(PYTHON) -m uvicorn api.main:app --reload

# ---------------------------------------------------------------------------
# Transformation (dbt)
# ---------------------------------------------------------------------------

dbt-deps: ## Install dbt packages and verify connection
	cd dbt_project && dbt deps && dbt debug

dbt-build: ## Run all dbt models, snapshots, and tests
	cd dbt_project && dbt build

dbt-docs: ## Generate and serve dbt documentation site
	cd dbt_project && dbt docs generate && dbt docs serve

# ---------------------------------------------------------------------------
# Testing & Quality
# ---------------------------------------------------------------------------

test: ## Run all tests with pytest
	$(PYTHON) -m pytest tests/ -v --tb=short

test-cov: ## Run tests with coverage report
	$(PYTHON) -m pytest tests/ --cov=ingestion --cov-report=term-missing

lint: ## Check code style with ruff
	$(PYTHON) -m ruff check ingestion/ tests/

format: ## Format code with black and ruff
	$(PYTHON) -m black ingestion/ tests/
	$(PYTHON) -m ruff check --fix ingestion/ tests/

# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------

snowflake-setup: ## Run Snowflake setup scripts (requires snowsql)
	@echo Running Snowflake setup scripts...
	@for %%f in (snowflake\setup\*.sql) do snowsql -f %%f

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean: ## Remove generated files (data, logs, caches)
	@if exist data rmdir /s /q data
	@if exist logs rmdir /s /q logs
	@if exist __pycache__ rmdir /s /q __pycache__
	@if exist .pytest_cache rmdir /s /q .pytest_cache
	@echo [OK] Cleaned generated files.
