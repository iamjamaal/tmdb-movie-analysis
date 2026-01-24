.PHONY: help setup build up down restart logs clean test lint format validate-config run-pipeline health-check

# Colors for output
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo '$(CYAN)Available commands:$(NC)'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

setup: ## Initial setup - create directories and .env file
	@echo '$(CYAN)Setting up project...$(NC)'
	@mkdir -p data/{raw,processed,output} logs/{airflow,spark,pipeline} notebooks tests/{unit,integration}
	@if [ ! -f .env ]; then \
		echo '$(YELLOW)Creating .env file from template...$(NC)'; \
		cp .env.example .env || echo "TMDB_API_KEY=your_api_key_here" > .env; \
		echo '$(RED)Please update .env with your TMDB API key!$(NC)'; \
	fi
	@echo '$(GREEN)Setup complete!$(NC)'

validate-env: ## Validate environment variables
	@echo '$(CYAN)Validating environment...$(NC)'
	@if [ -z "$$TMDB_API_KEY" ]; then \
		echo '$(RED)Error: TMDB_API_KEY not set!$(NC)'; \
		exit 1; \
	fi
	@echo '$(GREEN)Environment validated!$(NC)'

build: ## Build all Docker images
	@echo '$(CYAN)Building Docker images...$(NC)'
	docker-compose build
	@echo '$(GREEN)Build complete!$(NC)'

up: ## Start all services
	@echo '$(CYAN)Starting services...$(NC)'
	docker-compose up -d
	@echo '$(GREEN)Services started!$(NC)'
	@echo '$(YELLOW)Access points:$(NC)'
	@echo '  Airflow UI:     http://localhost:8083'
	@echo '  Spark Master:   http://localhost:8080'
	@echo '  JupyterLab:     http://localhost:8888'
	@echo '  Grafana:        http://localhost:3000'

down: ## Stop all services
	@echo '$(CYAN)Stopping services...$(NC)'
	docker-compose down
	@echo '$(GREEN)Services stopped!$(NC)'

restart: down up ## Restart all services

logs: ## View logs from all services
	docker-compose logs -f

logs-spark: ## View Spark logs
	docker-compose logs -f spark-master spark-worker-1 spark-worker-2

logs-airflow: ## View Airflow logs
	docker-compose logs -f airflow-webserver airflow-scheduler

logs-pipeline: ## View pipeline logs
	tail -f logs/pipeline/*.log

health-check: ## Check health of all services
	@echo '$(CYAN)Checking service health...$(NC)'
	@docker-compose ps
	@echo ''
	@echo '$(CYAN)Spark Master Status:$(NC)'
	@curl -s http://localhost:8080 > /dev/null && echo '$(GREEN)✓ Spark Master is running$(NC)' || echo '$(RED)✗ Spark Master is down$(NC)'
	@echo '$(CYAN)Airflow Status:$(NC)'
	@curl -s http://localhost:8083/health > /dev/null && echo '$(GREEN)✓ Airflow is running$(NC)' || echo '$(RED)✗ Airflow is down$(NC)'
	@echo '$(CYAN)JupyterLab Status:$(NC)'
	@curl -s http://localhost:8888 > /dev/null && echo '$(GREEN)✓ JupyterLab is running$(NC)' || echo '$(RED)✗ JupyterLab is down$(NC)'

spark-shell: ## Open Spark shell in container
	docker-compose exec spark-master bash

airflow-shell: ## Open Airflow shell in container
	docker-compose exec airflow-webserver bash

jupyter-shell: ## Open Jupyter shell in container
	docker-compose exec jupyter bash

run-pipeline: ## Run the full pipeline via Airflow
	@echo '$(CYAN)Triggering pipeline...$(NC)'
	docker-compose exec airflow-webserver airflow dags trigger tmdb_movie_pipeline
	@echo '$(GREEN)Pipeline triggered! Check Airflow UI for progress.$(NC)'

run-local: ## Run pipeline locally (without Airflow)
	@echo '$(CYAN)Running pipeline locally...$(NC)'
	docker-compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--driver-memory 4g \
		--executor-memory 4g \
		/opt/spark-apps/src/main.py

test: ## Run all tests
	@echo '$(CYAN)Running tests...$(NC)'
	docker-compose exec spark-master pytest tests/ -v --tb=short
	@echo '$(GREEN)Tests complete!$(NC)'

test-unit: ## Run unit tests only
	@echo '$(CYAN)Running unit tests...$(NC)'
	docker-compose exec spark-master pytest tests/unit/ -v -m unit

test-integration: ## Run integration tests only
	@echo '$(CYAN)Running integration tests...$(NC)'
	docker-compose exec spark-master pytest tests/integration/ -v -m integration

test-coverage: ## Run tests with coverage report
	@echo '$(CYAN)Running tests with coverage...$(NC)'
	docker-compose exec spark-master pytest tests/ --cov=src --cov-report=html --cov-report=term
	@echo '$(GREEN)Coverage report generated in htmlcov/$(NC)'

lint: ## Run code linting
	@echo '$(CYAN)Running linters...$(NC)'
	docker-compose exec spark-master flake8 src/ --max-line-length=120
	docker-compose exec spark-master pylint src/ --disable=C0111,C0103

format: ## Format code with black
	@echo '$(CYAN)Formatting code...$(NC)'
	docker-compose exec spark-master black src/ tests/
	docker-compose exec spark-master isort src/ tests/
	@echo '$(GREEN)Code formatted!$(NC)'

type-check: ## Run type checking with mypy
	@echo '$(CYAN)Running type checks...$(NC)'
	docker-compose exec spark-master mypy src/

validate-config: ## Validate configuration files
	@echo '$(CYAN)Validating configuration...$(NC)'
	@python -c "import yaml; yaml.safe_load(open('src/config/config.yaml'))" && \
		echo '$(GREEN)Configuration is valid!$(NC)' || \
		echo '$(RED)Configuration has errors!$(NC)'

init-airflow: ## Initialize Airflow (run once)
	@echo '$(CYAN)Initializing Airflow...$(NC)'
	docker-compose up airflow-init
	@echo '$(GREEN)Airflow initialized!$(NC)'

create-user: ## Create Airflow admin user
	@echo '$(CYAN)Creating Airflow user...$(NC)'
	docker-compose exec airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin

backup-data: ## Backup data directory
	@echo '$(CYAN)Backing up data...$(NC)'
	@tar -czf backup_$(shell date +%Y%m%d_%H%M%S).tar.gz data/
	@echo '$(GREEN)Backup complete!$(NC)'

clean-data: ## Clean data directories
	@echo '$(CYAN)Cleaning data directories...$(NC)'
	rm -rf data/raw/* data/processed/* data/output/*
	@echo '$(GREEN)Data cleaned!$(NC)'

clean-logs: ## Clean log files
	@echo '$(CYAN)Cleaning logs...$(NC)'
	rm -rf logs/**/*.log
	@echo '$(GREEN)Logs cleaned!$(NC)'

clean-cache: ## Clean Python cache files
	@echo '$(CYAN)Cleaning cache...$(NC)'
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo '$(GREEN)Cache cleaned!$(NC)'

clean: clean-data clean-logs clean-cache ## Clean all generated files
	@echo '$(GREEN)All cleanup complete!$(NC)'

reset: down clean ## Stop services and clean all data
	@echo '$(YELLOW)Removing Docker volumes...$(NC)'
	docker-compose down -v
	@echo '$(GREEN)Reset complete!$(NC)'

prune: ## Remove all unused Docker resources
	@echo '$(RED)Warning: This will remove all unused Docker resources!$(NC)'
	@read -p "Continue? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker system prune -af --volumes; \
		echo '$(GREEN)Pruning complete!$(NC)'; \
	fi

monitor: ## Open monitoring dashboards
	@echo '$(CYAN)Opening monitoring dashboards...$(NC)'
	@echo 'Spark UI:    http://localhost:8080'
	@echo 'Airflow UI:  http://localhost:8083'
	@echo 'Grafana:     http://localhost:3000'
	@xdg-open http://localhost:3000 2>/dev/null || open http://localhost:3000 2>/dev/null || echo "Please open http://localhost:3000 manually"

install-dev: ## Install development dependencies locally
	@echo '$(CYAN)Installing development dependencies...$(NC)'
	pip install -r requirements.txt
	@echo '$(GREEN)Dependencies installed!$(NC)'

notebook: ## Start Jupyter notebook
	@echo '$(CYAN)Starting Jupyter notebook...$(NC)'
	@echo 'Access JupyterLab at: http://localhost:8888'
	@xdg-open http://localhost:8888 2>/dev/null || open http://localhost:8888 2>/dev/null || echo "Please open http://localhost:8888 manually"

docs: ## Generate documentation
	@echo '$(CYAN)Generating documentation...$(NC)'
	cd docs && make html
	@echo '$(GREEN)Documentation generated in docs/_build/html/$(NC)'

version: ## Show versions of key components
	@echo '$(CYAN)Component Versions:$(NC)'
	@docker-compose exec spark-master spark-submit --version 2>&1 | grep "version"
	@docker-compose exec airflow-webserver airflow version
	@docker-compose exec jupyter python --version

status: ## Show detailed status of all services
	@echo '$(CYAN)Service Status:$(NC)'
	@docker-compose ps
	@echo ''
	@echo '$(CYAN)Resource Usage:$(NC)'
	@docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

pipeline-status: ## Check pipeline execution status
	@echo '$(CYAN)Pipeline Status:$(NC)'
	@docker-compose exec airflow-webserver airflow dags list
	@echo ''
	@docker-compose exec airflow-webserver airflow dags list-runs -d tmdb_movie_pipeline --limit 5