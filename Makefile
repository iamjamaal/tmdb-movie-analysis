.PHONY: help setup build up down restart logs clean test lint format validate-config run-pipeline

# Default target
.DEFAULT_GOAL := help

# Colors for output
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(CYAN)TMDB Movie Analysis Pipeline - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

setup: ## Initial project setup
	@echo "$(CYAN)Setting up project...$(NC)"
	@mkdir -p docker airflow/{dags,logs,plugins} src/{config,ingestion,processing,analytics,visualization,utils} tests/{unit,integration} notebooks data/{raw,processed,output} docs
	@echo "$(GREEN)✓ Directory structure created$(NC)"
	@if [ ! -f .env ]; then \
		echo "$(YELLOW)⚠ Creating .env file from template...$(NC)"; \
		echo "TMDB_API_KEY=your_api_key_here" > .env; \
		echo "AIRFLOW_UID=50000" >> .env; \
		echo "POSTGRES_USER=airflow" >> .env; \
		echo "POSTGRES_PASSWORD=airflow" >> .env; \
		echo "$(GREEN)✓ .env file created. Please update with your API key!$(NC)"; \
	else \
		echo "$(GREEN)✓ .env file already exists$(NC)"; \
	fi

validate-env: ## Validate environment configuration
	@echo "$(CYAN)Validating environment...$(NC)"
	@if [ ! -f .env ]; then \
		echo "$(RED)✗ .env file not found. Run 'make setup' first$(NC)"; \
		exit 1; \
	fi
	@if grep -q "your_api_key_here" .env; then \
		echo "$(RED)✗ TMDB_API_KEY not set in .env file$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)✓ Environment validated$(NC)"

build: validate-env ## Build all Docker images
	@echo "$(CYAN)Building Docker images...$(NC)"
	docker-compose build
	@echo "$(GREEN)✓ Images built successfully$(NC)"

up: validate-env ## Start all services
	@echo "$(CYAN)Starting services...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)✓ Services started$(NC)"
	@echo ""
	@echo "$(CYAN)Service URLs:$(NC)"
	@echo "  Spark Master UI:  http://localhost:8080"
	@echo "  Airflow UI:       http://localhost:8088 (admin/admin)"
	@echo "  JupyterLab:       http://localhost:8888"
	@echo "  Grafana:          http://localhost:3000 (admin/admin)"
	@echo ""

down: ## Stop all services
	@echo "$(CYAN)Stopping services...$(NC)"
	docker-compose down
	@echo "$(GREEN)✓ Services stopped$(NC)"

restart: ## Restart all services
	@echo "$(CYAN)Restarting services...$(NC)"
	docker-compose restart
	@echo "$(GREEN)✓ Services restarted$(NC)"

status: ## Show service status
	@echo "$(CYAN)Service Status:$(NC)"
	@docker-compose ps

logs: ## Show logs for all services
	docker-compose logs -f

logs-spark: ## Show Spark logs
	docker-compose logs -f spark-master spark-worker-1 spark-worker-2

logs-airflow: ## Show Airflow logs
	docker-compose logs -f airflow-webserver airflow-scheduler

init-airflow: ## Initialize Airflow (create admin user)
	@echo "$(CYAN)Initializing Airflow...$(NC)"
	docker-compose exec airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin
	@echo "$(GREEN)✓ Airflow initialized. Login: admin/admin$(NC)"

run-pipeline: ## Run the complete pipeline via Spark
	@echo "$(CYAN)Running TMDB analysis pipeline...$(NC)"
	docker-compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--conf spark.executor.memory=2g \
		--conf spark.driver.memory=2g \
		/opt/spark-apps/main.py
	@echo "$(GREEN)✓ Pipeline execution complete$(NC)"

trigger-dag: ## Trigger Airflow DAG
	@echo "$(CYAN)Triggering Airflow DAG...$(NC)"
	docker-compose exec airflow-scheduler airflow dags trigger tmdb_movie_analysis_pipeline
	@echo "$(GREEN)✓ DAG triggered. Check Airflow UI for status$(NC)"

shell-spark: ## Open Spark master shell
	@echo "$(CYAN)Opening Spark shell...$(NC)"
	docker-compose exec spark-master /bin/bash

shell-airflow: ## Open Airflow webserver shell
	@echo "$(CYAN)Opening Airflow shell...$(NC)"
	docker-compose exec airflow-webserver /bin/bash

jupyter-token: ## Get Jupyter lab token
	@echo "$(CYAN)Jupyter Lab Token:$(NC)"
	@docker-compose logs jupyter | grep "127.0.0.1" | tail -1

test: ## Run tests
	@echo "$(CYAN)Running tests...$(NC)"
	docker-compose exec spark-master pytest /opt/spark-apps/tests/ -v --cov=/opt/spark-apps/src
	@echo "$(GREEN)✓ Tests complete$(NC)"

lint: ## Run linting
	@echo "$(CYAN)Running linters...$(NC)"
	docker-compose exec spark-master flake8 /opt/spark-apps/src
	docker-compose exec spark-master pylint /opt/spark-apps/src
	@echo "$(GREEN)✓ Linting complete$(NC)"

format: ## Format code with black
	@echo "$(CYAN)Formatting code...$(NC)"
	docker-compose exec spark-master black /opt/spark-apps/src
	@echo "$(GREEN)✓ Code formatted$(NC)"

validate-config: ## Validate configuration file
	@echo "$(CYAN)Validating configuration...$(NC)"
	docker-compose exec spark-master python -c "import yaml; yaml.safe_load(open('config/config.yaml'))"
	@echo "$(GREEN)✓ Configuration valid$(NC)"

view-data: ## View processed data
	@echo "$(CYAN)Processed data files:$(NC)"
	docker-compose exec spark-master ls -lh /opt/spark-data/output/processed/

view-kpis: ## View KPI outputs
	@echo "$(CYAN)KPI files:$(NC)"
	docker-compose exec spark-master ls -lh /opt/spark-data/output/kpis/

clean-data: ## Clean all generated data
	@echo "$(CYAN)Cleaning data...$(NC)"
	docker-compose exec spark-master rm -rf /opt/spark-data/output/*
	docker-compose exec spark-master rm -rf /opt/spark-data/raw/*
	docker-compose exec spark-master rm -rf /opt/spark-data/processed/*
	@echo "$(GREEN)✓ Data cleaned$(NC)"

clean-logs: ## Clean all logs
	@echo "$(CYAN)Cleaning logs...$(NC)"
	rm -rf airflow/logs/*
	docker-compose exec spark-master rm -rf /opt/spark-data/logs/*
	@echo "$(GREEN)✓ Logs cleaned$(NC)"

clean: ## Clean all data, logs, and Docker volumes
	@echo "$(CYAN)Cleaning everything...$(NC)"
	$(MAKE) clean-data
	$(MAKE) clean-logs
	docker-compose down -v
	@echo "$(GREEN)✓ Complete cleanup done$(NC)"

backup-data: ## Backup processed data
	@echo "$(CYAN)Backing up data...$(NC)"
	@mkdir -p backups
	docker-compose exec spark-master tar -czf /tmp/tmdb_backup_$$(date +%Y%m%d_%H%M%S).tar.gz /opt/spark-data/output/
	docker cp spark-master:/tmp/tmdb_backup_*.tar.gz backups/
	@echo "$(GREEN)✓ Data backed up to backups/$(NC)"

health-check: ## Check health of all services
	@echo "$(CYAN)Checking service health...$(NC)"
	@echo ""
	@echo "$(CYAN)Spark Master:$(NC)"
	@curl -s http://localhost:8080 > /dev/null && echo "$(GREEN)✓ Running$(NC)" || echo "$(RED)✗ Not responding$(NC)"
	@echo "$(CYAN)Airflow:$(NC)"
	@curl -s http://localhost:8088/health > /dev/null && echo "$(GREEN)✓ Running$(NC)" || echo "$(RED)✗ Not responding$(NC)"
	@echo "$(CYAN)Jupyter:$(NC)"
	@curl -s http://localhost:8888 > /dev/null && echo "$(GREEN)✓ Running$(NC)" || echo "$(RED)✗ Not responding$(NC)"
	@echo "$(CYAN)Redis:$(NC)"
	@docker-compose exec -T redis redis-cli ping > /dev/null && echo "$(GREEN)✓ Running$(NC)" || echo "$(RED)✗ Not responding$(NC)"

install-dev: ## Install development dependencies
	@echo "$(CYAN)Installing development dependencies...$(NC)"
	pip install -r requirements.txt
	pip install pytest pytest-cov flake8 pylint black mypy
	@echo "$(GREEN)✓ Development dependencies installed$(NC)"

quick-start: setup build up init-airflow ## Quick start: setup, build, and start everything
	@echo ""
	@echo "$(GREEN)✓✓✓ Quick start complete! ✓✓✓$(NC)"
	@echo ""
	@echo "$(CYAN)Next steps:$(NC)"
	@echo "  1. Access Airflow UI: http://localhost:8088 (admin/admin)"
	@echo "  2. Access Spark UI: http://localhost:8080"
	@echo "  3. Run pipeline: make run-pipeline"
	@echo "  4. Or trigger DAG in Airflow UI"
	@echo ""

full-run: up run-pipeline view-kpis ## Run complete pipeline and show results
	@echo "$(GREEN)✓ Full pipeline execution complete!$(NC)"

ci-test: ## Run CI/CD tests
	@echo "$(CYAN)Running CI tests...$(NC)"
	$(MAKE) validate-config
	$(MAKE) lint
	$(MAKE) test
	@echo "$(GREEN)✓ All CI tests passed$(NC)"