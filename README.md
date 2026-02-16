# TMDB Movie Data Analysis Pipeline

A scalable data engineering pipeline for analyzing movie data from [The Movie Database (TMDB)](https://www.themoviedb.org/) API using **Apache Spark**, **Apache Airflow**, and modern data engineering practices.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Pipeline Stages](#pipeline-stages)
- [Testing](#testing)
- [CI/CD](#cicd)
- [Monitoring & Dashboards](#monitoring--dashboards)
- [Configuration](#configuration)
- [Makefile Commands](#makefile-commands)

---

## Project Overview

This project transforms raw movie data from the TMDB API into actionable insights through a fully orchestrated pipeline:

- **Distributed Data Processing** with Apache Spark (1 master + 2 workers)
- **Workflow Orchestration** with Apache Airflow
- **Intelligent Caching** with Redis
- **Data Quality Validation** with custom validators
- **Advanced Analytics** with comprehensive KPIs
- **Interactive Visualizations** with Matplotlib / Seaborn / Plotly

### Key Capabilities

| Category | Features |
|---|---|
| **Data Engineering** | Spark cluster processing, Airflow DAGs, Redis caching, token-bucket rate limiter, retry logic |
| **Data Quality** | Schema validation, business rules, completeness checks, outlier detection, quality scoring |
| **Analytics & KPIs** | Revenue / profit / ROI, performance rankings, genre analysis, franchise comparison, director metrics, temporal trends |
| **Visualization** | Revenue vs budget plots, genre distributions, yearly trends, ROI distributions, correlation matrices |

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                User Interface Layer                  │
│   JupyterLab  │  Airflow UI  │  Grafana  │  Reports │
└──────────────────────────────────────────────────────┘
                          ↓
┌──────────────────────────────────────────────────────┐
│              Orchestration Layer                     │
│              Apache Airflow (DAGs)                   │
└──────────────────────────────────────────────────────┘
                          ↓
┌──────────────────────────────────────────────────────┐
│              Processing Layer                        │
│     Spark Master  ←→  Worker 1  ←→  Worker 2        │
└──────────────────────────────────────────────────────┘
                          ↓
┌──────────────────────────────────────────────────────┐
│                 Data Layer                           │
│   TMDB API  │  PostgreSQL  │  Redis  │  Parquet     │
└──────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Processing Engine | Apache Spark (PySpark) | 3.5.0 |
| Orchestration | Apache Airflow | 2.x |
| Database | PostgreSQL | 14 |
| Cache | Redis | 7 |
| Notebooks | JupyterLab | latest |
| Monitoring | Grafana | latest |
| Containerisation | Docker Compose | 3.8 |
| CI/CD | GitHub Actions | v4 |
| Language | Python | 3.11 |

---

## Project Structure

```
tmdb-movie-analysis/
├── .github/workflows/          # CI/CD pipelines
│   ├── ci.yml                  #   Lint → Unit Tests → Integration → Build
│   ├── deploy.yml              #   Staging / Production deploy
│   └── nightly.yml             #   Nightly full suite + dependency audit
├── airflow/
│   ├── dags/                   # Airflow DAG definitions
│   ├── logs/                   # Airflow task logs
│   └── plugins/                # Custom Airflow plugins
├── data/
│   ├── raw/                    # Raw API data (Parquet)
│   ├── processed/              # Cleaned & transformed data
│   └── output/                 # KPIs, metrics, validation reports
├── docker/
│   ├── docker-compose.yml      # Full stack definition (8 services)
│   ├── Dockerfile.spark        # Spark master/worker image
│   ├── Dockerfile.airflow      # Airflow image
│   ├── Dockerfile.jupyter      # JupyterLab image
│   └── init-db.sql             # Database initialisation
├── grafana/                    # Grafana dashboards & datasources
├── notebooks/                  # Jupyter notebooks for exploration
├── src/
│   ├── main.py                 # Pipeline entry point
│   ├── config/                 # YAML config & logging setup
│   ├── ingestion/              # API client, data fetcher
│   ├── processing/             # Cleaner, transformer, validator
│   ├── analytics/              # KPI calculator, metrics aggregator, queries
│   ├── visualization/          # Chart generation
│   └── utils/                  # Helpers, Spark session manager
├── tests/
│   ├── conftest.py             # Shared fixtures (Spark session, sample data)
│   ├── unit/                   # 139 unit tests across 9 modules
│   └── integration/            # 11 integration tests (pipeline flows)
├── Makefile                    # Project automation commands
├── pytest.ini                  # Test configuration
├── requirements.txt            # Python dependencies
└── README.md                   # ← You are here
```

---

## Quick Start

### Prerequisites

- **Docker** & **Docker Compose** (recommended)
- **Python 3.11+** and **Java 17+** (for local development)
- A free [TMDB API key](https://developer.themoviedb.org/docs/getting-started)

### 1. Clone & configure

```bash
git clone <repository-url>
cd tmdb-movie-analysis
cp .env.example .env          # or create .env manually
```

Add your API key to `.env`:

```dotenv
TMDB_API_KEY=your_api_key_here
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
REDIS_PASSWORD=redis_password
AIRFLOW__CORE__FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
AIRFLOW_UID=50000
```

### 2. Start with Docker (recommended)

```bash
cd docker
docker compose up -d --build
```

### 3. Access the applications

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8083 | admin / admin |
| Spark Master | http://localhost:8080 | — |
| JupyterLab | http://localhost:8888 | — |
| Grafana | http://localhost:3000 | admin / admin |

### 4. Run the pipeline

```bash
# Via Airflow UI (recommended) — trigger the tmdb_movie_pipeline DAG

# Or via Makefile
make run-pipeline

# Or run locally without Docker
python src/main.py
```

> For detailed step-by-step execution instructions see [EXECUTION_GUIDE.md](EXECUTION_GUIDE.md).

---

## Pipeline Stages

### 1. Data Ingestion
Fetches movie data from the TMDB API with rate limiting (token-bucket), Redis caching, and automatic retries on failure.

### 2. Data Cleaning
Drops irrelevant columns, handles missing / placeholder values, converts data types, processes nested JSON fields (genres, companies, countries, languages), and extracts cast/crew.

### 3. Data Transformation
Converts budgets/revenues to millions, calculates profit, ROI, and revenue-per-minute, categorises budgets and ratings, extracts year/decade, flags franchise membership, and computes popularity scores.

### 4. Data Validation
Validates schema, checks completeness/uniqueness, enforces value ranges, applies business rules, and produces a quality report with pass/fail scoring.

### 5. Analytics & KPIs
Ranks movies by configurable metrics, analyses franchise vs standalone performance, calculates per-genre and per-director statistics, aggregates temporal trends, and runs advanced search queries.

### 6. Visualisation & Export
Generates charts (revenue vs budget, genre distributions, yearly trends, ROI distributions, correlation matrices), exports metrics to Parquet, and produces JSON summary reports.

---

## Testing

The project includes **150 tests** (139 unit + 11 integration):

```bash
# Run all tests
python -m pytest tests/ -v

# Unit tests only
python -m pytest tests/unit/ -v

# Integration tests only
python -m pytest tests/integration/ -v

# With coverage
python -m pytest tests/ --cov=src --cov-report=term-missing
```

| Test Module | Tests | Covers |
|---|---|---|
| test_api_client | 18 | RateLimiter, CacheManager, TMDBClient |
| test_data_fetcher | 6 | DataFetcher, schema, save/close |
| test_data_cleaner | 13 | DataCleaner full pipeline |
| test_data_transformer | 14 | Financial, categorical, advanced transforms |
| test_data_validator | 15 | Schema, completeness, ranges, business rules |
| test_kpi_calculator | 9 | Rankings, franchise, search queries |
| test_metrics_aggregator | 11 | Temporal, genre, director, tiers, correlation |
| test_advanced_queries | 11 | Search filters, collaborations, decade comparison |
| test_helpers | 42 | Utilities, decorators, formatters |
| test_pipeline_integration | 11 | End-to-end pipeline flows |

---

## CI/CD

Three GitHub Actions workflows in `.github/workflows/`:

### `ci.yml` — Main Pipeline
Runs on every push to `main`/`develop` and on pull requests.

```
lint  →  unit-tests (coverage ≥ 50%)  →  integration-tests  →  build (Docker images, main only)
```

### `deploy.yml` — Deployment
Triggers after CI passes on `main`, or manually with staging/production selection.

```
validate (Compose config + secrets)  →  deploy (build + health-check + smoke test)
```

### `nightly.yml` — Nightly Checks
Runs daily at 3 AM UTC.

```
full test suite + coverage  |  dependency audit (pip-audit)
```

---

## Monitoring & Dashboards

| Dashboard | URL | Purpose |
|---|---|---|
| Grafana | http://localhost:3000 | Real-time pipeline metrics |
| Spark UI | http://localhost:8080 | Job execution details |
| Airflow UI | http://localhost:8083 | Workflow status & task logs |

---

## Configuration

All pipeline behaviour is controlled via `src/config/config.yaml`:

- **API settings**: rate limits, timeouts, retry policy, cache TTL
- **Processing rules**: columns to drop, placeholder values, quality thresholds
- **Spark config**: driver/executor memory, shuffle partitions
- **Validation ranges**: budget, revenue, rating bounds
- **KPI definitions**: ranking metrics, top-N, filters
- **Search queries**: genre/cast/director search templates

---

## Makefile Commands

```bash
make help              # Show all available commands
make setup             # Create directories and .env
make build             # Build Docker images
make up                # Start all services
make down              # Stop all services
make restart           # Restart all services
make test              # Run test suite
make lint              # Run linters
make run-pipeline      # Trigger Airflow pipeline
make health-check      # Check service health
make logs              # Tail all service logs
make clean             # Remove containers and volumes
```


