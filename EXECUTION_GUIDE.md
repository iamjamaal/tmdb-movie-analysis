# TMDB Movie Analysis Pipeline — Execution Guide

A step-by-step walkthrough for setting up, running, and verifying every component of the pipeline.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Environment Setup](#2-environment-setup)
3. [Running with Docker (Recommended)](#3-running-with-docker-recommended)
4. [Running Locally (Without Docker)](#4-running-locally-without-docker)
5. [Running the Pipeline](#5-running-the-pipeline)
6. [Running Tests](#6-running-tests)
7. [Checking Outputs](#7-checking-outputs)
8. [Monitoring & Debugging](#8-monitoring--debugging)
9. [CI/CD Workflow](#9-cicd-workflow)
10. [Maintenance & Cleanup](#10-maintenance--cleanup)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Prerequisites

| Requirement | Version | Purpose |
|---|---|---|
| **Docker Desktop** | 20.x+ | Containerised deployment |
| **Docker Compose** | 2.x+ | Multi-service orchestration |
| **Python** | 3.11+ | Local development / testing |
| **Java (JDK)** | 17 | PySpark runtime |
| **Git** | 2.x+ | Version control |
| **TMDB API Key** | — | Data source ([get one free](https://developer.themoviedb.org/docs/getting-started)) |

> **Windows users**: install [Hadoop winutils](https://github.com/steveloughran/winutils) and set `HADOOP_HOME` if running PySpark locally.

---

## 2. Environment Setup

### Step 2.1 — Clone the repository

```bash
git clone <repository-url>
cd tmdb-movie-analysis
```

### Step 2.2 — Create the `.env` file

```bash
# Copy the template (or create manually)
cp .env.example .env
```

Edit `.env` with your values:

```dotenv
TMDB_API_KEY=your_tmdb_api_key_here
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
REDIS_PASSWORD=redis_password
AIRFLOW_UID=50000

# Generate a Fernet key for Airflow:
#   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here
```

### Step 2.3 — Create data directories

```bash
mkdir -p data/{raw,processed,output} logs
```

Or use the Makefile:

```bash
make setup
```

### Step 2.4 — Install Python dependencies (local dev only)

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

---

## 3. Running with Docker (Recommended)

### Step 3.1 — Build all images

```bash
cd docker
docker compose build
```

This builds three custom images:
- `tmdb-spark` — Spark master and workers
- `tmdb-airflow` — Airflow webserver and scheduler
- `tmdb-jupyter` — JupyterLab notebook server

### Step 3.2 — Initialise Airflow (first time only)

```bash
docker compose up airflow-init
```

This creates the Airflow metadata database and the default admin user (`admin` / `admin`).

### Step 3.3 — Start all services

```bash
docker compose up -d
```

This starts **8 services**:

| Service | Container | Port |
|---|---|---|
| PostgreSQL 14 | tmdb-postgres | 5432 |
| Redis 7 | tmdb-redis | 6379 |
| Spark Master | tmdb-spark-master | 8080, 7077 |
| Spark Worker 1 | tmdb-spark-worker-1 | 8081 |
| Spark Worker 2 | tmdb-spark-worker-2 | 8082 |
| Airflow Webserver | tmdb-airflow-webserver | 8083 |
| Airflow Scheduler | tmdb-airflow-scheduler | — |
| JupyterLab | tmdb-jupyter | 8888 |
| Grafana | tmdb-grafana | 3000 |

### Step 3.4 — Verify all services are healthy

```bash
docker compose ps
```

All containers should show `Up` or `healthy`. You can also run:

```bash
# From repository root
make health-check
```

### Step 3.5 — Access the UIs

Open in your browser:

- **Airflow**: http://localhost:8083 — login with `admin` / `admin`
- **Spark Master**: http://localhost:8080
- **JupyterLab**: http://localhost:8888
- **Grafana**: http://localhost:3000 — login with `admin` / `admin`

---

## 4. Running Locally (Without Docker)

Use this for development or when Docker is not available.

### Step 4.1 — Set environment variables

**Windows (PowerShell):**

```powershell
$env:TMDB_API_KEY = "your_key"
$env:ENVIRONMENT = "development"
$env:LOG_LEVEL = "INFO"
$env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PWD\.venv\Scripts\python.exe"
```

**macOS / Linux:**

```bash
export TMDB_API_KEY="your_key"
export ENVIRONMENT="development"
export LOG_LEVEL="INFO"
```

### Step 4.2 — Run the full pipeline

```bash
cd src
python main.py
```

The pipeline executes 7 stages sequentially:

```
Step 1: Data Ingestion      — Fetches movie data from TMDB API
Step 2: Data Cleaning        — Drops columns, handles nulls, parses JSON
Step 3: Data Transformation  — Calculates profit, ROI, categories, decades
Step 4: Data Validation      — Schema checks, business rules, quality score
Step 5: Analytics & KPIs     — Rankings, franchise analysis, genre metrics
Step 6: Advanced Queries     — Search queries (genre + cast + director)
Step 7: Visualisation        — Charts, dashboards, summary reports
```

On success you will see:

```
✓ Pipeline completed successfully in XX.XX seconds
✓ Processed NNNN records
✓ Data quality score: 0.XX
✓ Output location: /opt/spark-data/output
```

---

## 5. Running the Pipeline

### Option A — Via Airflow (production)

1. Open http://localhost:8083
2. Find the `tmdb_movie_pipeline` DAG
3. Toggle the DAG **ON** (if paused)
4. Click the **Play** button → **Trigger DAG**
5. Monitor task groups in the **Graph** or **Grid** view

The DAG runs these task groups:
- `ingestion` → `cleaning` → `transformation` → `validation` → `analytics` → `export`

### Option B — Via Makefile

```bash
make run-pipeline          # Trigger via Airflow
make run-local             # Spark-submit via container
```

### Option C — Via Spark Submit (inside container)

```bash
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 4g \
    --executor-memory 4g \
    /opt/spark-apps/src/main.py
```

### Option D — Via Python (local)

```bash
python src/main.py
```

---

## 6. Running Tests

### Step 6.1 — Run all tests

```bash
python -m pytest tests/ -v --tb=short
```

Expected result: **150 tests** — 148 passed, 2 xfail (expected failures for Hadoop-dependent parquet writes on local Windows).

### Step 6.2 — Run unit tests only

```bash
python -m pytest tests/unit/ -v
```

139 unit tests covering all source modules:

| Module | Tests |
|---|---|
| API Client (RateLimiter, CacheManager, TMDBClient) | 18 |
| Data Fetcher | 6 |
| Data Cleaner | 13 |
| Data Transformer | 14 |
| Data Validator | 15 |
| KPI Calculator | 9 |
| Metrics Aggregator | 11 |
| Advanced Queries | 11 |
| Helpers & Utilities | 42 |

### Step 6.3 — Run integration tests only

```bash
python -m pytest tests/integration/ -v
```

11 integration tests verifying end-to-end flows (clean → transform → validate → analyse → export).

### Step 6.4 — Run with coverage report

```bash
python -m pytest tests/ --cov=src --cov-report=term-missing --cov-report=html
```

Open `htmlcov/index.html` in a browser to inspect line-by-line coverage.

### Step 6.5 — Run tests inside Docker

```bash
docker compose exec spark-master pytest tests/ -v --tb=short
```

---

## 7. Checking Outputs

After a successful pipeline run, outputs are in the `data/` directory:

```
data/
├── raw/                          # Raw API data (Parquet)
│   └── movies_run_YYYYMMDD_HHMMSS/
├── processed/                    # Cleaned & transformed data
│   ├── movies_clean_run_.../
│   └── movies_transformed_run_.../
└── output/
    ├── pipeline_summary_run_....json    # Run summary
    ├── validation_report_run_....json   # Quality report
    ├── kpis_run_.../                    # KPI CSVs
    │   ├── rankings/
    │   │   ├── highest_revenue.csv
    │   │   ├── highest_rated.csv
    │   │   └── ...
    │   ├── franchise_comparison.csv
    │   ├── top_directors.csv
    │   └── summary_statistics.json
    ├── metrics_run_.../                 # Aggregated metrics (Parquet)
    │   ├── temporal/
    │   ├── genre/
    │   ├── director/
    │   └── ...
    └── visualizations/                  # Generated charts (PNG)
```

### Quick verification

```bash
# Check if outputs exist
ls data/output/

# View pipeline summary
cat data/output/pipeline_summary_run_*.json | python -m json.tool

# View validation report
cat data/output/validation_report_run_*.json | python -m json.tool

# Count processed records
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').getOrCreate()
df = spark.read.parquet('data/processed/movies_transformed_run_*')
print(f'Total records: {df.count()}')
print(f'Columns: {len(df.columns)}')
df.printSchema()
spark.stop()
"
```

---

## 8. Monitoring & Debugging

### Service logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f spark-master
docker compose logs -f airflow-scheduler

# Pipeline logs (Makefile)
make logs
make logs-spark
make logs-airflow
```

### Airflow task logs

1. Open http://localhost:8083
2. Click on the `tmdb_movie_pipeline` DAG
3. Click on a failed/running task
4. Click **Log** to see detailed output

### Spark job details

1. Open http://localhost:8080
2. Click on a running/completed application
3. View stages, tasks, and executor metrics

### Shell access

```bash
docker compose exec spark-master bash      # Spark container
docker compose exec airflow-webserver bash  # Airflow container
docker compose exec jupyter bash            # Jupyter container
```

### Common debugging commands

```bash
# Check service health
make health-check

# Check Airflow DAG status
make pipeline-status

# Validate config file
make validate-config

# Check component versions
make version
```

---

## 9. CI/CD Workflow

The project uses **GitHub Actions** with three workflows:

### `ci.yml` — Runs on every push & PR

```
┌──────┐    ┌────────────┐    ┌───────────────────┐    ┌───────┐
│ Lint │ →  │ Unit Tests │ →  │ Integration Tests │ →  │ Build │
└──────┘    └────────────┘    └───────────────────┘    └───────┘
 flake8       139 tests        11 tests                Docker images
 black        coverage ≥50%    xfail-tolerant          (main only)
 isort
```

### `deploy.yml` — After CI passes on main

```
┌──────────┐    ┌────────────────────────────────────────┐
│ Validate │ →  │ Deploy (staging or production)         │
└──────────┘    │  build → health-check → smoke test    │
 Compose cfg    └────────────────────────────────────────┘
 secrets
```

### `nightly.yml` — Runs daily at 3 AM UTC

```
Full test suite + coverage  │  Dependency audit (pip-audit)
```

### Required GitHub Secrets

| Secret | Purpose |
|---|---|
| `TMDB_API_KEY` | API access for production |
| `POSTGRES_PASSWORD` | Database password (deploy) |
| `REDIS_PASSWORD` | Cache password (deploy) |
| `AIRFLOW_FERNET_KEY` | Airflow encryption key (deploy) |

### Setting up CI/CD

1. Push the repository to GitHub
2. Go to **Settings → Secrets and variables → Actions**
3. Add the secrets listed above
4. Workflows will trigger automatically on push

---

## 10. Maintenance & Cleanup

### Routine maintenance

```bash
# Backup data before cleanup
make backup-data

# Clean generated data (raw + processed + output)
make clean-data

# Clean log files
make clean-logs

# Clean Python cache (__pycache__, .pyc)
make clean-cache

# All of the above
make clean
```

### Full reset

```bash
# Stop services + remove volumes + clean data
make reset
```

### Docker resource cleanup

```bash
# Remove all unused Docker images, volumes, networks
make prune
```

### Updating dependencies

```bash
# Check for outdated packages
pip list --outdated

# Update requirements.txt and rebuild
pip install --upgrade <package>
pip freeze > requirements.txt
docker compose build --no-cache
```

---

## 11. Troubleshooting

### PySpark won't start locally

**Symptom**: `Py4JError: Constructor org.apache.spark.sql.SparkSession does not exist`

**Cause**: `SPARK_HOME` points to a different Spark version than the installed PySpark.

**Fix** (PowerShell):
```powershell
$env:SPARK_HOME = ""
$env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PWD\.venv\Scripts\python.exe"
```

The `conftest.py` handles this automatically for tests.

---

### Airflow DAG not visible

**Symptom**: DAG doesn't appear in the Airflow UI.

**Fix**:
1. Check that `airflow/dags/tmdb_pipeline_dag.py` is mounted into the container
2. Check for import errors: `docker compose exec airflow-webserver airflow dags list-import-errors`
3. Restart the scheduler: `docker compose restart airflow-scheduler`

---

### Redis connection refused

**Symptom**: `ConnectionError: Error connecting to Redis`

**Fix**:
1. Verify Redis is running: `docker compose ps redis`
2. Check password matches `.env`: `REDIS_PASSWORD`
3. For local dev, set `cache.enabled: false` in `src/config/config.yaml`

---

### Docker services won't start

**Symptom**: Containers crash-loop or fail health checks.

**Fix**:
```bash
# Check logs for the failing service
docker compose logs <service-name>

# Rebuild from scratch
docker compose down -v
docker compose build --no-cache
docker compose up -d
```

---

### Tests fail with parquet write errors on Windows

**Symptom**: `CreateProcess error=2` or `Could not locate executable null\bin\winutils.exe`

**Cause**: Hadoop native libraries are not installed.

**Fix**: Install [winutils.exe](https://github.com/steveloughran/winutils) and set:
```powershell
$env:HADOOP_HOME = "C:\hadoop"
```

Or run tests inside Docker where Hadoop is pre-installed. The 2 affected integration tests are marked `xfail` so they appear as warnings, not failures.

---

### API rate limit exceeded

**Symptom**: `429 Too Many Requests` from TMDB API.

**Fix**: The pipeline has built-in rate limiting (token bucket). If you still hit limits:
1. Reduce `api.rate_limit.requests_per_second` in `src/config/config.yaml`
2. Enable Redis caching (`api.cache.enabled: true`) to avoid repeat calls
3. Wait a few minutes before retrying

---

### Out of memory errors

**Symptom**: `java.lang.OutOfMemoryError` during Spark processing.

**Fix**: Increase memory allocation in `src/config/config.yaml`:
```yaml
spark:
  driver_memory: 4g
  executor_memory: 4g
```

Or in Docker Compose, increase Spark worker memory:
```yaml
environment:
  - SPARK_WORKER_MEMORY=8G
```
