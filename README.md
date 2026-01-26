# 🎬 TMDB Movie Data Analysis Pipeline

A production-grade, scalable data engineering pipeline for analyzing movie data from The Movie Database (TMDB) API using Apache Spark, Airflow, and modern data engineering practices.

## 📊 Project Overview

This project transforms raw movie data from TMDB API into actionable insights through:
- **Distributed Data Processing** with Apache Spark
- **Workflow Orchestration** with Apache Airflow
- **Intelligent Caching** with Redis
- **Data Quality Validation** with custom validators
- **Advanced Analytics** with comprehensive KPIs
- **Interactive Visualizations** with Matplotlib/Seaborn

## Key Features

### Data Engineering
 **Scalable Architecture**: Spark cluster for distributed processing  
 **Automated Workflows**: Airflow DAGs for orchestration  
 **Smart Caching**: Redis-based caching to minimize API calls  
 **Rate Limiting**: Token bucket algorithm for API protection  
 **Error Handling**: Comprehensive retry logic and fallback mechanisms  

### Data Quality
 **Schema Validation**: Automated schema checking  
 **Business Rule Validation**: Custom validation rules  
 **Data Completeness Checks**: Missing value detection  
 **Outlier Detection**: Statistical anomaly identification  
 **Quality Scoring**: Overall data health metrics  

### Analytics & KPIs
 **Financial Metrics**: Revenue, profit, ROI calculations  
 **Performance Rankings**: Top/bottom movies by various metrics  
 **Temporal Analysis**: Yearly trends and patterns  
 **Genre Analysis**: Genre-specific performance metrics  
 **Franchise Comparison**: Franchise vs standalone analysis  
 **Director Analytics**: Director performance metrics  

### Visualization
 **Interactive Dashboards**: Web-based reporting    
 **Correlation Plots**: Multi-dimensional analysis  
 **Distribution Charts**: Statistical distributions  

## Architecture

```
┌─────────────────────────────────────────────────┐
│              User Interface Layer               │
│   JupyterLab │ Airflow UI │ Grafana │ Reports  │
└─────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────┐
│           Orchestration Layer                   │
│           Apache Airflow                        │
└─────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────┐
│           Processing Layer                      │
│   Spark Master ←→ Worker-1 ←→ Worker-2         │
└─────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────┐
│              Data Layer                         │
│   PostgreSQL │ Redis Cache │ File Storage      │
└─────────────────────────────────────────────────┘
```

##  Project Structure

```
tmdb-movie-analysis/
├── docker/                      # Docker configurations
│   ├── docker-compose.yml      # Multi-service orchestration
│   ├── Dockerfile.spark        # Spark image
│   ├── Dockerfile.airflow      # Airflow image
│   └── Dockerfile.jupyter      # Jupyter image
│
├── airflow/                     # Airflow components
│   ├── dags/
│   │   └── tmdb_pipeline_dag.py
│   ├── plugins/
│   └── config/
│
├── src/                         # Source code
│   ├── config/
│   │   ├── config.yaml         # Central configuration
│   │   └── logging_config.py   # Logging setup
│   │
│   ├── ingestion/              # Data fetching
│   │   ├── api_client.py       # TMDB API client
│   │   └── data_fetcher.py     # Data fetching logic
│   │
│   ├── processing/             # Data processing
│   │   ├── data_cleaner.py     # Data cleaning
│   │   ├── data_transformer.py # Feature engineering
│   │   └── data_validator.py   # Quality validation
│   │
│   ├── analytics/              # Analytics & KPIs
│   │   ├── kpi_calculator.py   # KPI calculations
│   │   ├── advanced_queries.py # Complex queries
│   │   └── metrics_aggregator.py
│   │
│   ├── visualization/          # Visualizations
│   │   └── dashboard_generator.py
│   │
│   ├── utils/                  # Utilities
│   │   ├── spark_session.py    # Spark management
│   │   └── helpers.py          # Helper functions
│   │
│   └── main.py                 # Main pipeline
│
├── tests/                       # Test suite
│   ├── unit/
│   ├── integration/
│   └── conftest.py
│
├── notebooks/                   # Jupyter notebooks
│   └── exploratory_analysis.ipynb
│
├── data/                        # Data directories
│   ├── raw/                    # Raw API data
│   ├── processed/              # Cleaned data
│   └── output/                 # Results & reports
│
├── docs/                        # Documentation
│   ├── architecture.md
│   └── api_documentation.md
│
├── requirements.txt             # Python dependencies
├── Makefile                    # Automation commands
└── README.md                   # This file
```

4. **Access the applications**
- **Airflow**: http://localhost:8081 (admin/admin)
- **Spark Master UI**: http://localhost:8080
- **JupyterLab**: http://localhost:8888
- **Grafana**: http://localhost:3000 (admin/admin)

5. **Run the pipeline**
```bash
# Via Airflow UI (recommended)
# Navigate to http://localhost:8081 and trigger the DAG

# Or via command line
make run-pipeline
```


##  Pipeline Stages

### 1. Data Ingestion
- Fetches movie data from TMDB API
- Implements rate limiting and caching
- Handles API failures with retry logic

### 2. Data Cleaning
- Removes irrelevant columns
- Handles missing values
- Fixes data type issues
- Removes duplicates
- Standardizes formats

### 3. Data Transformation
- Feature engineering
- Calculated fields (profit, ROI)
- Date parsing and extraction
- Multi-value field processing

### 4. Data Validation
- Schema validation
- Business rule checks
- Data quality scoring
- Outlier detection

### 5. Analytics & KPIs
- Financial metrics (revenue, profit, ROI)
- Performance rankings
- Genre analysis
- Director/franchise analytics
- Temporal trends

### 6. Visualization
- Revenue vs budget plots
- Genre distributions
- Yearly trends
- ROI distributions
- Rating correlations
- Franchise comparisons

##  Sample KPIs

### Financial Metrics
- **Highest Revenue Movies**
- **Highest Profit Movies**
- **Best ROI (Budget ≥ $10M)**
- **Worst ROI**

### Quality Metrics
- **Highest Rated Movies** (min 10 votes)
- **Most Popular Movies**
- **Most Voted Movies**

### Analysis Queries
- Franchise vs Standalone comparison
- Genre-specific performance
- Director performance metrics
- Yearly box office trends



### Dashboards
- **Grafana**: Real-time metrics (http://localhost:3000)
- **Spark UI**: Job execution details (http://localhost:8080)
- **Airflow UI**: Workflow status (http://localhost:8081)


