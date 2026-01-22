# 🎬 TMDB Movie Data Analysis Pipeline

[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://docker.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.7.3-red)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

A **production-grade, scalable** data engineering pipeline for analyzing TMDB (The Movie Database) movie data using **PySpark**, **Docker**, **Apache Airflow**, and modern data engineering best practices.

---

## 📋 Table of Contents
- [Features](#-features)
- [Architecture](#-architecture)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Pipeline Stages](#-pipeline-stages)
- [Key Performance Indicators](#-key-performance-indicators)
- [Usage](#-usage)
- [Monitoring](#-monitoring)
- [Development](#-development)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)

---

## ✨ Features

### 🚀 Production-Ready Architecture
- **Distributed Processing**: Apache Spark cluster with master and workers
- **Orchestration**: Apache Airflow for workflow management
- **Containerization**: Docker Compose for easy deployment
- **Caching**: Redis for API response caching
- **Monitoring**: Grafana dashboards and Spark UI

### 📊 Comprehensive Analysis
- **19 Movie IDs** from TMDB API
- **40+ KPIs** including ROI, profit, ratings, popularity
- **Advanced Queries**: Genre-based searches, director analysis
- **Franchise Analysis**: Franchise vs standalone performance
- **Temporal Trends**: Yearly and decade-based insights

### 🔧 Best Practices
- **Modular Design**: Reusable, testable components
- **Data Validation**: Automated quality checks
- **Error Handling**: Comprehensive retry logic and fallbacks
- **Logging**: Structured logging throughout pipeline
- **Type Hints**: Full type annotations for maintainability

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Environment                │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Spark Master │  │ Spark Worker│  │ Spark Worker│      │
│  │   (Master)   │──│      1      │  │      2      │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         │                                                     │
│         │          ┌──────────────┐  ┌──────────────┐      │
│         └──────────│   Airflow    │  │   Airflow    │      │
│                    │  Webserver   │──│  Scheduler   │      │
│                    └──────────────┘  └──────────────┘      │
│                           │                 │                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  PostgreSQL  │  │    Redis     │  │  JupyterLab  │      │
│  │  (Metadata)  │  │   (Cache)    │  │ (Analysis)   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                            │
                    ┌───────┴────────┐
                    │   TMDB API     │
                    └────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites
```bash
- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM (16GB recommended)
- TMDB API Key (get from: https://www.themoviedb.org/settings/api)
```

### 1. Clone and Setup
```bash
# Clone repository
git clone <your-repo-url>
cd tmdb-movie-analysis

# Run setup and start everything
make quick-start
```

This single command will:
- Create directory structure
- Generate `.env` file
- Build Docker images
- Start all services
- Initialize Airflow with admin user

### 2. Configure API Key
```bash
# Edit .env file and add your TMDB API key
nano .env

# Update this line:
TMDB_API_KEY=your_actual_api_key_here
```

### 3. Restart Services
```bash
make restart
```

### 4. Run Pipeline

#### Option A: Via Airflow UI (Recommended)
1. Open http://localhost:8088
2. Login: `admin` / `admin`
3. Enable the DAG: `tmdb_movie_analysis_pipeline`
4. Click "Trigger DAG"

#### Option B: Via Command Line
```bash
make run-pipeline
```

#### Option C: Via Makefile (Complete Run)
```bash
make full-run
```

### 5. View Results
```bash
# View processed data
make view-data

# View KPI results
make view-kpis

# Open dashboards
open data/output/dashboards/tmdb_dashboard.html
```

---

## 📁 Project Structure

```
tmdb-movie-analysis/
├── docker/                      # Docker configurations
│   ├── docker-compose.yml
│   ├── Dockerfile.spark
│   ├── Dockerfile.airflow
│   └── Dockerfile.jupyter
│
├── src/                         # Source code
│   ├── config/
│   │   ├── config.yaml         # Central configuration
│   │   └── logging_config.py
│   ├── ingestion/
│   │   ├── api_client.py       # TMDB API client with caching
│   │   └── data_fetcher.py     # Data ingestion orchestrator
│   ├── processing/
│   │   ├── data_cleaner.py     # Data cleaning logic
│   │   ├── data_transformer.py # Feature engineering
│   │   └── data_validator.py   # Data quality checks
│   ├── analytics/
│   │   ├── kpi_calculator.py   # KPI calculations
│   │   └── advanced_queries.py # Complex queries
│   ├── visualization/
│   │   └── dashboard_generator.py
│   ├── utils/
│   │   └── spark_session.py
│   └── main.py                 # Main pipeline orchestrator
│
├── airflow/
│   └── dags/
│       └── tmdb_pipeline_dag.py
│
├── tests/                       # Test suite
│   ├── unit/
│   └── integration/
│
├── notebooks/                   # Jupyter notebooks
│   └── exploratory_analysis.ipynb
│
├── data/                        # Data storage
│   ├── raw/
│   ├── processed/
│   └── output/
│
├── Makefile                     # Automation commands
├── requirements.txt
├── README.md
└── IMPLEMENTATION_GUIDE.md
```

---

## 🔄 Pipeline Stages

### Stage 1: Data Ingestion
- Fetch movie data from TMDB API
- Apply rate limiting and caching
- Handle API errors gracefully
- Store raw data

### Stage 2: Data Cleaning
- Drop irrelevant columns
- Process JSON columns (genres, companies, etc.)
- Convert datatypes
- Handle missing values
- Remove duplicates
- Filter for released movies

### Stage 3: Data Transformation
- Convert currency to millions USD
- Calculate profit and ROI
- Extract release year and decade
- Categorize by budget and rating
- Add franchise indicators
- Calculate advanced metrics

### Stage 4: Data Validation
- Schema validation
- Null checks
- Range validation
- Duplicate detection
- Completeness checks
- Generate quality score

### Stage 5: Analytics & KPIs
- Calculate 40+ KPIs
- Rank movies by various metrics
- Analyze franchises vs standalone
- Director and genre analysis
- Temporal trends

### Stage 6: Advanced Queries
- Genre-based searches
- Actor-director combinations
- Custom filtering

### Stage 7: Visualization
- Interactive dashboards
- Trend visualizations
- Comparative analysis charts

---

## 📊 Key Performance Indicators

### Financial Metrics
- **Highest/Lowest Revenue** (Top 10)
- **Highest/Lowest Budget** (Top 10)
- **Highest/Lowest Profit** (Top 10)
- **ROI Analysis** (Budget ≥ $10M)

### Rating Metrics
- **Highest Rated** (≥10 votes, Top 10)
- **Lowest Rated** (≥10 votes, Top 10)
- **Most Voted** (Top 10)
- **Most Popular** (Top 10)

### Comparative Analysis
- **Franchise vs Standalone**: Revenue, ROI, Budget, Popularity, Rating
- **Top Franchises**: Movies count, Budget, Revenue, Rating
- **Top Directors**: Movies count, Revenue, Rating
- **Genre Performance**: Revenue, ROI, Rating by genre
- **Budget Category Performance**: Micro, Low, Medium, High, Blockbuster

### Temporal Analysis
- **Yearly Trends**: Revenue, Budget, ROI over time
- **Decade Analysis**: Performance by decade

---

## 💻 Usage

### Common Commands

```bash
# Start services
make up

# Stop services
make down

# View logs
make logs
make logs-spark      # Spark only
make logs-airflow    # Airflow only

# Run pipeline
make run-pipeline

# View results
make view-data
make view-kpis

# Health check
make health-check

# Clean data
make clean-data
make clean           # Clean everything
```

### Accessing Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark Master UI | http://localhost:8080 | - |
| Airflow UI | http://localhost:8088 | admin / admin |
| JupyterLab | http://localhost:8888 | Get token: `make jupyter-token` |
| Grafana | http://localhost:3000 | admin / admin |

### Interactive Development

```bash
# Open Spark shell
make shell-spark

# Open JupyterLab
make jupyter-token
# Then open http://localhost:8888 with the token
```

---

## 📈 Monitoring

### Spark Monitoring
- **Spark Master UI**: http://localhost:8080
  - Active/Completed Applications
  - Workers status
  - Resource utilization

### Airflow Monitoring
- **Airflow UI**: http://localhost:8088
  - DAG execution status
  - Task logs
  - Execution timeline

### Application Logs
```bash
# View real-time logs
make logs

# View pipeline log file
docker-compose exec spark-master cat /opt/spark-data/logs/tmdb_pipeline.log
```

---

## 🛠️ Development

### Running Tests
```bash
make test
```

### Code Quality
```bash
# Linting
make lint

# Formatting
make format

# Validate config
make validate-config
```

### Adding New Features

1. **Create new module** in appropriate `src/` directory
2. **Add tests** in `tests/` directory
3. **Update configuration** in `config.yaml`
4. **Update pipeline** in `main.py`
5. **Run tests** with `make test`

---

## 🐛 Troubleshooting

### Issue: "TMDB_API_KEY not set"
```bash
# Check .env file
cat .env | grep TMDB_API_KEY

# Update and restart
nano .env
make restart
```

### Issue: "Connection refused to Spark Master"
```bash
# Check Spark status
make status

# Restart Spark
docker-compose restart spark-master spark-worker-1 spark-worker-2
```

### Issue: "Out of Memory"
```bash
# Adjust memory in docker-compose.yml
# Increase SPARK_WORKER_MEMORY and SPARK_DRIVER_MEMORY
# Then rebuild: make build && make up
```

### Issue: "Data not found"
```bash
# Check output directory
make view-data

# Re-run pipeline
make clean-data
make run-pipeline
```

For more troubleshooting, see [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md)

---

## 📝 Configuration

### config.yaml
Main configuration file for:
- API settings (rate limiting, caching)
- Spark configuration
- Movie IDs to fetch
- Processing parameters
- KPI definitions
- Output settings

### Environment Variables (.env)
- `TMDB_API_KEY`: Your TMDB API key
- `AIRFLOW_UID`: Airflow user ID
- `POSTGRES_*`: Database credentials

---

## 🎯 Future Enhancements

- [ ] Real-time streaming data ingestion
- [ ] Machine learning for movie success prediction
- [ ] Recommendation system
- [ ] Cloud deployment (AWS/GCP/Azure)
- [ ] Enhanced visualizations with Tableau/PowerBI
- [ ] Sentiment analysis on movie reviews
- [ ] Integration with additional APIs (OMDB, IMDB)

---

## 📚 Documentation

- [Implementation Guide](IMPLEMENTATION_GUIDE.md) - Detailed setup instructions
- [API Documentation](docs/api_documentation.md) - TMDB API reference
- [Architecture](docs/architecture.md) - System design details

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **TMDB** for providing the movie database API
- **Apache Spark** community for the excellent framework
- **Apache Airflow** for workflow orchestration
- **Docker** for containerization

---

## 📧 Contact

For questions or support, please open an issue on GitHub.

---

**⭐ If you find this project helpful, please consider giving it a star!**