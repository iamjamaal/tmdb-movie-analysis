"""
Airflow DAG for TMDB Movie Analysis Pipeline
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-engineering@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

# DAG definition
dag = DAG(
    'tmdb_movie_pipeline',
    default_args=default_args,
    description='TMDB Movie Data Analysis Pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['tmdb', 'movies', 'analytics', 'spark']
)


# ============================================================================
# Task Functions
# ============================================================================

def validate_environment(**context):
    """Validate environment and prerequisites"""
    import logging
    logger = logging.getLogger(__name__)
    
    # Check required environment variables
    required_vars = ['TMDB_API_KEY', 'POSTGRES_HOST', 'REDIS_HOST']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    logger.info("Environment validation passed")
    
    # Push run metadata to XCom
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    context['task_instance'].xcom_push(key='run_id', value=run_id)
    
    return run_id


def fetch_movie_data(**context):
    """Fetch movie data from TMDB API"""
    from src.ingestion.data_fetcher import MovieDataFetcher
    from src.utils.helpers import load_config
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("Starting movie data fetch")
    
    # Load configuration
    config = load_config('/opt/spark-apps/src/config/config.yaml')
    
    # Get run_id from XCom
    run_id = context['task_instance'].xcom_pull(
        task_ids='validate_environment',
        key='run_id'
    )
    
    # Fetch data
    fetcher = MovieDataFetcher(config)
    movie_ids = config.get('pipeline', {}).get('movie_ids', [])
    
    if not movie_ids:
        raise ValueError("No movie IDs configured")
    
    raw_df = fetcher.fetch_movies(movie_ids)
    
    # Save raw data
    output_path = f"/opt/spark-data/raw/movies_{run_id}"
    raw_df.write.mode('overwrite').parquet(output_path)
    
    # Push metadata to XCom
    context['task_instance'].xcom_push(key='raw_data_path', value=output_path)
    context['task_instance'].xcom_push(key='raw_record_count', value=raw_df.count())
    
    logger.info(f"Fetched {raw_df.count()} movies, saved to {output_path}")
    
    return output_path


def clean_data(**context):
    """Clean and preprocess movie data"""
    from src.processing.data_cleaner import DataCleaner
    from src.utils.spark_session import SparkSessionManager
    from src.utils.helpers import load_config
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("Starting data cleaning")
    
    # Get input path from XCom
    raw_data_path = context['task_instance'].xcom_pull(
        task_ids='fetch_movie_data',
        key='raw_data_path'
    )
    
    run_id = context['task_instance'].xcom_pull(
        task_ids='validate_environment',
        key='run_id'
    )
    
    # Initialize Spark and cleaner
    config = load_config('/opt/spark-apps/src/config/config.yaml')
    spark = SparkSessionManager.get_or_create_session('data-cleaning', config)
    
    # Read raw data
    raw_df = spark.read.parquet(raw_data_path)
    
    # Clean data
    cleaner = DataCleaner(config)
    clean_df = cleaner.clean_data(raw_df)
    
    # Save cleaned data
    output_path = f"/opt/spark-data/processed/movies_clean_{run_id}"
    clean_df.write.mode('overwrite').parquet(output_path)
    
    # Push metadata
    context['task_instance'].xcom_push(key='clean_data_path', value=output_path)
    context['task_instance'].xcom_push(key='clean_record_count', value=clean_df.count())
    
    logger.info(f"Cleaned {clean_df.count()} movies, saved to {output_path}")
    
    return output_path


def transform_data(**context):
    """Transform and enrich data"""
    from src.processing.data_transformer import DataTransformer
    from src.utils.spark_session import SparkSessionManager
    from src.utils.helpers import load_config
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("Starting data transformation")
    
    # Get input path
    clean_data_path = context['task_instance'].xcom_pull(
        task_ids='clean_data',
        key='clean_data_path'
    )
    
    run_id = context['task_instance'].xcom_pull(
        task_ids='validate_environment',
        key='run_id'
    )
    
    # Initialize
    config = load_config('/opt/spark-apps/src/config/config.yaml')
    spark = SparkSessionManager.get_or_create_session('data-transformation', config)
    
    # Read and transform
    clean_df = spark.read.parquet(clean_data_path)
    transformer = DataTransformer(config)
    transformed_df = transformer.transform_data(clean_df)
    
    # Save
    output_path = f"/opt/spark-data/processed/movies_transformed_{run_id}"
    transformed_df.write.mode('overwrite').parquet(output_path)
    
    # Push metadata
    context['task_instance'].xcom_push(key='transformed_data_path', value=output_path)
    
    logger.info(f"Transformed data saved to {output_path}")
    
    return output_path


def validate_data(**context):
    """Validate data quality"""
    from src.processing.data_validator import DataValidator
    from src.utils.spark_session import SparkSessionManager
    from src.utils.helpers import load_config
    import logging
    import json
    
    logger = logging.getLogger(__name__)
    logger.info("Starting data validation")
    
    # Get input path
    transformed_data_path = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data_path'
    )
    
    run_id = context['task_instance'].xcom_pull(
        task_ids='validate_environment',
        key='run_id'
    )
    
    # Initialize
    config = load_config('/opt/spark-apps/src/config/config.yaml')
    spark = SparkSessionManager.get_or_create_session('data-validation', config)
    
    # Read and validate
    df = spark.read.parquet(transformed_data_path)
    validator = DataValidator(config)
    validation_report = validator.generate_validation_report(df)
    
    # Save validation report
    report_path = f"/opt/spark-data/output/validation_report_{run_id}.json"
    with open(report_path, 'w') as f:
        json.dump(validation_report, f, indent=2, default=str)
    
    # Check if validation passed
    health_score = validation_report.get('health_score', 0)
    min_score = config.get('validation', {}).get('min_health_score', 80)
    
    if health_score < min_score:
        logger.warning(f"Data quality score ({health_score}%) below threshold ({min_score}%)")
        # Don't fail, but log warning
    
    # Push metadata
    context['task_instance'].xcom_push(key='validation_report_path', value=report_path)
    context['task_instance'].xcom_push(key='health_score', value=health_score)
    
    logger.info(f"Validation completed. Health score: {health_score}%")
    
    return validation_report


def calculate_kpis(**context):
    """Calculate KPIs and metrics"""
    from src.analytics.kpi_calculator import KPICalculator
    from src.utils.spark_session import SparkSessionManager
    from src.utils.helpers import load_config
    import logging
    import json
    
    logger = logging.getLogger(__name__)
    logger.info("Starting KPI calculation")
    
    # Get input path
    transformed_data_path = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data_path'
    )
    
    run_id = context['task_instance'].xcom_pull(
        task_ids='validate_environment',
        key='run_id'
    )
    
    # Initialize
    config = load_config('/opt/spark-apps/src/config/config.yaml')
    spark = SparkSessionManager.get_or_create_session('kpi-calculation', config)
    
    # Calculate KPIs
    df = spark.read.parquet(transformed_data_path)
    calculator = KPICalculator(config)
    all_kpis = calculator.calculate_all_kpis(df)
    
    # Save KPIs
    kpi_output_dir = f"/opt/spark-data/output/kpis_{run_id}"
    calculator.save_kpis(all_kpis, kpi_output_dir)
    
    # Save summary
    summary_path = f"{kpi_output_dir}/summary.json"
    with open(summary_path, 'w') as f:
        json.dump({k: len(v) for k, v in all_kpis.items()}, f, indent=2)
    
    # Push metadata
    context['task_instance'].xcom_push(key='kpi_output_dir', value=kpi_output_dir)
    
    logger.info(f"KPIs calculated and saved to {kpi_output_dir}")
    
    return kpi_output_dir


def aggregate_metrics(**context):
    """Aggregate metrics for reporting"""
    from src.analytics.metrics_aggregator import MetricsAggregator
    from src.utils.spark_session import SparkSessionManager
    from src.utils.helpers import load_config
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("Starting metrics aggregation")
    
    # Get input path
    transformed_data_path = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data_path'
    )
    
    run_id = context['task_instance'].xcom_pull(
        task_ids='validate_environment',
        key='run_id'
    )
    
    # Initialize
    config = load_config('/opt/spark-apps/src/config/config.yaml')
    spark = SparkSessionManager.get_or_create_session('metrics-aggregation', config)
    
    # Aggregate metrics
    df = spark.read.parquet(transformed_data_path)
    aggregator = MetricsAggregator(config)
    
    metrics_output_dir = f"/opt/spark-data/output/metrics_{run_id}"
    export_paths = aggregator.export_all_metrics(df, metrics_output_dir)
    
    # Push metadata
    context['task_instance'].xcom_push(key='metrics_output_dir', value=metrics_output_dir)
    
    logger.info(f"Metrics aggregated and saved to {metrics_output_dir}")
    
    return export_paths


def generate_visualizations(**context):
    """Generate visualizations"""
    from src.visualization.dashboard_generator import DashboardGenerator
    from src.utils.spark_session import SparkSessionManager
    from src.utils.helpers import load_config
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("Starting visualization generation")
    
    # Get input path
    transformed_data_path = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data_path'
    )
    
    run_id = context['task_instance'].xcom_pull(
        task_ids='validate_environment',
        key='run_id'
    )
    
    # Initialize
    config = load_config('/opt/spark-apps/src/config/config.yaml')
    spark = SparkSessionManager.get_or_create_session('visualization', config)
    
    # Generate visualizations
    df = spark.read.parquet(transformed_data_path)
    generator = DashboardGenerator(config)
    
    visualizations = generator.generate_all_visualizations(df)
    
    # Push metadata
    context['task_instance'].xcom_push(key='visualizations', value=visualizations)
    
    logger.info(f"Generated {len(visualizations)} visualizations")
    
    return visualizations


def publish_results(**context):
    """Publish results and notify stakeholders"""
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("Publishing results")
    
    # Get all metadata
    run_id = context['task_instance'].xcom_pull(
        task_ids='validate_environment',
        key='run_id'
    )
    
    health_score = context['task_instance'].xcom_pull(
        task_ids='validate_data',
        key='health_score'
    )
    
    raw_count = context['task_instance'].xcom_pull(
        task_ids='fetch_movie_data',
        key='raw_record_count'
    )
    
    clean_count = context['task_instance'].xcom_pull(
        task_ids='clean_data',
        key='clean_record_count'
    )
    
    # Create summary
    summary = {
        'run_id': run_id,
        'execution_date': context['execution_date'].isoformat(),
        'raw_records': raw_count,
        'clean_records': clean_count,
        'data_quality_score': health_score,
        'status': 'SUCCESS'
    }
    
    logger.info(f"Pipeline execution summary: {summary}")
    
    # Save summary
    summary_path = f"/opt/spark-data/output/pipeline_summary_{run_id}.json"
    import json
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    return summary


# ============================================================================
# Task Definitions
# ============================================================================

# Validation task
validate_env_task = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_environment,
    dag=dag
)

# Data ingestion task group
with TaskGroup('data_ingestion', dag=dag) as ingestion_group:
    fetch_task = PythonOperator(
        task_id='fetch_movie_data',
        python_callable=fetch_movie_data
    )

# Data processing task group
with TaskGroup('data_processing', dag=dag) as processing_group:
    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )
    
    clean_task >> transform_task >> validate_task

# Analytics task group
with TaskGroup('analytics', dag=dag) as analytics_group:
    kpi_task = PythonOperator(
        task_id='calculate_kpis',
        python_callable=calculate_kpis
    )
    
    metrics_task = PythonOperator(
        task_id='aggregate_metrics',
        python_callable=aggregate_metrics
    )
    
    [kpi_task, metrics_task]

# Visualization task
viz_task = PythonOperator(
    task_id='generate_visualizations',
    python_callable=generate_visualizations,
    dag=dag
)

# Publishing task
publish_task = PythonOperator(
    task_id='publish_results',
    python_callable=publish_results,
    dag=dag
)

# Cleanup task
cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='echo "Cleaning up temporary files..." && find /opt/spark-data/raw -mtime +7 -delete',
    dag=dag
)

# ============================================================================
# Task Dependencies
# ============================================================================

validate_env_task >> ingestion_group >> processing_group >> analytics_group >> viz_task >> publish_task >> cleanup_task