"""
Airflow DAG for TMDB Movie Analysis Pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Define the DAG
dag = DAG(
    'tmdb_movie_analysis_pipeline',
    default_args=default_args,
    description='Complete TMDB movie data analysis pipeline with PySpark',
    schedule_interval='@weekly',  # Run weekly
    start_date=days_ago(1),
    catchup=False,
    tags=['tmdb', 'movies', 'analytics', 'pyspark'],
)

# Task 1: Validate Environment
validate_environment = BashOperator(
    task_id='validate_environment',
    bash_command="""
    echo "Validating environment..."
    
    # Check if TMDB_API_KEY is set
    if [ -z "$TMDB_API_KEY" ]; then
        echo "ERROR: TMDB_API_KEY not set"
        exit 1
    fi
    
    # Check if Spark Master is accessible
    curl -s http://spark-master:8080 > /dev/null
    if [ $? -ne 0 ]; then
        echo "ERROR: Cannot reach Spark Master"
        exit 1
    fi
    
    # Check output directory
    mkdir -p /opt/airflow/data/output/raw
    mkdir -p /opt/airflow/data/output/cleaned
    mkdir -p /opt/airflow/data/output/processed
    mkdir -p /opt/airflow/data/output/kpis
    mkdir -p /opt/airflow/data/output/queries
    mkdir -p /opt/airflow/data/output/dashboards
    mkdir -p /opt/airflow/data/output/reports
    
    echo "✓ Environment validation complete"
    """,
    dag=dag,
)

# Task 2: Run Data Ingestion
run_ingestion = SparkSubmitOperator(
    task_id='run_ingestion',
    application='/opt/airflow/src/ingestion/data_fetcher.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
        'spark.executor.cores': '2',
    },
    application_args=['--step', 'ingestion'],
    dag=dag,
)

# Task 3: Run Data Cleaning
run_cleaning = SparkSubmitOperator(
    task_id='run_cleaning',
    application='/opt/airflow/src/processing/data_cleaner.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
    },
    application_args=['--step', 'cleaning'],
    dag=dag,
)

# Task 4: Run Data Transformation
run_transformation = SparkSubmitOperator(
    task_id='run_transformation',
    application='/opt/airflow/src/processing/data_transformer.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
    },
    application_args=['--step', 'transformation'],
    dag=dag,
)

# Task 5: Run Data Validation
def run_validation_task(**context):
    """Run data validation"""
    import sys
    sys.path.append('/opt/airflow/src')
    
    from pyspark.sql import SparkSession
    from processing.data_validator import DataValidator
    from config import load_config
    import json
    
    spark = SparkSession.builder \
        .appName("TMDB Validation") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    config = load_config('/opt/airflow/src/config/config.yaml')
    validator = DataValidator(config)
    
    # Load processed data
    df = spark.read.parquet('/opt/airflow/data/output/processed/movies.parquet')
    
    # Validate
    results = validator.validate(df)
    
    # Save report
    with open('/opt/airflow/data/output/reports/validation_report.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Check if quality score meets threshold
    quality_score = results.get('overall_score', 0)
    if quality_score < 0.8:
        raise ValueError(f"Data quality score {quality_score} below threshold 0.8")
    
    print(f"✓ Validation complete. Quality score: {quality_score:.2f}")
    
    spark.stop()
    return results

run_validation = PythonOperator(
    task_id='run_validation',
    python_callable=run_validation_task,
    provide_context=True,
    dag=dag,
)

# Task 6: Calculate KPIs
def calculate_kpis_task(**context):
    """Calculate all KPIs"""
    import sys
    sys.path.append('/opt/airflow/src')
    
    from pyspark.sql import SparkSession
    from analytics.kpi_calculator import KPICalculator
    from config import load_config
    import json
    
    spark = SparkSession.builder \
        .appName("TMDB KPI Calculation") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    config = load_config('/opt/airflow/src/config/config.yaml')
    calculator = KPICalculator(config)
    
    # Load processed data
    df = spark.read.parquet('/opt/airflow/data/output/processed/movies.parquet')
    
    # Calculate KPIs
    kpis = calculator.calculate_all_kpis(df)
    
    # Save KPI results
    kpi_output_path = '/opt/airflow/data/output/kpis'
    
    # Save rankings
    for kpi_name, kpi_df in kpis['rankings'].items():
        kpi_df.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{kpi_output_path}/rankings/{kpi_name}.csv")
    
    # Save other KPIs
    for kpi_name in ['franchise_comparison', 'top_franchises', 'top_directors', 
                     'genre_performance', 'yearly_trends', 'decade_trends',
                     'language_performance', 'budget_category_performance']:
        if kpi_name in kpis:
            kpis[kpi_name].coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{kpi_output_path}/{kpi_name}.csv")
    
    # Save summary statistics
    with open(f"{kpi_output_path}/summary_statistics.json", 'w') as f:
        json.dump(kpis['summary_statistics'], f, indent=2, default=str)
    
    print("✓ All KPIs calculated and saved")
    
    spark.stop()
    return kpis['summary_statistics']

calculate_kpis = PythonOperator(
    task_id='calculate_kpis',
    python_callable=calculate_kpis_task,
    provide_context=True,
    dag=dag,
)

# Task 7: Run Advanced Queries
def run_queries_task(**context):
    """Execute advanced queries"""
    import sys
    sys.path.append('/opt/airflow/src')
    
    from pyspark.sql import SparkSession
    from analytics.advanced_queries import AdvancedQueries
    from config import load_config
    
    spark = SparkSession.builder \
        .appName("TMDB Advanced Queries") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    config = load_config('/opt/airflow/src/config/config.yaml')
    queries = AdvancedQueries(config)
    
    # Load processed data
    df = spark.read.parquet('/opt/airflow/data/output/processed/movies.parquet')
    
    # Execute queries
    queries_output_path = '/opt/airflow/data/output/queries'
    
    # Search 1
    search_1 = queries.execute_search_1(df)
    search_1.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{queries_output_path}/search_1_scifi_action_bruce_willis.csv")
    
    # Search 2
    search_2 = queries.execute_search_2(df)
    search_2.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{queries_output_path}/search_2_uma_tarantino.csv")
    
    print("✓ Advanced queries executed")
    
    spark.stop()
    return {'search_1_count': search_1.count(), 'search_2_count': search_2.count()}

run_queries = PythonOperator(
    task_id='run_advanced_queries',
    python_callable=run_queries_task,
    provide_context=True,
    dag=dag,
)

# Task 8: Generate Report
generate_report = BashOperator(
    task_id='generate_report',
    bash_command="""
    echo "Generating pipeline execution report..."
    
    # Create report
    cat > /opt/airflow/data/output/reports/pipeline_report.txt << EOF
================================================================================
TMDB MOVIE ANALYSIS PIPELINE - EXECUTION REPORT
================================================================================
Execution Date: $(date)
Status: SUCCESS

Pipeline Stages:
1. ✓ Data Ingestion
2. ✓ Data Cleaning
3. ✓ Data Transformation
4. ✓ Data Validation
5. ✓ KPI Calculation
6. ✓ Advanced Queries
7. ✓ Report Generation

Output Locations:
- Raw Data:         /opt/airflow/data/output/raw/
- Cleaned Data:     /opt/airflow/data/output/cleaned/
- Processed Data:   /opt/airflow/data/output/processed/
- KPI Results:      /opt/airflow/data/output/kpis/
- Query Results:    /opt/airflow/data/output/queries/
- Reports:          /opt/airflow/data/output/reports/

================================================================================
EOF
    
    cat /opt/airflow/data/output/reports/pipeline_report.txt
    echo "✓ Report generated"
    """,
    dag=dag,
)

# Task 9: Send Success Notification
send_notification = BashOperator(
    task_id='send_notification',
    bash_command="""
    echo "TMDB Pipeline completed successfully at $(date)"
    echo "All outputs available in /opt/airflow/data/output/"
    """,
    dag=dag,
)

# Define task dependencies
validate_environment >> run_ingestion >> run_cleaning >> run_transformation
run_transformation >> run_validation
run_validation >> [calculate_kpis, run_queries]
[calculate_kpis, run_queries] >> generate_report >> send_notification