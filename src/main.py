"""
Main Pipeline Orchestrator for TMDB Movie Analysis
"""

import os
import sys
import logging
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import SparkSession

# Add src to path
sys.path.append(str(Path(__file__).parent))

from config.logging_config import setup_logging
from ingestion.data_fetcher import DataFetcher
from processing.data_cleaner import DataCleaner
from processing.data_transformer import DataTransformer
from processing.data_validator import DataValidator
from analytics.kpi_calculator import KPICalculator
from analytics.advanced_queries import AdvancedQueries
from utils.spark_session import create_spark_session
from visualization.dashboard_generator import DashboardGenerator


logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_path: Path to config file
        
    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


class TMDBPipeline:
    """
    Main pipeline orchestrator
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize pipeline
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        setup_logging(self.config)
        
        self.spark = create_spark_session(self.config)
        self.output_path = self.config.get('output', {}).get('base_path', '/opt/spark-data/output')
        
        # Initialize components
        self.fetcher = DataFetcher(self.spark, self.config)
        self.cleaner = DataCleaner(self.config)
        self.transformer = DataTransformer(self.config)
        self.validator = DataValidator(self.config)
        self.kpi_calculator = KPICalculator(self.config)
        self.advanced_queries = AdvancedQueries(self.config)
        self.dashboard_generator = DashboardGenerator(self.config)
        
        logger.info("TMDBPipeline initialized successfully")
    
    def run_ingestion(self):
        """
        Step 1: Data Ingestion
        Fetch data from TMDB API
        """
        logger.info("=" * 80)
        logger.info("STEP 1: DATA INGESTION")
        logger.info("=" * 80)
        
        df_raw = self.fetcher.fetch_movies()
        
        # Save raw data
        self.fetcher.save_raw_data(df_raw, self.output_path)
        
        logger.info(f"✓ Ingestion complete. Records: {df_raw.count()}")
        return df_raw
    
    def run_cleaning(self, df_raw):
        """
        Step 2: Data Cleaning
        Clean and preprocess raw data
        """
        logger.info("=" * 80)
        logger.info("STEP 2: DATA CLEANING")
        logger.info("=" * 80)
        
        df_clean = self.cleaner.clean(df_raw)
        
        # Save cleaned data
        df_clean.write.mode("overwrite").parquet(f"{self.output_path}/cleaned/movies.parquet")
        
        logger.info(f"✓ Cleaning complete. Records: {df_clean.count()}")
        return df_clean
    
    def run_transformation(self, df_clean):
        """
        Step 3: Data Transformation
        Apply feature engineering and transformations
        """
        logger.info("=" * 80)
        logger.info("STEP 3: DATA TRANSFORMATION")
        logger.info("=" * 80)
        
        df_transformed = self.transformer.transform(df_clean)
        
        # Save transformed data
        df_transformed.write.mode("overwrite").parquet(f"{self.output_path}/processed/movies.parquet")
        
        # Also save as CSV for easy access
        df_transformed.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{self.output_path}/processed/movies.csv")
        
        logger.info(f"✓ Transformation complete. Records: {df_transformed.count()}, Columns: {len(df_transformed.columns)}")
        return df_transformed
    
    def run_validation(self, df_transformed):
        """
        Step 4: Data Validation
        Validate data quality
        """
        logger.info("=" * 80)
        logger.info("STEP 4: DATA VALIDATION")
        logger.info("=" * 80)
        
        validation_results = self.validator.validate(df_transformed)
        
        # Save validation report
        report_path = f"{self.output_path}/reports/validation_report.json"
        self.validator.save_validation_report(validation_results, report_path)
        
        logger.info(f"✓ Validation complete. Quality score: {validation_results.get('overall_score', 0):.2f}")
        return validation_results
    
    def run_analytics(self, df_transformed):
        """
        Step 5: Analytics & KPI Calculation
        Calculate all KPIs and perform analysis
        """
        logger.info("=" * 80)
        logger.info("STEP 5: ANALYTICS & KPI CALCULATION")
        logger.info("=" * 80)
        
        # Calculate KPIs
        kpis = self.kpi_calculator.calculate_all_kpis(df_transformed)
        
        # Save KPI results
        kpi_output_path = f"{self.output_path}/kpis"
        
        for kpi_name, kpi_df in kpis['rankings'].items():
            kpi_df.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{kpi_output_path}/rankings/{kpi_name}.csv")
            logger.info(f"  ✓ Saved ranking: {kpi_name}")
        
        # Save other KPIs
        for kpi_name in ['franchise_comparison', 'top_franchises', 'top_directors', 
                         'genre_performance', 'yearly_trends', 'decade_trends',
                         'language_performance', 'budget_category_performance']:
            if kpi_name in kpis:
                kpis[kpi_name].coalesce(1).write.mode("overwrite") \
                    .option("header", "true") \
                    .csv(f"{kpi_output_path}/{kpi_name}.csv")
                logger.info(f"  ✓ Saved KPI: {kpi_name}")
        
        # Save summary statistics
        import json
        with open(f"{kpi_output_path}/summary_statistics.json", 'w') as f:
            json.dump(kpis['summary_statistics'], f, indent=2, default=str)
        
        logger.info("✓ Analytics complete. All KPIs calculated and saved")
        return kpis
    
    def run_advanced_queries(self, df_transformed):
        """
        Step 6: Advanced Queries
        Execute specific search queries
        """
        logger.info("=" * 80)
        logger.info("STEP 6: ADVANCED QUERIES")
        logger.info("=" * 80)
        
        queries_output_path = f"{self.output_path}/queries"
        
        # Search 1: Sci-Fi Action with Bruce Willis
        search_1 = self.advanced_queries.execute_search_1(df_transformed)
        search_1.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{queries_output_path}/search_1_scifi_action_bruce_willis.csv")
        logger.info(f"  ✓ Search 1: Found {search_1.count()} movies")
        
        # Search 2: Uma Thurman & Quentin Tarantino
        search_2 = self.advanced_queries.execute_search_2(df_transformed)
        search_2.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{queries_output_path}/search_2_uma_tarantino.csv")
        logger.info(f"  ✓ Search 2: Found {search_2.count()} movies")
        
        logger.info("✓ Advanced queries complete")
        return {'search_1': search_1, 'search_2': search_2}
    
    def run_visualization(self, df_transformed, kpis):
        """
        Step 7: Visualization
        Generate dashboards and visualizations
        """
        logger.info("=" * 80)
        logger.info("STEP 7: VISUALIZATION")
        logger.info("=" * 80)
        
        dashboard_path = f"{self.output_path}/dashboards"
        
        # Generate comprehensive dashboard
        self.dashboard_generator.generate_dashboard(
            df_transformed,
            kpis,
            output_path=dashboard_path
        )
        
        logger.info(f"✓ Visualizations generated at {dashboard_path}")
        return dashboard_path
    
    def run(self):
        """
        Execute complete pipeline
        """
        start_time = datetime.now()
        
        logger.info("#" * 80)
        logger.info("# TMDB MOVIE DATA ANALYSIS PIPELINE")
        logger.info(f"# Started at: {start_time}")
        logger.info("#" * 80)
        
        try:
            # Step 1: Ingestion
            df_raw = self.run_ingestion()
            
            # Step 2: Cleaning
            df_clean = self.run_cleaning(df_raw)
            
            # Step 3: Transformation
            df_transformed = self.run_transformation(df_clean)
            
            # Step 4: Validation
            validation_results = self.run_validation(df_transformed)
            
            # Step 5: Analytics
            kpis = self.run_analytics(df_transformed)
            
            # Step 6: Advanced Queries
            query_results = self.run_advanced_queries(df_transformed)
            
            # Step 7: Visualization
            dashboard_path = self.run_visualization(df_transformed, kpis)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("#" * 80)
            logger.info("# PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"# Duration: {duration:.2f} seconds")
            logger.info(f"# Output location: {self.output_path}")
            logger.info("#" * 80)
            
            return {
                'status': 'success',
                'duration': duration,
                'output_path': self.output_path,
                'records_processed': df_transformed.count(),
                'validation_score': validation_results.get('overall_score', 0)
            }
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            return {
                'status': 'failed',
                'error': str(e)
            }
        
        finally:
            # Cleanup
            self.fetcher.close()
            self.spark.stop()
            logger.info("Pipeline resources cleaned up")


def main():
    """Main entry point"""
    config_path = os.getenv('CONFIG_PATH', 'config/config.yaml')
    
    pipeline = TMDBPipeline(config_path)
    result = pipeline.run()
    
    if result['status'] == 'success':
        print(f"\n✓ Pipeline completed successfully in {result['duration']:.2f} seconds")
        print(f"✓ Processed {result['records_processed']} records")
        print(f"✓ Data quality score: {result['validation_score']:.2f}")
        print(f"✓ Output location: {result['output_path']}")
        sys.exit(0)
    else:
        print(f"\n✗ Pipeline failed: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()