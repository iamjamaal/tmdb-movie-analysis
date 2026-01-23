"""
Metrics Aggregator - Aggregate and summarize analytics metrics
"""

import logging
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class MetricsAggregator:
    """Aggregates various metrics for analysis and reporting"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    def aggregate_temporal_metrics(self, df: DataFrame) -> DataFrame:
        """Aggregate metrics by time periods"""
        logger.info("Aggregating temporal metrics")
        
        # Extract year and month from release_date
        df_temporal = df.withColumn('year', F.year('release_date')) \
                       .withColumn('month', F.month('release_date')) \
                       .withColumn('quarter', F.quarter('release_date'))
        
        # Yearly aggregations
        yearly_metrics = df_temporal.groupBy('year').agg(
            F.count('*').alias('movie_count'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.sum('budget_musd').alias('total_budget'),
            F.avg('budget_musd').alias('avg_budget'),
            F.avg('vote_average').alias('avg_rating'),
            F.avg('popularity').alias('avg_popularity'),
            F.avg('runtime').alias('avg_runtime')
        ).orderBy('year')
        
        return yearly_metrics
    
    def aggregate_genre_metrics(self, df: DataFrame) -> DataFrame:
        """Aggregate metrics by genre"""
        logger.info("Aggregating genre metrics")
        
        # Explode genres to handle multiple genres per movie
        df_genres = df.withColumn('genre', F.explode(F.split('genres', '\\|')))
        
        genre_metrics = df_genres.groupBy('genre').agg(
            F.count('*').alias('movie_count'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('budget_musd').alias('avg_budget'),
            F.avg('vote_average').alias('avg_rating'),
            F.avg('popularity').alias('avg_popularity'),
            F.sum(F.when(F.col('revenue_musd') > F.col('budget_musd'), 1).otherwise(0)).alias('profitable_count')
        ).withColumn(
            'profitability_rate',
            F.round((F.col('profitable_count') / F.col('movie_count') * 100), 2)
        ).orderBy(F.desc('total_revenue'))
        
        return genre_metrics
    
    def aggregate_language_metrics(self, df: DataFrame) -> DataFrame:
        """Aggregate metrics by original language"""
        logger.info("Aggregating language metrics")
        
        language_metrics = df.groupBy('original_language').agg(
            F.count('*').alias('movie_count'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('vote_average').alias('avg_rating'),
            F.avg('popularity').alias('avg_popularity')
        ).filter(
            F.col('movie_count') >= 5  # Only languages with at least 5 movies
        ).orderBy(F.desc('total_revenue'))
        
        return language_metrics
    
    def aggregate_production_company_metrics(self, df: DataFrame) -> DataFrame:
        """Aggregate metrics by production company"""
        logger.info("Aggregating production company metrics")
        
        # Explode production companies
        df_companies = df.withColumn(
            'company', 
            F.explode(F.split('production_companies', '\\|'))
        )
        
        company_metrics = df_companies.groupBy('company').agg(
            F.count('*').alias('movie_count'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('vote_average').alias('avg_rating'),
            F.max('revenue_musd').alias('max_revenue'),
            F.min('release_date').alias('first_release'),
            F.max('release_date').alias('latest_release')
        ).filter(
            F.col('movie_count') >= 3
        ).orderBy(F.desc('total_revenue'))
        
        return company_metrics
    
    def aggregate_director_metrics(self, df: DataFrame) -> DataFrame:
        """Aggregate metrics by director"""
        logger.info("Aggregating director metrics")
        
        director_metrics = df.filter(
            F.col('director').isNotNull()
        ).groupBy('director').agg(
            F.count('*').alias('movie_count'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('vote_average').alias('avg_rating'),
            F.avg('popularity').alias('avg_popularity'),
            F.collect_list('title').alias('movies')
        ).filter(
            F.col('movie_count') >= 2
        ).orderBy(F.desc('total_revenue'))
        
        return director_metrics
    
    def aggregate_franchise_metrics(self, df: DataFrame) -> DataFrame:
        """Aggregate metrics by franchise/collection"""
        logger.info("Aggregating franchise metrics")
        
        franchise_metrics = df.filter(
            F.col('belongs_to_collection').isNotNull()
        ).groupBy('belongs_to_collection').agg(
            F.count('*').alias('movie_count'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.sum('budget_musd').alias('total_budget'),
            F.avg('budget_musd').alias('avg_budget'),
            F.avg('vote_average').alias('avg_rating'),
            F.min('release_date').alias('first_release'),
            F.max('release_date').alias('latest_release'),
            F.collect_list('title').alias('movies')
        ).withColumn(
            'total_profit',
            F.col('total_revenue') - F.col('total_budget')
        ).withColumn(
            'avg_roi',
            F.round(F.col('total_revenue') / F.col('total_budget'), 2)
        ).orderBy(F.desc('total_revenue'))
        
        return franchise_metrics
    
    def aggregate_budget_tier_metrics(self, df: DataFrame) -> DataFrame:
        """Aggregate metrics by budget tiers"""
        logger.info("Aggregating budget tier metrics")
        
        # Define budget tiers
        df_tiers = df.withColumn(
            'budget_tier',
            F.when(F.col('budget_musd') < 10, 'Low (<$10M)')
             .when(F.col('budget_musd') < 50, 'Medium ($10M-$50M)')
             .when(F.col('budget_musd') < 100, 'High ($50M-$100M)')
             .otherwise('Blockbuster (>$100M)')
        )
        
        tier_metrics = df_tiers.groupBy('budget_tier').agg(
            F.count('*').alias('movie_count'),
            F.avg('budget_musd').alias('avg_budget'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('vote_average').alias('avg_rating'),
            F.avg('popularity').alias('avg_popularity'),
            F.avg((F.col('revenue_musd') - F.col('budget_musd')) / F.col('budget_musd') * 100).alias('avg_roi_pct')
        ).orderBy('budget_tier')
        
        return tier_metrics
    
    def aggregate_rating_tier_metrics(self, df: DataFrame) -> DataFrame:
        """Aggregate metrics by rating tiers"""
        logger.info("Aggregating rating tier metrics")
        
        df_rating_tiers = df.withColumn(
            'rating_tier',
            F.when(F.col('vote_average') < 5, 'Poor (<5.0)')
             .when(F.col('vote_average') < 7, 'Average (5.0-7.0)')
             .when(F.col('vote_average') < 8, 'Good (7.0-8.0)')
             .otherwise('Excellent (>8.0)')
        )
        
        rating_tier_metrics = df_rating_tiers.groupBy('rating_tier').agg(
            F.count('*').alias('movie_count'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('budget_musd').alias('avg_budget'),
            F.avg('popularity').alias('avg_popularity'),
            F.avg('vote_count').alias('avg_vote_count')
        ).orderBy('rating_tier')
        
        return rating_tier_metrics
    
    def calculate_correlation_metrics(self, df: DataFrame) -> Dict[str, float]:
        """Calculate correlation between key metrics"""
        logger.info("Calculating correlation metrics")
        
        numeric_cols = ['budget_musd', 'revenue_musd', 'vote_average', 
                       'vote_count', 'popularity', 'runtime']
        
        # Filter to rows with all numeric values present
        df_numeric = df.select(numeric_cols).filter(
            F.col('budget_musd').isNotNull() &
            F.col('revenue_musd').isNotNull()
        )
        
        correlations = {}
        
        # Calculate key correlations
        correlations['budget_revenue'] = df_numeric.stat.corr('budget_musd', 'revenue_musd')
        correlations['budget_rating'] = df_numeric.stat.corr('budget_musd', 'vote_average')
        correlations['revenue_rating'] = df_numeric.stat.corr('revenue_musd', 'vote_average')
        correlations['popularity_revenue'] = df_numeric.stat.corr('popularity', 'revenue_musd')
        correlations['votes_rating'] = df_numeric.stat.corr('vote_count', 'vote_average')
        
        logger.info(f"Correlation metrics: {correlations}")
        return correlations
    
    def generate_summary_statistics(self, df: DataFrame) -> Dict[str, Any]:
        """Generate comprehensive summary statistics"""
        logger.info("Generating summary statistics")
        
        stats = df.select(
            F.count('*').alias('total_movies'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.min('revenue_musd').alias('min_revenue'),
            F.max('revenue_musd').alias('max_revenue'),
            F.avg('budget_musd').alias('avg_budget'),
            F.avg('vote_average').alias('avg_rating'),
            F.avg('popularity').alias('avg_popularity'),
            F.avg('runtime').alias('avg_runtime'),
            F.countDistinct('director').alias('unique_directors'),
            F.countDistinct('original_language').alias('unique_languages')
        ).collect()[0]
        
        summary = {
            'overview': {
                'total_movies': stats['total_movies'],
                'unique_directors': stats['unique_directors'],
                'unique_languages': stats['unique_languages']
            },
            'financial': {
                'total_revenue_musd': round(stats['total_revenue'], 2),
                'avg_revenue_musd': round(stats['avg_revenue'], 2),
                'min_revenue_musd': round(stats['min_revenue'], 2),
                'max_revenue_musd': round(stats['max_revenue'], 2),
                'avg_budget_musd': round(stats['avg_budget'], 2)
            },
            'quality': {
                'avg_rating': round(stats['avg_rating'], 2),
                'avg_popularity': round(stats['avg_popularity'], 2),
                'avg_runtime_min': round(stats['avg_runtime'], 2)
            }
        }
        
        logger.info(f"Summary statistics: {summary}")
        return summary
    
    def export_all_metrics(
        self, 
        df: DataFrame, 
        output_path: str
    ) -> Dict[str, str]:
        """Export all aggregated metrics to parquet files"""
        logger.info(f"Exporting all metrics to {output_path}")
        
        export_paths = {}
        
        # Export each metric type
        metrics_map = {
            'temporal': self.aggregate_temporal_metrics,
            'genre': self.aggregate_genre_metrics,
            'language': self.aggregate_language_metrics,
            'production_company': self.aggregate_production_company_metrics,
            'director': self.aggregate_director_metrics,
            'franchise': self.aggregate_franchise_metrics,
            'budget_tier': self.aggregate_budget_tier_metrics,
            'rating_tier': self.aggregate_rating_tier_metrics
        }
        
        for metric_name, metric_func in metrics_map.items():
            try:
                metric_df = metric_func(df)
                path = f"{output_path}/metrics_{metric_name}"
                metric_df.write.mode('overwrite').parquet(path)
                export_paths[metric_name] = path
                logger.info(f"Exported {metric_name} metrics to {path}")
            except Exception as e:
                logger.error(f"Failed to export {metric_name} metrics: {str(e)}")
        
        return export_paths