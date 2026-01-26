"""
KPI Calculator - Calculate and analyze Key Performance Indicators
"""
import os
import json
import logging
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class KPICalculator:
    """
    Calculates Key Performance Indicators for movie analysis
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize KPICalculator
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.kpi_config = config.get('kpis', {})
        logger.info("KPICalculator initialized")
    
    def rank_movies_by_metric(
        self,
        df: DataFrame,
        metric_name: str,
        column: str,
        ascending: bool = False,
        filter_condition: Optional[str] = None,
        top_n: int = 10
    ) -> DataFrame:
        """
        Generic function to rank movies by any metric
        
        Args:
            df: Input DataFrame
            metric_name: Name of the metric (for logging)
            column: Column to rank by
            ascending: Sort order
            filter_condition: Optional SQL filter condition
            top_n: Number of top movies to return
            
        Returns:
            DataFrame with top N movies
        """
        logger.info(f"Calculating {metric_name}")
        
        # Apply filter if specified
        if filter_condition:
            filtered_df = df.filter(filter_condition)
        else:
            filtered_df = df
        
        # Rank movies
        ranked_df = filtered_df.orderBy(
            F.col(column).asc() if ascending else F.col(column).desc()
        ).limit(top_n)
        
        # Add rank column
        ranked_df = ranked_df.withColumn(
            "rank",
            F.row_number().over(Window.orderBy(F.col(column).asc() if ascending else F.col(column).desc()))
        )
        
        # Select relevant columns (avoid duplicate columns)
        base_columns = ["rank", "id", "title", "release_year"]
        metric_columns = ["budget_musd", "revenue_musd", "vote_average", "vote_count"]
        
        # Add the ranking column if it's not already in the base metrics
        if column not in base_columns and column not in metric_columns:
            columns_to_select = base_columns + [column] + metric_columns
        else:
            columns_to_select = base_columns + metric_columns
        
        result = ranked_df.select(*columns_to_select)
        
        count = result.count()
        logger.info(f"{metric_name}: {count} movies ranked")
        
        return result
    
    def analyze_franchise_performance(self, df: DataFrame) -> DataFrame:
        """
        Compare movie franchises (belongs_to_collection) vs. standalone movies
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with franchise vs standalone metrics
        """
        logger.info("Analyzing franchise vs standalone performance")
        
        # Create flag for franchise membership
        df_with_flag = df.withColumn(
            "is_franchise",
            F.when(F.col("belongs_to_collection").isNotNull(), "Franchise").otherwise("Standalone")
        )
        
        # Calculate metrics
        stats = df_with_flag.groupBy("is_franchise").agg(
            F.mean("revenue_musd").alias("mean_revenue"),
            F.expr("percentile_approx(roi, 0.5)").alias("median_roi"),
            F.mean("budget_musd").alias("mean_budget"),
            F.mean("popularity").alias("mean_popularity"),
            F.mean("vote_average").alias("mean_rating"),
            F.count("id").alias("movie_count")
        )
        
        return stats

    def get_most_successful_franchises(self, df: DataFrame) -> DataFrame:
        """
        Find the Most Successful Movie Franchises
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with franchise statistics
        """
        logger.info("Identifying most successful franchises")
        
        franchise_df = df.filter(F.col("belongs_to_collection").isNotNull())
        
        stats = franchise_df.groupBy("belongs_to_collection").agg(
            F.count("id").alias("movie_count"),
            F.sum("budget_musd").alias("total_budget"),
            F.mean("budget_musd").alias("mean_budget"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.mean("revenue_musd").alias("mean_revenue"),
            F.mean("vote_average").alias("mean_rating")
        ).orderBy(F.col("total_revenue").desc())
        
        return stats

    def get_most_successful_directors(self, df: DataFrame) -> DataFrame:
        """
        Find the Most Successful Directors
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with director statistics
        """
        logger.info("Identifying most successful directors")
        
        # Ensure director column exists and filter nulls
        director_df = df.filter(F.col("director").isNotNull())
        
        stats = director_df.groupBy("director").agg(
            F.count("id").alias("movie_count"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.mean("vote_average").alias("mean_rating")
        ).orderBy(F.col("total_revenue").desc())
        
        return stats

    def run_search_queries(self, df: DataFrame) -> Dict[str, DataFrame]:
        """
        Run specific search queries from requirements
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary of search result DataFrames
        """
        results = {}
        
        # Search 1: Best-rated Science Fiction Action movies starring Bruce Willis
        # Note: This requires complex array searching in PySpark
        # For simplicity in this example, we'll implement the logic assuming flattened structures or UDFs could be used
        # But given Spark's capabilities, we can use array_contains
        
        # We need to check genres array for both "Science Fiction" and "Action"
        # And cast array for "Bruce Willis"
        # However, our current schema has genres as pipe-separated string and cast as array of strings
        
        # Assuming genres and cast are processed
        
        # This is a placeholder for the specific complex query logic
        # In a real implementation, we would ensure the schema supports this querying
        pass
        
        return results

    def get_all_rankings(self, df: DataFrame) -> Dict[str, DataFrame]:
        """
        Calculate all configured ranking KPIs
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary of ranking DataFrames
        """
        logger.info("Calculating all ranking KPIs")
        
        rankings = {}
        
        # 1. Highest Revenue
        rankings["highest_revenue"] = self.rank_movies_by_metric(df, "Highest Revenue", "revenue_musd", ascending=False)
        
        # 2. Highest Budget
        rankings["highest_budget"] = self.rank_movies_by_metric(df, "Highest Budget", "budget_musd", ascending=False)
        
        # 3. Highest Profit
        rankings["highest_profit"] = self.rank_movies_by_metric(df, "Highest Profit", "profit_musd", ascending=False)
        
        # 4. Lowest Profit
        rankings["lowest_profit"] = self.rank_movies_by_metric(df, "Lowest Profit", "profit_musd", ascending=True)
        
        # 5. Highest ROI (Budget >= 10M)
        rankings["highest_roi"] = self.rank_movies_by_metric(
            df, "Highest ROI", "roi", ascending=False, filter_condition="budget_musd >= 10"
        )
        
        # 6. Lowest ROI (Budget >= 10M)
        rankings["lowest_roi"] = self.rank_movies_by_metric(
            df, "Lowest ROI", "roi", ascending=True, filter_condition="budget_musd >= 10"
        )
        
        # 7. Most Voted Movies
        rankings["most_voted"] = self.rank_movies_by_metric(df, "Most Voted", "vote_count", ascending=False)
        
        # 8. Highest Rated Movies (votes >= 10)
        rankings["highest_rated"] = self.rank_movies_by_metric(
            df, "Highest Rated", "vote_average", ascending=False, filter_condition="vote_count >= 10"
        )
        
        # 9. Lowest Rated Movies (votes >= 10)
        rankings["lowest_rated"] = self.rank_movies_by_metric(
            df, "Lowest Rated", "vote_average", ascending=True, filter_condition="vote_count >= 10"
        )
        
        # 10. Most Popular Movies
        rankings["most_popular"] = self.rank_movies_by_metric(df, "Most Popular", "popularity", ascending=False)
        
        return rankings
        
        rankings = {}
        metrics = self.kpi_config.get('rankings', {}).get('metrics', [])
        top_n = self.kpi_config.get('rankings', {}).get('top_n', 10)
        
        for metric in metrics:
            name = metric.get('name')
            column = metric.get('column')
            ascending = metric.get('ascending', False)
            filter_cond = metric.get('filter', None)
            
            rankings[name] = self.rank_movies_by_metric(
                df=df,
                metric_name=name,
                column=column,
                ascending=ascending,
                filter_condition=filter_cond,
                top_n=top_n
            )
        
        logger.info(f"Calculated {len(rankings)} ranking KPIs")
        return rankings
    
    def analyze_franchise_vs_standalone(self, df: DataFrame) -> DataFrame:
        """
        Compare franchise movies vs standalone movies
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with comparison metrics
        """
        logger.info("Analyzing franchise vs standalone movies")
        
        comparison = df.groupBy("has_franchise").agg(
            F.count("*").alias("movie_count"),
            F.mean("revenue_musd").alias("mean_revenue"),
            F.median("revenue_musd").alias("median_revenue"),
            F.mean("roi").alias("mean_roi"),
            F.median("roi").alias("median_roi"),
            F.mean("budget_musd").alias("mean_budget"),
            F.median("budget_musd").alias("median_budget"),
            F.mean("popularity").alias("mean_popularity"),
            F.mean("vote_average").alias("mean_rating"),
            F.mean("profit_musd").alias("mean_profit")
        ).orderBy("has_franchise", ascending=False)
        
        return comparison
    
    def get_top_franchises(self, df: DataFrame) -> DataFrame:
        """
        Identify most successful movie franchises
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with franchise statistics
        """
        logger.info("Calculating top franchises")
        
        # Filter for franchise movies only
        franchise_df = df.filter(F.col("has_franchise") == True)
        
        franchise_stats = franchise_df.groupBy("belongs_to_collection").agg(
            F.count("*").alias("total_movies"),
            F.sum("budget_musd").alias("total_budget"),
            F.mean("budget_musd").alias("mean_budget"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.mean("revenue_musd").alias("mean_revenue"),
            F.sum("profit_musd").alias("total_profit"),
            F.mean("profit_musd").alias("mean_profit"),
            F.mean("roi").alias("mean_roi"),
            F.mean("vote_average").alias("mean_rating"),
            F.mean("popularity").alias("mean_popularity"),
            F.sum("vote_count").alias("total_votes")
        ).orderBy("total_revenue", ascending=False)
        
        logger.info(f"Analyzed {franchise_stats.count()} franchises")
        
        return franchise_stats
    
    def get_top_directors(self, df: DataFrame) -> DataFrame:
        """
        Identify most successful directors
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with director statistics
        """
        logger.info("Calculating top directors")
        
        # Filter for movies with directors
        director_df = df.filter(F.col("director").isNotNull())
        
        min_movies = self.kpi_config.get('director', {}).get('min_movies', 1)
        
        director_stats = director_df.groupBy("director").agg(
            F.count("*").alias("total_movies"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.mean("revenue_musd").alias("avg_revenue"),
            F.mean("vote_average").alias("avg_rating"),
            F.sum("profit_musd").alias("total_profit"),
            F.mean("profit_musd").alias("avg_profit"),
            F.mean("roi").alias("avg_roi"),
            F.sum("vote_count").alias("total_votes"),
            F.mean("budget_musd").alias("avg_budget")
        ).filter(F.col("total_movies") >= min_movies) \
          .orderBy("total_revenue", ascending=False)
        
        logger.info(f"Analyzed {director_stats.count()} directors")
        
        return director_stats
    
    def calculate_genre_performance(self, df: DataFrame) -> DataFrame:
        """
        Calculate performance metrics by genre
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with genre performance
        """
        logger.info("Calculating genre performance")
        
        # Split genres and explode
        genre_df = df.withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
        
        genre_stats = genre_df.groupBy("genre").agg(
            F.count("*").alias("movie_count"),
            F.mean("revenue_musd").alias("avg_revenue"),
            F.median("revenue_musd").alias("median_revenue"),
            F.mean("roi").alias("avg_roi"),
            F.median("roi").alias("median_roi"),
            F.mean("budget_musd").alias("avg_budget"),
            F.mean("vote_average").alias("avg_rating"),
            F.mean("popularity").alias("avg_popularity"),
            F.sum("revenue_musd").alias("total_revenue")
        ).orderBy("total_revenue", ascending=False)
        
        logger.info(f"Analyzed {genre_stats.count()} genres")
        
        return genre_stats
    
    def calculate_yearly_trends(self, df: DataFrame) -> DataFrame:
        """
        Calculate yearly trends in box office performance
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with yearly statistics
        """
        logger.info("Calculating yearly trends")
        
        yearly_stats = df.groupBy("release_year").agg(
            F.count("*").alias("movies_released"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.mean("revenue_musd").alias("avg_revenue"),
            F.sum("budget_musd").alias("total_budget"),
            F.mean("budget_musd").alias("avg_budget"),
            F.mean("roi").alias("avg_roi"),
            F.mean("vote_average").alias("avg_rating"),
            F.mean("popularity").alias("avg_popularity")
        ).orderBy("release_year")
        
        return yearly_stats
    
    def calculate_decade_trends(self, df: DataFrame) -> DataFrame:
        """
        Calculate trends by decade
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with decade statistics
        """
        logger.info("Calculating decade trends")
        
        decade_stats = df.groupBy("decade").agg(
            F.count("*").alias("movies_released"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.mean("revenue_musd").alias("avg_revenue"),
            F.mean("budget_musd").alias("avg_budget"),
            F.mean("roi").alias("avg_roi"),
            F.mean("vote_average").alias("avg_rating")
        ).orderBy("decade")
        
        return decade_stats
    
    def calculate_language_performance(self, df: DataFrame) -> DataFrame:
        """
        Calculate performance by original language
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with language statistics
        """
        logger.info("Calculating language performance")
        
        language_stats = df.groupBy("original_language").agg(
            F.count("*").alias("movie_count"),
            F.mean("revenue_musd").alias("avg_revenue"),
            F.mean("budget_musd").alias("avg_budget"),
            F.mean("vote_average").alias("avg_rating"),
            F.sum("revenue_musd").alias("total_revenue")
        ).filter(F.col("movie_count") >= 2) \
          .orderBy("total_revenue", ascending=False)
        
        return language_stats
    
    def calculate_budget_category_performance(self, df: DataFrame) -> DataFrame:
        """
        Calculate performance by budget category
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with budget category statistics
        """
        logger.info("Calculating budget category performance")
        
        budget_stats = df.groupBy("budget_category").agg(
            F.count("*").alias("movie_count"),
            F.mean("revenue_musd").alias("avg_revenue"),
            F.mean("roi").alias("avg_roi"),
            F.mean("vote_average").alias("avg_rating"),
            F.mean("popularity").alias("avg_popularity")
        ).orderBy(
            F.when(F.col("budget_category") == "Micro", 1)
            .when(F.col("budget_category") == "Low", 2)
            .when(F.col("budget_category") == "Medium", 3)
            .when(F.col("budget_category") == "High", 4)
            .otherwise(5)
        )
        
        return budget_stats
    
    def generate_summary_statistics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate overall summary statistics
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        logger.info("Generating summary statistics")
        
        summary = df.agg(
            F.count("*").alias("total_movies"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.sum("budget_musd").alias("total_budget"),
            F.sum("profit_musd").alias("total_profit"),
            F.mean("revenue_musd").alias("avg_revenue"),
            F.median("revenue_musd").alias("median_revenue"),
            F.mean("budget_musd").alias("avg_budget"),
            F.mean("roi").alias("avg_roi"),
            F.mean("vote_average").alias("avg_rating"),
            F.mean("popularity").alias("avg_popularity"),
            F.max("revenue_musd").alias("max_revenue"),
            F.min("revenue_musd").alias("min_revenue"),
            F.countDistinct("director").alias("unique_directors"),
            F.sum(F.when(F.col("has_franchise"), 1).otherwise(0)).alias("franchise_movies")
        ).collect()[0].asDict()
        
        return summary
    
    def calculate_all_kpis(self, df: DataFrame) -> Dict[str, Any]:
        """
        Calculate all KPIs
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with all KPI results
        """
        logger.info("Calculating all KPIs")
        
        kpis = {
            'rankings': self.get_all_rankings(df),
            'franchise_comparison': self.analyze_franchise_vs_standalone(df),
            'top_franchises': self.get_top_franchises(df),
            'top_directors': self.get_top_directors(df),
            'genre_performance': self.calculate_genre_performance(df),
            'yearly_trends': self.calculate_yearly_trends(df),
            'decade_trends': self.calculate_decade_trends(df),
            'language_performance': self.calculate_language_performance(df),
            'budget_category_performance': self.calculate_budget_category_performance(df),
            'summary_statistics': self.generate_summary_statistics(df)
        }
        
        logger.info("All KPIs calculated successfully")
        
        return kpis


    def save_kpis(self, kpis: Dict[str, Any], output_dir: str) -> None:
        """
        Persist KPI results to disk

        - DataFrames → Parquet
        - Dict summaries → JSON
        - Nested rankings handled properly
        """
        logger.info(f"Saving KPIs to {output_dir}")

        os.makedirs(output_dir, exist_ok=True)

        for kpi_name, kpi_value in kpis.items():

            # Case 1: Spark DataFrame
            if isinstance(kpi_value, DataFrame):
                path = os.path.join(output_dir, kpi_name)
                logger.info(f"Saving KPI: {kpi_name}")
                kpi_value.write.mode("overwrite").parquet(path)

            # Case 2: Dict - check if it contains DataFrames or primitives
            elif isinstance(kpi_value, dict):
                # Check first value to determine dict type
                first_value = next(iter(kpi_value.values())) if kpi_value else None
                
                if isinstance(first_value, DataFrame):
                    # Dict of DataFrames (e.g., rankings)
                    for sub_name, df in kpi_value.items():
                        path = os.path.join(output_dir, kpi_name, sub_name)
                        logger.info(f"Saving KPI: {kpi_name}/{sub_name}")
                        df.write.mode("overwrite").parquet(path)
                else:
                    # Dict of primitives (e.g., summary statistics)
                    path = os.path.join(output_dir, f"{kpi_name}.json")
                    logger.info(f"Saving KPI summary: {kpi_name}")
                    with open(path, "w") as f:
                        json.dump(kpi_value, f, indent=2)

            else:
                logger.warning(f"Skipping unsupported KPI type: {kpi_name}")

        logger.info("All KPIs saved successfully")
