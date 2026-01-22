"""
Data Transformer - Feature Engineering and Data Transformation
"""

import logging
from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Handles feature engineering and data transformation
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize DataTransformer
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        logger.info("DataTransformer initialized")
    
    def convert_to_millions(self, df: DataFrame) -> DataFrame:
        """
        Convert budget and revenue to millions of USD
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with converted values
        """
        logger.info("Converting budget and revenue to millions USD")
        
        df = df.withColumn("budget_musd", F.col("budget") / 1_000_000)
        df = df.withColumn("revenue_musd", F.col("revenue") / 1_000_000)
        
        # Drop original columns
        df = df.drop("budget", "revenue")
        
        return df
    
    def calculate_profit(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit (revenue - budget)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with profit column
        """
        logger.info("Calculating profit")
        
        return df.withColumn(
            "profit_musd",
            F.when(
                F.col("revenue_musd").isNotNull() & F.col("budget_musd").isNotNull(),
                F.col("revenue_musd") - F.col("budget_musd")
            ).otherwise(F.lit(None))
        )
    
    def calculate_roi(self, df: DataFrame) -> DataFrame:
        """
        Calculate ROI (Return on Investment)
        ROI = (Revenue / Budget) - 1
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with ROI column
        """
        logger.info("Calculating ROI")
        
        return df.withColumn(
            "roi",
            F.when(
                (F.col("budget_musd").isNotNull()) & 
                (F.col("budget_musd") > 0) & 
                (F.col("revenue_musd").isNotNull()),
                (F.col("revenue_musd") / F.col("budget_musd")) - 1
            ).otherwise(F.lit(None))
        )
    
    def extract_release_year(self, df: DataFrame) -> DataFrame:
        """
        Extract year from release_date
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with release_year column
        """
        logger.info("Extracting release year")
        
        return df.withColumn(
            "release_year",
            F.year(F.col("release_date"))
        )
    
    def add_franchise_flag(self, df: DataFrame) -> DataFrame:
        """
        Add flag indicating if movie belongs to a franchise
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with franchise flag
        """
        logger.info("Adding franchise flag")
        
        return df.withColumn(
            "has_franchise",
            F.when(F.col("belongs_to_collection").isNotNull(), True).otherwise(False)
        )
    
    def calculate_revenue_per_minute(self, df: DataFrame) -> DataFrame:
        """
        Calculate revenue per minute of runtime
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with revenue per minute
        """
        logger.info("Calculating revenue per minute")
        
        return df.withColumn(
            "revenue_per_minute",
            F.when(
                (F.col("runtime").isNotNull()) & 
                (F.col("runtime") > 0) & 
                (F.col("revenue_musd").isNotNull()),
                F.col("revenue_musd") / F.col("runtime")
            ).otherwise(F.lit(None))
        )
    
    def calculate_popularity_score(self, df: DataFrame) -> DataFrame:
        """
        Calculate normalized popularity score combining vote_average, vote_count, and popularity
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with popularity score
        """
        logger.info("Calculating popularity score")
        
        # Normalize vote_average to 0-1 scale
        df = df.withColumn("norm_rating", F.col("vote_average") / 10.0)
        
        # Log-scale vote_count
        df = df.withColumn("log_votes", F.log1p(F.col("vote_count")))
        
        # Combined popularity score
        df = df.withColumn(
            "popularity_score",
            F.when(
                F.col("norm_rating").isNotNull() & 
                F.col("log_votes").isNotNull() & 
                F.col("popularity").isNotNull(),
                (F.col("norm_rating") * 0.3 + 
                 F.col("log_votes") / 10.0 * 0.3 + 
                 F.col("popularity") / 100.0 * 0.4)
            ).otherwise(F.lit(None))
        )
        
        # Drop intermediate columns
        df = df.drop("norm_rating", "log_votes")
        
        return df
    
    def categorize_budget(self, df: DataFrame) -> DataFrame:
        """
        Categorize movies by budget size
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with budget category
        """
        logger.info("Categorizing budget")
        
        return df.withColumn(
            "budget_category",
            F.when(F.col("budget_musd") < 1, "Micro")
            .when(F.col("budget_musd") < 10, "Low")
            .when(F.col("budget_musd") < 50, "Medium")
            .when(F.col("budget_musd") < 100, "High")
            .otherwise("Blockbuster")
        )
    
    def categorize_rating(self, df: DataFrame) -> DataFrame:
        """
        Categorize movies by rating
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with rating category
        """
        logger.info("Categorizing rating")
        
        return df.withColumn(
            "rating_category",
            F.when(F.col("vote_average") < 3, "Poor")
            .when(F.col("vote_average") < 5, "Below Average")
            .when(F.col("vote_average") < 6.5, "Average")
            .when(F.col("vote_average") < 7.5, "Good")
            .when(F.col("vote_average") < 8.5, "Very Good")
            .otherwise("Excellent")
        )
    
    def add_decade(self, df: DataFrame) -> DataFrame:
        """
        Add decade column based on release year
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with decade column
        """
        logger.info("Adding decade")
        
        return df.withColumn(
            "decade",
            (F.floor(F.col("release_year") / 10) * 10).cast("int")
        )
    
    def reorder_columns(self, df: DataFrame) -> DataFrame:
        """
        Reorder columns to match specified order
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with reordered columns
        """
        logger.info("Reordering columns")
        
        # Desired column order
        column_order = [
            'id', 'title', 'tagline', 'release_date', 'release_year', 'decade',
            'genres', 'belongs_to_collection', 'has_franchise', 'original_language',
            'budget_musd', 'budget_category', 'revenue_musd', 'profit_musd', 'roi',
            'revenue_per_minute', 'production_companies', 'production_countries',
            'vote_count', 'vote_average', 'rating_category', 'popularity', 'popularity_score',
            'runtime', 'overview', 'spoken_languages', 'poster_path', 'backdrop_path',
            'cast', 'cast_size', 'director', 'crew_size'
        ]
        
        # Select only existing columns in specified order
        existing_columns = [col for col in column_order if col in df.columns]
        
        # Add any remaining columns not in the order
        remaining_columns = [col for col in df.columns if col not in existing_columns]
        
        return df.select(existing_columns + remaining_columns)
    
    def reset_index(self, df: DataFrame) -> DataFrame:
        """
        Reset index by adding monotonically increasing ID
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with reset index
        """
        logger.info("Resetting index")
        
        return df.withColumn("index", F.monotonically_increasing_id())
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Execute complete transformation pipeline
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting data transformation pipeline")
        
        # Financial transformations
        df = self.convert_to_millions(df)
        df = self.calculate_profit(df)
        df = self.calculate_roi(df)
        df = self.calculate_revenue_per_minute(df)
        
        # Date transformations
        df = self.extract_release_year(df)
        df = self.add_decade(df)
        
        # Categorical features
        df = self.add_franchise_flag(df)
        df = self.categorize_budget(df)
        df = self.categorize_rating(df)
        
        # Advanced metrics
        df = self.calculate_popularity_score(df)
        
        # Final organization
        df = self.reorder_columns(df)
        df = self.reset_index(df)
        
        logger.info(f"Data transformation complete. Final columns: {len(df.columns)}")
        
        return df