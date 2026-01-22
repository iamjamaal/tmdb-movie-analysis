"""
Data Cleaner - Handles all data cleaning operations with PySpark
"""

import logging
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Comprehensive data cleaning for TMDB movie data
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize DataCleaner
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.processing_config = config.get('processing', {})
        logger.info("DataCleaner initialized")
    
    def drop_irrelevant_columns(self, df: DataFrame) -> DataFrame:
        """
        Drop columns that are not needed for analysis
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with irrelevant columns removed
        """
        columns_to_drop = self.processing_config.get('columns_to_drop', [])
        logger.info(f"Dropping columns: {columns_to_drop}")
        
        return df.drop(*columns_to_drop)
    
    def extract_collection_name(self, df: DataFrame) -> DataFrame:
        """
        Extract collection name from belongs_to_collection JSON
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with extracted collection name
        """
        logger.info("Extracting collection names")
        
        return df.withColumn(
            "belongs_to_collection",
            F.when(
                F.col("belongs_to_collection").isNotNull(),
                F.col("belongs_to_collection").getItem("name")
            ).otherwise(F.lit(None))
        )
    
    def extract_genres(self, df: DataFrame) -> DataFrame:
        """
        Extract and concatenate genre names
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with genres as pipe-separated string
        """
        logger.info("Extracting genres")
        
        # Extract genre names and join with pipe
        return df.withColumn(
            "genres",
            F.when(
                F.size(F.col("genres")) > 0,
                F.concat_ws("|", F.transform(F.col("genres"), lambda x: x.getItem("name")))
            ).otherwise(F.lit(None))
        )
    
    def extract_spoken_languages(self, df: DataFrame) -> DataFrame:
        """
        Extract and concatenate spoken language names
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with languages as pipe-separated string
        """
        logger.info("Extracting spoken languages")
        
        return df.withColumn(
            "spoken_languages",
            F.when(
                F.size(F.col("spoken_languages")) > 0,
                F.concat_ws("|", F.transform(F.col("spoken_languages"), lambda x: x.getItem("english_name")))
            ).otherwise(F.lit(None))
        )
    
    def extract_production_countries(self, df: DataFrame) -> DataFrame:
        """
        Extract and concatenate production country names
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with countries as pipe-separated string
        """
        logger.info("Extracting production countries")
        
        return df.withColumn(
            "production_countries",
            F.when(
                F.size(F.col("production_countries")) > 0,
                F.concat_ws("|", F.transform(F.col("production_countries"), lambda x: x.getItem("name")))
            ).otherwise(F.lit(None))
        )
    
    def extract_production_companies(self, df: DataFrame) -> DataFrame:
        """
        Extract and concatenate production company names
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with companies as pipe-separated string
        """
        logger.info("Extracting production companies")
        
        return df.withColumn(
            "production_companies",
            F.when(
                F.size(F.col("production_companies")) > 0,
                F.concat_ws("|", F.transform(F.col("production_companies"), lambda x: x.getItem("name")))
            ).otherwise(F.lit(None))
        )
    
    def convert_cast_to_string(self, df: DataFrame) -> DataFrame:
        """
        Convert cast array to pipe-separated string
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with cast as string
        """
        logger.info("Converting cast to string")
        
        return df.withColumn(
            "cast",
            F.when(
                F.size(F.col("cast")) > 0,
                F.concat_ws("|", F.col("cast"))
            ).otherwise(F.lit(None))
        )
    
    def process_json_columns(self, df: DataFrame) -> DataFrame:
        """
        Process all JSON-like columns
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with processed JSON columns
        """
        logger.info("Processing JSON columns")
        
        df = self.extract_collection_name(df)
        df = self.extract_genres(df)
        df = self.extract_spoken_languages(df)
        df = self.extract_production_countries(df)
        df = self.extract_production_companies(df)
        df = self.convert_cast_to_string(df)
        
        return df
    
    def convert_datatypes(self, df: DataFrame) -> DataFrame:
        """
        Convert columns to appropriate datatypes
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with corrected datatypes
        """
        logger.info("Converting datatypes")
        
        df = df.withColumn("budget", F.col("budget").cast("double"))
        df = df.withColumn("revenue", F.col("revenue").cast("double"))
        df = df.withColumn("popularity", F.col("popularity").cast("double"))
        df = df.withColumn("vote_average", F.col("vote_average").cast("double"))
        df = df.withColumn("runtime", F.col("runtime").cast("double"))
        
        # Convert release_date to date type
        df = df.withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))
        
        return df
    
    def replace_zero_with_null(self, df: DataFrame) -> DataFrame:
        """
        Replace zero values with NULL for specified columns
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with zeros replaced
        """
        zero_to_null_cols = self.processing_config.get('replacements', {}).get('zero_to_null', [])
        logger.info(f"Replacing zeros with NULL for: {zero_to_null_cols}")
        
        for col in zero_to_null_cols:
            if col in df.columns:
                df = df.withColumn(
                    col,
                    F.when(F.col(col) == 0, F.lit(None)).otherwise(F.col(col))
                )
        
        return df
    
    def replace_placeholder_values(self, df: DataFrame) -> DataFrame:
        """
        Replace known placeholder values with NULL
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with placeholders replaced
        """
        placeholders = self.processing_config.get('replacements', {}).get('placeholder_values', [])
        logger.info(f"Replacing placeholder values: {placeholders}")
        
        # Apply to overview and tagline columns
        for col in ['overview', 'tagline']:
            if col in df.columns:
                for placeholder in placeholders:
                    df = df.withColumn(
                        col,
                        F.when(F.col(col) == placeholder, F.lit(None)).otherwise(F.col(col))
                    )
        
        return df
    
    def handle_vote_count_zero(self, df: DataFrame) -> DataFrame:
        """
        Handle movies with vote_count = 0
        Set their vote_average to NULL
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with vote_average adjusted
        """
        logger.info("Handling zero vote counts")
        
        return df.withColumn(
            "vote_average",
            F.when(F.col("vote_count") == 0, F.lit(None)).otherwise(F.col("vote_average"))
        )
    
    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate records based on ID
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame without duplicates
        """
        logger.info("Removing duplicates")
        
        initial_count = df.count()
        df = df.dropDuplicates(['id'])
        final_count = df.count()
        
        logger.info(f"Removed {initial_count - final_count} duplicate records")
        
        return df
    
    def drop_invalid_records(self, df: DataFrame) -> DataFrame:
        """
        Drop records with NULL id or title
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame without invalid records
        """
        logger.info("Dropping records with NULL id or title")
        
        initial_count = df.count()
        df = df.filter(F.col("id").isNotNull() & F.col("title").isNotNull())
        final_count = df.count()
        
        logger.info(f"Dropped {initial_count - final_count} invalid records")
        
        return df
    
    def keep_sufficient_data(self, df: DataFrame) -> DataFrame:
        """
        Keep only rows with at least N non-NULL columns
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with sufficient data
        """
        min_cols = self.processing_config.get('quality', {}).get('min_non_null_columns', 10)
        logger.info(f"Keeping rows with at least {min_cols} non-NULL columns")
        
        # Count non-null values in each row
        non_null_count = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in df.columns)
        
        initial_count = df.count()
        df = df.withColumn("_non_null_count", non_null_count)
        df = df.filter(F.col("_non_null_count") >= min_cols)
        df = df.drop("_non_null_count")
        final_count = df.count()
        
        logger.info(f"Kept {final_count}/{initial_count} records with sufficient data")
        
        return df
    
    def filter_released_movies(self, df: DataFrame) -> DataFrame:
        """
        Filter to include only released movies and drop status column
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with only released movies
        """
        logger.info("Filtering for released movies only")
        
        initial_count = df.count()
        df = df.filter(F.col("status") == "Released")
        final_count = df.count()
        
        logger.info(f"Filtered to {final_count} released movies (removed {initial_count - final_count})")
        
        # Drop status column
        df = df.drop("status")
        
        return df
    
    def clean(self, df: DataFrame) -> DataFrame:
        """
        Execute complete cleaning pipeline
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Starting data cleaning pipeline")
        
        # Step 1: Drop irrelevant columns
        df = self.drop_irrelevant_columns(df)
        
        # Step 2: Process JSON columns
        df = self.process_json_columns(df)
        
        # Step 3: Convert datatypes
        df = self.convert_datatypes(df)
        
        # Step 4: Replace zeros with NULL
        df = self.replace_zero_with_null(df)
        
        # Step 5: Replace placeholder values
        df = self.replace_placeholder_values(df)
        
        # Step 6: Handle zero vote counts
        df = self.handle_vote_count_zero(df)
        
        # Step 7: Remove duplicates
        df = self.remove_duplicates(df)
        
        # Step 8: Drop invalid records
        df = self.drop_invalid_records(df)
        
        # Step 9: Keep sufficient data
        df = self.keep_sufficient_data(df)
        
        # Step 10: Filter for released movies
        df = self.filter_released_movies(df)
        
        logger.info(f"Data cleaning complete. Final record count: {df.count()}")
        
        return df