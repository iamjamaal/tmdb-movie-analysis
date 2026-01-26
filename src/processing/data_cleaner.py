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
        # Default columns to drop based on requirements
        columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
        # Add any additional columns from config
        columns_to_drop.extend(self.processing_config.get('columns_to_drop', []))
        
        logger.info(f"Dropping columns: {columns_to_drop}")
        
        # Only drop columns that exist in the dataframe
        existing_columns = df.columns
        columns_to_drop = [c for c in columns_to_drop if c in existing_columns]
        
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

    def handle_missing_and_incorrect_data(self, df: DataFrame) -> DataFrame:
        """
        Handle missing values, incorrect data types, and realistic value checks
        """
        logger.info("Handling missing and incorrect data")
        
        # 5. Convert column datatypes (handled by schema, but ensuring numerics)
        # 6. Replace unrealistic values
        # Budget/Revenue/Runtime = 0 -> Replace with None (Spark's NaN equivalent for Nullable)
        df = df.withColumn("budget", F.when(F.col("budget") == 0, F.lit(None)).otherwise(F.col("budget")))
        df = df.withColumn("revenue", F.when(F.col("revenue") == 0, F.lit(None)).otherwise(F.col("revenue")))
        df = df.withColumn("runtime", F.when(F.col("runtime") == 0, F.lit(None)).otherwise(F.col("runtime")))
        
        # 'overview' and 'tagline' -> Replace known placeholders
        df = df.withColumn("overview", F.when(F.col("overview") == "No Data", F.lit(None)).otherwise(F.col("overview")))
        df = df.withColumn("tagline", F.when(F.col("tagline") == "No Data", F.lit(None)).otherwise(F.col("tagline")))
        
        # 7. Remove duplicates and drop rows with unknown 'id' or 'title'
        df = df.dropDuplicates(['id'])
        df = df.filter(F.col("id").isNotNull() & F.col("title").isNotNull())
        
        # 9. Filter to include only 'Released' movies, then drop 'status'
        df = df.filter(F.col("status") == "Released").drop("status")
        
        return df

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
        
        return df

    def clean(self, df: DataFrame) -> DataFrame:
        """
        Execute full cleaning pipeline
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Starting full cleaning pipeline")
        
        # 1. Drop irrelevant columns
        df = self.drop_irrelevant_columns(df)
        
        # 2. Process JSON columns
        df = self.process_json_columns(df)
        
        # 3. Handle missing/incorrect data
        df = self.handle_missing_and_incorrect_data(df)
        
        return df
