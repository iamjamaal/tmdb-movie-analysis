"""
Data Fetcher - Orchestrates API calls and creates Spark DataFrame
"""

import logging
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, ArrayType, MapType
from  src.ingestion.api_client import TMDBClient

logger = logging.getLogger(__name__)


class DataFetcher:
    """
    Handles data fetching from TMDB API and conversion to Spark DataFrame
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize DataFetcher
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.api_client = TMDBClient(config)
        logger.info("DataFetcher initialized")
    
    @staticmethod
    def get_movie_schema() -> StructType:
        """
        Define explicit schema for movie data
        
        Returns:
            StructType schema for movies
        """
        return StructType([
            StructField("adult", StringType(), True),
            StructField("backdrop_path", StringType(), True),
            StructField("belongs_to_collection", MapType(StringType(), StringType()), True),
            StructField("budget", LongType(), True),
            StructField("genres", ArrayType(MapType(StringType(), StringType())), True),
            StructField("homepage", StringType(), True),
            StructField("id", IntegerType(), False),
            StructField("imdb_id", StringType(), True),
            StructField("original_language", StringType(), True),
            StructField("original_title", StringType(), True),
            StructField("overview", StringType(), True),
            StructField("popularity", FloatType(), True),
            StructField("poster_path", StringType(), True),
            StructField("production_companies", ArrayType(MapType(StringType(), StringType())), True),
            StructField("production_countries", ArrayType(MapType(StringType(), StringType())), True),
            StructField("release_date", StringType(), True),
            StructField("revenue", LongType(), True),
            StructField("runtime", IntegerType(), True),
            StructField("spoken_languages", ArrayType(MapType(StringType(), StringType())), True),
            StructField("status", StringType(), True),
            StructField("tagline", StringType(), True),
            StructField("title", StringType(), True),
            StructField("video", StringType(), True),
            StructField("vote_average", FloatType(), True),
            StructField("vote_count", IntegerType(), True),
            # Credits fields
            StructField("cast", ArrayType(StringType()), True),
            StructField("cast_size", IntegerType(), True),
            StructField("director", StringType(), True),
            StructField("crew_size", IntegerType(), True),
        ])
    
    def fetch_movies(self, movie_ids: List[int] = None) -> DataFrame:
        """
        Fetch movies from API and create Spark DataFrame
        
        Args:
            movie_ids: List of movie IDs to fetch. If None, uses config
            
        Returns:
            Spark DataFrame with movie data
        """
        if movie_ids is None:
            movie_ids = self.config.get('data', {}).get('movie_ids', [])
        
        logger.info(f"Starting data fetch for {len(movie_ids)} movies")
        
        # Fetch data from API
        movies_data = self.api_client.fetch_movies_batch(movie_ids)
        
        if not movies_data:
            logger.error("No movie data fetched")
            return self.spark.createDataFrame([], self.get_movie_schema())
        
        logger.info(f"Successfully fetched {len(movies_data)} movies")
        
        # Create DataFrame with schema inference (faster than strict schema)
        df = self.spark.createDataFrame(movies_data)
        
        # Cache for reuse
        df.cache()
        
        logger.info(f"Created Spark DataFrame with {df.count()} records and {len(df.columns)} columns")
        
        return df
    
    def save_raw_data(self, df: DataFrame, output_path: str):
        """
        Save raw data to disk
        
        Args:
            df: DataFrame to save
            output_path: Path to save data
        """
        logger.info(f"Saving raw data to {output_path}")
        
        # Save as Parquet for efficient storage
        df.write.mode("overwrite").parquet(f"{output_path}/raw/movies.parquet")
        
        # Also save as JSON for human readability
        df.write.mode("overwrite").json(f"{output_path}/raw/movies.json")
        
        logger.info("Raw data saved successfully")
    
    def close(self):
        """Close API client"""
        self.api_client.close()
        logger.info("DataFetcher closed")