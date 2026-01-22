where is this suppose to go?



import sys

sys.path.append('/home/jovyan/work/src')

from pyspark.sql import SparkSession

from config import load_config

from ingestion.data_fetcher import DataFetcher

from processing.data_cleaner import DataCleaner

from processing.data_transformer import DataTransformer

# Initialize Spark

spark = SparkSession.builder \

    .appName("TMDB Analysis - Jupyter") \

    .master("spark://spark-master:7077") \

    .getOrCreate()

# Load config

config = load_config('/home/jovyan/work/src/config/config.yaml')

# Run pipeline

fetcher = DataFetcher(spark, config)

df = fetcher.fetch_movies()

cleaner = DataCleaner(config)

df_clean = cleaner.clean(df)

transformer = DataTransformer(config)

df_final = transformer.transform(df_clean)

# Display results

df_final.show()