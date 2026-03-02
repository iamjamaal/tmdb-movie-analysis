"""
Unit tests for DataCleaner
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    LongType, ArrayType
)

from src.processing.data_cleaner import DataCleaner


@pytest.mark.unit
class TestDataCleaner:
    """Test suite for DataCleaner class"""

    def test_drop_irrelevant_columns(self, spark_session, test_config):
        """Test dropping irrelevant columns"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("adult", StringType(), True),
            StructField("budget", LongType(), True),
            StructField("video", StringType(), True),
            StructField("homepage", StringType(), True),
            StructField("imdb_id", StringType(), True),
            StructField("original_title", StringType(), True),
        ])

        data = [
            (1, "Movie 1", "false", 100000000, "false", "http://x", "tt1", "M1"),
            (2, "Movie 2", "false", 200000000, "false", None, "tt2", "M2"),
        ]

        df = spark_session.createDataFrame(data, schema)
        cleaner = DataCleaner(test_config)
        result_df = cleaner.drop_irrelevant_columns(df)

        assert "adult" not in result_df.columns
        assert "video" not in result_df.columns
        assert "homepage" not in result_df.columns
        assert "imdb_id" not in result_df.columns
        assert "original_title" not in result_df.columns
        assert "id" in result_df.columns
        assert "title" in result_df.columns
        assert "budget" in result_df.columns

    def test_handle_missing_and_incorrect_data(self, spark_session, test_config):
        """Test zero-to-null replacement and placeholder removal"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("budget", LongType(), True),
            StructField("revenue", LongType(), True),
            StructField("runtime", IntegerType(), True),
            StructField("overview", StringType(), True),
            StructField("tagline", StringType(), True),
            StructField("status", StringType(), True),
        ])

        data = [
            (1, "Movie 1", 100000000, 300000000, 120, "Great movie", "Boom", "Released"),
            (2, "Movie 2", 0, 0, 0, "No Data", "No Data", "Released"),
            (3, "Movie 3", 50000000, 100000000, 90, "Nice", "Tag", "Post Production"),
        ]

        df = spark_session.createDataFrame(data, schema)
        cleaner = DataCleaner(test_config)
        result_df = cleaner.handle_missing_and_incorrect_data(df)

        rows = {r["id"]: r for r in result_df.collect()}
        # Movie 2: budget/revenue/runtime zeroes → null, overview/tagline placeholder → null
        assert rows[2]["budget"] is None
        assert rows[2]["revenue"] is None
        assert rows[2]["runtime"] is None
        assert rows[2]["overview"] is None
        assert rows[2]["tagline"] is None
        # Movie 1: values unchanged
        assert rows[1]["budget"] == 100000000
        # Only "Released" movies remain; Movie 3 (Post Production) filtered out
        assert 3 not in rows
        # status column dropped
        assert "status" not in result_df.columns

    def test_handle_duplicates(self, spark_session, test_config):
        """Test duplicate removal by id"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("budget", LongType(), True),
            StructField("revenue", LongType(), True),
            StructField("runtime", IntegerType(), True),
            StructField("overview", StringType(), True),
            StructField("tagline", StringType(), True),
            StructField("status", StringType(), True),
        ])

        data = [
            (1, "Movie 1", 100, 300, 120, "Good", "Tag", "Released"),
            (1, "Movie 1", 100, 300, 120, "Good", "Tag", "Released"),
            (2, "Movie 2", 200, 400, 90, "Nice", "T2", "Released"),
        ]

        df = spark_session.createDataFrame(data, schema)
        cleaner = DataCleaner(test_config)
        result_df = cleaner.handle_missing_and_incorrect_data(df)

        assert result_df.count() == 2

    def test_extract_genres(self, spark_session, raw_movie_data, test_config):
        """Test genre extraction from struct array to pipe-separated string"""
        cleaner = DataCleaner(test_config)
        result_df = cleaner.extract_genres(raw_movie_data)

        rows = {r["id"]: r for r in result_df.select("id", "genres").collect()}
        assert "Action" in rows[1]["genres"]
        assert "Adventure" in rows[1]["genres"]
        assert "|" in rows[1]["genres"]  # pipe-separated
        assert rows[2]["genres"] == "Comedy"

    def test_extract_collection_name(self, spark_session, raw_movie_data, test_config):
        """Test belongs_to_collection extraction"""
        cleaner = DataCleaner(test_config)
        result_df = cleaner.extract_collection_name(raw_movie_data)

        rows = {r["id"]: r for r in result_df.select("id", "belongs_to_collection").collect()}
        assert rows[1]["belongs_to_collection"] == "Test Collection"
        assert rows[2]["belongs_to_collection"] is None  # no collection

    def test_convert_cast_to_string(self, spark_session, raw_movie_data, test_config):
        """Test cast array converted to pipe-separated string"""
        cleaner = DataCleaner(test_config)
        result_df = cleaner.convert_cast_to_string(raw_movie_data)

        rows = {r["id"]: r for r in result_df.select("id", "cast").collect()}
        assert "Actor A" in rows[1]["cast"]
        assert "|" in rows[1]["cast"]
        # Movie 4 has null cast
        assert rows[4]["cast"] is None

    def test_process_json_columns(self, spark_session, raw_movie_data, test_config):
        """Test processing all JSON columns at once"""
        cleaner = DataCleaner(test_config)
        result_df = cleaner.process_json_columns(raw_movie_data)

        row = result_df.filter(F.col("id") == 5).collect()[0]
        # genres should be pipe-separated string
        assert isinstance(row["genres"], str)
        # production_companies should be pipe-separated string
        assert isinstance(row["production_companies"], str)
        assert "Studio A" in row["production_companies"]
        # spoken_languages
        assert isinstance(row["spoken_languages"], str)

    def test_clean_pipeline(self, spark_session, raw_movie_data, test_config):
        """Test complete cleaning pipeline"""
        cleaner = DataCleaner(test_config)
        result_df = cleaner.clean(raw_movie_data)

        assert result_df.count() > 0
        # Irrelevant columns dropped
        assert "adult" not in result_df.columns
        assert "video" not in result_df.columns
        # Status column dropped after filtering
        assert "status" not in result_df.columns
        # No null ids or titles
        null_count = result_df.filter(
            F.col("id").isNull() | F.col("title").isNull()
        ).count()
        assert null_count == 0

    @pytest.mark.parametrize("budget_val,should_be_null", [
        (0, True),
        (None, True),
        (100000000, False),
    ])
    def test_zero_budget_handling(self, spark_session, test_config, budget_val, should_be_null):
        """Test zero and null budget handling"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("budget", LongType(), True),
            StructField("revenue", LongType(), True),
            StructField("runtime", IntegerType(), True),
            StructField("overview", StringType(), True),
            StructField("tagline", StringType(), True),
            StructField("status", StringType(), True),
        ])

        data = [(1, "Test", budget_val, 100, 90, "OK", "T", "Released")]
        df = spark_session.createDataFrame(data, schema)

        cleaner = DataCleaner(test_config)
        result_df = cleaner.handle_missing_and_incorrect_data(df)

        is_null = result_df.collect()[0]["budget"] is None
        assert is_null == should_be_null


@pytest.mark.unit
class TestDataCleanerEdgeCases:
    """Test edge cases for DataCleaner"""

    def test_empty_dataframe(self, spark_session, raw_movie_schema, test_config):
        """Test cleaning empty dataframe"""
        df = spark_session.createDataFrame([], raw_movie_schema)
        cleaner = DataCleaner(test_config)
        result_df = cleaner.clean(df)
        assert result_df.count() == 0

    def test_special_characters_in_title(self, spark_session, test_config):
        """Test that special characters are preserved"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("budget", LongType(), True),
            StructField("revenue", LongType(), True),
            StructField("runtime", IntegerType(), True),
            StructField("overview", StringType(), True),
            StructField("tagline", StringType(), True),
            StructField("status", StringType(), True),
        ])

        data = [
            (1, "Movie: The Sequel!", 100, 200, 90, "OK", "Tag", "Released"),
            (2, "Film & Co.", 100, 200, 90, "OK", "Tag", "Released"),
            (3, "Title with 'quotes'", 100, 200, 90, "OK", "Tag", "Released"),
        ]

        df = spark_session.createDataFrame(data, schema)
        cleaner = DataCleaner(test_config)
        result_df = cleaner.handle_missing_and_incorrect_data(df)

        titles = [r["title"] for r in result_df.collect()]
        assert "Movie: The Sequel!" in titles
        assert "Film & Co." in titles
