"""
Unit tests for DataCleaner
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from src.processing.data_cleaner import DataCleaner


@pytest.mark.unit
class TestDataCleaner:
    """Test suite for DataCleaner class"""
    
    def test_drop_irrelevant_columns(self, spark_session, test_config):
        """Test dropping irrelevant columns"""
        # Create test data with irrelevant columns
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("adult", StringType(), True),  # Should be dropped
            StructField("budget", DoubleType(), True),
            StructField("video", StringType(), True)  # Should be dropped
        ])
        
        data = [
            (1, "Movie 1", "false", 100.0, "false"),
            (2, "Movie 2", "false", 200.0, "false")
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.drop_irrelevant_columns(df)
        
        # Verify irrelevant columns are dropped
        assert "adult" not in result_df.columns
        assert "video" not in result_df.columns
        assert "id" in result_df.columns
        assert "title" in result_df.columns
    
    def test_handle_missing_values(self, spark_session, test_config):
        """Test handling of missing values"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("budget", DoubleType(), True),
            StructField("revenue", DoubleType(), True)
        ])
        
        data = [
            (1, "Movie 1", 100.0, 300.0),
            (2, "Movie 2", 0.0, 0.0),  # Zero values should be converted to NaN
            (3, "Movie 3", None, None),  # Already None
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.handle_missing_values(df)
        
        # Check that zeros are converted to nulls
        zero_budget_count = result_df.filter(
            (F.col('budget').isNull()) & (F.col('id') == 2)
        ).count()
        assert zero_budget_count == 1
    
    def test_convert_datatypes(self, spark_session, test_config):
        """Test datatype conversion"""
        schema = StructType([
            StructField("id", StringType(), False),  # Should be converted to int
            StructField("budget", StringType(), True),  # Should be converted to double
            StructField("release_date", StringType(), True)
        ])
        
        data = [
            ("1", "100.5", "2023-01-01"),
            ("2", "200.75", "2023-02-01")
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.convert_datatypes(df)
        
        # Verify datatypes
        assert result_df.schema['id'].dataType.typeName() == 'integer'
        assert result_df.schema['budget'].dataType.typeName() == 'double'
    
    def test_remove_duplicates(self, spark_session, test_config):
        """Test duplicate removal"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("budget", DoubleType(), True)
        ])
        
        data = [
            (1, "Movie 1", 100.0),
            (1, "Movie 1", 100.0),  # Duplicate
            (2, "Movie 2", 200.0)
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.remove_duplicates(df)
        
        # Verify duplicates are removed
        assert result_df.count() == 2
    
    def test_filter_released_movies(self, spark_session, test_config):
        """Test filtering for released movies"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("status", StringType(), True)
        ])
        
        data = [
            (1, "Released Movie", "Released"),
            (2, "Upcoming Movie", "Post Production"),
            (3, "Another Released", "Released")
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.filter_released_movies(df)
        
        # Verify only released movies remain
        assert result_df.count() == 2
        assert "status" not in result_df.columns  # Status column should be dropped
    
    def test_clean_data_pipeline(self, sample_movie_data, test_config):
        """Test complete cleaning pipeline"""
        cleaner = DataCleaner(test_config)
        
        # Run full cleaning pipeline
        result_df = cleaner.clean_data(sample_movie_data)
        
        # Basic validations
        assert result_df.count() > 0
        assert "id" in result_df.columns
        assert "title" in result_df.columns
        
        # Verify no null ids or titles
        null_count = result_df.filter(
            F.col('id').isNull() | F.col('title').isNull()
        ).count()
        assert null_count == 0
    
    def test_budget_revenue_conversion_to_millions(self, spark_session, test_config):
        """Test conversion of budget and revenue to millions"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("budget", DoubleType(), True),
            StructField("revenue", DoubleType(), True)
        ])
        
        data = [
            (1, 100000000.0, 300000000.0),  # 100M and 300M
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.convert_to_millions(df)
        
        # Verify conversion
        row = result_df.collect()[0]
        assert abs(row['budget_musd'] - 100.0) < 0.01
        assert abs(row['revenue_musd'] - 300.0) < 0.01
    
    def test_parse_json_columns(self, spark_session, test_config):
        """Test parsing of JSON-like columns"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("genres", StringType(), True)
        ])
        
        # Simulate JSON string from API
        data = [
            (1, '[{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}]'),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.parse_genres(df)
        
        # Verify genres are extracted
        genres = result_df.select('genres').collect()[0]['genres']
        assert 'Action' in genres
        assert 'Adventure' in genres
    
    @pytest.mark.parametrize("invalid_value,expected_null", [
        (0, True),
        (-1, True),
        (None, True),
        (100, False)
    ])
    def test_invalid_value_handling(self, spark_session, test_config, invalid_value, expected_null):
        """Test handling of various invalid values"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("budget", DoubleType(), True)
        ])
        
        data = [(1, invalid_value)]
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.handle_missing_values(df)
        
        is_null = result_df.select('budget').collect()[0]['budget'] is None
        assert is_null == expected_null


@pytest.mark.unit
class TestDataCleanerEdgeCases:
    """Test edge cases for DataCleaner"""
    
    def test_empty_dataframe(self, spark_session, test_config):
        """Test cleaning empty dataframe"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False)
        ])
        
        df = spark_session.createDataFrame([], schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.clean_data(df)
        
        assert result_df.count() == 0
    
    def test_all_null_columns(self, spark_session, test_config):
        """Test dataframe with all null values in some columns"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("budget", DoubleType(), True)
        ])
        
        data = [
            (1, "Movie 1", None),
            (2, "Movie 2", None),
            (3, "Movie 3", None)
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.clean_data(df)
        
        # Should still return results even with all nulls in budget
        assert result_df.count() > 0
    
    def test_special_characters_in_title(self, spark_session, test_config):
        """Test handling of special characters"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False)
        ])
        
        data = [
            (1, "Movie: The Sequel!"),
            (2, "Film & Co."),
            (3, "Title with 'quotes'")
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        cleaner = DataCleaner(test_config)
        result_df = cleaner.clean_data(df)
        
        # Should preserve special characters
        titles = [row['title'] for row in result_df.collect()]
        assert "Movie: The Sequel!" in titles
        assert "Film & Co." in titles